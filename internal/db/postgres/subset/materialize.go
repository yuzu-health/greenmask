package subset

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	SubsetMaterializationNone       = "none"
	SubsetMaterializationInline     = "inline"
	SubsetMaterializationTempTables = "temp_tables"

	// inlineValuesLimit - the maximum size of a materialized key set that is inlined as literal
	// values. Larger sets are referenced through a single-level subquery over the source table
	// with its materialized conditions, which keeps the query text bounded
	inlineValuesLimit = 10000
)

// materializationSupported - subset materialization derives the key sets through plain key
// columns, which does not cover polymorphic references or virtual reference expressions
func materializationSupported(g *Graph) bool {
	for _, c := range g.scc {
		if _, ok := g.paths[c.id]; !ok {
			continue
		}
		if c.hasPolymorphicExpressions() {
			log.Warn().Msg("subset materialization is not supported with polymorphic references: keeping non-materialized subset queries")
			return false
		}
	}
	for _, e := range g.edges {
		for _, k := range append(e.from.keys, e.to.keys...) {
			if k.Expression != "" {
				log.Warn().Msg("subset materialization is not supported with virtual reference expressions: keeping non-materialized subset queries")
				return false
			}
		}
	}
	return true
}

// materializer - executes the per-table subset id derivation once during planning and rewrites
// each table dump query to filter by the materialized key values inline. This avoids re-deriving
// the whole FK-traversal logic inside every table's COPY query.
type materializer struct {
	ctx        context.Context
	tx         pgx.Tx
	g          *Graph
	vertexComp map[int]int
	// conds - memoized WHERE conditions per acyclic restricted vertex
	conds map[int][]string
	// vals - memoized materialized key values, keyed by table oid + column list
	vals map[string][][]string
	// componentCtes - memoized per-cyclic-component CTE queries regenerated with the
	// materialized external conditions instead of the full out-of-component traversal
	componentCtes map[int]*cteQuery
}

// MaterializeSubsetQueries - replaces the generated subset queries with queries that filter by
// inline key-value lists. The key sets are derived once per table by executing the derivation
// queries inside the provided transaction, so they observe the same snapshot as the dump.
func MaterializeSubsetQueries(ctx context.Context, tx pgx.Tx, g *Graph) error {
	if !materializationSupported(g) {
		return nil
	}

	m := &materializer{
		ctx:           ctx,
		tx:            tx,
		g:             g,
		vertexComp:    make(map[int]int),
		conds:         make(map[int][]string),
		vals:          make(map[string][][]string),
		componentCtes: make(map[int]*cteQuery),
	}
	for compIdx, vertexes := range g.componentsToOriginalVertexes {
		for _, v := range vertexes {
			m.vertexComp[v] = compIdx
		}
	}

	for compIdx := range g.paths {
		c := g.scc[compIdx]
		if c.hasCycle() {
			for v, t := range c.tables {
				if err := m.rewriteCyclicTableQuery(v, t); err != nil {
					return err
				}
			}
			continue
		}
		t := c.getOneTable()
		v := g.componentsToOriginalVertexes[compIdx][0]
		conds, err := m.condsForVertex(v)
		if err != nil {
			return err
		}
		t.Query = fmt.Sprintf(
			`%s FROM "%s"."%s" %s`,
			generateSelectAllColumns(t), t.Schema, t.Name, generateWhereClause(conds),
		)
	}
	return nil
}

// rewriteCyclicTableQuery - replaces a cyclic-component table query with a primary-key filter
// over its materialized key set. Tables without a primary key keep the SCC-generated query.
func (m *materializer) rewriteCyclicTableQuery(v int, t *entries.Table) error {
	if len(t.PrimaryKey) == 0 {
		return nil
	}
	vals, err := m.values(v, t, t.PrimaryKey)
	if err != nil {
		return err
	}
	if len(vals) > inlineValuesLimit {
		cq, err := m.componentCte(m.g.scc[m.vertexComp[v]])
		if err != nil {
			return err
		}
		t.Query = cq.generateQuery(t)
		return nil
	}
	cond, err := inlineInCond(qualifiedColRefs(t, t.PrimaryKey), t, t.PrimaryKey, vals, false)
	if err != nil {
		return err
	}
	t.Query = fmt.Sprintf(
		`%s FROM "%s"."%s" %s`,
		generateSelectAllColumns(t), t.Schema, t.Name, generateWhereClause([]string{cond}),
	)
	return nil
}

// edgeCond - builds the membership condition of the referencing side of the edge against the kept
// rows of the referenced table: small key sets are inlined as literal values, large ones are
// referenced through a subquery over the referenced table with its materialized conditions
func (m *materializer) edgeCond(e *Edge) (string, error) {
	t := e.to.table
	var cols []string
	for _, k := range e.to.keys {
		cols = append(cols, k.Name)
	}
	vals, err := m.values(e.to.idx, t, cols)
	if err != nil {
		return "", err
	}
	var fromRefs []string
	for _, k := range e.from.keys {
		fromRefs = append(fromRefs, k.GetKeyReference(e.from.table))
	}
	if len(vals) <= inlineValuesLimit {
		return inlineInCond(fromRefs, t, cols, vals, e.isNullable)
	}

	var subQuery string
	if comp := m.g.scc[m.vertexComp[e.to.idx]]; comp.hasCycle() {
		cq, err := m.componentCte(comp)
		if err != nil {
			return "", err
		}
		subQuery = cq.generateQuerySelect(t, qualifiedColRefs(t, cols))
	} else {
		conds, err := m.condsForVertex(e.to.idx)
		if err != nil {
			return "", err
		}
		subQuery = fmt.Sprintf(
			`SELECT %s FROM "%s"."%s" %s`,
			strings.Join(qualifiedColRefs(t, cols), ", "), t.Schema, t.Name, generateWhereClause(conds),
		)
	}
	cond := fmt.Sprintf(`(%s) IN (%s)`, strings.Join(fromRefs, ", "), subQuery)
	if e.isNullable {
		nullChecks := make([]string, 0, len(fromRefs))
		for _, ref := range fromRefs {
			nullChecks = append(nullChecks, fmt.Sprintf(`%s IS NULL`, ref))
		}
		cond = fmt.Sprintf(`(%s OR %s)`, strings.Join(nullChecks, " OR "), cond)
	}
	return cond, nil
}

// componentCte - builds the CTE query of a cyclic component with the traversal restricted by the
// materialized key sets of the referenced tables outside the component, instead of embedding the
// whole out-of-component derivation into the recursive queries
func (m *materializer) componentCte(c *Component) (*cteQuery, error) {
	if cq, ok := m.componentCtes[c.id]; ok {
		return cq, nil
	}

	externalConds := make(map[toolkit.Oid][]string)
	vertexes := slices.Clone(m.g.componentsToOriginalVertexes[c.id])
	slices.Sort(vertexes)
	for _, v := range vertexes {
		t := m.g.tables[v]
		for _, e := range m.g.graph[v] {
			if m.vertexComp[e.to.idx] == c.id {
				continue
			}
			if _, ok := m.g.paths[m.vertexComp[e.to.idx]]; !ok {
				continue
			}
			cond, err := m.edgeCond(e)
			if err != nil {
				return nil, err
			}
			externalConds[t.Oid] = append(externalConds[t.Oid], cond)
		}
	}

	cq := newCteQuery(c)
	if len(c.groupedCycles) == 1 {
		cycleGroup := c.getOneCycleGroup()
		overlapMap := m.g.getOverlapMap(cycleGroup)
		var extraConds []string
		for _, t := range getTablesFromCycle(cycleGroup[0]) {
			extraConds = append(extraConds, externalConds[t.Oid]...)
		}
		for _, cycle := range cycleGroup {
			m.g.generateRecursiveQueriesForCycle(cq, rootScopeId, cycle, nil, nil, overlapMap, extraConds)
		}
		m.g.generateFilteredQueries(cq, cycleGroup, rootScopeId)
		m.g.generateQueriesForVertexesInCycle(cq, rootScopeId, cycleGroup)
	} else {
		m.g.generateQueriesForMultiGroupScc(cq, rootScopeId, c, nil, nil, externalConds)
	}
	m.componentCtes[c.id] = cq
	return cq, nil
}

// condsForVertex - builds the WHERE conditions for an acyclic restricted table: its own subset
// conditions plus one inline key-list condition per FK edge that points to a restricted table
func (m *materializer) condsForVertex(v int) ([]string, error) {
	if conds, ok := m.conds[v]; ok {
		return conds, nil
	}
	t := m.g.tables[v]
	var conds []string
	conds = append(conds, t.SubsetConds...)
	for _, e := range m.g.graph[v] {
		if _, ok := m.g.paths[m.vertexComp[e.to.idx]]; !ok {
			continue
		}
		cond, err := m.edgeCond(e)
		if err != nil {
			return nil, err
		}
		conds = append(conds, cond)
	}
	m.conds[v] = conds
	return conds, nil
}

// values - returns the distinct values of the given columns among the kept rows of the table,
// executing the derivation query once and caching the result
func (m *materializer) values(v int, t *entries.Table, cols []string) ([][]string, error) {
	cacheKey := fmt.Sprintf("%d:%s", t.Oid, strings.Join(cols, ","))
	if vals, ok := m.vals[cacheKey]; ok {
		return vals, nil
	}

	var selectCols []string
	var query string
	if comp := m.g.scc[m.vertexComp[v]]; comp.hasCycle() {
		cq, err := m.componentCte(comp)
		if err != nil {
			return nil, err
		}
		for _, c := range cols {
			selectCols = append(selectCols, fmt.Sprintf(`"%s"::text`, c))
		}
		query = fmt.Sprintf(
			`SELECT DISTINCT %s FROM (%s) AS __mat_src`,
			strings.Join(selectCols, ", "), cq.generateQuery(t),
		)
	} else {
		conds, err := m.condsForVertex(v)
		if err != nil {
			return nil, err
		}
		for _, c := range cols {
			selectCols = append(selectCols, fmt.Sprintf(`"%s"."%s"."%s"::text`, t.Schema, t.Name, c))
		}
		query = fmt.Sprintf(
			`SELECT DISTINCT %s FROM "%s"."%s" %s`,
			strings.Join(selectCols, ", "), t.Schema, t.Name, generateWhereClause(conds),
		)
	}

	rows, err := m.tx.Query(m.ctx, query)
	if err != nil {
		return nil, fmt.Errorf(`error materializing subset keys for table "%s"."%s": %w`, t.Schema, t.Name, err)
	}
	defer rows.Close()
	var vals [][]string
	for rows.Next() {
		row := make([]*string, len(cols))
		scanTargets := make([]any, len(cols))
		for i := range row {
			scanTargets[i] = &row[i]
		}
		if err = rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf(`error scanning materialized subset keys for table "%s"."%s": %w`, t.Schema, t.Name, err)
		}
		// NULL key values cannot be referenced by a foreign key and are not part of the set
		textRow := make([]string, len(cols))
		hasNull := false
		for i, v := range row {
			if v == nil {
				hasNull = true
				break
			}
			textRow[i] = *v
		}
		if hasNull {
			continue
		}
		vals = append(vals, textRow)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf(`error materializing subset keys for table "%s"."%s": %w`, t.Schema, t.Name, err)
	}
	log.Debug().
		Str("SchemaName", t.Schema).
		Str("TableName", t.Name).
		Int("KeysCount", len(vals)).
		Msg("materialized subset keys")
	m.vals[cacheKey] = vals
	return vals, nil
}

// inlineInCond - builds an inline membership condition of the referencing key columns against the
// materialized value list, with the same nullability semantics as the generated join conditions
func inlineInCond(fromRefs []string, refTable *entries.Table, refCols []string, vals [][]string, nullable bool) (string, error) {
	var cond string
	if len(vals) == 0 {
		cond = "FALSE"
	} else if len(refCols) == 1 {
		typeName, err := columnTypeName(refTable, refCols[0])
		if err != nil {
			return "", err
		}
		items := make([]string, 0, len(vals))
		for _, row := range vals {
			items = append(items, quoteSqlLiteral(row[0]))
		}
		cond = fmt.Sprintf(`%s = ANY(ARRAY[%s]::%s[])`, fromRefs[0], strings.Join(items, ","), typeName)
	} else {
		typeNames := make([]string, len(refCols))
		for i, c := range refCols {
			typeName, err := columnTypeName(refTable, c)
			if err != nil {
				return "", err
			}
			typeNames[i] = typeName
		}
		rowItems := make([]string, 0, len(vals))
		for _, row := range vals {
			cells := make([]string, len(row))
			for i, v := range row {
				cells[i] = fmt.Sprintf("%s::%s", quoteSqlLiteral(v), typeNames[i])
			}
			rowItems = append(rowItems, fmt.Sprintf("(%s)", strings.Join(cells, ",")))
		}
		cond = fmt.Sprintf(`(%s) IN (%s)`, strings.Join(fromRefs, ", "), strings.Join(rowItems, ","))
	}
	if nullable {
		nullChecks := make([]string, 0, len(fromRefs))
		for _, ref := range fromRefs {
			nullChecks = append(nullChecks, fmt.Sprintf(`%s IS NULL`, ref))
		}
		cond = fmt.Sprintf(`(%s OR %s)`, strings.Join(nullChecks, " OR "), cond)
	}
	return cond, nil
}

// columnTypeName - returns the actual source column type: the materialized values are compared
// against untransformed source columns, so transformer type overrides must not affect the cast
func columnTypeName(t *entries.Table, colName string) (string, error) {
	for _, c := range t.Columns {
		if c.Name == colName {
			return c.TypeName, nil
		}
	}
	return "", fmt.Errorf(`cannot find column "%s" in table "%s"."%s"`, colName, t.Schema, t.Name)
}

func qualifiedColRefs(t *entries.Table, cols []string) []string {
	refs := make([]string, 0, len(cols))
	for _, c := range cols {
		refs = append(refs, fmt.Sprintf(`"%s"."%s"."%s"`, t.Schema, t.Name, c))
	}
	return refs
}

func quoteSqlLiteral(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}
