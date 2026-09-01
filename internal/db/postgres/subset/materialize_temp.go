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

// tempTablesMaterializer - materializes the per-table subset key sets into unlogged tables in a
// scratch schema, building them in FK dependency order. Acyclic tables are built with a single
// filtered select over their referenced key-set tables. Cyclic components execute the recursive
// cycle-path CTE once per member with the out-of-component references replaced by semi-joins
// against the already built key-set tables.
type tempTablesMaterializer struct {
	ctx        context.Context
	conn       *pgx.Conn
	g          *Graph
	schema     string
	vertexComp map[int]int
	// idsTables - scratch table name per vertex with a built key-set table
	idsTables map[int]string
	// building - vertexes of the component currently being built, to satisfy in-component
	// references from the ensure recursion
	building map[int]bool
}

// MaterializeSubsetQueriesTempTables - derives the subset key sets into unlogged tables in the
// given scratch schema and rewrites the table dump queries to filter through them. The queries
// are executed on the provided connection outside the dump snapshot, so the source database must
// not receive concurrent writes during the dump.
func MaterializeSubsetQueriesTempTables(ctx context.Context, conn *pgx.Conn, g *Graph, scratchSchema string) (bool, error) {
	if !materializationSupported(g) {
		return false, nil
	}
	m := &tempTablesMaterializer{
		ctx:        ctx,
		conn:       conn,
		g:          g,
		schema:     scratchSchema,
		vertexComp: make(map[int]int),
		idsTables:  make(map[int]string),
		building:   make(map[int]bool),
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
					return true, err
				}
			}
			continue
		}
		t := c.getOneTable()
		v := g.componentsToOriginalVertexes[compIdx][0]
		conds, err := m.condsForVertex(v)
		if err != nil {
			return true, err
		}
		t.Query = fmt.Sprintf(
			`%s FROM "%s"."%s" %s`,
			generateSelectAllColumns(t), t.Schema, t.Name, generateWhereClause(conds),
		)
	}
	return true, nil
}

// rewriteCyclicTableQuery - replaces a cyclic-component table query with a primary-key semi-join
// against its materialized key-set table. Tables without a primary key keep the SCC-generated query.
func (m *tempTablesMaterializer) rewriteCyclicTableQuery(v int, t *entries.Table) error {
	if len(t.PrimaryKey) == 0 {
		log.Warn().
			Str("SchemaName", t.Schema).
			Str("TableName", t.Name).
			Msg("cyclic table without primary key: keeping non-materialized subset query")
		return nil
	}
	if err := m.ensureIdsTable(v); err != nil {
		return err
	}
	cond := fmt.Sprintf(
		`(%s) IN (SELECT %s FROM %s)`,
		strings.Join(qualifiedColRefs(t, t.PrimaryKey), ", "),
		strings.Join(quotedCols(t.PrimaryKey), ", "),
		m.scratchTableRef(v),
	)
	t.Query = fmt.Sprintf(
		`%s FROM "%s"."%s" %s`,
		generateSelectAllColumns(t), t.Schema, t.Name, generateWhereClause([]string{cond}),
	)
	return nil
}

// condsForVertex - builds the WHERE conditions for an acyclic restricted table: its own subset
// conditions plus one key-set semi-join condition per FK edge that points to a restricted table
func (m *tempTablesMaterializer) condsForVertex(v int) ([]string, error) {
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
	return conds, nil
}

// edgeCond - builds the membership condition of the referencing side of the edge against the
// key-set table of the referenced table
func (m *tempTablesMaterializer) edgeCond(e *Edge) (string, error) {
	if err := m.ensureIdsTable(e.to.idx); err != nil {
		return "", err
	}
	var fromRefs []string
	for _, k := range e.from.keys {
		fromRefs = append(fromRefs, k.GetKeyReference(e.from.table))
	}
	var toCols []string
	for _, k := range e.to.keys {
		toCols = append(toCols, k.Name)
	}
	cond := fmt.Sprintf(
		`(%s) IN (SELECT %s FROM %s)`,
		strings.Join(fromRefs, ", "),
		strings.Join(quotedCols(toCols), ", "),
		m.scratchTableRef(e.to.idx),
	)
	if e.isNullable {
		nullChecks := make([]string, 0, len(fromRefs))
		for _, ref := range fromRefs {
			nullChecks = append(nullChecks, fmt.Sprintf(`%s IS NULL`, ref))
		}
		cond = fmt.Sprintf(`(%s OR %s)`, strings.Join(nullChecks, " OR "), cond)
	}
	return cond, nil
}

// ensureIdsTable - builds the key-set table for the vertex if it is not built yet: the whole
// cyclic component is built at once, an acyclic table is built after its referenced tables
func (m *tempTablesMaterializer) ensureIdsTable(v int) error {
	if _, ok := m.idsTables[v]; ok {
		return nil
	}
	if m.building[v] {
		return nil
	}
	comp := m.g.scc[m.vertexComp[v]]
	if comp.hasCycle() {
		return m.buildCyclicComponent(comp)
	}
	return m.buildAcyclicTable(v)
}

func (m *tempTablesMaterializer) buildAcyclicTable(v int) error {
	t := m.g.tables[v]
	conds, err := m.condsForVertex(v)
	if err != nil {
		return err
	}
	cols := m.neededCols(v)
	return m.createIdsTable(v, t, cols, conds)
}

// buildCyclicComponent - builds the key-set table of every component member by executing the
// component's recursive cycle-path CTE once per member, with the references outside the component
// replaced by semi-joins against the already materialized key-set tables
func (m *tempTablesMaterializer) buildCyclicComponent(c *Component) error {
	vertexes := slices.Clone(m.g.componentsToOriginalVertexes[c.id])
	slices.Sort(vertexes)
	for _, v := range vertexes {
		m.building[v] = true
	}
	defer func() {
		for _, v := range vertexes {
			delete(m.building, v)
		}
	}()

	externalConds := make(map[toolkit.Oid][]string)
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
				return err
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

	for _, v := range vertexes {
		t := m.g.tables[v]
		if len(t.PrimaryKey) == 0 {
			continue
		}
		cols := m.neededCols(v)
		selectCols := make([]string, 0, len(cols))
		for _, col := range cols {
			selectCols = append(selectCols, fmt.Sprintf(`"%s"."%s"."%s" AS "%s"`, t.Schema, t.Name, col, col))
		}
		query := fmt.Sprintf(
			`SELECT DISTINCT * FROM (%s) AS __gm_keys`,
			cq.generateQuerySelect(t, selectCols),
		)
		if err := m.createIdsTableAs(v, t, cols, query); err != nil {
			return err
		}
	}
	return nil
}

// neededCols - the columns the key-set table of the vertex must hold: the primary key and the
// key columns referenced by any restricted incoming edge
func (m *tempTablesMaterializer) neededCols(v int) []string {
	t := m.g.tables[v]
	var cols []string
	cols = append(cols, t.PrimaryKey...)
	for _, edges := range m.g.graph {
		for _, e := range edges {
			if e.to.idx != v {
				continue
			}
			if _, ok := m.g.paths[m.vertexComp[e.from.idx]]; !ok {
				continue
			}
			for _, k := range e.to.keys {
				cols = append(cols, k.Name)
			}
		}
	}
	slices.Sort(cols)
	return slices.Compact(cols)
}

func (m *tempTablesMaterializer) createIdsTable(v int, t *entries.Table, cols []string, conds []string) error {
	selectCols := make([]string, 0, len(cols))
	for _, c := range cols {
		selectCols = append(selectCols, fmt.Sprintf(`"%s"."%s"."%s" AS "%s"`, t.Schema, t.Name, c, c))
	}
	return m.createIdsTableAs(v, t, cols, fmt.Sprintf(
		`SELECT DISTINCT %s FROM "%s"."%s" %s`,
		strings.Join(selectCols, ", "), t.Schema, t.Name, generateWhereClause(conds),
	))
}

func (m *tempTablesMaterializer) createIdsTableAs(v int, t *entries.Table, cols []string, selectQuery string) error {
	name := shortenIdentifier(fmt.Sprintf("gm_ids_%s__%s", t.Schema, t.Name))
	m.idsTables[v] = name
	query := fmt.Sprintf(
		`CREATE UNLOGGED TABLE "%s"."%s" AS %s`,
		m.schema, name, selectQuery,
	)
	if _, err := m.conn.Exec(m.ctx, query); err != nil {
		return fmt.Errorf(`error materializing subset key set for table "%s"."%s": %w`, t.Schema, t.Name, err)
	}
	for i, c := range cols {
		idxName := shortenIdentifier(fmt.Sprintf("%s__i%d", name, i))
		query = fmt.Sprintf(
			`CREATE INDEX "%s" ON "%s"."%s" ("%s")`,
			idxName, m.schema, name, c,
		)
		if _, err := m.conn.Exec(m.ctx, query); err != nil {
			return fmt.Errorf(`error indexing subset key set for table "%s"."%s": %w`, t.Schema, t.Name, err)
		}
	}
	var keysCount int64
	if err := m.conn.QueryRow(
		m.ctx, fmt.Sprintf(`SELECT count(*) FROM "%s"."%s"`, m.schema, name),
	).Scan(&keysCount); err != nil {
		return fmt.Errorf(`error counting subset key set for table "%s"."%s": %w`, t.Schema, t.Name, err)
	}
	log.Debug().
		Str("SchemaName", t.Schema).
		Str("TableName", t.Name).
		Int64("KeysCount", keysCount).
		Msg("materialized subset key set table")
	return nil
}

func (m *tempTablesMaterializer) scratchTableRef(v int) string {
	return fmt.Sprintf(`"%s"."%s"`, m.schema, m.idsTables[v])
}

func quotedCols(cols []string) []string {
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted = append(quoted, fmt.Sprintf(`"%s"`, c))
	}
	return quoted
}
