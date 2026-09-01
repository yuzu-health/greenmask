package subset

import (
	"fmt"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	joinTypeInner = "INNER"
	joinTypeLeft  = "LEFT"
)

func generateJoinClauseForDroppedEdge(edge *Edge, initTableName string) string {
	var conds []string

	var leftTableKeys []string
	table := edge.from.table
	for _, key := range edge.from.keys {
		leftTableKeys = append(leftTableKeys, fmt.Sprintf(`%s__%s__%s`, table.Schema, table.Name, key.Name))
	}

	rightTable := edge.to
	for idx := 0; idx < len(edge.to.keys); idx++ {

		leftPart := fmt.Sprintf(
			`"%s"."%s"`,
			initTableName,
			leftTableKeys[idx],
		)

		rightPart := edge.to.keys[idx].GetKeyReference(rightTable.table)
		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
	}
	if len(edge.from.polymorphicExprs) > 0 {
		conds = append(conds, edge.from.polymorphicExprs...)
	}
	if len(edge.to.polymorphicExprs) > 0 {
		conds = append(conds, edge.to.polymorphicExprs...)
	}

	rightTableName := fmt.Sprintf(`"%s"."%s"`, edge.to.table.Schema, edge.to.table.Name)

	joinClause := fmt.Sprintf(
		`JOIN %s ON %s`,
		rightTableName,
		strings.Join(conds, " AND "),
	)
	return joinClause
}

func generateJoinClauseV2(edge *Edge, joinType string, overriddenTables map[toolkit.Oid]string) string {
	if joinType != joinTypeInner && joinType != joinTypeLeft {
		panic(fmt.Sprintf("invalid join type: %s", joinType))
	}

	var conds []string

	leftTable, rightTable := edge.from.table, edge.to.table
	for idx := 0; idx < len(edge.from.keys); idx++ {

		leftPart := edge.from.keys[idx].GetKeyReference(leftTable)
		rightPart := edge.to.keys[idx].GetKeyReference(rightTable)

		if override, ok := overriddenTables[rightTable.Oid]; ok {
			rightPart = fmt.Sprintf(
				`"%s"."%s"`,
				override,
				edge.to.keys[idx].Name,
			)
		}

		conds = append(conds, fmt.Sprintf(`%s = %s`, leftPart, rightPart))
		if len(edge.to.table.SubsetConds) > 0 {
			conds = append(conds, edge.to.table.SubsetConds...)
		}
	}

	if len(edge.from.polymorphicExprs) > 0 {
		conds = append(conds, edge.from.polymorphicExprs...)
	}
	if len(edge.to.polymorphicExprs) > 0 {
		conds = append(conds, edge.to.polymorphicExprs...)
	}

	rightTableName := fmt.Sprintf(`"%s"."%s"`, rightTable.Schema, rightTable.Name)
	if override, ok := overriddenTables[rightTable.Oid]; ok {
		rightTableName = fmt.Sprintf(`"%s"`, override)
	}

	joinClause := fmt.Sprintf(
		`%s JOIN %s ON %s`,
		joinType,
		rightTableName,
		strings.Join(conds, " AND "),
	)
	return joinClause
}

// generateOverriddenTableInClause - generates a membership condition for an edge whose
// referenced table is overridden by an ids CTE. The CTE cannot be joined directly because
// several edges may reference it, which would produce duplicate relation names in the FROM
// clause; an IN subquery keeps the same semantics without a join.
func generateOverriddenTableInClause(edge *Edge, overriddenName string, nullable bool) string {
	if len(edge.from.polymorphicExprs) > 0 || len(edge.to.polymorphicExprs) > 0 {
		panic("IMPLEMENT ME: polymorphic expression for overridden table")
	}
	var leftKeys, rightKeys, nullChecks []string
	for idx := range edge.from.keys {
		lk := edge.from.keys[idx].GetKeyReference(edge.from.table)
		leftKeys = append(leftKeys, lk)
		nullChecks = append(nullChecks, fmt.Sprintf("%s IS NULL", lk))
		rightKeys = append(rightKeys, fmt.Sprintf(`"%s"`, edge.to.keys[idx].Name))
	}
	rightTable := edge.to.table
	refKeysArePk := len(edge.to.keys) == len(rightTable.PrimaryKey)
	if refKeysArePk {
		for idx, k := range edge.to.keys {
			if k.Name != rightTable.PrimaryKey[idx] {
				refKeysArePk = false
				break
			}
		}
	}
	var inClause string
	if refKeysArePk {
		inClause = fmt.Sprintf(
			`(%s) IN (SELECT %s FROM "%s")`,
			strings.Join(leftKeys, ", "), strings.Join(rightKeys, ", "), overriddenName,
		)
	} else {
		// The ids CTE only exposes the primary key columns, while the FK references other
		// (unique) columns: select those columns from the table itself restricted by the CTE
		var refCols, pkCols, ctePkCols []string
		for _, k := range edge.to.keys {
			refCols = append(refCols, k.GetKeyReference(rightTable))
		}
		for _, k := range rightTable.PrimaryKey {
			pkCols = append(pkCols, fmt.Sprintf(`"%s"."%s"."%s"`, rightTable.Schema, rightTable.Name, k))
			ctePkCols = append(ctePkCols, fmt.Sprintf(`"%s"`, k))
		}
		inClause = fmt.Sprintf(
			`(%s) IN (SELECT %s FROM "%s"."%s" WHERE (%s) IN (SELECT %s FROM "%s"))`,
			strings.Join(leftKeys, ", "), strings.Join(refCols, ", "),
			rightTable.Schema, rightTable.Name,
			strings.Join(pkCols, ", "), strings.Join(ctePkCols, ", "), overriddenName,
		)
	}
	if nullable {
		return fmt.Sprintf(`(%s OR %s)`, strings.Join(nullChecks, " OR "), inClause)
	}
	return inClause
}

func generateWhereClause(subsetConds []string) string {
	if len(subsetConds) == 0 {
		return "WHERE TRUE"
	}
	escapedConds := make([]string, 0, len(subsetConds))
	seen := make(map[string]struct{}, len(subsetConds))
	for _, cond := range subsetConds {
		if _, ok := seen[cond]; ok {
			continue
		}
		seen[cond] = struct{}{}
		escapedConds = append(escapedConds, fmt.Sprintf(`( %s )`, cond))
	}
	return "WHERE " + strings.Join(escapedConds, " AND ")
}

func generateSelectByPrimaryKey(table *entries.Table, pk []string) string {
	var keys []string
	for _, key := range pk {
		keys = append(keys, fmt.Sprintf(`"%s"."%s"."%s"`, table.Schema, table.Name, key))
	}
	return fmt.Sprintf(
		`SELECT %s`,
		strings.Join(keys, ", "),
	)
}

// generateSelectAllColumns - returns the select clause with all the table columns except the
// generated ones, since those cannot be restored via COPY
func generateSelectAllColumns(table *entries.Table) string {
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if column.IsGenerated {
			continue
		}
		columns = append(
			columns,
			fmt.Sprintf(`"%s"."%s"."%s"`, table.Schema, table.Name, column.Name),
		)
	}
	if len(columns) == 0 {
		return fmt.Sprintf(`SELECT "%s"."%s".*`, table.Schema, table.Name)
	}
	return fmt.Sprintf(`SELECT %s`, strings.Join(columns, ", "))
}
