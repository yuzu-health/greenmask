package subset

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

type cteQuery struct {
	items      []*cteItem
	addedNames map[string]struct{}
	c          *Component
}

func newCteQuery(c *Component) *cteQuery {
	return &cteQuery{
		c:          c,
		addedNames: make(map[string]struct{}),
	}
}

func (c *cteQuery) addItem(name, query string) {
	if _, exists := c.addedNames[name]; exists {
		return
	}
	c.addedNames[name] = struct{}{}
	c.items = append(c.items, &cteItem{
		name:  name,
		query: query,
	})
}

func (c *cteQuery) generateQuery(targetTable *entries.Table) string {
	return c.generateQuerySelect(targetTable, []string{"*"})
}

func (c *cteQuery) generateQuerySelect(targetTable *entries.Table, selectCols []string) string {
	var queries []string
	var excludedCteQueries []string
	componentTables := make([]*entries.Table, 0, len(c.c.tables))
	for _, t := range c.c.tables {
		componentTables = append(componentTables, t)
	}
	slices.SortFunc(componentTables, func(a, b *entries.Table) int {
		return cmp.Compare(a.Oid, b.Oid)
	})
	for _, t := range componentTables {
		if t.Oid == targetTable.Oid {
			continue
		}
		excludedCteQuery := fmt.Sprintf("%s__%s__ids", t.Schema, t.Name)
		excludedCteQueries = append(excludedCteQueries, excludedCteQuery)
	}

	for _, item := range c.items {
		if slices.Contains(excludedCteQueries, item.name) {
			continue
		}
		queries = append(queries, fmt.Sprintf(` "%s" AS (%s)`, item.name, item.query))
	}
	var leftTableKeys, rightTableKeys []string
	rightTableName := fmt.Sprintf("%s__%s__ids", targetTable.Schema, targetTable.Name)
	for _, key := range targetTable.PrimaryKey {
		leftTableKeys = append(leftTableKeys, fmt.Sprintf(`"%s"."%s"."%s"`, targetTable.Schema, targetTable.Name, key))
		rightTableKeys = append(rightTableKeys, fmt.Sprintf(`"%s"."%s"`, rightTableName, key))
	}

	resultingQuery := fmt.Sprintf(
		`SELECT %s FROM "%s"."%s" WHERE %s IN (SELECT %s FROM "%s")`,
		strings.Join(selectCols, ", "),
		targetTable.Schema,
		targetTable.Name,
		fmt.Sprintf("(%s)", strings.Join(leftTableKeys, ",")),
		strings.Join(rightTableKeys, ","),
		rightTableName,
	)
	res := fmt.Sprintf("WITH RECURSIVE %s %s", strings.Join(queries, ","), resultingQuery)
	return res
}

type cteItem struct {
	name  string
	query string
}
