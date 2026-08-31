package subset

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type Component struct {
	id int
	// componentGraph - contains the mapping of the vertexes in the component to the edges in the original graph
	// if the component contains one vertex and no edges, then there is only one vertex with no cycles
	componentGraph map[int][]*Edge
	// tables - the vertexes in the component
	tables map[int]*entries.Table
	// Cycles
	cycles       [][]*Edge
	cyclesIdents map[string]struct{}
	// groupedCycles - cycles grouped by the vertexes
	groupedCycles map[string][]int
	// groupedCyclesGraph - contains the mapping of the vertexes in the component to the edges in the original graph
	// for grouped cycles. This required to join the separated cycles together
	groupedCyclesGraph map[string][]*CycleEdge
}

func NewComponent(id int, componentGraph map[int][]*Edge, tables map[int]*entries.Table) *Component {
	c := &Component{
		id:             id,
		componentGraph: componentGraph,
		tables:         tables,
		cyclesIdents:   make(map[string]struct{}),
	}
	c.findCycles()
	c.groupCycles()
	c.buildCyclesGraph()

	return c
}

func (c *Component) hasPolymorphicExpressions() bool {
	for _, edges := range c.componentGraph {
		for _, edge := range edges {
			if len(edge.from.polymorphicExprs) > 0 {
				return true
			}
			if len(edge.to.polymorphicExprs) > 0 {
				return true
			}
		}
	}
	return false
}

func (c *Component) getSubsetConds() []string {
	var subsetConds []string
	for _, table := range c.tables {
		if len(table.SubsetConds) > 0 {
			subsetConds = append(subsetConds, table.SubsetConds...)
		}
	}
	return subsetConds
}

func (c *Component) getOneTable() *entries.Table {
	if !c.hasCycle() {
		for _, table := range c.tables {
			return table
		}
	}
	panic("cannot call get one table method for cycled scc")
}

func (c *Component) getOneCycleGroup() [][]*Edge {
	if len(c.groupedCycles) == 1 {
		for _, g := range c.groupedCycles {
			var res [][]*Edge
			for _, idx := range g {
				res = append(res, c.cycles[idx])
			}
			return res
		}
	}
	panic("get one group cycle group is not allowed for multy cycles")
}

func (c *Component) hasCycle() bool {
	return len(c.cycles) > 0
}

// getCycleGroup - returns the cycles of the group by the group id
func (c *Component) getCycleGroup(groupId string) [][]*Edge {
	cycleIdxs, ok := c.groupedCycles[groupId]
	if !ok {
		panic(fmt.Sprintf("cycle group %s does not exist", groupId))
	}
	res := make([][]*Edge, 0, len(cycleIdxs))
	for _, idx := range cycleIdxs {
		res = append(res, c.cycles[idx])
	}
	return res
}

// getCycleGroupTables - returns the tables of the group vertexes sorted by oid.
// All cycles in the group share the same vertex set, so the first cycle is enough.
func (c *Component) getCycleGroupTables(groupId string) []*entries.Table {
	return getTablesFromCycle(c.getCycleGroup(groupId)[0])
}

// groupHasOwnSubsetConds - returns true if any table of the group vertexes has its own subset conditions
func (c *Component) groupHasOwnSubsetConds(groupId string) bool {
	for _, t := range c.getCycleGroupTables(groupId) {
		if len(t.SubsetConds) > 0 {
			return true
		}
	}
	return false
}

// groupsShareVertexes - returns true if two groups have at least one common vertex (table)
func (c *Component) groupsShareVertexes(groupIdA, groupIdB string) bool {
	tablesA := c.getCycleGroupTables(groupIdA)
	for _, tb := range c.getCycleGroupTables(groupIdB) {
		if slices.ContainsFunc(tablesA, func(ta *entries.Table) bool { return ta.Oid == tb.Oid }) {
			return true
		}
	}
	return false
}

// getSortedGroupIds - returns the cycle group ids in a deterministic processing order: the groups
// that carry their own subset conditions go first, then the groups that share vertexes with the
// already ordered groups. This way each conditionless group can be restricted by the results of
// the previously generated groups it overlaps with.
func (c *Component) getSortedGroupIds() []string {
	remaining := make([]string, 0, len(c.groupedCycles))
	for groupId := range c.groupedCycles {
		remaining = append(remaining, groupId)
	}
	slices.Sort(remaining)

	var ordered []string
	take := func(cond func(groupId string) bool) {
		for idx := 0; idx < len(remaining); {
			if cond(remaining[idx]) {
				ordered = append(ordered, remaining[idx])
				remaining = slices.Delete(remaining, idx, idx+1)
			} else {
				idx++
			}
		}
	}

	take(c.groupHasOwnSubsetConds)
	for len(remaining) > 0 {
		prevLen := len(remaining)
		take(func(groupId string) bool {
			return slices.ContainsFunc(ordered, func(orderedId string) bool {
				return c.groupsShareVertexes(orderedId, groupId)
			})
		})
		if len(remaining) == prevLen {
			// No group shares vertexes with the ordered ones - take the next one as is
			ordered = append(ordered, remaining[0])
			remaining = remaining[1:]
		}
	}
	return ordered
}

// findCycles - finds all cycles in the component
func (c *Component) findCycles() {
	visited := make(map[int]bool)
	var path []*Edge
	recStack := make(map[int]bool)

	// Collect and sort all vertices
	var vertices []int
	for v := range c.componentGraph {
		vertices = append(vertices, v)
	}
	sort.Ints(vertices) // Ensure deterministic order

	for _, v := range vertices {
		if !visited[v] {
			c.findAllCyclesDfs(v, visited, recStack, path)
		}
	}
}

// findAllCyclesDfs - the basic DFS algorithm adapted to find all cycles in the graph and collect the cycle vertices
func (c *Component) findAllCyclesDfs(v int, visited map[int]bool, recStack map[int]bool, path []*Edge) {
	visited[v] = true
	recStack[v] = true

	// Sort edges to ensure deterministic order
	var edges []*Edge
	edges = append(edges, c.componentGraph[v]...)
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].to.idx < edges[j].to.idx
	})

	for _, to := range edges {

		path = append(path, to)
		if !visited[to.idx] {
			c.findAllCyclesDfs(to.idx, visited, recStack, path)
		} else if recStack[to.idx] {
			// Cycle detected
			var cycle []*Edge
			for idx := len(path) - 1; idx >= 0; idx-- {
				cycle = append(cycle, path[idx])
				if path[idx].from.idx == to.to.idx {
					break
				}
			}
			cycleId := getCycleId(cycle)
			if _, ok := c.cyclesIdents[cycleId]; !ok {
				res := slices.Clone(cycle)
				slices.Reverse(res)
				c.cycles = append(c.cycles, res)
				c.cyclesIdents[cycleId] = struct{}{}
			}
		}
		path = path[:len(path)-1]
	}

	recStack[v] = false
}

// getCycleGroupId - returns the group id for the cycle based on the vertexes ID
func getCycleGroupId(cycle []*Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.to.idx))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}

// getCycleId - returns the unique identifier for the cycle based on the edges ID
func getCycleId(cycle []*Edge) string {
	ids := make([]string, 0, len(cycle))
	for _, edge := range cycle {
		ids = append(ids, fmt.Sprintf("%d", edge.id))
	}
	slices.Sort(ids)
	return strings.Join(ids, "_")
}

func (c *Component) groupCycles() {
	c.groupedCycles = make(map[string][]int)
	for cycleIdx, cycle := range c.cycles {
		cycleId := getCycleGroupId(cycle)
		c.groupedCycles[cycleId] = append(c.groupedCycles[cycleId], cycleIdx)
	}
}

func (c *Component) buildCyclesGraph() {
	// TODO: Need to loop through c.groupedCycles instead of c.cycles
	var idSeq int
	c.groupedCyclesGraph = make(map[string][]*CycleEdge)
	for groupIdI, cyclesI := range c.groupedCycles {
		for groupIdJ, cyclesJ := range c.groupedCycles {
			if groupIdI == groupIdJ {
				continue
			}
			commonVertexes := c.findCommonVertexes(cyclesI[0], cyclesJ[0])
			if len(commonVertexes) == 0 {
				continue
			}
			if c.areCyclesLinked(cyclesI[0], cyclesJ[0]) {
				continue
			}
			e := NewCycleEdge(idSeq, groupIdI, groupIdJ, commonVertexes)
			c.groupedCyclesGraph[groupIdI] = append(c.groupedCyclesGraph[groupIdJ], e)
			idSeq++
		}
	}
}

func (c *Component) findCommonVertexes(i, j int) (res []*entries.Table) {
	common := make(map[toolkit.Oid]*entries.Table)
	for _, edgeI := range c.cycles[i] {
		for _, edgeJ := range c.cycles[j] {
			if edgeI.to.idx == edgeJ.to.idx {
				common[edgeI.to.table.Oid] = edgeI.to.table
			}
		}
	}
	for _, table := range common {
		res = append(res, table)
	}
	slices.SortFunc(res, func(i, j *entries.Table) int {
		switch {
		case i.Oid < j.Oid:
			return -1
		case i.Oid > j.Oid:
			return 1
		}
		return 0
	})
	return
}

func (c *Component) areCyclesLinked(i, j int) bool {
	iId := getCycleGroupId(c.cycles[i])
	jId := getCycleGroupId(c.cycles[j])
	for _, to := range c.groupedCyclesGraph[iId] {
		if to.to == jId {
			return true
		}
	}
	for _, to := range c.groupedCyclesGraph[jId] {
		if to.to == iId {
			return true
		}
	}
	return false
}

func (c *Component) getComponentKeys() []string { //nolint: unused
	if !c.hasCycle() {
		return c.getOneTable().PrimaryKey
	}

	vertexes := make(map[int]struct{})
	for _, cycle := range c.cycles {
		for _, edge := range cycle {
			vertexes[edge.to.idx] = struct{}{}
		}
	}

	var keys []string
	for v := range vertexes {
		table := c.tables[v]
		for _, key := range table.PrimaryKey {
			keys = append(keys, fmt.Sprintf(`%s__%s__%s`, table.Schema, table.Name, key))
		}
	}
	return keys
}
