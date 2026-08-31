package subset

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestComponent_findCycles(t *testing.T) {
	c := &Component{
		cyclesIdents: make(map[string]struct{}),
		componentGraph: map[int][]*Edge{
			1: {
				{
					id:  1,
					idx: 2,
					from: &TableLink{
						idx: 1,
					},
					to: &TableLink{
						idx: 2,
					},
				},
			},
			2: {
				{
					id:  2,
					idx: 3,
					from: &TableLink{
						idx: 2,
					},
					to: &TableLink{
						idx: 3,
					},
				},
			},
			3: {
				{
					id:  3,
					idx: 1,
					from: &TableLink{
						idx: 3,
					},
					to: &TableLink{
						idx: 1,
					},
				},
				{
					id:  4,
					idx: 1,
					from: &TableLink{
						idx: 3,
					},
					to: &TableLink{
						idx: 1,
					},
				},
				{
					id:  5,
					idx: 4,
					from: &TableLink{
						idx: 3,
					},
					to: &TableLink{
						idx: 4,
					},
				},
			},
			4: {
				{
					id:  6,
					idx: 3,
					from: &TableLink{
						idx: 4,
					},
					to: &TableLink{
						idx: 3,
					},
				},
				{
					id:  7,
					idx: 1,
					from: &TableLink{
						idx: 4,
					},
					to: &TableLink{
						idx: 1,
					},
				},
			},
		},
		tables: map[int]*entries.Table{},
	}

	c.findCycles()
	require.Len(t, c.cycles, 4)
}

func TestComponent_findCycles_pt2(t *testing.T) {
	c := &Component{
		componentGraph: map[int][]*Edge{
			1: {
				{
					id:  1,
					idx: 2,
					from: &TableLink{
						idx: 1,
					},
					to: &TableLink{
						idx: 2,
					},
				},
			},
			2: {
				{
					id:  2,
					idx: 1,
					from: &TableLink{
						idx: 2,
					},
					to: &TableLink{
						idx: 1,
					},
				},
				{
					id:  3,
					idx: 1,
					from: &TableLink{
						idx: 2,
					},
					to: &TableLink{
						idx: 1,
					},
				},
			},
		},
		tables:       map[int]*entries.Table{},
		cyclesIdents: make(map[string]struct{}),
	}

	c.findCycles()
	require.Len(t, c.cycles, 2)
}

func TestComponent_getSortedGroupIds(t *testing.T) {
	// The component contains three cycle groups:
	// * the self-reference of table 1
	// * the cycle {1, 2}
	// * the cycle {1, 2, 3}
	// Table 3 carries its own subset condition, therefore the group {1, 2, 3} must go first,
	// and the remaining groups follow because they share vertexes with it.
	table1 := &entries.Table{Table: &toolkit.Table{Oid: 1, Schema: "public", Name: "a"}}
	table2 := &entries.Table{Table: &toolkit.Table{Oid: 2, Schema: "public", Name: "b"}}
	table3 := &entries.Table{
		Table:       &toolkit.Table{Oid: 3, Schema: "public", Name: "c"},
		SubsetConds: []string{"public.c.id IN (1, 2)"},
	}
	c := &Component{
		cyclesIdents: make(map[string]struct{}),
		componentGraph: map[int][]*Edge{
			1: {
				{id: 1, idx: 1, from: &TableLink{idx: 1, table: table1}, to: &TableLink{idx: 1, table: table1}},
				{id: 2, idx: 2, from: &TableLink{idx: 1, table: table1}, to: &TableLink{idx: 2, table: table2}},
			},
			2: {
				{id: 3, idx: 1, from: &TableLink{idx: 2, table: table2}, to: &TableLink{idx: 1, table: table1}},
				{id: 4, idx: 3, from: &TableLink{idx: 2, table: table2}, to: &TableLink{idx: 3, table: table3}},
			},
			3: {
				{id: 5, idx: 1, from: &TableLink{idx: 3, table: table3}, to: &TableLink{idx: 1, table: table1}},
			},
		},
		tables: map[int]*entries.Table{1: table1, 2: table2, 3: table3},
	}

	c.findCycles()
	c.groupCycles()
	require.Len(t, c.cycles, 3)
	require.Len(t, c.groupedCycles, 3)

	groupIds := c.getSortedGroupIds()
	require.Len(t, groupIds, 3)
	require.Equal(t, []string{"1_2_3", "1", "1_2"}, groupIds)

	require.Len(t, c.getCycleGroup(groupIds[0]), 1)
	require.Len(t, c.getCycleGroupTables(groupIds[0]), 3)
	require.True(t, c.groupHasOwnSubsetConds("1_2_3"))
	require.False(t, c.groupHasOwnSubsetConds("1"))
	require.True(t, c.groupsShareVertexes("1", "1_2"))
}

func BenchmarkComponent_findCycles(b *testing.B) {
	c := &Component{
		cyclesIdents: make(map[string]struct{}),
		componentGraph: map[int][]*Edge{
			1: {
				{
					id:  1,
					idx: 2,
					from: &TableLink{
						idx: 1,
					},
					to: &TableLink{
						idx: 2,
					},
				},
			},
			2: {
				{
					id:  2,
					idx: 1,
					from: &TableLink{
						idx: 2,
					},
					to: &TableLink{
						idx: 1,
					},
				},
				{
					id:  3,
					idx: 1,
					from: &TableLink{
						idx: 2,
					},
					to: &TableLink{
						idx: 1,
					},
				},
			},
		},
		tables: map[int]*entries.Table{},
	}

	// Reset the timer to exclude the setup time from the benchmark
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.findCycles()
	}
}
