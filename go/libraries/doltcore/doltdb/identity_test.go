// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package doltdb_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type identityTest struct {
	name    string
	left    []table
	right   []table
	matches []match
	// non-matching tables omitted
}

type match struct {
	leftTbl, rightTbl string
	columnMatches     [][2]string
	// non-matching columns omitted
}

type table struct {
	name string
	cols []column
	rows [][]int64
}

type column struct {
	name string
	enc  val.Encoding
	pk   bool
}

// Table matching follows a conservative algorithm:
// matching tables must have the same name and the same set
// of primary key column types (empty set for keyless tables).
//
// This algorithm could be extended to handle table renames
// by matching tables with equal primary key column types
// based on a heuristic sampling method. We could also expose
// user-defined mappings that manually specify table matches.
func TestTableMatching(t *testing.T) {
	var tests = []identityTest{
		{
			name: "smoke test",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.Int64Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.Int64Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						{"c0", "c0"},
					},
				},
			},
		},
		{
			name: "primary key rename",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "a", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.Int64Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "b", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.Int64Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"a", "b"},
						{"c0", "c0"},
					},
				},
			},
		},
		{
			name: "keyless table",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int64Enc},
						{name: "c1", enc: val.Int64Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int64Enc},
						{name: "c1", enc: val.Int64Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"c0", "c0"},
						{"c1", "c1"},
					},
				},
			},
		},
		{
			name: "table rename",
			left: []table{
				{
					name: "t1",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.Int64Enc},
					},
				},
			},
			right: []table{
				{
					name: "t2",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.Int64Enc},
					},
				},
			},
			matches: []match{ // no matches
				{rightTbl: "t2"},
				{leftTbl: "t1"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testIdentity(t, test)
		})
	}
}

// Column matching follows table matching,
// primary keys have already been matched.
// Matching for non-primary-key is as follows:
//  1. equal name and type are matched
//  2. keyless tables take union of remaining columns
//  3. pk tables attempt to heuristically match remaining
//     columns of equal types by sampling rows values
func TestColumnMatching(t *testing.T) {
	var tests = []identityTest{
		{
			name: "extra unmatched columns",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "a", enc: val.DatetimeEnc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "b", enc: val.GeometryEnc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						// columns 'a', 'b' unmatched
					},
				},
			},
		},
		{
			name: "matched columns with differing types",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.YearEnc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int64Enc, pk: true},
						{name: "c0", enc: val.JSONEnc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						{"c0", "c0"},
					},
				},
			},
		},
		{
			name: "keyless table union",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int64Enc},
						{name: "c1", enc: val.Int64Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int64Enc},
						{name: "c2", enc: val.Int64Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"c0", "c0"},
						// columns 'c1', 'c2' unmatched
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testIdentity(t, test)
		})
	}
}

func testIdentity(t *testing.T, test identityTest) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	vrw := dEnv.DoltDB.ValueReadWriter()
	ns := dEnv.DoltDB.NodeStore()

	root, err := doltdb.EmptyRootValue(ctx, vrw, ns)
	require.NoError(t, err)

	left := root
	for i := range test.left {
		dt := buildTestTable(t, vrw, ns, test.left[i])
		left, err = left.PutTable(ctx, test.left[i].name, dt)
		require.NoError(t, err)
	}
	leftTables, err := allTablesFromRoot(ctx, left)
	require.NoError(t, err)

	right := root
	for i := range test.right {
		dt := buildTestTable(t, vrw, ns, test.right[i])
		right, err = right.PutTable(ctx, test.right[i].name, dt)
		require.NoError(t, err)
	}
	rightTables, err := allTablesFromRoot(ctx, right)
	require.NoError(t, err)

	tableMatches, err := doltdb.MatchTablesForRoots(ctx, left, right)
	require.NoError(t, err)

	assert.Equal(t, len(test.matches), len(tableMatches))
	for i, m := range test.matches {
		// todo: needs sorting for multi-table
		assert.Equal(t, m.leftTbl, tableMatches[i][0])
		assert.Equal(t, m.rightTbl, tableMatches[i][1])
	}

	for _, m := range test.matches {
		if len(m.leftTbl) == 0 || len(m.rightTbl) == 0 {
			continue // no match
		}
		lt, ok := leftTables[m.leftTbl]
		assert.True(t, ok)
		rt, ok := rightTables[m.rightTbl]
		assert.True(t, ok)

		rsch, err := rt.GetSchema(ctx)
		require.NoError(t, err)
		lsch, err := lt.GetSchema(ctx)
		require.NoError(t, err)

		tagMap, err := doltdb.MatchSchemaColumnsByName(rsch, lsch)
		require.NoError(t, err)

		var columnMatches [][2]string
		for l, r := range tagMap {
			columnMatches = append(columnMatches, [2]string{
				lsch.GetAllCols().TagToCol[l].Name,
				rsch.GetAllCols().TagToCol[r].Name,
			})
		}

		sortStringPairs(columnMatches)
		sortStringPairs(m.columnMatches)
		assert.Equal(t, m.columnMatches, columnMatches)
	}
}

func buildTestTable(t *testing.T, vrw types.ValueReadWriter, ns tree.NodeStore, tbl table) *doltdb.Table {
	cols := make([]schema.Column, len(tbl.cols))
	for i, c := range tbl.cols {
		kind := encodingToKind[c.enc]
		cols[i] = schema.NewColumn(c.name, uint64(i), kind, c.pk)
	}
	sch, err := schema.SchemaFromCols(schema.NewColCollection(cols...))
	require.NoError(t, err)

	ctx := context.Background()
	idx, err := durable.NewEmptyIndex(ctx, vrw, ns, sch)
	require.NoError(t, err)

	mut := durable.ProllyMapFromIndex(idx).Mutate()
	kb := val.NewTupleBuilder(sch.GetKeyDescriptor())
	vb := val.NewTupleBuilder(sch.GetValueDescriptor())

	for i := range tbl.rows {
		for j := range tbl.rows[i] {
			if tbl.cols[j].enc != val.Int64Enc {
				continue // null fill
			}
			if j < kb.Desc.Count() {
				kb.PutInt64(j, int64(tbl.rows[i][j]))
			} else {
				v := int64(tbl.rows[i][j])
				vb.PutInt64(j-kb.Desc.Count(), v)
			}
		}
		err = mut.Put(ctx, kb.Build(ns.Pool()), vb.Build(ns.Pool()))
		require.NoError(t, err)
	}
	pm, err := mut.Map(ctx)
	require.NoError(t, err)

	idx = durable.IndexFromProllyMap(pm)
	doltTbl, err := doltdb.NewTable(ctx, vrw, ns, sch, idx, nil, nil)
	require.NoError(t, err)
	return doltTbl
}

func allTablesFromRoot(ctx context.Context, root *doltdb.RootValue) (map[string]*doltdb.Table, error) {
	tables := make(map[string]*doltdb.Table)
	err := root.IterTables(ctx, func(name string, table *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		tables[name] = table
		return
	})
	return tables, err
}

func sortStringPairs(pairs [][2]string) {
	// sort by left, then right
	sort.Slice(pairs, func(i, j int) bool {
		l, r := pairs[i], pairs[j]
		return l[0] < r[0] || (l[0] == r[0] && l[1] < r[1])
	})
}

var encodingToKind = map[val.Encoding]types.NomsKind{
	val.NullEnc:      types.NullKind,
	val.Int8Enc:      types.IntKind,
	val.Uint8Enc:     types.UintKind,
	val.Int16Enc:     types.IntKind,
	val.Uint16Enc:    types.UintKind,
	val.Int32Enc:     types.IntKind,
	val.Uint32Enc:    types.UintKind,
	val.Int64Enc:     types.IntKind,
	val.Uint64Enc:    types.UintKind,
	val.Float32Enc:   types.FloatKind,
	val.Float64Enc:   types.FloatKind,
	val.Bit64Enc:     types.UintKind,
	val.Hash128Enc:   types.UUIDKind,
	val.YearEnc:      types.UintKind,
	val.DateEnc:      types.TimestampKind,
	val.TimeEnc:      types.IntKind,
	val.DatetimeEnc:  types.TimestampKind,
	val.EnumEnc:      types.UintKind,
	val.SetEnc:       types.UintKind,
	val.BytesAddrEnc: types.BlobKind,
	//val.CommitAddrEnc: types.RefKind,
	val.StringAddrEnc: types.BlobKind,
	val.JSONAddrEnc:   types.JSONKind,
	val.StringEnc:     types.StringKind,
	val.ByteStringEnc: types.InlineBlobKind,
	val.DecimalEnc:    types.DecimalKind,
	val.JSONEnc:       types.JSONKind,
	val.GeometryEnc:   types.GeometryKind,
}

func TestArePrimaryKeySetsDiffable(t *testing.T) {
	tests := []struct {
		Name     string
		From     schema.Schema
		To       schema.Schema
		Diffable bool
		KeyMap   val.OrdinalMapping
	}{
		{
			Name: "Basic",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 0, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 0, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "PK-Column renames",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 1, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk2", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "Only pk ordering should matter for diffability",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("pk", 1, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "Only pk ordering should matter for diffability - inverse",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 1, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("col1", 2, types.IntKind, false),
				schema.NewColumn("pk", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "Only pk ordering should matter for diffability - compound",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk1", 0, types.IntKind, true),
				schema.NewColumn("col1", 1, types.IntKind, false),
				schema.NewColumn("pk2", 2, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk1", 0, types.IntKind, true),
				schema.NewColumn("pk2", 2, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0, 1},
		},
		{
			Name: "Tag mismatches",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 0, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "PK Ordinal mismatches",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk1", 0, types.IntKind, true),
				schema.NewColumn("pk2", 1, types.UintKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk2", 1, types.UintKind, true),
				schema.NewColumn("pk1", 0, types.IntKind, true))),
			Diffable: false,
		},
		{
			Name: "Int -> String",
			From: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 0, types.IntKind, true))),
			To: schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("pk", 0, types.StringKind, true))),
			Diffable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			d := doltdb.ArePrimaryKeySetsDiffable(test.From, test.To)
			require.Equal(t, test.Diffable, d)

			// If they are diffable then we should be able to map their schemas from one to another.
			if d {
				keyMap, _, err := doltdb.MapSchemaByColumnName(test.From, test.To)
				require.NoError(t, err)
				require.Equal(t, test.KeyMap, keyMap)
			}
		})
	}
}
