// Copyright 2022 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/zeebo/xxh3"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	heuristicThreshold = 0.5
	heuristicSamples   = 30
)

// ArePrimaryKeySetsDiffable checks if two schemas are diffable. Assumes the
// passed in schema are from the same table between commits. If __DOLT__, then
// it also checks if the underlying SQL types of the columns are equal.
func ArePrimaryKeySetsDiffable(format *types.NomsBinFormat, fromSch, toSch schema.Schema) bool {
	if fromSch == nil && toSch == nil {
		return false
		// Empty case
	} else if fromSch == nil || fromSch.GetAllCols().Size() == 0 ||
		toSch == nil || toSch.GetAllCols().Size() == 0 {
		return true
	}

	// Keyless case for comparing
	if schema.IsKeyless(fromSch) && schema.IsKeyless(toSch) {
		return true
	}

	cc1 := fromSch.GetPKCols()
	cc2 := toSch.GetPKCols()

	if cc1.Size() != cc2.Size() {
		return false
	}

	for i := 0; i < cc1.Size(); i++ {
		c1 := cc1.GetByIndex(i)
		c2 := cc2.GetByIndex(i)
		if (c1.Tag != c2.Tag) || (c1.IsPartOfPK != c2.IsPartOfPK) {
			return false
		}
		if types.IsFormat_DOLT(format) && !c1.TypeInfo.ToSqlType().Equals(c2.TypeInfo.ToSqlType()) {
			return false
		}
	}

	return true
}

// MapSchemaBasedOnTagAndName can be used to map column values from one schema to another schema.
// A primary key column in |inSch| is mapped to |outSch| if they share the same tag.
// A non-primary key column in |inSch| is mapped to |outSch| purely based on the name.
// It returns ordinal mappings that can be use to map key, value val.Tuple's of schema |inSch| to |outSch|.
// The first ordinal map is for keys, and the second is for values.
// If a column of |inSch| is missing in |outSch| then that column's index in the ordinal map holds -1.
func MapSchemaBasedOnTagAndName(inSch, outSch schema.Schema) ([]int, []int, error) {
	keyMapping := make([]int, inSch.GetPKCols().Size())
	valMapping := make([]int, inSch.GetNonPKCols().Size())

	// if inSch or outSch is empty schema. This can be from added or dropped table.
	if len(inSch.GetAllCols().GetColumns()) == 0 || len(outSch.GetAllCols().GetColumns()) == 0 {
		return keyMapping, valMapping, nil
	}

	err := inSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		i := inSch.GetPKCols().TagToIdx[tag]
		if col, ok := outSch.GetPKCols().GetByTag(tag); ok {
			j := outSch.GetPKCols().TagToIdx[col.Tag]
			keyMapping[i] = j
		} else {
			return true, fmt.Errorf("could not map primary key column %s", col.Name)
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	err = inSch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		i := inSch.GetNonPKCols().TagToIdx[col.Tag]
		if col, ok := outSch.GetNonPKCols().GetByName(col.Name); ok {
			j := outSch.GetNonPKCols().TagToIdx[col.Tag]
			valMapping[i] = j
		} else {
			valMapping[i] = -1
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	return keyMapping, valMapping, nil
}

// MatchTablesForRoots matches tables between two root values. Table matching follows a conservative algorithm:
// matching tables must have the same name and the same set of pk column types (empty set for keyless tables).
func MatchTablesForRoots(ctx context.Context, left, right *RootValue) (matches [][2]string, err error) {
	var leftSchemas, rightSchemas map[string]schema.Schema
	if leftSchemas, err = left.GetAllSchemas(ctx); err != nil {
		return nil, err
	}
	if rightSchemas, err = right.GetAllSchemas(ctx); err != nil {
		return nil, err
	}

	for name, lsch := range leftSchemas {
		rsch, ok := rightSchemas[name]
		if !ok {
			matches = append(matches, [2]string{name, ""})
			continue // no match
		}

		// validate equal column lengths
		l := lsch.GetPKCols().GetColumns()
		r := rsch.GetPKCols().GetColumns()
		if len(l) != len(r) {
			matches = append(matches, [2]string{name, ""})
			continue // no match
		}

		// validate equal column encodings
		for i := range lsch.GetPKCols().GetColumns() {
			// todo: does kind work for new format?
			if l[i].Kind != r[i].Kind {
				ok = false
			}
		}
		if ok {
			matches = append(matches, [2]string{name, name})
			delete(rightSchemas, name)
		}
	}

	// append unmatched right hand schemas
	for name := range rightSchemas {
		matches = append(matches, [2]string{"", name})
	}

	// sort by left, then right
	sort.Slice(matches, func(i, j int) bool {
		l, r := matches[i], matches[j]
		return l[0] < r[0] || (l[0] == r[0] && l[1] < r[1])
	})

	return matches, nil
}

func MatchColumnsForTables(ctx context.Context, leftTbl, rightTbl *Table) (matches [][2]string, err error) {
	lsch, err := leftTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	rsch, err := rightTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	matches = make([][2]string, 0, lsch.GetAllCols().Size())

	// we assume primary keys match, if tables were matched
	rcols := rsch.GetPKCols().GetColumns()
	for i, lcol := range lsch.GetPKCols().GetColumns() {
		matches = append(matches, [2]string{
			lcol.Name, rcols[i].Name,
		})
	}

	// same name, same kind is a match
	for _, lc := range lsch.GetNonPKCols().GetColumns() {
		rc, ok := rsch.GetNonPKCols().GetByName(lc.Name)
		// todo: does kind work for new format?
		if !ok || lc.Kind != rc.Kind {
			continue
		}
		matches = append(matches, [2]string{
			lc.Name, rc.Name,
		})
	}

	// short-circuit if we're done matching, or schema is keyless
	if len(matches) == lsch.GetAllCols().Size() || schema.IsKeyless(lsch) {
		return matches, nil
	}

	// attempt to match remaining columns with heuristic sampling
	return heuristicColumnMatching(ctx, matches, leftTbl, rightTbl, lsch, rsch)
}

func heuristicColumnMatching(
	ctx context.Context,
	matches [][2]string,
	ltbl, rtbl *Table,
	lsch, rsch schema.Schema,
) ([][2]string, error) {

	lcols := lsch.GetNonPKCols()
	rcols := rsch.GetNonPKCols()
	lmap := lcols.NameToIdxMap()
	rmap := rcols.NameToIdxMap()

	// remove matched columns
	for i := range matches {
		delete(lmap, matches[i][0])
		delete(rmap, matches[i][1])
	}

	var candidates [][2]int
	for ln := range lmap {
		for rn := range rmap {
			lc, rc := lcols.NameToCol[ln], rcols.NameToCol[rn]
			// todo: does kind work for new format?
			if lc.Kind != rc.Kind {
				// columns kinds must match
				continue
			}
			candidates = append(candidates, [2]int{
				lmap[ln], rmap[rn],
			})
		}
	}

	// check if any matches are possible
	if len(candidates) == 0 {
		return matches, nil
	}

	matrix := make([][]float32, lsch.GetNonPKCols().Size())
	for i := range matrix {
		matrix[i] = make([]float32, rsch.GetNonPKCols().Size())
	}

	samples, err := getPairwiseRowSamples(ctx, ltbl, rtbl)
	if err != nil {
		return nil, err
	}

	// for each candidate pairing, sum the number of
	// matching field values in the sampled rows
	for _, c := range candidates {
		for _, s := range samples {
			l, r := c[0], c[1]
			if s[0][l] == s[1][r] {
				matrix[l][r]++
			}
		}
	}

	// normalize matrix
	k := float32(len(samples))
	for i := range matrix {
		for j := range matrix[i] {
			matrix[i][j] /= k
		}
	}

	// greedy match
	for lname, i := range lmap {
		var idx int
		var max float32
		for j := range matrix[i] {
			if matrix[i][j] > max {
				idx, max = j, matrix[i][j]
			}
		}
		if max >= heuristicThreshold {
			matches = append(matches, [2]string{
				lname, rcols.GetByIndex(idx).Name,
			})
		}
	}
	return matches, nil
}

// rowSample is a slice of checksums for each
// field value in a sampled row. We can use
// these checksums to quickly check equality.
type rowSample []uint32

func getPairwiseRowSamples(ctx context.Context, ltbl, rtbl *Table) ([][2]rowSample, error) {
	lidx, err := ltbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	ridx, err := rtbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	switch lidx.Format() {
	case types.Format_DOLT:
		return getPairwiseProllySamples(ctx,
			durable.ProllyMapFromIndex(lidx),
			durable.ProllyMapFromIndex(ridx))

	case types.Format_LD_1, types.Format_DOLT_DEV:
		return getPairwiseNomsSamples(ctx,
			durable.NomsMapFromIndex(lidx),
			durable.NomsMapFromIndex(ridx))

	default:
		return nil, fmt.Errorf("unknown NomsBinFormat (%s)", lidx.Format().VersionString())
	}
}

func getPairwiseProllySamples(ctx context.Context, left, right prolly.Map) ([][2]rowSample, error) {
	samples := make([][2]rowSample, 0, heuristicSamples)
	_, ldesc := left.Descriptors()
	_, rdesc := right.Descriptors()

	lcnt, err := left.Count()
	if err != nil {
		return nil, err
	}
	for _, ord := range getSampleOrdinals(lcnt, heuristicSamples/2) {
		var key val.Tuple
		// sample a random ordinal |ord| in |left|
		l := make(rowSample, ldesc.Count())
		err = left.GetOrdinal(ctx, uint64(ord), func(k, v val.Tuple) error {
			hashProllyTuple(l, v)
			key = k
			return nil
		})
		if err != nil {
			return nil, err
		}
		// sample the same key in |right|
		r := make(rowSample, rdesc.Count())
		err = right.Get(ctx, key, func(k, v val.Tuple) error {
			hashProllyTuple(r, v)
			return nil
		})
		samples = append(samples, [2]rowSample{l, r})
	}

	rcnt, err := right.Count()
	if err != nil {
		return nil, err
	}
	for _, ord := range getSampleOrdinals(rcnt, heuristicSamples/2) {
		var key val.Tuple
		// sample a random ordinal |ord| in |right|
		r := make(rowSample, rdesc.Count())
		err = right.GetOrdinal(ctx, uint64(ord), func(k, v val.Tuple) error {
			hashProllyTuple(r, v)
			key = k
			return nil
		})
		if err != nil {
			return nil, err
		}
		// sample the same key in |left|
		l := make(rowSample, ldesc.Count())
		err = left.Get(ctx, key, func(k, v val.Tuple) error {
			hashProllyTuple(l, v)
			return nil
		})
		samples = append(samples, [2]rowSample{l, r})
	}

	return samples, nil
}

func hashProllyTuple(hashes []uint32, tup val.Tuple) {
	for i := range hashes {
		if tup.FieldIsNull(i) {
			hashes[i] = 0
		} else {
			h := xxh3.Hash(tup.GetField(i))
			hashes[i] = uint32(h % math.MaxUint32)
		}
	}
}

func getPairwiseNomsSamples(ctx context.Context, l, r types.Map) ([][2]rowSample, error) {
	panic("unimplemented")
}

func getSampleOrdinals(n, k int) (ordinals []int) {
	if k > n {
		k = n
	}

	ordinals = make([]int, k)
	if n > 256 {
		// random sample, allow duplicates
		for i := range ordinals {
			ordinals[i] = rand.Intn(n)
		}
	} else {
		// avoid duplicates for small tables
		shuf := make([]int, n)
		for i := range shuf {
			shuf[i] = i
		}
		rand.Shuffle(n, func(i, j int) {
			shuf[i], shuf[j] = shuf[j], shuf[i]
		})
		copy(ordinals, shuf)
	}
	sort.Ints(ordinals)
	return
}
