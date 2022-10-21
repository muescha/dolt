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
	"sort"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/val"
)

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
			// todo: this is underspecified for NBF __DOLT__
			//  val.Encoding is more granular than types.Kind
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

// ArePrimaryKeySetsDiffable checks if two schemas are diffable.
func ArePrimaryKeySetsDiffable(fromSch, toSch schema.Schema) bool {
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

	f, t := fromSch.GetPKCols(), toSch.GetPKCols()
	if f.Size() != t.Size() {
		return false
	}
	for i := range f.GetColumns() {
		fc, tc := f.GetByIndex(i), t.GetByIndex(i)
		// todo(andy): we probably want to compare encoding here
		if !fc.TypeInfo.Equals(tc.TypeInfo) {
			return false
		}
	}
	return true
}

// MapSchemaByColumnName can be used to map column values from one schema to another schema.
// A primary key column in |inSch| is mapped to |outSch| if they share the same tag.
// A non-primary key column in |inSch| is mapped to |outSch| purely based on the name.
// It returns ordinal mappings that can be use to map key, value val.Tuple's of schema |inSch| to |outSch|.
// The first ordinal map is for keys, and the second is for values.
// If a column of |inSch| is missing in |outSch| then that column's index in the ordinal map holds -1.
func MapSchemaByColumnName(lsch, rsch schema.Schema) (keyMap, valMap val.OrdinalMapping, err error) {
	tagMap, err := MatchSchemaColumnsByName(lsch, rsch)
	if err != nil {
		return nil, nil, err
	}

	keyMap = make(val.OrdinalMapping, lsch.GetPKCols().Size())
	for l, c := range lsch.GetPKCols().GetColumns() {
		rtag := tagMap[c.Tag]
		keyMap[l] = rsch.GetPKCols().TagToIdx[rtag]
	}

	valMap = make(val.OrdinalMapping, lsch.GetNonPKCols().Size())
	for l, c := range lsch.GetNonPKCols().GetColumns() {
		rtag := tagMap[c.Tag]
		valMap[l] = rsch.GetNonPKCols().TagToIdx[rtag]
	}
	return
}

type TagMapping map[uint64]uint64

func MatchSchemaColumnsByName(lsch, rsch schema.Schema) (TagMapping, error) {
	matches := make(TagMapping, lsch.GetAllCols().Size())
	if lsch == nil || lsch.GetAllCols().Size() == 0 ||
		rsch == nil || rsch.GetAllCols().Size() == 0 {
		return matches, nil
	}

	// match primary keys by ordinal position
	rcols := rsch.GetPKCols().GetColumns()
	for i, l := range lsch.GetPKCols().GetColumns() {
		matches[l.Tag] = rcols[i].Tag
	}

	// match non-primary keys by name
	for _, lc := range lsch.GetNonPKCols().GetColumns() {
		rc, ok := rsch.GetNonPKCols().GetByName(lc.Name)
		if !ok {
			continue
		}
		matches[lc.Tag] = rc.Tag
	}

	// short-circuit if we're done matching, or schema is keyless
	if len(matches) == lsch.GetAllCols().Size() || schema.IsKeyless(lsch) {
		return matches, nil
	}

	return matches, nil
}
