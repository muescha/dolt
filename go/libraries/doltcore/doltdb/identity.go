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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
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
		c1 := cc1.GetAtIndex(i)
		c2 := cc2.GetAtIndex(i)
		if (c1.Tag != c2.Tag) || (c1.IsPartOfPK != c2.IsPartOfPK) {
			return false
		}
		if types.IsFormat_DOLT(format) && !c1.TypeInfo.ToSqlType().Equals(c2.TypeInfo.ToSqlType()) {
			return false
		}
	}

	return true
}

// MapSchemaBasedOnTagAndName can be used to map column values from one schema
// to another schema. A primary key column in |inSch| is mapped to |outSch| if
// they share the same tag. A non-primary key column in |inSch| is mapped to
// |outSch| purely based on the name. It returns ordinal mappings that can be
// use to map key, value val.Tuple's of schema |inSch| to |outSch|. The first
// ordinal map is for keys, and the second is for values. If a column of |inSch|
// is missing in |outSch| then that column's index in the ordinal map holds -1.
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
