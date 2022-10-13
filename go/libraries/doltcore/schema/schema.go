// Copyright 2019 Dolthub, Inc.
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

package schema

import (
	"errors"

	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/store/val"
)

// Schema is an interface for retrieving the columns that make up a schema
type Schema interface {
	// GetPKCols gets the collection of columns which make the primary key. They
	// are always returned in ordinal order.
	GetPKCols() *ColCollection

	// GetNonPKCols gets the collection of columns which are not part of the primary key.
	GetNonPKCols() *ColCollection

	// GetAllCols gets the collection of all columns (pk and non-pk)
	GetAllCols() *ColCollection

	// Indexes returns a collection of all indexes on the table that this schema belongs to.
	Indexes() IndexCollection

	// Checks returns a collection of all check constraints on the table that this schema belongs to.
	Checks() CheckCollection

	// GetPkOrdinals returns a slice of the primary key ordering indexes relative to the schema column ordering
	GetPkOrdinals() []int

	// SetPkOrdinals specifies a primary key column ordering
	SetPkOrdinals([]int) error

	// AddColumn adds a column to this schema in the order given and returns the resulting Schema.
	// The new column cannot be a primary key. To alter primary keys, create a new schema with those keys.
	AddColumn(column Column, order *ColumnOrder) (Schema, error)

	// GetMapDescriptors returns the key and value tuple descriptors for this schema.
	GetMapDescriptors() (keyDesc, valueDesc val.TupleDesc)

	// GetKeyDescriptor returns the key tuple descriptor for this schema.
	GetKeyDescriptor() val.TupleDesc

	// GetValueDescriptor returns the value tuple descriptor for this schema.
	GetValueDescriptor() val.TupleDesc

	// GetCollation returns the table's collation.
	GetCollation() Collation

	// SetCollation sets the table's collation.
	SetCollation(collation Collation)
}

// ColumnOrder is used in ALTER TABLE statements to change the order of inserted / modified columns.
type ColumnOrder struct {
	First       bool   // True if this column should come first
	AfterColumn string // Set to the name of the column after which this column should appear
}

func IsKeyless(sch Schema) bool {
	return sch != nil &&
		sch.GetPKCols().Size() == 0 &&
		sch.GetAllCols().Size() != 0
}

func HasAutoIncrement(sch Schema) (ok bool) {
	_ = sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		if col.AutoIncrement {
			ok = true
			stop = true
		}
		return
	})
	return
}

// SchemasAreEqual tests equality of two schemas.
func SchemasAreEqual(sch1, sch2 Schema) bool {
	if sch1 == nil && sch2 == nil {
		return true
	} else if sch1 == nil || sch2 == nil {
		return false
	}
	colCollIsEqual := ColCollsAreEqual(sch1.GetAllCols(), sch2.GetAllCols())
	if !colCollIsEqual {
		return false
	}
	return sch1.Indexes().Equals(sch2.Indexes())
}

// TODO: this function never returns an error
// VerifyInSchema tests that the incoming schema matches the schema from the original table
// based on the presence of the column name in the original schema.
func VerifyInSchema(inSch, outSch Schema) (bool, error) {
	inSchCols := inSch.GetAllCols()
	outSchCols := outSch.GetAllCols()

	if inSchCols.Size() != outSchCols.Size() {
		return false, nil
	}

	match := true
	err := inSchCols.Iter(func(tag uint64, inCol Column) (stop bool, err error) {
		_, isValid := outSchCols.GetByNameCaseInsensitive(inCol.Name)

		if !isValid {
			match = false
			return true, nil
		}

		return false, nil
	})

	if err != nil {
		return false, err
	}

	return match, nil
}

// IsColSpatialType returns whether a column's type is a spatial type
func IsColSpatialType(c Column) bool {
	return c.TypeInfo.ToSqlType().Type() == query.Type_GEOMETRY
}

// IsUsingSpatialColAsKey is a utility function that checks for any spatial types being used as a primary key
func IsUsingSpatialColAsKey(sch Schema) bool {
	pkCols := sch.GetPKCols()
	cols := pkCols.GetColumns()
	for _, c := range cols {
		if IsColSpatialType(c) {
			return true
		}
	}
	return false
}

// CopyChecksConstraints copies check constraints from the |from| schema to the |to| schema and returns it
func CopyChecksConstraints(from, to Schema) Schema {
	fromSch, toSch := from.(*schemaImpl), to.(*schemaImpl)
	toSch.checkCollection = fromSch.checkCollection
	return toSch
}

// CopyIndexes copies secondary indexes from the |from| schema to the |to| schema and returns it
func CopyIndexes(from, to Schema) Schema {
	fromSch, toSch := from.(*schemaImpl), to.(*schemaImpl)
	toSch.indexCollection = fromSch.indexCollection
	return toSch
}

var ErrPrimaryKeySetsIncompatible = errors.New("primary key sets incompatible")

// ModifyPkOrdinals tries to create primary key ordinals for a newSch maintaining
// the relative positions of PKs from the oldSch. Return an ErrPrimaryKeySetsIncompatible
// error if the two schemas have a different number of primary keys, or a primary
// key column's tag changed between the two sets.
func ModifyPkOrdinals(oldSch, newSch Schema) ([]int, error) {
	if newSch.GetPKCols().Size() != oldSch.GetPKCols().Size() {
		return nil, ErrPrimaryKeySetsIncompatible
	}

	newPkOrdinals := make([]int, len(newSch.GetPkOrdinals()))
	for _, newCol := range newSch.GetPKCols().GetColumns() {
		// ordIdx is the relative primary key order (that stays the same)
		ordIdx, ok := oldSch.GetPKCols().TagToIdx[newCol.Tag]
		if !ok {
			// if pk tag changed, use name to find the new newCol tag
			oldCol, ok := oldSch.GetPKCols().NameToCol[newCol.Name]
			if !ok {
				return nil, ErrPrimaryKeySetsIncompatible
			}
			ordIdx = oldSch.GetPKCols().TagToIdx[oldCol.Tag]
		}

		// ord is the schema ordering index, which may have changed in newSch
		ord := newSch.GetAllCols().TagToIdx[newCol.Tag]
		newPkOrdinals[ordIdx] = ord
	}

	return newPkOrdinals, nil
}
