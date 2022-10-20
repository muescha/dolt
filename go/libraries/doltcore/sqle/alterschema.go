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

package sqle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	errors2 "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrUsingSpatialKey = errors2.NewKind("can't use Spatial Types as Primary Key for table %s")

// renameTable renames a table with in a RootValue and returns the updated root.
func renameTable(ctx context.Context, root *doltdb.RootValue, oldName, newName string) (*doltdb.RootValue, error) {
	if newName == oldName {
		return root, nil
	} else if root == nil {
		panic("invalid parameters")
	}

	return root.RenameTable(ctx, oldName, newName)
}

// Nullable represents whether a column can have a null value.
type Nullable bool

const (
	NotNull Nullable = false
	Null    Nullable = true
)

// addColumnToTable adds a new column to the schema given and returns the new table value. Non-null column additions
// rewrite the entire table, since we must write a value for each row. If the column is not nullable, a default value
// must be provided.
//
// Returns an error if the column added conflicts with the existing schema in tag or name.
func addColumnToTable(
	ctx context.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	tblName string,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
	nullable Nullable,
	defaultVal *sql.ColumnDefaultValue,
	comment string,
	order *sql.ColumnOrder,
) (*doltdb.Table, error) {
	oldSchema, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(oldSchema) {
		return nil, ErrKeylessAltTbl
	}

	if err := validateNewColumn(ctx, root, tbl, tblName, tag, newColName, typeInfo); err != nil {
		return nil, err
	}

	newCol, err := createColumn(nullable, newColName, tag, typeInfo, defaultVal.String(), comment)
	if err != nil {
		return nil, err
	}

	newSchema, err := oldSchema.AddColumn(newCol, orderToOrder(order))
	if err != nil {
		return nil, err
	}

	newTable, err := tbl.UpdateSchema(ctx, newSchema)
	if err != nil {
		return nil, err
	}

	// TODO: we do a second pass in the engine to set a default if there is one. We should only do a single table scan.
	return newTable.AddColumnToRows(ctx, newColName, newSchema)
}

func orderToOrder(order *sql.ColumnOrder) *schema.ColumnOrder {
	if order == nil {
		return nil
	}
	return &schema.ColumnOrder{
		First:       order.First,
		AfterColumn: order.AfterColumn,
	}
}

func createColumn(nullable Nullable, newColName string, tag uint64, typeInfo typeinfo.TypeInfo, defaultVal, comment string) (schema.Column, error) {
	if nullable {
		return schema.NewColumnWithTypeInfo(newColName, tag, typeInfo, false, defaultVal, false, comment)
	} else {
		return schema.NewColumnWithTypeInfo(newColName, tag, typeInfo, false, defaultVal, false, comment, schema.NotNullConstraint{})
	}
}

// ValidateNewColumn returns an error if the column as specified cannot be added to the schema given.
func validateNewColumn(
	ctx context.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	tblName string,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
) error {
	if typeInfo == nil {
		return fmt.Errorf(`typeinfo may not be nil`)
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return err
	}

	cols := sch.GetAllCols()
	err = cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool, err error) {
		if strings.ToLower(currCol.Name) == strings.ToLower(newColName) {
			return true, fmt.Errorf("A column with the name %s already exists in table %s.", newColName, tblName)
		}
		return false, nil
	})
	return err
}

// modifyColumn modifies the column with the name given, replacing it with the new definition provided. A column with
// the name given must exist in the schema of the table.
func modifyColumn(
	ctx context.Context,
	tbl *doltdb.Table,
	existingCol schema.Column,
	newCol schema.Column,
	order *sql.ColumnOrder,
) (*doltdb.Table, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: write test of changing column case

	// Modify statements won't include key info, so fill it in from the old column
	// TODO: fix this in GMS
	if existingCol.IsPartOfPK {
		newCol.IsPartOfPK = true
		if schema.IsColSpatialType(newCol) {
			return nil, fmt.Errorf("can't use Spatial Types as Primary Key for table")
		}
		foundNotNullConstraint := false
		for _, constraint := range newCol.Constraints {
			if _, ok := constraint.(schema.NotNullConstraint); ok {
				foundNotNullConstraint = true
				break
			}
		}
		if !foundNotNullConstraint {
			newCol.Constraints = append(newCol.Constraints, schema.NotNullConstraint{})
		}
	}

	newSchema, err := replaceColumnInSchema(sch, existingCol, newCol, order)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateSchema(ctx, newSchema)
}

// replaceColumnInSchema replaces the column with the name given with its new definition, optionally reordering it.
// TODO: make this a schema API?
func replaceColumnInSchema(sch schema.Schema, oldCol schema.Column, newCol schema.Column, order *sql.ColumnOrder) (schema.Schema, error) {
	// If no order is specified, insert in the same place as the existing column
	prevColumn := ""
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name == oldCol.Name {
			if prevColumn == "" {
				if order == nil {
					order = &sql.ColumnOrder{First: true}
				}
			}
			break
		} else {
			prevColumn = col.Name
		}
	}

	if order == nil {
		if prevColumn != "" {
			order = &sql.ColumnOrder{AfterColumn: prevColumn}
		} else {
			return nil, fmt.Errorf("Couldn't find column %s", oldCol.Name)
		}
	}

	var newCols []schema.Column
	if order.First {
		newCols = append(newCols, newCol)
	}

	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name != oldCol.Name {
			newCols = append(newCols, col)
		}

		if order.AfterColumn == col.Name {
			newCols = append(newCols, newCol)
		}
	}

	collection := schema.NewColCollection(newCols...)

	err := schema.ValidateForInsert(collection)
	if err != nil {
		return nil, err
	}

	newSch, err := schema.SchemaFromCols(collection)
	if err != nil {
		return nil, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		tags := index.IndexedColumnTags()
		for i := range tags {
			if tags[i] == oldCol.Tag {
				tags[i] = newCol.Tag
			}
		}
		_, err = newSch.Indexes().AddIndexByColTags(index.Name(), tags, schema.IndexProperties{
			IsUnique:      index.IsUnique(),
			IsUserDefined: index.IsUserDefined(),
			Comment:       index.Comment(),
		})
		if err != nil {
			return nil, err
		}
	}

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSch.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	pkOrds, err := schema.ModifyPkOrdinals(sch, newSch)
	if err != nil {
		return nil, err
	}
	err = newSch.SetPkOrdinals(pkOrds)
	if err != nil {
		return nil, err
	}
	return newSch, nil
}

func fmtPrimaryKeyError(sch schema.Schema, keylessRow row.Row) error {
	pkTags := sch.GetPKCols().Tags

	vals := make([]string, len(pkTags))
	for i, tg := range sch.GetPKCols().Tags {
		val, ok := keylessRow.GetColVal(tg)
		if !ok {
			panic("tag for primary key wasn't found")
		}

		vals[i] = val.HumanReadableString()
	}

	return sql.NewUniqueKeyErr(fmt.Sprintf("[%s]", strings.Join(vals, ",")), true, nil)
}

func duplicatePkFunction(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	return sql.NewUniqueKeyErr(fmt.Sprintf("%s", keyString), true, nil)
}

var ErrKeylessAltTbl = errors.New("schema alterations not supported for keyless tables")

// backupFkcIndexesForKeyDrop finds backup indexes to cover foreign key references during a primary
// key drop. If multiple indexes are valid, we sort by unique and select the first.
// This will not work with a non-pk index drop without an additional index filter argument.
func backupFkcIndexesForPkDrop(ctx *sql.Context, sch schema.Schema, fkc *doltdb.ForeignKeyCollection) ([]doltdb.FkIndexUpdate, error) {
	indexes := sch.Indexes().AllIndexes()

	// pkBackups is a mapping from the table's PK tags to potentially compensating indexes
	pkBackups := make(map[uint64][]schema.Index, len(sch.GetPKCols().TagToIdx))
	for tag, _ := range sch.GetPKCols().TagToIdx {
		pkBackups[tag] = nil
	}

	// prefer unique key backups
	sort.Slice(indexes[:], func(i, j int) bool {
		return indexes[i].IsUnique() && !indexes[j].IsUnique()
	})

	for _, idx := range indexes {
		if !idx.IsUserDefined() {
			continue
		}

		for _, tag := range idx.AllTags() {
			if _, ok := pkBackups[tag]; ok {
				pkBackups[tag] = append(pkBackups[tag], idx)
			}
		}
	}

	fkUpdates := make([]doltdb.FkIndexUpdate, 0)
	for _, fk := range fkc.AllKeys() {
		// check if this FK references a parent PK tag we are trying to change
		if backups, ok := pkBackups[fk.ReferencedTableColumns[0]]; ok {
			covered := false
			for _, idx := range backups {
				idxTags := idx.AllTags()
				if len(fk.TableColumns) > len(idxTags) {
					continue
				}
				failed := false
				for i := 0; i < len(fk.ReferencedTableColumns); i++ {
					if idxTags[i] != fk.ReferencedTableColumns[i] {
						failed = true
						break
					}
				}
				if failed {
					continue
				}
				fkUpdates = append(fkUpdates, doltdb.FkIndexUpdate{FkName: fk.Name, FromIdx: fk.ReferencedTableIndex, ToIdx: idx.Name()})
				covered = true
				break
			}
			if !covered {
				return nil, sql.ErrCantDropIndex.New("PRIMARY")
			}
		}
	}
	return fkUpdates, nil
}
