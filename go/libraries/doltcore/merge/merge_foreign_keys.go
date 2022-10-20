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

package merge

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

type FKConflict struct {
	Kind  conflictKind
	Cause string
}

func (c FKConflict) empty() bool {
	return c.Kind == emptyConflict
}

// ForeignKeysMerge performs a three-way merge of (ourRoot, theirRoot, ancRoot) and using mergeRoot to validate FKs.
func ForeignKeysMerge(ctx context.Context, mergedRoot, ourRoot, theirRoot, ancRoot *doltdb.RootValue) (*doltdb.ForeignKeyCollection, []FKConflict, error) {
	var ours, theirs, anc fkSet
	var err error

	if ours, err = fkSetFromRoot(ctx, ourRoot); err != nil {
		return nil, nil, err
	}
	if theirs, err = fkSetFromRoot(ctx, theirRoot); err != nil {
		return nil, nil, err
	}
	if anc, err = fkSetFromRoot(ctx, ancRoot); err != nil {
		return nil, nil, err
	}

	var conflicts []FKConflict

	// find FKs added in |theirs|
	for name, t := range theirs.keys {
		_, ok := anc.findByName(name)
		if ok {
			continue
		}

		var cnf FKConflict

		// |t| added since ancRoot
		o, ok := ours.findByName(name)
		if !ok {
			t, cnf = translateDefinition(t, theirs, ours)
			if !cnf.empty() {
				conflicts = append(conflicts, cnf)
			} else {
				ours.add(t)
			}
			continue
		}

		// |ours| contains |name|, check for conflict
		t, cnf = translateDefinition(t, theirs, ours)
		if !cnf.empty() {
			conflicts = append(conflicts, cnf)
		} else if !o.EqualDefs(t) {
			s := "conflicting foreign key definitions with name " + name
			conflicts = append(conflicts, FKConflict{
				Kind: NameCollision, Cause: s,
			})
		}
	}

	// find FKs dropped in |theirs|
	for name := range anc.keys {
		_, ok := theirs.findByName(name)
		if ok {
			continue
		}
		ours.drop(name)
	}

	merged, err := ours.toForeignKeyCollection()
	if err != nil {
		// todo: check for duplicate FK definitions
		return nil, nil, err
	}

	// removes FKs referencing dropped tables
	merged, err = pruneInvalidForeignKeys(ctx, merged, mergedRoot)
	if err != nil {
		return nil, nil, err
	}

	return merged, conflicts, nil
}

// pruneInvalidForeignKeys removes from a ForeignKeyCollection any ForeignKey whose parent/child table/columns have been removed.
func pruneInvalidForeignKeys(ctx context.Context, fkColl *doltdb.ForeignKeyCollection, mergedRoot *doltdb.RootValue) (pruned *doltdb.ForeignKeyCollection, err error) {
	pruned, _ = doltdb.NewForeignKeyCollection()
	err = fkColl.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		parentTbl, ok, err := mergedRoot.GetTable(ctx, fk.ReferencedTableName)
		if err != nil || !ok {
			return false, err
		}
		parentSch, err := parentTbl.GetSchema(ctx)
		if err != nil {
			return false, err
		}
		for _, tag := range fk.ReferencedTableColumns {
			if _, ok := parentSch.GetAllCols().GetByTag(tag); !ok {
				return false, nil
			}
		}

		childTbl, ok, err := mergedRoot.GetTable(ctx, fk.TableName)
		if err != nil || !ok {
			return false, err
		}
		childSch, err := childTbl.GetSchema(ctx)
		if err != nil {
			return false, err
		}
		for _, tag := range fk.TableColumns {
			if _, ok := childSch.GetAllCols().GetByTag(tag); !ok {
				return false, nil
			}
		}

		err = pruned.AddKeys(fk)
		return false, err
	})

	if err != nil {
		return nil, err
	}

	return pruned, nil
}

// foreignKeySet is a collection of doltdb.ForeignKeys for a doltdb.RootValue.
// It is similar to doltdb.ForeignKeyCollection, but embeds a map of table schemas
// to match foreign key definitions across RootValues by resolving columns tags to
// columns names. Column tags themselves cannot be matched between RootValues.
type fkSet struct {
	keys    map[string]doltdb.ForeignKey
	schemas map[string]schema.Schema
}

func fkSetFromRoot(ctx context.Context, root *doltdb.RootValue) (fkSet, error) {
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return fkSet{}, err
	}

	keys := make(map[string]doltdb.ForeignKey)
	err = fkc.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		keys[fk.Name] = fk
		return
	})
	if err != nil {
		return fkSet{}, err
	}

	schemas, err := root.GetAllSchemas(ctx)
	if err != nil {
		return fkSet{}, err
	}
	return fkSet{keys: keys, schemas: schemas}, nil
}

// translateDefinition attempts to translate |fk| from the source fkSet to the destination fkSet.
// ForeignKey definitions reference columns by tags, which are not stable between RootValues.
// Instead we match |src| and |dest| columns by name and modify tags in |fk| accordingly.
func translateDefinition(fk doltdb.ForeignKey, src, dest fkSet) (doltdb.ForeignKey, FKConflict) {
	srcParent, srcChild := src.getSchemas(fk.Name)
	destParent, destChild := dest.getSchemas(fk.Name)

	for i, tag := range fk.TableColumns {
		scol := srcChild.GetAllCols().TagToCol[tag]
		dcol, ok := destChild.GetAllCols().GetByName(scol.Name)
		if !ok {
			return doltdb.ForeignKey{}, FKConflict{
				Kind: DeletedColumn,
				Cause: fmt.Sprintf("foreign key %s references missing column %s in table %s",
					fk.Name, scol.Name, fk.TableName),
			}
		}
		fk.TableColumns[i] = dcol.Tag
	}

	for i, tag := range fk.ReferencedTableColumns {
		scol := srcParent.GetAllCols().TagToCol[tag]
		dcol, ok := destParent.GetAllCols().GetByName(scol.Name)
		if !ok {
			return doltdb.ForeignKey{}, FKConflict{
				Kind: DeletedColumn,
				Cause: fmt.Sprintf("foreign key %s references missing column %s in table %s",
					fk.Name, scol.Name, fk.ReferencedTableName),
			}
		}
		fk.ReferencedTableColumns[i] = dcol.Tag
	}
	return fk, FKConflict{}
}

func (s fkSet) getSchemas(name string) (parent, child schema.Schema) {
	fk := s.keys[name]
	parent = s.schemas[fk.ReferencedTableIndex]
	child = s.schemas[fk.TableName]
	return
}

// findByName searches |s| for a foreign key named |name|
func (s fkSet) findByName(name string) (doltdb.ForeignKey, bool) {
	match, ok := s.keys[name]
	return match, ok
}

func (s fkSet) add(fk doltdb.ForeignKey) {
	s.keys[fk.Name] = fk
}

func (s fkSet) drop(name string) {
	delete(s.keys, name)
}

func (s fkSet) toForeignKeyCollection() (*doltdb.ForeignKeyCollection, error) {
	fks := make([]doltdb.ForeignKey, 0, len(s.keys))
	for _, fk := range s.keys {
		fks = append(fks, fk)
	}
	return doltdb.NewForeignKeyCollection(fks...)
}
