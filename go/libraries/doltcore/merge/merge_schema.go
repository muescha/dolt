// Copyright 2020 Dolthub, Inc.
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
	"errors"
	"fmt"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type conflictKind byte

const (
	emptyConflict conflictKind = iota

	TagCollision
	NameCollision
	ColumnCollision

	DeletedColumn
	InvalidCheck
	DeletedCheck
)

type SchemaConflict struct {
	TableName    string
	ColConflicts []ColConflict
	IdxConflicts []IdxConflict
	ChkConflicts []ChkConflict
}

var EmptySchConflicts = SchemaConflict{}

func (sc SchemaConflict) Count() int {
	return len(sc.ColConflicts) + len(sc.IdxConflicts) + len(sc.ChkConflicts)
}

func (sc SchemaConflict) AsError() error {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("schema conflicts for table %s:\n", sc.TableName))
	for _, c := range sc.ColConflicts {
		b.WriteString(fmt.Sprintf("\t%s\n", c.String()))
	}
	for _, c := range sc.IdxConflicts {
		b.WriteString(fmt.Sprintf("\t%s\n", c.String()))
	}
	for _, c := range sc.ChkConflicts {
		b.WriteString(fmt.Sprintf("\t%s\n", c.String()))
	}
	return fmt.Errorf(b.String())
}

type ColConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Column
}

func (c ColConflict) String() string {
	switch c.Kind {
	case NameCollision:
		return fmt.Sprintf("two columns with the same name '%s' have different tags. See https://github.com/dolthub/dolt/issues/3963", c.Ours.Name)
	case TagCollision:
		return fmt.Sprintf("different column definitions for our column %s and their column %s", c.Ours.Name, c.Theirs.Name)
	}
	return ""
}

type IdxConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Index
}

func (c IdxConflict) String() string {
	return ""
}

type ChkConflict struct {
	Kind         conflictKind
	Ours, Theirs schema.Check
}

func (c ChkConflict) String() string {
	switch c.Kind {
	case NameCollision:
		return fmt.Sprintf("two checks with the name '%s' but different definitions", c.Ours.Name())
	case ColumnCollision:
		return fmt.Sprintf("our check '%s' and their check '%s' both reference the same column(s)", c.Ours.Name(), c.Theirs.Name())
	case InvalidCheck:
		return fmt.Sprintf("check '%s' references a column that will be deleted after merge", c.Ours.Name())
	case DeletedCheck:
		if c.Theirs == nil {
			return fmt.Sprintf("check '%s' was deleted in theirs but modified in ours", c.Ours.Name())
		} else {
			return fmt.Sprintf("check '%s' was deleted in ours but modified in theirs", c.Theirs.Name())
		}
	}
	return ""
}

var ErrMergeWithDifferentPkSets = errors.New("error: cannot merge two tables with different primary key sets")

// SchemaMerge performs a three-way merge of ourSch, theirSch, and ancSch.
func SchemaMerge(format *types.NomsBinFormat, ourSch, theirSch, ancSch schema.Schema, tblName string) (sch schema.Schema, sc SchemaConflict, err error) {
	// (sch - ancSch) ∪ (mergeSch - ancSch) ∪ (sch ∩ mergeSch)
	sc = SchemaConflict{
		TableName: tblName,
	}

	// TODO: We'll remove this once it's possible to get diff and merge on different primary key sets
	// TODO: decide how to merge different orders of PKS
	if !doltdb.ArePrimaryKeySetsDiffable(format, ourSch, theirSch) {
		return nil, SchemaConflict{}, ErrMergeWithDifferentPkSets
	}

	var mergedCC *schema.ColCollection
	mergedCC, sc.ColConflicts, err = mergeColumns(ourSch.GetAllCols(), theirSch.GetAllCols(), ancSch.GetAllCols())
	if err != nil {
		return nil, EmptySchConflicts, err
	}
	if len(sc.ColConflicts) > 0 {
		return nil, sc, nil
	}

	var mergedIdxs schema.IndexCollection
	mergedIdxs, sc.IdxConflicts = mergeIndexes(mergedCC, ourSch, theirSch, ancSch)
	if len(sc.IdxConflicts) > 0 {
		return nil, sc, nil
	}

	sch, err = schema.SchemaFromCols(mergedCC)
	if err != nil {
		return nil, sc, err
	}

	// TODO: Merge conflict should have blocked any primary key ordinal changes
	err = sch.SetPkOrdinals(ourSch.GetPkOrdinals())
	if err != nil {
		return nil, sc, err
	}

	_ = mergedIdxs.Iter(func(index schema.Index) (stop bool, err error) {
		sch.Indexes().AddIndex(index)
		return false, nil
	})

	// Merge checks
	var mergedChks []schema.Check
	mergedChks, sc.ChkConflicts, err = mergeChecks(ourSch.Checks(), theirSch.Checks(), ancSch.Checks())
	if err != nil {
		return nil, EmptySchConflicts, err
	}
	if len(sc.ChkConflicts) > 0 {
		return nil, sc, nil
	}

	// Look for invalid CHECKs
	for _, chk := range mergedChks {
		// CONFLICT: a CHECK now references a column that no longer exists in schema
		if ok, err := isCheckReferenced(sch, chk); err != nil {
			return nil, sc, err
		} else if !ok {
			// Append to conflicts
			sc.ChkConflicts = append(sc.ChkConflicts, ChkConflict{
				Kind: InvalidCheck,
				Ours: chk,
			})
		}
	}

	// Add all merged CHECKs to merged schema
	for _, chk := range mergedChks {
		sch.Checks().AddCheck(chk.Name(), chk.Expression(), chk.Enforced())
	}

	return sch, sc, nil
}

func mergeColumns(ourCC, theirCC, ancCC *schema.ColCollection) (merged *schema.ColCollection, conflicts []ColConflict, err error) {
	var common *schema.ColCollection
	common, conflicts = columnsInCommon(ourCC, theirCC, ancCC)

	ourNewCols := schema.ColCollectionSetDifference(ourCC, ancCC)
	theirNewCols := schema.ColCollectionSetDifference(theirCC, ancCC)

	// check for name conflicts between columns added on each branch since the ancestor
	_ = ourNewCols.Iter(func(tag uint64, ourCol schema.Column) (stop bool, err error) {
		theirCol, ok := theirNewCols.GetByNameCaseInsensitive(ourCol.Name)
		if ok && ourCol.Tag != theirCol.Tag {
			conflicts = append(conflicts, ColConflict{
				Kind:   NameCollision,
				Ours:   ourCol,
				Theirs: theirCol,
			})
		}
		return false, nil
	})

	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// order of args here is important for correct column ordering in sch schema
	// to be before any column in the intersection
	// TODO: sch column ordering doesn't respect sql "MODIFY ... AFTER ..." statements
	merged, err = schema.ColCollUnion(common, ourNewCols, theirNewCols)
	if err != nil {
		return nil, nil, err
	}

	return merged, conflicts, nil
}

func columnsInCommon(ourCC, theirCC, ancCC *schema.ColCollection) (common *schema.ColCollection, conflicts []ColConflict) {
	common = schema.NewColCollection()
	_ = ourCC.Iter(func(tag uint64, ourCol schema.Column) (stop bool, err error) {
		theirCol, ok := theirCC.GetByTag(ourCol.Tag)
		if !ok {
			return false, nil
		}

		if ourCol.Equals(theirCol) {
			common = common.Append(ourCol)
			return false, nil
		}

		ancCol, ok := ancCC.GetByTag(ourCol.Tag)
		if !ok {
			// col added on our branch and their branch with different def
			conflicts = append(conflicts, ColConflict{
				Kind:   TagCollision,
				Ours:   ourCol,
				Theirs: theirCol,
			})
			return false, nil
		}

		if ancCol.Equals(theirCol) {
			// col modified on our branch
			col, ok := common.GetByNameCaseInsensitive(ourCol.Name)
			if ok {
				conflicts = append(conflicts, ColConflict{
					Kind:   NameCollision,
					Ours:   ourCol,
					Theirs: col,
				})
			} else {
				common = common.Append(ourCol)
			}
			return false, nil
		}

		if ancCol.Equals(ourCol) {
			// col modified on their branch
			col, ok := common.GetByNameCaseInsensitive(theirCol.Name)
			if ok {
				conflicts = append(conflicts, ColConflict{
					Kind:   NameCollision,
					Ours:   col,
					Theirs: theirCol,
				})
			} else {
				common = common.Append(theirCol)
			}
			return false, nil
		}

		// col modified on our branch and their branch with different def
		conflicts = append(conflicts, ColConflict{
			Kind:   TagCollision,
			Ours:   ourCol,
			Theirs: theirCol,
		})
		return false, nil
	})

	return common, conflicts
}

// assumes indexes are unique over their column sets
func mergeIndexes(mergedCC *schema.ColCollection, ourSch, theirSch, ancSch schema.Schema) (merged schema.IndexCollection, conflicts []IdxConflict) {
	merged, conflicts = indexesInCommon(mergedCC, ourSch.Indexes(), theirSch.Indexes(), ancSch.Indexes())

	ourNewIdxs := indexCollSetDifference(ourSch.Indexes(), ancSch.Indexes(), mergedCC)
	theirNewIdxs := indexCollSetDifference(theirSch.Indexes(), ancSch.Indexes(), mergedCC)

	// check for conflicts between indexes added on each branch since the ancestor
	_ = ourNewIdxs.Iter(func(ourIdx schema.Index) (stop bool, err error) {
		theirIdx, ok := theirNewIdxs.GetByNameCaseInsensitive(ourIdx.Name())
		// If both indexes are exactly equal then there isn't a conflict
		if ok && !ourIdx.DeepEquals(theirIdx) {
			conflicts = append(conflicts, IdxConflict{
				Kind:   NameCollision,
				Ours:   ourIdx,
				Theirs: theirIdx,
			})
		}
		return false, nil
	})

	merged.AddIndex(ourNewIdxs.AllIndexes()...)
	merged.AddIndex(theirNewIdxs.AllIndexes()...)

	return merged, conflicts
}

func indexesInCommon(mergedCC *schema.ColCollection, ours, theirs, anc schema.IndexCollection) (common schema.IndexCollection, conflicts []IdxConflict) {
	common = schema.NewIndexCollection(mergedCC, nil)
	_ = ours.Iter(func(ourIdx schema.Index) (stop bool, err error) {
		idxTags := ourIdx.IndexedColumnTags()
		for _, t := range idxTags {
			// if column doesn't exist anymore, drop index
			// however, it shouldn't be possible for an index
			// over a dropped column to exist in the intersection
			if _, ok := mergedCC.GetByTag(t); !ok {
				return false, nil
			}
		}

		theirIdx, ok := theirs.GetIndexByTags(idxTags...)
		if !ok {
			return false, nil
		}

		if ourIdx.Equals(theirIdx) {
			common.AddIndex(ourIdx)
			return false, nil
		}

		ancIdx, ok := anc.GetIndexByTags(idxTags...)

		if !ok {
			// index added on our branch and their branch with different defs, conflict
			conflicts = append(conflicts, IdxConflict{
				Kind:   TagCollision,
				Ours:   ourIdx,
				Theirs: theirIdx,
			})
			return false, nil
		}

		if ancIdx.Equals(theirIdx) {
			// index modified on our branch
			idx, ok := common.GetByNameCaseInsensitive(ourIdx.Name())
			if ok {
				conflicts = append(conflicts, IdxConflict{
					Kind:   NameCollision,
					Ours:   ourIdx,
					Theirs: idx,
				})
			} else {
				common.AddIndex(ourIdx)
			}
			return false, nil
		}

		if ancIdx.Equals(ourIdx) {
			// index modified on their branch
			idx, ok := common.GetByNameCaseInsensitive(theirIdx.Name())
			if ok {
				conflicts = append(conflicts, IdxConflict{
					Kind:   NameCollision,
					Ours:   idx,
					Theirs: theirIdx,
				})
			} else {
				common.AddIndex(theirIdx)
			}
			return false, nil
		}

		// index modified on our branch and their branch, conflict
		conflicts = append(conflicts, IdxConflict{
			Kind:   TagCollision,
			Ours:   ourIdx,
			Theirs: theirIdx,
		})
		return false, nil
	})
	return common, conflicts
}

func indexCollSetDifference(left, right schema.IndexCollection, cc *schema.ColCollection) (d schema.IndexCollection) {
	d = schema.NewIndexCollection(cc, nil)
	_ = left.Iter(func(idx schema.Index) (stop bool, err error) {
		idxTags := idx.IndexedColumnTags()
		for _, t := range idxTags {
			// if column doesn't exist anymore, drop index
			if _, ok := cc.GetByTag(t); !ok {
				return false, nil
			}
		}

		_, ok := right.GetIndexByTags(idxTags...)
		if !ok {
			d.AddIndex(idx)
		}
		return false, nil
	})
	return d
}

// checksInCommon finds all the common checks between ourChks, theirChks, and ancChks, and detects varying conflicts
func checksInCommon(ourChks, theirChks, ancChks []schema.Check) ([]schema.Check, []ChkConflict) {
	// Make map of their checks for fast lookup
	theirChkMap := make(map[string]schema.Check)
	for _, chk := range theirChks {
		theirChkMap[chk.Name()] = chk
	}

	// Make map of ancestor checks for fast lookup
	ancChkMap := make(map[string]schema.Check)
	for _, chk := range ancChks {
		ancChkMap[chk.Name()] = chk
	}

	// Iterate over our checks
	var common []schema.Check
	var conflicts []ChkConflict
	for _, ourChk := range ourChks {
		// See if ours and theirs both have a CHECK by this name
		theirChk, ok := theirChkMap[ourChk.Name()]
		// Ours and theirs do have this CHECK in common, will be dealt with elsewhere
		if !ok {
			continue
		}

		// NO CONFLICT: our and their check are defined exactly the same
		if ourChk == theirChk {
			common = append(common, ourChk)
			continue
		}

		// See if ancestor also has this check
		ancChk, ok := ancChkMap[ourChk.Name()]
		// CONFLICT: our and their CHECK have the same name, but different definitions
		if !ok {
			conflicts = append(conflicts, ChkConflict{
				Kind:   NameCollision,
				Ours:   ourChk,
				Theirs: theirChk,
			})
			continue
		}

		// NO CONFLICT: CHECK was only modified in our branch, so update check definition with ours
		if ancChk == theirChk {
			common = append(common, ourChk)
			continue
		}

		// NO CONFLICT: CHECK was only modified in their branch, so update check definition with theirs
		if ancChk == ourChk {
			common = append(common, ourChk)
			continue
		}

		// CONFLICT: CHECK was modified on both
		conflicts = append(conflicts, ChkConflict{
			Kind:   NameCollision,
			Ours:   ourChk,
			Theirs: theirChk,
		})
	}

	return common, conflicts
}

// chkCollectionSetDifference returns the set difference left - right.
func chkCollectionSetDifference(left, right []schema.Check) []schema.Check {
	// Make map of right check for fast look up
	rChkMap := make(map[string]bool)
	for _, chk := range right {
		rChkMap[chk.Name()] = true
	}

	// Add everything except what's in right
	var result []schema.Check
	for _, chk := range left {
		if _, ok := rChkMap[chk.Name()]; ok {
			continue
		}
		result = append(result, chk)
	}
	return result
}

// chkCollectionSetIntersection returns the set union of left and right.
func chkCollectionSetIntersection(left, right []schema.Check) []schema.Check {
	// Make map of right check for fast look up
	rChkMap := make(map[string]bool)
	for _, chk := range right {
		rChkMap[chk.Name()] = true
	}

	// Add everything from left that is also in right
	var result []schema.Check
	for _, chk := range left {
		if _, ok := rChkMap[chk.Name()]; !ok {
			continue
		}
		result = append(result, chk)
	}
	return result
}

// chkCollectionModified finds all checks that have been modified from ancestor to child
func chkCollectionModified(anc, child []schema.Check) []schema.Check {
	// Make map of ancestor for fast look up
	ancChkMap := make(map[string]schema.Check)
	for _, chk := range anc {
		ancChkMap[chk.Name()] = chk
	}

	// Add everything with same name, but different definition
	var result []schema.Check
	for _, childChk := range child {
		if ancChk, ok := ancChkMap[childChk.Name()]; ok {
			if ancChk != childChk {
				result = append(result, childChk)
			}
		}
	}
	return result
}

// mergeChecks attempts to combine ourChks, theirChks, and ancChks into a single collection, or gathers the conflicts
func mergeChecks(ourChks, theirChks, ancChks schema.CheckCollection) ([]schema.Check, []ChkConflict, error) {
	// Handles modifications
	common, conflicts := checksInCommon(ourChks.AllChecks(), theirChks.AllChecks(), ancChks.AllChecks())

	// Get all new checks
	ourNewChks := chkCollectionSetDifference(ourChks.AllChecks(), ancChks.AllChecks())
	theirNewChks := chkCollectionSetDifference(theirChks.AllChecks(), ancChks.AllChecks())

	// Create map for fast lookup
	theirNewChksMap := make(map[string]schema.Check)
	for _, chk := range theirNewChks {
		theirNewChksMap[chk.Name()] = chk
	}

	// Compare CHECKs with the same name
	for _, ourChk := range ourNewChks {
		theirChk, ok := theirNewChksMap[ourChk.Name()]
		// CONFLICT: our and their CHECK have the same name, but different definitions
		if ok && ourChk != theirChk {
			conflicts = append(conflicts, ChkConflict{
				Kind:   NameCollision,
				Ours:   ourChk,
				Theirs: theirChk,
			})
		}
	}

	// There are conflicts, don't merge
	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// Create a map from each column to any CHECKs that reference that column
	theirNewChkColsMap := make(map[string]map[schema.Check]bool)
	for _, chk := range theirNewChks {
		// Extract columns referenced by CHECK
		chkDef := sql.CheckDefinition{
			Name:            chk.Name(),
			CheckExpression: chk.Expression(),
			Enforced:        chk.Enforced(),
		}
		colNames, err := sqle.ColumnsFromCheckDefinition(nil, &chkDef)
		if err != nil {
			return nil, nil, err
		}

		// Mark that col as referenced by CHECK
		for _, col := range colNames {
			if _, ok := theirNewChkColsMap[col]; !ok {
				theirNewChkColsMap[col] = make(map[schema.Check]bool)
			}
			theirNewChkColsMap[col][chk] = true
		}
	}

	// Look for overlapping columns between our new CHECKs and their new CHECKs
	for _, ourChk := range ourNewChks {
		// Extract columns referenced by CHECK
		chkDef := sql.CheckDefinition{
			Name:            ourChk.Name(),
			CheckExpression: ourChk.Expression(),
			Enforced:        ourChk.Enforced(),
		}
		colNames, err := sqle.ColumnsFromCheckDefinition(nil, &chkDef)
		if err != nil {
			return nil, nil, err
		}

		// TODO: redundant for checks that are defined exactly the same
		for _, col := range colNames {
			// See if this column is referenced in their new CHECKs
			if _, ok := theirNewChkColsMap[col]; ok {
				// CONFLICT: our and their CHECK reference the same column and are not the same CHECK
				if _, ok := theirNewChkColsMap[col][ourChk]; !ok {
					for k := range theirNewChkColsMap[col] {
						conflicts = append(conflicts, ChkConflict{
							Kind:   ColumnCollision,
							Ours:   ourChk,
							Theirs: k,
						})
					}
					// Finding one column collision is enough
					break
				}
			}
		}
	}

	// There are conflicts, don't merge
	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// CONFLICT: deleted constraint in ours that is modified in theirs
	ourDeletedChks := chkCollectionSetDifference(ancChks.AllChecks(), ourChks.AllChecks())
	theirModifiedChks := chkCollectionModified(ancChks.AllChecks(), theirChks.AllChecks())
	deletedInOursButModifiedInTheirs := chkCollectionSetIntersection(theirModifiedChks, ourDeletedChks)
	for _, chk := range deletedInOursButModifiedInTheirs {
		conflicts = append(conflicts, ChkConflict{
			Kind:   DeletedCheck,
			Theirs: chk,
		})
	}

	// CONFLICT: deleted constraint in theirs that is modified in ours
	theirDeletedChks := chkCollectionSetDifference(ancChks.AllChecks(), theirChks.AllChecks())
	ourModifiedChks := chkCollectionModified(ancChks.AllChecks(), ourChks.AllChecks())
	deletedInTheirsButModifiedInOurs := chkCollectionSetIntersection(ourModifiedChks, theirDeletedChks)
	for _, chk := range deletedInTheirsButModifiedInOurs {
		conflicts = append(conflicts, ChkConflict{
			Kind: DeletedCheck,
			Ours: chk,
		})
	}

	// There are conflicts, don't merge
	if len(conflicts) > 0 {
		return nil, conflicts, nil
	}

	// Create map to track names
	var allChecks []schema.Check
	allNames := make(map[string]bool)

	// Combine all checks into one collection
	for _, chk := range common {
		if _, ok := allNames[chk.Name()]; !ok {
			allNames[chk.Name()] = true
			allChecks = append(allChecks, chk)
		}
	}
	for _, chk := range ourNewChks {
		if _, ok := allNames[chk.Name()]; !ok {
			allNames[chk.Name()] = true
			allChecks = append(allChecks, chk)
		}
	}
	for _, chk := range theirNewChks {
		if _, ok := allNames[chk.Name()]; !ok {
			allNames[chk.Name()] = true
			allChecks = append(allChecks, chk)
		}
	}

	return allChecks, conflicts, nil
}

// isCheckReferenced determine if columns referenced in check are in schema
func isCheckReferenced(sch schema.Schema, chk schema.Check) (bool, error) {
	chkDef := sql.CheckDefinition{
		Name:            chk.Name(),
		CheckExpression: chk.Expression(),
		Enforced:        chk.Enforced(),
	}
	colNames, err := sqle.ColumnsFromCheckDefinition(sql.NewEmptyContext(), &chkDef)
	if err != nil {
		return false, err
	}

	for _, col := range colNames {
		_, ok := sch.GetAllCols().GetByName(col)
		if !ok {
			return false, nil
		}
	}

	return true, nil
}
