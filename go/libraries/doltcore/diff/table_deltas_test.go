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

package diff

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

var sch = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk", 1, types.StringKind, true),
))
var sch2 = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk2", 1, types.StringKind, false),
))
var sch3 = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn("pk3", 1, types.IntKind, true),
))

func TestMatchTableDeltas(t *testing.T) {
	var fromDeltas = []TableDelta{
		{FromName: "name_match", FromSch: sch},
		{FromName: "keyless_name_match", FromSch: sch2},
		{FromName: "from_no_match", FromSch: sch},
		{FromName: "no_match", FromSch: sch},
	}
	var toDeltas = []TableDelta{
		{ToName: "name_match", ToSch: sch},
		{ToName: "keyless_name_match", ToSch: sch2},
		{ToName: "to_no_match", ToSch: sch},
		{ToName: "no_match", ToSch: sch3},
	}
	expected := []TableDelta{
		{FromName: "name_match", ToName: "name_match", FromSch: sch, ToSch: sch},
		{FromName: "keyless_name_match", ToName: "keyless_name_match", FromSch: sch2, ToSch: sch2},
		{FromName: "from_no_match", FromSch: sch},
		{FromName: "no_match", FromSch: sch},
		{ToName: "to_no_match", ToSch: sch},
		{ToName: "no_match", ToSch: sch3},
	}

	for i := 0; i < 100; i++ {
		received := matchTableDeltas(fromDeltas, toDeltas)
		require.ElementsMatch(t, expected, received)
	}
}
