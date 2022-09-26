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

package commands

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

type DedupeCmd struct{}

func (cmd DedupeCmd) Name() string {
	return "dedupe"
}

func (cmd DedupeCmd) Description() string {
	return ""
}

func (cmd DedupeCmd) RequiresRepo() bool {
	return true
}

func (cmd DedupeCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd DedupeCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd DedupeCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreatePullArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, pullDocs, ap))

	var verr errhand.VerboseError
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 1 {
		verr = errhand.VerboseErrorFromError(errors.New("dedupe takes exactly one argument"))
		return HandleVErrAndExitCode(verr, usage)
	}

	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		verr = errhand.VerboseErrorFromError(err)
		return HandleVErrAndExitCode(verr, usage)
	}
	root := ws.WorkingRoot()

	table := apr.Arg(0)
	t, ok, err := root.GetTable(ctx, table)
	if err == nil && !ok {
		err = fmt.Errorf("table %s does not exist in working root", table)
	}
	if err != nil {
		verr = errhand.VerboseErrorFromError(err)
		return HandleVErrAndExitCode(verr, usage)
	}

	cli.Println("deduplicating primary keys for table: ", table)

	t, err = deduplicateTableData(ctx, t)
	if err != nil {
		verr = errhand.VerboseErrorFromError(err)
		return HandleVErrAndExitCode(verr, usage)
	}

	root, err = root.PutTable(ctx, table, t)
	if err != nil {
		verr = errhand.VerboseErrorFromError(err)
		return HandleVErrAndExitCode(verr, usage)
	}

	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(root))
	if err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}
	return HandleVErrAndExitCode(verr, usage)
}

func deduplicateTableData(ctx context.Context, t *doltdb.Table) (*doltdb.Table, error) {
	rows, err := t.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	m := durable.ProllyMapFromIndex(rows)
	m, err = deduplicateIndexData(ctx, m)
	if err != nil {
		return nil, err
	}

	t, err = t.UpdateRows(ctx, durable.IndexFromProllyMap(m))
	if err != nil {
		return nil, err
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	for _, def := range sch.Indexes().AllIndexes() {
		idx, err := t.GetIndexRowData(ctx, def.Name())
		if err != nil {
			return nil, err
		}

		m := durable.ProllyMapFromIndex(idx)
		m, err = deduplicateIndexData(ctx, m)
		if err != nil {
			return nil, err
		}

		t, err = t.SetIndexRows(ctx, def.Name(), durable.IndexFromProllyMap(m))
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

type tupleSlice struct {
	edits []val.Tuple
}

var _ prolly.TupleIter = &tupleSlice{}

func (ts *tupleSlice) Next(ctx context.Context) (k, v val.Tuple) {
	if len(ts.edits) > 0 {
		k, v = ts.edits[0], nil
		ts.edits = ts.edits[1:]
	}
	return
}

func deduplicateIndexData(ctx context.Context, m prolly.Map) (prolly.Map, error) {
	iter, err := m.IterAll(ctx)
	if err != nil {
		return prolly.Map{}, err
	}
	desc, _ := m.Descriptors()

	var key, prev val.Tuple
	dupes := make([]val.Tuple, 0, 65536)

	for {
		key, _, err = iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return prolly.Map{}, err
		}
		if prev == nil {
			prev = key
			continue
		}

		if desc.Compare(prev, key) == 0 {
			dupes = append(dupes, prev)
		}
		prev = key

		// todo: batching
	}

	return prolly.MutateMapWithTupleIter(ctx, m, &tupleSlice{edits: dupes})
}
