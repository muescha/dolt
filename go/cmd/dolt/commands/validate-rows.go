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
	"github.com/dolthub/dolt/go/store/types"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type ValidateRowsCmd struct{}

func (cmd ValidateRowsCmd) Name() string {
	return "validate-rows"
}

func (cmd ValidateRowsCmd) Description() string {
	return ""
}

func (cmd ValidateRowsCmd) RequiresRepo() bool {
	return true
}

func (cmd ValidateRowsCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd ValidateRowsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// Exec executes the command
func (cmd ValidateRowsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreatePullArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, pullDocs, ap))

	var verr errhand.VerboseError
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 1 {
		verr = errhand.VerboseErrorFromError(errors.New("validate-rows takes exactly one argument"))
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

	cli.Println("validating rows for table: ", table)

	err = validateTableData(ctx, t, func(msg string) { cli.PrintErrln(msg) })
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

func validateTableData(ctx context.Context, t *doltdb.Table, cb func(msg string)) error {
	rows, err := t.GetRowData(ctx)
	if err != nil {
		return err
	}

	m := durable.NomsMapFromIndex(rows)
	iter, err := m.IteratorAt(ctx, 0)
	if err != nil {
		return err
	}

	prev, _, err := iter.Next(ctx)
	if err != nil {
		return err
	}

	for {
		var k types.Value
		k, _, err = iter.Next(ctx)
		if err == io.EOF || k == nil {
			break
		} else if err != nil {
			return err
		}

		var less bool
		less, err = k.Less(m.Format(), prev)
		if err != nil {
			return err
		} else if less {
			cb(fmt.Sprintf("misordered keys (%s < %s)",
				k.HumanReadableString(), prev.HumanReadableString()))
		}
		prev = k
	}
	return nil
}
