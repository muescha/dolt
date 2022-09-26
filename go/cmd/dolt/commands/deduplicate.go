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
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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

	table := apr.Arg(0)
	cli.Println("deduplicating clustered index for table: ", table)

	if err := deduplicateClusteredIndex(ctx, dEnv, table); err != nil {
		verr = errhand.VerboseErrorFromError(err)
	}
	return HandleVErrAndExitCode(verr, usage)
}

func deduplicateClusteredIndex(ctx context.Context, dEnv *env.DoltEnv, table string) error {
	return nil
}
