// Copyright 2021 Dolthub, Inc.
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

package parquet

import (
	"context"
	"github.com/apache/arrow/go/v9/parquet"
	"github.com/apache/arrow/go/v9/parquet/file"
	"github.com/apache/arrow/go/v9/parquet/metadata"
	sch "github.com/apache/arrow/go/v9/parquet/schema"
	"gopkg.in/src-d/go-errors.v1"
	"io"
	"reflect"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
)

type ParquetWriter struct {
	filewriter *file.Writer
	//pwriter    *writer.CSVWriter
	sch schema.Schema
}

var typeMap = map[typeinfo.Identifier]string{
	typeinfo.DatetimeTypeIdentifier:   "type=INT64, convertedtype=TIME_MICROS",
	typeinfo.DecimalTypeIdentifier:    "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.EnumTypeIdentifier:       "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.InlineBlobTypeIdentifier: "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.SetTypeIdentifier:        "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.TimeTypeIdentifier:       "type=INT64, convertedtype=TIME_MICROS",
	typeinfo.TupleTypeIdentifier:      "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.UuidTypeIdentifier:       "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.VarBinaryTypeIdentifier:  "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.YearTypeIdentifier:       "type=INT32, convertedtype=DATE",
	typeinfo.UnknownTypeIdentifier:    "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.JSONTypeIdentifier:       "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.BlobStringTypeIdentifier: "type=BYTE_ARRAY, convertedtype=UTF8",

	typeinfo.BitTypeIdentifier:       "type=INT32, convertedtype=INT_16",
	typeinfo.BoolTypeIdentifier:      "type=BOOLEAN",
	typeinfo.VarStringTypeIdentifier: "type=BYTE_ARRAY, convertedtype=UTF8",
	typeinfo.UintTypeIdentifier:      "type=INT64, convertedtype=UINT_64",
	typeinfo.IntTypeIdentifier:       "type=INT64, convertedtype=INT_64",
	typeinfo.FloatTypeIdentifier:     "type=DOUBLE",
}

func NewParquetWriter(wr io.WriteCloser, outSch schema.Schema, destName string) (*ParquetWriter, error) {
	props := parquet.NewWriterProperties()

	pqschema, err := DoltToParquet(outSch, props)
	if err != nil {
		return nil, err
	}

	meta := make(metadata.KeyValueMetadata, 0)

	schemaNode := pqschema.Root()
	baseWriter := file.NewParquetWriter(wr, schemaNode, file.WithWriterProps(props), file.WithWriteMetadata(meta))

	// pw.CompressionType defaults to parquet.CompressionCodec_SNAPPY
	return &ParquetWriter{filewriter: baseWriter, sch: outSch}, nil
}

func (pwr *ParquetWriter) GetSchema() schema.Schema {
	return pwr.sch
}

// WriteRow will write a row to a table
func (pwr *ParquetWriter) WriteRow(ctx context.Context, r row.Row) error {
	sqlRow, err := sqlutil.DoltRowToSqlRow(r, pwr.GetSchema())
	if err != nil {
		return err
	}
	return pwr.WriteSqlRow(ctx, sqlRow)
}

func (pwr *ParquetWriter) WriteSqlRow(ctx context.Context, r sql.Row) error {
	colValStrs := make([]*string, pwr.sch.GetAllCols().Size())

	for i, val := range r {
		colT := pwr.sch.GetAllCols().GetByIndex(i)
		if val == nil {
			colValStrs[i] = nil
		} else {
			sqlType := colT.TypeInfo.ToSqlType()
			// convert datetime and time types to int64
			switch colT.TypeInfo.GetTypeIdentifier() {
			case typeinfo.DatetimeTypeIdentifier:
				val = val.(time.Time).Unix()
				sqlType = sql.Int64
			case typeinfo.TimeTypeIdentifier:
				val = int64(val.(sql.Timespan))
				sqlType = sql.Int64
			case typeinfo.BitTypeIdentifier:
				sqlType = sql.Uint64
			}
			v, err := sqlutil.SqlColToStr(sqlType, val)
			if err != nil {
				return err
			}
			colValStrs[i] = &v
		}
	}

	err := pwr.pwriter.WriteString(colValStrs)
	if err != nil {
		return err
	}
	return nil
}

// Close should flush all writes, release resources being held
func (pwr *ParquetWriter) Close(ctx context.Context) error {
	// WriteStop writes footer, stops writing and flushes
	err := pwr.pwriter.WriteStop()
	if err != nil {
		return err
	}
	pwr.filewriter.Close()
	return nil
}

var typeToParquetTypeMap = map[reflect.Type]parquet.Type{
	reflect.TypeOf(true):                        parquet.Types.Boolean,
	reflect.TypeOf(int32(0)):                    parquet.Types.Int32,
	reflect.TypeOf(int64(0)):                    parquet.Types.Int64,
	reflect.TypeOf(float32(0)):                  parquet.Types.Float,
	reflect.TypeOf(float64(0)):                  parquet.Types.Double,
	reflect.TypeOf(parquet.ByteArray{}):         parquet.Types.ByteArray,
	reflect.TypeOf(parquet.Int96{}):             parquet.Types.Int96,
	reflect.TypeOf(parquet.FixedLenByteArray{}): parquet.Types.FixedLenByteArray,
}

func TypeToParquetType(typ reflect.Type) parquet.Type {
	ret, ok := typeToParquetTypeMap[typ]
	if !ok {
		panic("invalid type for parquet type")
	}
	return ret
}

var typeInfoToParquetTypeMap = map[typeinfo.TypeInfo]parquet.Type{}

func TypeInfoToParquetType(col schema.Column) (*sch.PrimitiveNode, error) {
	var (
		logicalType sch.LogicalType = sch.NoLogicalType{}
		typ         parquet.Type
		repType     = parquet.Repetitions.Optional
		length      = -1
		//precision   = -1
		//scale       = -1
		err error
	)

	switch col.TypeInfo {
	case typeinfo.UnknownType:
		typ = parquet.Types.Int32
		logicalType = &sch.NullLogicalType{}
		if !col.IsNullable() {
			// TODO : can there be null column type?
			err = errors.NewKind("NullType col must be nullable").New()
		}
	case typeinfo.BoolType:
		typ = parquet.Types.Boolean
	case typeinfo.Int8Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(8, true)
	case typeinfo.Int16Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(16, true)
	case typeinfo.Int24Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(24, true)
	case typeinfo.Int32Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(32, true)
	case typeinfo.Int64Type:
		typ = parquet.Types.Int64
		logicalType = sch.NewIntLogicalType(64, true)
	case typeinfo.Uint8Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(8, false)
	case typeinfo.Uint16Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(16, false)
	case typeinfo.Uint24Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(24, false)
	case typeinfo.Uint32Type:
		typ = parquet.Types.Int32
		logicalType = sch.NewIntLogicalType(24, false)
	case typeinfo.Uint64Type:
		typ = parquet.Types.Int64
		logicalType = sch.NewIntLogicalType(64, false)
	case typeinfo.Float32Type:
		typ = parquet.Types.Float
	case typeinfo.Float64Type:
		typ = parquet.Types.Double
	case typeinfo.StringDefaultType:
		logicalType = sch.StringLogicalType{}
		switch col.TypeInfo.GetTypeIdentifier() {
		case typeinfo.VarStringTypeIdentifier:
			typ = parquet.Types.FixedLenByteArray
			//length = field.Type.(*arrow.FixedSizeBinaryType).ByteWidth
		case typeinfo.VarBinaryTypeIdentifier:
			typ = parquet.Types.ByteArray
		}
	//case typeinfo.DecimalType:
	//	typ = parquet.Types.FixedLenByteArray
	//	dectype := field.Type.(*arrow.Decimal128Type)
	//	precision = int(dectype.Precision)
	//	scale = int(dectype.Scale)
	//	length = int(DecimalSize(int32(precision)))
	//	logicalType = schema.NewDecimalLogicalType(int32(precision), int32(scale))
	case typeinfo.TimestampType:
		typ = parquet.Types.Int64
		logicalType = sch.NewTimestampLogicalType(true, sch.TimeUnitMillis)
	case typeinfo.DateType:
		typ = parquet.Types.Int32
		logicalType = sch.DateLogicalType{}
	case typeinfo.TimeType:
		typ = parquet.Types.Int64
		logicalType = sch.NewTimestampLogicalType(true, sch.TimeUnitMillis)
	case typeinfo.DatetimeType:
		typ = parquet.Types.Int64
		logicalType = sch.NewTimestampLogicalType(true, sch.TimeUnitMillis)
	case typeinfo.YearType:
		typ = parquet.Types.Int32
		logicalType = sch.DateLogicalType{}
	default:
		err = errors.NewKind("not implemented yet").New()
	}

	if err != nil {
		return nil, err
	}
	return sch.NewPrimitiveNodeLogical(col.Name, repType, logicalType, typ, length, -1)
}

func DoltToParquet(doltSchema schema.Schema, props *parquet.WriterProperties) (*sch.Schema, error) {
	if props == nil {
		props = parquet.NewWriterProperties()
	}

	columns := doltSchema.GetAllCols().GetColumns()
	nodes := make([]sch.Node, len(columns))

	//var repType parquet.Repetition

	for _, c := range columns {
		//repType = parquet.Repetitions.Undefined
		//colType := col.TypeInfo.GetTypeIdentifier()
		//if col.IsNullable() {
		//	repType = parquet.Repetitions.Optional
		//}
		n, err := TypeInfoToParquetType(c)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	root, err := sch.NewGroupNode("schema", parquet.Repetitions.Repeated, nodes, -1)
	if err != nil {
		return nil, err
	}

	return sch.NewSchema(root), err
}
