package driver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"

	"cloud.google.com/go/bigquery"
	"github.com/basemachina/go-bigquery/adaptor"
	"google.golang.org/api/iterator"
)

type bigQueryRows struct {
	source  bigQuerySource
	schema  bigQuerySchema
	adaptor adaptor.SchemaAdaptor
}

func (rows *bigQueryRows) ensureSchema() {
	if rows.schema == nil {
		rows.schema = rows.source.GetSchema()
	}
}

func (rows *bigQueryRows) Columns() []string {
	rows.ensureSchema()
	return rows.schema.ColumnNames()
}

func (rows *bigQueryRows) Close() error {
	return nil
}

func (rows *bigQueryRows) Next(dest []driver.Value) error {

	rows.ensureSchema()

	values, err := rows.source.Next()
	if err == iterator.Done {
		return io.EOF
	}

	if err != nil {
		return err
	}

	var length = len(values)
	for i := range dest {
		if i < length {
			value := values[i]

			if str, ok := convertBaseMachinaUnsupportedValueToString(value); ok {
				dest[i] = str
				continue
			}

			dest[i], err = rows.schema.ConvertColumnValue(i, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// convertBaseMachinaUnsupportedValueToString converts values that are not supported by BaseMachina to strings.
// It returns a string that represents the value and a boolean indicating if the conversion was successful.
// If the conversion was not successful, the string is empty and the boolean is false.
func convertBaseMachinaUnsupportedValueToString(value driver.Value) (string, bool) {
	switch value := value.(type) {
	// NUMERIC, BIGNUMERIC type
	case *big.Rat:
		return value.String(), true
	// INTERVAL type
	case *bigquery.IntervalValue:
		return value.String(), true
	// ARRAY or STRUCT type
	case []bigquery.Value:
		return "<ARRAY or STRUCT>", true
	// RANGE type
	case *bigquery.RangeValue:
		return fmt.Sprintf("%v,%v", value.Start, value.End), true
	}
	return "", false
}

var _ driver.RowsColumnTypeDatabaseTypeName = (*bigQueryRows)(nil)

func (rows *bigQueryRows) ColumnTypeDatabaseTypeName(index int) string {
	types := rows.schema.columnTypes()
	return types[index]
}

var _ driver.RowsColumnTypeNullable = (*bigQueryRows)(nil)

func (rows *bigQueryRows) ColumnTypeNullable(index int) (bool, bool) {
	requiredFlags := rows.schema.RequiredFlags()
	return !requiredFlags[index], true
}
