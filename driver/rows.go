package driver

import (
	"database/sql/driver"
	"google.golang.org/api/iterator"
	"gorm.io/driver/bigquery/adaptor"
	"io"
)

const outOfRangeErrorTypeName = "ERR_OUT_OF_RANGE"

type bigQueryRows struct {
	source  bigQuerySource
	schema  bigQuerySchema
	adaptor adaptor.SchemaAdaptor
}

var _ driver.RowsColumnTypeDatabaseTypeName = &bigQueryRows{}

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
			dest[i], err = rows.schema.ConvertColumnValue(i, values[i])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (rows *bigQueryRows) ColumnTypeDatabaseTypeName(index int) string {
	types := rows.schema.ColumnTypes()
	if index >= len(types) {
		return outOfRangeErrorTypeName
	}
	return types[index]
}
