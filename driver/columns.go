package driver

import (
	"database/sql/driver"
	"encoding/json"

	"cloud.google.com/go/bigquery"
	"github.com/basemachina/go-bigquery/adaptor"
)

type bigQuerySchema interface {
	ColumnNames() []string
	ConvertColumnValue(index int, value bigquery.Value) (driver.Value, error)
	columnTypes() []bigquery.FieldType
	RequiredFlags() []bool
}

type bigQueryColumns struct {
	names         []string
	columns       []bigQueryColumn
	types         []bigquery.FieldType
	requiredFlags []bool
}

func (columns bigQueryColumns) ConvertColumnValue(index int, value bigquery.Value) (driver.Value, error) {
	if index > -1 && len(columns.columns) > index {
		column := columns.columns[index]
		return column.ConvertValue(value)
	}

	return value, nil
}

func (columns bigQueryColumns) ColumnNames() []string {
	return columns.names
}

func (columns bigQueryColumns) columnTypes() []bigquery.FieldType {
	return columns.types
}

func (columns bigQueryColumns) RequiredFlags() []bool {
	return columns.requiredFlags
}

type bigQueryReroutedColumn struct {
	values []bigquery.Value
	schema bigquery.Schema
}

func (c bigQueryReroutedColumn) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.values)
}

type bigQueryColumn struct {
	Name    string
	Schema  bigquery.Schema
	Adaptor adaptor.SchemaColumnAdaptor
}

func (column bigQueryColumn) ConvertValue(value bigquery.Value) (driver.Value, error) {

	if len(column.Schema) == 0 {
		return value, nil
	}

	values, ok := value.([]bigquery.Value)
	if ok {

		if len(values) > 0 {
			if _, isRows := values[0].([]bigquery.Value); !isRows {
				values = []bigquery.Value{values}
			}
		}

		value = bigQueryReroutedColumn{values: values, schema: column.Schema}
	}

	if columnAdaptor := column.Adaptor; columnAdaptor != nil {
		return columnAdaptor.AdaptValue(value)
	}

	return value, nil
}

func createBigQuerySchema(schema bigquery.Schema, schemaAdaptor adaptor.SchemaAdaptor) bigQuerySchema {
	var names []string
	var columns []bigQueryColumn
	var types []bigquery.FieldType
	var requiredFlags []bool
	for _, column := range schema {

		name := column.Name

		var columnAdaptor adaptor.SchemaColumnAdaptor
		if schemaAdaptor != nil {
			columnAdaptor = schemaAdaptor.GetColumnAdaptor(name)
		}

		names = append(names, name)
		columns = append(columns, bigQueryColumn{
			Name:    name,
			Schema:  column.Schema,
			Adaptor: columnAdaptor,
		})
		if column.Repeated {
			types = append(types, "ARRAY")
		} else {
			types = append(types, column.Type)
		}
		requiredFlags = append(requiredFlags, column.Required)
	}
	return &bigQueryColumns{
		names,
		columns,
		types,
		requiredFlags,
	}
}
