package driver

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	rows := &bigQueryRows{
		schema: bigQueryColumns{
			types: []bigquery.FieldType{
				bigquery.StringFieldType,
				bigquery.NumericFieldType,
			},
		},
	}

	testCases := map[int]string{
		0: "STRING",
		1: "NUMERIC",
		2: OutOfRangeErrorTypeName,
	}
	for index, expected := range testCases {
		assert.Equal(t, expected, rows.ColumnTypeDatabaseTypeName(index))
	}
}
