package driver

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	rows := &bigQueryRows{
		schema: bigQueryColumns{
			types: []string{
				string(bigquery.StringFieldType),
				string(bigquery.NumericFieldType),
			},
		},
	}

	testCases := map[int]string{
		0: "STRING",
		1: "NUMERIC",
		2: outOfRangeErrorTypeName,
	}
	for index, expected := range testCases {
		assert.Equal(t, expected, rows.ColumnTypeDatabaseTypeName(index))
	}
}
