package driver

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	t.Parallel()

	rows := &bigQueryRows{
		schema: createBigQuerySchema(
			bigquery.Schema{
				{Name: "string", Type: bigquery.StringFieldType, Repeated: false},
				{Name: "numeric", Type: bigquery.NumericFieldType, Repeated: false},
				{Name: "boolean", Type: bigquery.BooleanFieldType, Repeated: false},
				{Name: "array_of_string", Type: bigquery.StringFieldType, Repeated: true},
			},
			nil,
		),
	}

	testCases := map[int]string{
		0: "STRING",
		1: "NUMERIC",
		2: "BOOLEAN",
		3: "ARRAY",
	}

	for index, want := range testCases {
		t.Run(fmt.Sprintf("column %d: %s", index, want), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, want, rows.ColumnTypeDatabaseTypeName(index))
		})
	}
}
