package driver

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

func Test_buildParameter(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		arg  driver.Value
		want bigquery.QueryParameter
	}{
		"simple value": {
			arg:  123,
			want: bigquery.QueryParameter{Name: "", Value: 123},
		},
		"string value": {
			arg:  "hello",
			want: bigquery.QueryParameter{Name: "", Value: "hello"},
		},
		"named value": {
			arg:  driver.NamedValue{Name: "param", Value: "hello"},
			want: bigquery.QueryParameter{Name: "param", Value: "hello"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := buildParameter(tt.arg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildParameter() = %v, want %v", got, tt.want)
			}
		})
	}
}
