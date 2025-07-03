package driver

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

func Test_buildParameter(t *testing.T) {
	t.Parallel()

	var floatVal float64 = 3.14
	var nilFloat64 *float64

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
		"nil float64 pointer": {
			arg:  nilFloat64,
			want: bigquery.QueryParameter{Name: "", Value: bigquery.NullFloat64{Valid: false, Float64: 0}},
		},
		"named nil float64 pointer": {
			arg:  driver.NamedValue{Name: "param", Value: nilFloat64},
			want: bigquery.QueryParameter{Name: "param", Value: bigquery.NullFloat64{Valid: false, Float64: 0}},
		},
		"non-nil float64 pointer": {
			arg:  &floatVal,
			want: bigquery.QueryParameter{Name: "", Value: &floatVal},
		},
		"named non-nil float64 pointer": {
			arg:  driver.NamedValue{Name: "param", Value: &floatVal},
			want: bigquery.QueryParameter{Name: "param", Value: &floatVal},
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
