package driver

import (
	"database/sql/driver"
	"math/big"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestConvertBaseMachinaUnsupportedValueToString(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		value      driver.Value
		wantString string
		wantBool   bool
	}{
		"int": {
			value:      123,
			wantString: "",
			wantBool:   false,
		},
		"big.Rat": {
			value:      big.NewRat(1, 2),
			wantString: "1/2",
			wantBool:   true,
		},
		"bigquery.IntervalValue": {
			value: &bigquery.IntervalValue{
				Months:         1,
				Days:           2,
				SubSecondNanos: 3000000,
			},
			wantString: "0-1 2 0:0:0.003",
			wantBool:   true,
		},
		"bigquery.Value slice": {
			value:      []bigquery.Value{"a", "b"},
			wantString: "<ARRAY or STRUCT>",
			wantBool:   true,
		},
		"bigquery.RangeValue": {
			value: &bigquery.RangeValue{
				Start: "2023-01-01",
				End:   "2023-12-31",
			},
			wantString: "2023-01-01,2023-12-31",
			wantBool:   true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			str, ok := convertBaseMachinaUnsupportedValueToString(tt.value)
			if str != tt.wantString {
				t.Errorf("Expected string %q, but got %q", tt.wantString, str)
			}
			if ok != tt.wantBool {
				t.Errorf("Expected bool %v, but got %v", tt.wantBool, ok)
			}
		})
	}
}
