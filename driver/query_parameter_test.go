package driver

import (
	"testing"

	"github.com/goccy/go-zetasql"
)

func Test_countQueryParameters(t *testing.T) {
	testCases := map[string]struct {
		sql  string
		want int
	}{
		"positional query parameters": {
			sql:  "SELECT * FROM users WHERE id = ? AND age = ?",
			want: 2,
		},
		"named query parameters": {
			sql:  "SELECT * FROM users WHERE id = @id AND name = @name",
			want: 2,
		},
		// ZetaSQLの仕様上、positinal query parametersとnamed query parametersを同時に使うことはできないため、
		// 両方を使ったSQLはテストしない。
		// > Named query parameters can't be used alongside positional query parameters.
		// https://github.com/google/zetasql/blob/94ff7f5f95b42218193b61184b8797d6ae527004/docs/lexical.md#named-query-parameters
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			node, err := zetasql.ParseStatement(tc.sql, nil)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			count := countQueryParameters(node)
			if count != tc.want {
				t.Errorf("expected %d query parameters, got %d", tc.want, count)
			}
		})
	}
}
