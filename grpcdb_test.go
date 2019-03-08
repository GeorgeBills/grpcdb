package grpcdb_test

import (
	"github.com/GeorgeBills/grpcdb"
	"testing"
)

func TestTranslation(t *testing.T) {
	table := []struct {
		sql       string
		statement *grpcdb.Statement
	}{
		{
			"SELECT * FROM mytable1",
			&grpcdb.Statement{
				Statement: &grpcdb.Statement_Select{
					Select: &grpcdb.Select{
						ResultColumn: []string{"*"},
						From:         "mytable1",
					},
				},
			},
		},
		{
			"SELECT a, b FROM mytable2",
			&grpcdb.Statement{
				Statement: &grpcdb.Statement_Select{
					Select: &grpcdb.Select{
						ResultColumn: []string{"a", "b"},
						From:         "mytable2",
					},
				},
			},
		},
	}
	for _, tt := range table {
		t.Run(tt.sql, func(t *testing.T) {
			result, err := grpcdb.Translate(tt.statement)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.sql {
				t.Errorf("Expected result '%s' for statement %+v to be '%s'", result, tt.statement, tt.sql)
			}
		})
	}
}
