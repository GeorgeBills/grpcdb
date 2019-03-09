package grpcdb_test

import (
	"github.com/GeorgeBills/grpcdb"
	"testing"
)

func TestTranslation(t *testing.T) {
	table := []struct {
		sql              string
		statementBuilder *grpcdb.StatementBuilder
	}{
		{
			"SELECT * FROM mytable1",
			grpcdb.
				NewSelect("mytable1", "*"),
		},
		{
			"SELECT a, b FROM mytable2",
			grpcdb.
				NewSelect("mytable2", "a", "b"),
		},
		{
			"SELECT a FROM t WHERE x > 3",
			grpcdb.
				NewSelect("t", "a").
				AddWhere(grpcdb.NewBinaryExpression(grpcdb.NewColumn("x"), grpcdb.NewLiteral("3"), grpcdb.BinaryOp_GT)),
		},
		{
			"SELECT x FROM t1 JOIN t2 ON t1.y = t2.z",
			grpcdb.
				NewSelect("t1", "x").
				AddJoinEq("t2", grpcdb.NewTableColumn("t1", "y"), grpcdb.NewTableColumn("t2", "z")),
		},
	}
	for _, tt := range table {
		t.Run(tt.sql, func(t *testing.T) {
			statement, err := tt.statementBuilder.Statement()
			if err != nil {
				t.Fatalf("Couldn't build statement: %v", err)
			}
			result, err := grpcdb.TranslateStatement(statement)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if result != tt.sql {
				t.Errorf("Expected result '%s' for statement %+v to be '%s'", result, statement, tt.sql)
			}
		})
	}
}
