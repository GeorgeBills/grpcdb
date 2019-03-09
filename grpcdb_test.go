package grpcdb_test

import (
	"github.com/GeorgeBills/grpcdb"
	pb "github.com/GeorgeBills/grpcdb/api"
	"testing"
)

func TestTranslation(t *testing.T) {
	table := []struct {
		sql              string
		statementBuilder grpcdb.StatementBuilder
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
				AddWhere(grpcdb.NewBinaryExpression(grpcdb.NewColumn("x"), grpcdb.NewLiteral("3"), pb.BinaryOp_GT)),
		},
		{
			"SELECT a FROM t WHERE 3 < x AND 2 != y",
			grpcdb.
				NewSelect("t", "a").
				AddWhere(grpcdb.NewBinaryExpression(grpcdb.NewLiteral("3"), grpcdb.NewColumn("x"), pb.BinaryOp_LT)).
				AddWhere(grpcdb.NewBinaryExpression(grpcdb.NewLiteral("2"), grpcdb.NewColumn("y"), pb.BinaryOp_NE)),
		},
		{
			"SELECT x FROM t1 JOIN t2 ON t1.y = t2.z",
			grpcdb.
				NewSelect("t1", "x").
				AddJoinEq("t2", grpcdb.NewTableColumn("t1", "y"), grpcdb.NewTableColumn("t2", "z")),
		},
		{
			"SELECT x FROM t ORDER BY y DESC",
			grpcdb.
				NewSelect("t", "x").
				AddOrderBy(grpcdb.NewColumn("y"), pb.OrderingDirection_DESC),
		},
		{
			"SELECT x FROM t LIMIT 123",
			grpcdb.
				NewSelect("t", "x").
				SetLimit(123),
		},
		{
			"SELECT x FROM t OFFSET 456",
			grpcdb.
				NewSelect("t", "x").
				SetOffset(456),
		},
		{
			"SELECT x FROM t LIMIT 10 OFFSET 10",
			grpcdb.
				NewSelect("t", "x").
				SetLimit(10).
				SetOffset(10),
		},
		{
			"INSERT INTO t (x, y, z) VALUES (1, 2, 3)",
			grpcdb.
				NewInsert(
					grpcdb.NewTable("t"),
					grpcdb.NewLiteralInsertValues([][]string{{"1", "2", "3"}}),
					"x", "y", "z",
				),
		},
		{
			"INSERT INTO t (x, y) VALUES (1, 2), (3, 4)",
			grpcdb.
				NewInsert(
					grpcdb.NewTable("t"),
					grpcdb.NewLiteralInsertValues([][]string{{"1", "2"}, {"3", "4"}}),
					"x", "y",
				),
		},
		{
			"DELETE FROM t",
			grpcdb.NewDelete(grpcdb.NewTable("t")),
		},
		{
			"DELETE FROM t WHERE x <= 0",
			grpcdb.
				NewDelete(grpcdb.NewTable("t")).
				AddWhere(grpcdb.NewBinaryExpression(grpcdb.NewColumn("x"), grpcdb.NewLiteral("0"), pb.BinaryOp_LTE)),
		},
		{
			"UPDATE t SET a = 0, b = 1, c = 2 WHERE d >= 3",
			grpcdb.
				NewUpdate(grpcdb.NewTable("t")).
				Set("a", grpcdb.NewLiteral("0")).
				Set("b", grpcdb.NewLiteral("1")).
				Set("c", grpcdb.NewLiteral("2")).
				AddWhere(grpcdb.NewBinaryExpression(grpcdb.NewColumn("d"), grpcdb.NewLiteral("3"), pb.BinaryOp_GTE)),
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
