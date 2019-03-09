package grpcdb_test

import (
	"github.com/GeorgeBills/grpcdb"
	pb "github.com/GeorgeBills/grpcdb/api"
	"github.com/GeorgeBills/grpcdb/builder"
	"testing"
)

func TestTranslation(t *testing.T) {
	table := []struct {
		name             string
		sql              string
		statementBuilder builder.StatementBuilder
	}{
		{
			"SELECT *",
			"SELECT * FROM t",
			builder.
				NewSelect("t", "*"),
		},
		{
			"SELECT columns",
			"SELECT a, b, c FROM t",
			builder.
				NewSelect("t", "a", "b", "c"),
		},
		{
			"SELECT WHERE",
			"SELECT a FROM t WHERE x > 3",
			builder.
				NewSelect("t", "a").
				AddWhere(builder.NewBinaryExpression(builder.NewColumn("x"), builder.NewLiteral("3"), pb.BinaryOp_GT)),
		},
		{
			"WHERE AND",
			"SELECT a FROM t WHERE 3 < x AND 2 != y",
			builder.
				NewSelect("t", "a").
				AddWhere(builder.NewBinaryExpression(builder.NewLiteral("3"), builder.NewColumn("x"), pb.BinaryOp_LT)).
				AddWhere(builder.NewBinaryExpression(builder.NewLiteral("2"), builder.NewColumn("y"), pb.BinaryOp_NE)),
		},
		{
			"JOIN",
			"SELECT x FROM t1 JOIN t2 ON t1.y = t2.z",
			builder.
				NewSelect("t1", "x").
				AddJoinEq("t2", builder.NewTableColumn("t1", "y"), builder.NewTableColumn("t2", "z")),
		},
		{
			"ORDER BY",
			"SELECT x FROM t ORDER BY y DESC",
			builder.
				NewSelect("t", "x").
				AddOrderBy(builder.NewColumn("y"), pb.OrderingDirection_DESC),
		},
		{
			"LIMIT",
			"SELECT x FROM t LIMIT 123",
			builder.
				NewSelect("t", "x").
				SetLimit(123),
		},
		{
			"OFFSET",
			"SELECT x FROM t OFFSET 456",
			builder.
				NewSelect("t", "x").
				SetOffset(456),
		},
		{
			"LIMIT OFFSET",
			"SELECT x FROM t LIMIT 10 OFFSET 10",
			builder.
				NewSelect("t", "x").
				SetLimit(10).
				SetOffset(10),
		},
		{
			"GROUP BY",
			"SELECT x FROM t GROUP BY a, b",
			builder.
				NewSelect("t", "x").
				GroupBy(builder.NewLiteral("a"), builder.NewLiteral("b")),
		},
		{
			"HAVING",
			"SELECT x FROM t GROUP BY a HAVING c < 0 AND d = 3",
			builder.
				NewSelect("t", "x").
				GroupBy(builder.NewLiteral("a")).
				Having(builder.NewBinaryExpression(builder.NewColumn("c"), builder.NewLiteral("0"), pb.BinaryOp_LT)).
				Having(builder.NewBinaryExpression(builder.NewColumn("d"), builder.NewLiteral("3"), pb.BinaryOp_EQ)),
		},
		{
			"INSERT INTO (single row)",
			"INSERT INTO t (x, y, z) VALUES (1, 2, 3)",
			builder.
				NewInsert(builder.NewTable("t"), "x", "y", "z").
				Values([][]string{{"1", "2", "3"}}),
		},
		{
			"INSERT INTO (multiple rows)",
			"INSERT INTO t (x, y) VALUES (1, 2), (3, 4)",
			builder.
				NewInsert(builder.NewTable("t"), "x", "y").
				Values([][]string{{"1", "2"}, {"3", "4"}}),
		},
		{
			"INSERT INTO SELECT FROM",
			"INSERT INTO t1 (a, b) SELECT c, d FROM t2",
			builder.
				NewInsert(builder.NewTable("t1"), "a", "b").
				From(builder.NewSelect("t2", "c", "d")),
		},
		{
			"DELETE FROM",
			"DELETE FROM t",
			builder.NewDelete(builder.NewTable("t")),
		},
		{
			"DELETE FROM WHERE",
			"DELETE FROM t WHERE x <= 0",
			builder.
				NewDelete(builder.NewTable("t")).
				AddWhere(builder.NewBinaryExpression(builder.NewColumn("x"), builder.NewLiteral("0"), pb.BinaryOp_LTE)),
		},
		{
			"UPDATE",
			"UPDATE t SET a = b, c = d",
			builder.
				NewUpdate(builder.NewTable("t")).
				Set("a", builder.NewLiteral("b")).
				Set("c", builder.NewLiteral("d")),
		},
		{
			"UPDATE WHERE",
			"UPDATE t SET a = 0, b = 1, c = 2 WHERE d >= 3",
			builder.
				NewUpdate(builder.NewTable("t")).
				Set("a", builder.NewLiteral("0")).
				Set("b", builder.NewLiteral("1")).
				Set("c", builder.NewLiteral("2")).
				AddWhere(builder.NewBinaryExpression(builder.NewColumn("d"), builder.NewLiteral("3"), pb.BinaryOp_GTE)),
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
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
