package grpcdb_test

import (
	"github.com/GeorgeBills/grpcdb"
	pb "github.com/GeorgeBills/grpcdb/api"
	. "github.com/GeorgeBills/grpcdb/builder"
	"testing"
)

func TestTranslation(t *testing.T) {
	table := []struct {
		name             string
		sql              string
		statementBuilder StatementBuilder
	}{
		{
			"SELECT *",
			"SELECT * FROM t",
			NewSelect("t", "*"),
		},
		{
			"SELECT columns",
			"SELECT a, b, c FROM t",
			NewSelect("t", "a", "b", "c"),
		},
		{
			"SELECT WHERE",
			"SELECT a FROM t WHERE x > 3",
			NewSelect("t", "a").
				Where(GT(Col("x"), Lit("3"))),
		},
		{
			"WHERE AND",
			"SELECT a FROM t WHERE 3 < x AND 2 != y",
			NewSelect("t", "a").
				Where(LT(Lit("3"), Col("x"))).
				Where(NEq(Lit("2"), Col("y"))),
		},
		{
			"JOIN",
			"SELECT x FROM t1 JOIN t2 ON t1.y = t2.z",
			NewSelect("t1", "x").
				AddJoinEq("t2", TableCol("t1", "y"), TableCol("t2", "z")),
		},
		{
			"ORDER BY",
			"SELECT x FROM t ORDER BY y DESC",
			NewSelect("t", "x").
				AddOrderBy(Col("y"), pb.OrderingDirection_DESC),
		},
		{
			"LIMIT",
			"SELECT x FROM t LIMIT 123",
			NewSelect("t", "x").
				SetLimit(123),
		},
		{
			"OFFSET",
			"SELECT x FROM t OFFSET 456",
			NewSelect("t", "x").
				SetOffset(456),
		},
		{
			"LIMIT OFFSET",
			"SELECT x FROM t LIMIT 10 OFFSET 10",
			NewSelect("t", "x").
				SetLimit(10).
				SetOffset(10),
		},
		{
			"GROUP BY",
			"SELECT x FROM t GROUP BY a, b",
			NewSelect("t", "x").
				GroupBy(Lit("a"), Lit("b")),
		},
		{
			"HAVING",
			"SELECT x FROM t GROUP BY a HAVING c < 0 AND d = 3",
			NewSelect("t", "x").
				GroupBy(Lit("a")).
				Having(LT(Col("c"), Lit("0"))).
				Having(Eq(Col("d"), Lit("3"))),
		},
		{
			"INSERT INTO (single row)",
			"INSERT INTO t (x, y, z) VALUES (1, 2, 3)",
			NewInsert(NewTable("t"), "x", "y", "z").
				Values([][]string{{"1", "2", "3"}}),
		},
		{
			"INSERT INTO (multiple rows)",
			"INSERT INTO t (x, y) VALUES (1, 2), (3, 4)",
			NewInsert(NewTable("t"), "x", "y").
				Values([][]string{{"1", "2"}, {"3", "4"}}),
		},
		{
			"INSERT INTO SELECT FROM",
			"INSERT INTO t1 (a, b) SELECT c, d FROM t2",
			NewInsert(NewTable("t1"), "a", "b").
				From(NewSelect("t2", "c", "d")),
		},
		{
			"DELETE FROM",
			"DELETE FROM t",
			NewDelete(NewTable("t")),
		},
		{
			"DELETE FROM WHERE",
			"DELETE FROM t WHERE NOT x <= 0",
			NewDelete(NewTable("t")).
				Where(Not(LTE(Col("x"), Lit("0")))),
		},
		{
			"UPDATE",
			"UPDATE t SET a = b, c = d",
			NewUpdate(NewTable("t")).
				Set("a", Lit("b")).
				Set("c", Lit("d")),
		},
		{
			"UPDATE WHERE",
			"UPDATE t SET a = 0, b = 1, c = 2 WHERE d >= 3",
			NewUpdate(NewTable("t")).
				Set("a", Lit("0")).
				Set("b", Lit("1")).
				Set("c", Lit("2")).
				Where(GTE(Col("d"), Lit("3"))),
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
				t.Errorf("Expected: '%s'\nActual: '%s'\nStatement: %#v", tt.sql, result, statement)
			}
		})
	}
}
