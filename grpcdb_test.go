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
			Select("t", "*"),
		},
		{
			"SELECT columns",
			"SELECT a, b, c FROM t",
			Select("t", "a", "b", "c"),
		},
		{
			"SELECT WHERE",
			"SELECT a FROM t WHERE x > 3",
			Select("t", "a").
				Where(GT(Col("x"), Num(3))),
		},
		{
			"WHERE AND",
			"SELECT a FROM t WHERE 3 < x AND 2 != y",
			Select("t", "a").
				Where(LT(Num(3), Col("x"))).
				Where(NEq(Num(2), Col("y"))),
		},
		{
			"JOIN",
			"SELECT x FROM t1 JOIN t2 ON t1.y = t2.z",
			Select("t1", "x").
				JoinEq("t2", TableCol("t1", "y"), TableCol("t2", "z")),
		},
		{
			"ORDER BY",
			"SELECT x FROM t ORDER BY y DESC",
			Select("t", "x").
				OrderBy(Col("y"), pb.OrderingDirection_DESC),
		},
		{
			"LIMIT",
			"SELECT x FROM t LIMIT 123",
			Select("t", "x").
				Limit(123),
		},
		{
			"OFFSET",
			"SELECT x FROM t OFFSET 456",
			Select("t", "x").
				Offset(456),
		},
		{
			"LIMIT OFFSET",
			"SELECT x FROM t LIMIT 10 OFFSET 10",
			Select("t", "x").
				Limit(10).
				Offset(10),
		},
		{
			"GROUP BY",
			"SELECT x FROM t GROUP BY a, b",
			Select("t", "x").
				GroupBy(Col("a"), Col("b")),
		},
		{
			"HAVING",
			"SELECT x FROM t GROUP BY a HAVING c < 0 AND d = 3",
			Select("t", "x").
				GroupBy(Col("a")).
				Having(LT(Col("c"), Num(0))).
				Having(Eq(Col("d"), Num(3))),
		},
		{
			"INSERT INTO (single row)",
			"INSERT INTO t (x, y, z) VALUES (1, 2, 3)",
			Insert(Table("t"), "x", "y", "z").
				Values([][]string{{"1", "2", "3"}}),
		},
		{
			"INSERT INTO (multiple rows)",
			"INSERT INTO t (x, y) VALUES (1, 2), (3, 4)",
			Insert(Table("t"), "x", "y").
				Values([][]string{{"1", "2"}, {"3", "4"}}),
		},
		{
			"INSERT INTO SELECT FROM",
			"INSERT INTO t1 (a, b) SELECT c, d FROM t2",
			Insert(Table("t1"), "a", "b").
				From(Select("t2", "c", "d")),
		},
		{
			"DELETE FROM",
			"DELETE FROM t",
			Delete(Table("t")),
		},
		{
			"DELETE FROM WHERE",
			"DELETE FROM t WHERE NOT x <= 0",
			Delete(Table("t")).
				Where(Not(LTE(Col("x"), Num(0)))),
		},
		{
			"UPDATE",
			"UPDATE t SET a = b, c = d",
			Update(Table("t")).
				Set("a", Col("b")).
				Set("c", Col("d")),
		},
		{
			"UPDATE WHERE",
			"UPDATE t SET a = 0, b = 1, c = 2 WHERE d >= 3",
			Update(Table("t")).
				Set("a", Num(0)).
				Set("b", Num(1)).
				Set("c", Num(2)).
				Where(GTE(Col("d"), Num(3))),
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
