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
		{
			"SELECT x FROM t1 JOIN t2 ON t1.y = t2.z",
			&grpcdb.Statement{
				Statement: &grpcdb.Statement_Select{
					Select: &grpcdb.Select{
						ResultColumn: []string{"x"},
						From:         "t1",
						Join: []*grpcdb.Join{
							&grpcdb.Join{
								Table: "t2",
								Expr: &grpcdb.Expr{
									Expr: &grpcdb.Expr_BinaryExpr{
										BinaryExpr: &grpcdb.BinaryExpr{
											Expr1: &grpcdb.Expr{Expr: &grpcdb.Expr_Col{Col: &grpcdb.Col{Table: "t1", Column: "y"}}},
											Op:    grpcdb.BinaryOp_EQ,
											Expr2: &grpcdb.Expr{Expr: &grpcdb.Expr_Col{Col: &grpcdb.Col{Table: "t2", Column: "z"}}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range table {
		t.Run(tt.sql, func(t *testing.T) {
			result, err := grpcdb.TranslateStatement(tt.statement)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.sql {
				t.Errorf("Expected result '%s' for statement %+v to be '%s'", result, tt.statement, tt.sql)
			}
		})
	}
}
