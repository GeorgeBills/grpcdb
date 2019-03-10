package builder

import (
	pb "github.com/GeorgeBills/grpcdb/api"
)

// StatementBuilder supports fluent building of statements.
type StatementBuilder interface {
	Statement() (*pb.Statement, error)
}

/*
 * A fully built statement looks something like:
 *
 *   &pb.Statement{
 *     Statement: &pb.Statement_Select{
 *       Select: &pb.Select{
 *         ResultColumn: []string{"x"},
 *         From: "t1",
 *         Join: []*pb.Join{
 *           pb.Join{
 *             Table: "t2",
 *             Expr &pb.Expr{
 *               Expr &pb.Expr_BinaryExpr{
 *                 BinaryExpr: &pb.BinaryExpr{
 *                   Expr1: &pb.Expr{Expr &pb.Expr_Col{Col: &pb.Col{Table: "t1", Column: "y"}}},
 *                   Op:  pb.BinaryOp_EQ,
 *                   Expr2: &pb.Expr{Expr &pb.Expr_Col{Col: &pb.Col{Table: "t2", Column: "z"}}},
 *                 },
 *               },
 *             },
 *           },
 *         },
 *       },
 *     },
 *   },
 */

func Statement(statement *pb.Statement, err error) (*pb.Statement, error) {
	if err != nil {
		return nil, err
	}
	return statement, nil
}

// Table returns a new schema table where only the table is set.
func Table(table string) *pb.SchemaTable {
	return NewSchemaTable("", table)
}

// NewSchemaTable returns a new schema table.
func NewSchemaTable(schema, table string) *pb.SchemaTable {
	return &pb.SchemaTable{
		Schema: schema,
		Table:  table,
	}
}
