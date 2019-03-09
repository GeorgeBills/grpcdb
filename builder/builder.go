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

func And(expr, and *pb.Expr) *pb.Expr {
	if expr == nil {
		return and
	}
	return NewBinaryExpression(expr, and, pb.BinaryOp_AND)
}

func Statement(statement *pb.Statement, err error) (*pb.Statement, error) {
	if err != nil {
		return nil, err
	}
	return statement, nil
}

// NewTable returns a new schema table where only the table is set.
func NewTable(table string) *pb.SchemaTable {
	return NewSchemaTable("", table)
}

// NewSchemaTable returns a new schema table.
func NewSchemaTable(schema, table string) *pb.SchemaTable {
	return &pb.SchemaTable{
		Schema: schema,
		Table:  table,
	}
}

// NewColumn returns a new column expression where only the column is set.
func NewColumn(column string) *pb.Expr {
	return NewSchemaTableColumn("", "", column)
}

// NewTableColumn returns a new column expression where only the table and
// column are set.
func NewTableColumn(table, column string) *pb.Expr {
	return NewSchemaTableColumn("", table, column)
}

// NewSchemaTableColumn returns a new column expression where schema, table and
// column are all set.
func NewSchemaTableColumn(schema, table, column string) *pb.Expr {
	return &pb.Expr{
		Expr: &pb.Expr_Col{
			Col: &pb.Col{
				Schema: schema,
				Table:  table,
				Column: column,
			},
		},
	}
}

// NewBinaryExpression returns a new binary expression.
func NewBinaryExpression(expr1, expr2 *pb.Expr, op pb.BinaryOp) *pb.Expr {
	return &pb.Expr{
		Expr: &pb.Expr_BinaryExpr{
			BinaryExpr: &pb.BinaryExpr{
				Expr1: expr1,
				Op:    op,
				Expr2: expr2,
			},
		},
	}
}

// NewLiteral returns a new literal.
func NewLiteral(lit string) *pb.Expr {
	return &pb.Expr{
		Expr: &pb.Expr_Lit{
			Lit: lit,
		},
	}
}
