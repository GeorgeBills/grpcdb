package grpcdb

import (
	"fmt"
	pb "github.com/GeorgeBills/grpcdb/api"
)

// StatementBuilder supports fluent building of statements.
type StatementBuilder struct {
	statement *pb.Statement
	err       error
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

// NewSelect returns a new select statement builder.
func NewSelect(from string, columns ...string) *StatementBuilder {
	return &StatementBuilder{
		statement: &pb.Statement{
			Statement: &pb.Statement_Select{
				Select: &pb.Select{
					ResultColumn: columns,
					From:         from,
				},
			},
		},
	}
}

// AddWhere adds a where clause.
func (sb *StatementBuilder) AddWhere(expr *pb.Expr) *StatementBuilder {
	if sb.err != nil {
		return sb
	}
	switch sb.statement.Statement.(type) {
	case *pb.Statement_Select:
		sel := sb.statement.GetSelect()
		sel.Where = append(sel.Where, expr)
	default:
		sb.err = fmt.Errorf("Statement type %T does not support AddWhere()", sb.statement.Statement)
	}
	return sb
}

// AddJoin adds a join clause.
func (sb *StatementBuilder) AddJoin(table string, joinExpr *pb.Expr) *StatementBuilder {
	if sb.err != nil {
		return sb
	}
	switch sb.statement.Statement.(type) {
	case *pb.Statement_Select:
		sel := sb.statement.GetSelect()
		join := &pb.Join{
			Table: table,
			Expr:  joinExpr,
		}
		sel.Join = append(sel.Join, join)
	default:
		sb.err = fmt.Errorf("Statement type %T does not support AddJoin()", sb.statement.Statement)
	}
	return sb
}

// AddJoinEq adds a join clause where two columns are equal.
func (sb *StatementBuilder) AddJoinEq(table string, expr1, expr2 *pb.Expr) *StatementBuilder {
	if sb.err != nil {
		return sb
	}
	eq := NewBinaryExpression(expr1, expr2, pb.BinaryOp_EQ)
	return sb.AddJoin(table, eq)
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *StatementBuilder) Statement() (*pb.Statement, error) {
	if sb.err != nil {
		return nil, sb.err
	}
	return sb.statement, nil
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
