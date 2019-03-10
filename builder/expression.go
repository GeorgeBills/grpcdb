package builder

import (
	pb "github.com/GeorgeBills/grpcdb/api"
)

func lit(lit *pb.Lit) *pb.Expr {
	return &pb.Expr{
		Expr: &pb.Expr_Lit{
			Lit: lit,
		},
	}
}

// Str returns a new string literal.
func Str(str string) *pb.Expr {
	return lit(&pb.Lit{
		Lit: &pb.Lit_Str{
			Str: str,
		},
	})
}

// Num returns a new numeric literal.
func Num(num float64) *pb.Expr {
	return lit(&pb.Lit{
		Lit: &pb.Lit_Num{
			Num: num,
		},
	})
}

var null = lit(&pb.Lit{
	Lit: &pb.Lit_Null{},
})

// Null returns the null literal.
func Null() *pb.Expr {
	return null
}

// Col returns a new column expression where only the column is set.
func Col(column string) *pb.Expr {
	return SchemaTableCol("", "", column)
}

// TableCol returns a new column expression where only the table and column are
// set.
func TableCol(table, column string) *pb.Expr {
	return SchemaTableCol("", table, column)
}

// SchemaTableCol returns a new column expression where schema, table and column
// are all set.
func SchemaTableCol(schema, table, column string) *pb.Expr {
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

// newBinaryExpression returns a new binary expression.
func newBinaryExpression(expr1, expr2 *pb.Expr, op pb.BinaryOp) *pb.Expr {
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

func All(exprs ...*pb.Expr) *pb.Expr {
	var all *pb.Expr
	for _, expr := range exprs {
		if expr != nil {
			if all != nil {
				all = And(all, expr)
			} else {
				all = expr
			}
		}
	}
	return all
}

func Any(exprs ...*pb.Expr) *pb.Expr {
	var any *pb.Expr
	for _, expr := range exprs {
		if expr != nil {
			if any != nil {
				any = Or(any, expr)
			} else {
				any = expr
			}
		}
	}
	return any
}

func newUnaryExpr(expr *pb.Expr, op pb.UnaryOp) *pb.Expr {
	return &pb.Expr{
		Expr: &pb.Expr_UnaryExpr{
			UnaryExpr: &pb.UnaryExpr{
				Expr: expr,
				Op:   op,
			},
		},
	}
}

func Not(expr *pb.Expr) *pb.Expr {
	return newUnaryExpr(expr, pb.UnaryOp_NOT)
}

func Eq(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_EQ)
}

func NEq(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_NE)
}

func GT(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_GT)
}

func GTE(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_GTE)
}

func LT(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_LT)
}

func LTE(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_LTE)
}

func And(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_AND)
}

func Or(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_OR)
}

func Is(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_IS)
}

func IsNot(expr1, expr2 *pb.Expr) *pb.Expr {
	return newBinaryExpression(expr1, expr2, pb.BinaryOp_IS_NOT)
}
