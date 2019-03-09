package grpcdb

import (
	"fmt"
)

// StatementBuilder supports fluent building of statements.
type StatementBuilder struct {
	statement *Statement
	err       error
}

/*
 * A fully built statement looks something like:
 *
 *   &grpcdb.Statement{
 *     Statement: &grpcdb.Statement_Select{
 *       Select: &grpcdb.Select{
 *         ResultColumn: []string{"x"},
 *         From: "t1",
 *         Join: []*grpcdb.Join{
 *           grpcdb.Join{
 *             Table: "t2",
 *             Expr: &grpcdb.Expr{
 *               Expr: &grpcdb.Expr_BinaryExpr{
 *                 BinaryExpr: &grpcdb.BinaryExpr{
 *                   Expr1: &grpcdb.Expr{Expr: &grpcdb.Expr_Col{Col: &grpcdb.Col{Table: "t1", Column: "y"}}},
 *                   Op:  grpcdb.BinaryOp_EQ,
 *                   Expr2: &grpcdb.Expr{Expr: &grpcdb.Expr_Col{Col: &grpcdb.Col{Table: "t2", Column: "z"}}},
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
		statement: &Statement{
			Statement: &Statement_Select{
				Select: &Select{
					ResultColumn: columns,
					From:         from,
				},
			},
		},
	}
}

// AddWhere adds a where clause.
func (sb *StatementBuilder) AddWhere() *StatementBuilder {
	if sb.err != nil {
		return sb
	}
	switch sb.statement.Statement.(type) {
	case *Statement_Select:
		sel := sb.statement.GetSelect()
		sel.Where = append(sel.Where, &Where{})
	default:
		sb.err = fmt.Errorf("Statement type %T does not support AddWhere()", sb.statement.Statement)
	}
	return sb
}

// AddJoin adds a join clause.
func (sb *StatementBuilder) AddJoin(table string, joinExpr *Expr) *StatementBuilder {
	if sb.err != nil {
		return sb
	}
	switch sb.statement.Statement.(type) {
	case *Statement_Select:
		sel := sb.statement.GetSelect()
		join := &Join{
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
func (sb *StatementBuilder) AddJoinEq(table string, expr1, expr2 *Expr) *StatementBuilder {
	if sb.err != nil {
		return sb
	}
	eq := NewBinaryExpression(expr1, expr2, BinaryOp_EQ)
	return sb.AddJoin(table, eq)
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *StatementBuilder) Statement() (*Statement, error) {
	if sb.err != nil {
		return nil, sb.err
	}
	return sb.statement, nil
}

// NewColumn returns a new column expression where only the column is set.
func NewColumn(column string) *Expr {
	return NewSchemaTableColumn("", "", column)
}

// NewTableColumn returns a new column expression where only the table and
// column are set.
func NewTableColumn(table, column string) *Expr {
	return NewSchemaTableColumn("", table, column)
}

// NewSchemaTableColumn returns a new column expression where schema, table and
// column are all set.
func NewSchemaTableColumn(schema, table, column string) *Expr {
	return &Expr{
		Expr: &Expr_Col{
			Col: &Col{
				Schema: schema,
				Table:  table,
				Column: column,
			},
		},
	}
}

// NewBinaryExpression returns a new binary expression.
func NewBinaryExpression(expr1, expr2 *Expr, op BinaryOp) *Expr {
	return &Expr{
		Expr: &Expr_BinaryExpr{
			BinaryExpr: &BinaryExpr{
				Expr1: expr1,
				Op:    op,
				Expr2: expr2,
			},
		},
	}
}
