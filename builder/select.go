package builder

import (
	"errors"
	pb "github.com/GeorgeBills/grpcdb/api"
)

type SelectStatementBuilder struct {
	sel *pb.Select // select is a keyword
	err error
}

// Select returns a new select statement builder.
func Select(from string, columns ...string) *SelectStatementBuilder {
	return &SelectStatementBuilder{
		sel: &pb.Select{
			ResultColumn: columns,
			From:         from,
		},
	}
}

// Where adds a where clause.
func (sb *SelectStatementBuilder) Where(expr *pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.sel.Where = All(sb.sel.Where, expr)
	return sb
}

func (sb *SelectStatementBuilder) GroupBy(expr ...*pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	for _, e := range expr {
		sb.sel.GroupBy = append(sb.sel.GroupBy, e)
	}
	return sb
}

func (sb *SelectStatementBuilder) Having(expr *pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	if sb.sel.GroupBy == nil {
		sb.err = errors.New("HAVING without GROUP BY is invalid; you must add the GROUP BY first")
		return sb
	}
	sb.sel.Having = All(sb.sel.Having, expr)
	return sb
}

// AddJoin adds a join clause.
func (sb *SelectStatementBuilder) AddJoin(table string, joinExpr *pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	join := &pb.Join{
		Table: table,
		On:    joinExpr,
	}
	sb.sel.Join = append(sb.sel.Join, join)
	return sb
}

// JoinEq adds a join clause where two columns are equal.
func (sb *SelectStatementBuilder) JoinEq(table string, expr1, expr2 *pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	eq := newBinaryExpression(expr1, expr2, pb.BinaryOp_EQ)
	return sb.AddJoin(table, eq)
}

// OrderBy adds an ordering clause.
func (sb *SelectStatementBuilder) OrderBy(expr *pb.Expr, dir pb.OrderingDirection) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.sel.OrderBy = append(sb.sel.OrderBy, &pb.OrderingTerm{
		By:  expr,
		Dir: dir,
	})
	return sb
}

// Limit sets the limit on the statement.
func (sb *SelectStatementBuilder) Limit(limit uint64) *SelectStatementBuilder {
	sb.sel.Limit = limit
	return sb
}

// Offset sets the offset on the statement.
func (sb *SelectStatementBuilder) Offset(offset uint64) *SelectStatementBuilder {
	sb.sel.Offset = offset
	return sb
}

func (sb *SelectStatementBuilder) Select() (*pb.Select, error) {
	return sb.sel, sb.err
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *SelectStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Select{Select: sb.sel}}, sb.err)
}
