package builder

import (
	pb "github.com/GeorgeBills/grpcdb/api"
)

type UpdateStatementBuilder struct {
	update *pb.Update
	err    error
}

// NewUpdate returns a new update statement builder.
func NewUpdate(table *pb.SchemaTable) *UpdateStatementBuilder {
	return &UpdateStatementBuilder{
		update: &pb.Update{
			Table: table,
		},
	}
}

func (sb *UpdateStatementBuilder) Set(col string, to *pb.Expr) *UpdateStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.update.Set = append(sb.update.Set, &pb.Set{
		Column: col,
		To:     to,
	})
	return sb
}

func (sb *UpdateStatementBuilder) AddWhere(expr *pb.Expr) *UpdateStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.update.Where = And(sb.update.Where, expr)
	return sb
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *UpdateStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Update{Update: sb.update}}, sb.err)
}
