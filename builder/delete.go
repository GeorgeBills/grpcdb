package builder

import (
	pb "github.com/GeorgeBills/grpcdb/api"
)

type DeleteStatementBuilder struct {
	delete *pb.Delete
	err    error
}

// Delete returns a new delete statement builder.
func Delete(from *pb.SchemaTable) *DeleteStatementBuilder {
	return &DeleteStatementBuilder{
		delete: &pb.Delete{
			From: from,
		},
	}
}

func (sb *DeleteStatementBuilder) Where(expr *pb.Expr) *DeleteStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.delete.Where = Any(sb.delete.Where, expr)
	return sb
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *DeleteStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Delete{Delete: sb.delete}}, sb.err)
}
