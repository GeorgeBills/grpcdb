package grpcdb

import (
	"fmt"
	pb "github.com/GeorgeBills/grpcdb/api"
	"strings"
)

//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/expression.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/grpcdb.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/select.proto

// TranslateStatement takes a grpcdb.Statement and returns SQL.
func TranslateStatement(s *pb.Statement) (string, error) {
	sb := &strings.Builder{}
	switch s.Statement.(type) {
	case *pb.Statement_Select:
		sel := s.GetSelect()
		sb.WriteString("SELECT ")
		sb.WriteString(strings.Join(sel.ResultColumn, ", ") + " ")
		sb.WriteString("FROM " + sel.From)
		for _, join := range sel.Join {
			err := translateJoin(sb, join)
			if err != nil {
				return "", err
			}
		}
		for _, where := range sel.Where {
			err := translateWhere(sb, where)
			if err != nil {
				return "", err
			}
		}
		return sb.String(), nil
	default:
		return "", fmt.Errorf("Unrecognized statement type: %T", s.Statement)
	}
}

func translateJoin(sb *strings.Builder, j *pb.Join) error {
	if j.Natural {
		sb.WriteString("NATURAL ")
	}
	if j.JoinType != pb.JoinType_INNER {
		switch j.JoinType {
		case pb.JoinType_LEFT:
			sb.WriteString("LEFT ")
		case pb.JoinType_LEFT_OUTER:
			sb.WriteString("LEFT OUTER ")
		case pb.JoinType_RIGHT:
			sb.WriteString("RIGHT ")
		case pb.JoinType_RIGHT_OUTER:
			sb.WriteString("RIGHT OUTER ")
		case pb.JoinType_CROSS:
			sb.WriteString("CROSS ")
		default:
			return fmt.Errorf("Unrecognized join type: %d", j.JoinType)
		}
	}
	sb.WriteString(" JOIN ")
	sb.WriteString(j.Table)
	sb.WriteString(" ON ")
	if j.Expr == nil {
		return fmt.Errorf("nil expression in join %+v", j)
	}
	translateExpr(sb, j.Expr)
	return nil
}

func translateWhere(sb *strings.Builder, e *pb.Expr) error {
	sb.WriteString(" WHERE ")
	err := translateExpr(sb, e)
	return err
}

func translateExpr(sb *strings.Builder, e *pb.Expr) error {
	switch e.Expr.(type) {
	case *pb.Expr_Lit:
		lit := e.GetLit()
		sb.WriteString(lit)
	case *pb.Expr_Col:
		col := e.GetCol()
		if col.Schema != "" {
			sb.WriteString(col.Schema + ".")
		}
		if col.Table != "" {
			sb.WriteString(col.Table + ".")
		}
		if col.Column == "" {
			return fmt.Errorf("column is required in %T", col)
		}
		sb.WriteString(col.Column)
	case *pb.Expr_UnaryExpr:
	case *pb.Expr_BinaryExpr:
		be := e.GetBinaryExpr()
		err := translateExpr(sb, be.Expr1)
		if err != nil {
			return err
		}
		switch be.Op {
		case pb.BinaryOp_EQ:
			sb.WriteString(" = ")
		case pb.BinaryOp_NE:
			sb.WriteString(" != ")
		case pb.BinaryOp_GT:
			sb.WriteString(" > ")
		case pb.BinaryOp_LT:
			sb.WriteString(" < ")
		case pb.BinaryOp_LTE:
			sb.WriteString(" <= ")
		case pb.BinaryOp_GTE:
			sb.WriteString(" >= ")
		default:
			return fmt.Errorf("Unrecognized binary op: %d", be.Op)
		}
		err = translateExpr(sb, be.Expr2)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Unrecognized expression type: %T", e.Expr)
	}
	return nil
}
