package grpcdb

import (
	"fmt"
	"strings"
)

//go:generate protoc -I api/ --go_out=plugins=grpc:. api/expression.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:. api/grpcdb.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:. api/select.proto

// TranslateStatement takes a grpcdb.Statement and returns SQL.
func TranslateStatement(s *Statement) (string, error) {
	sb := &strings.Builder{}
	switch s.Statement.(type) {
	case *Statement_Select:
		sel := s.GetSelect()
		sb.WriteString("SELECT ")
		sb.WriteString(strings.Join(sel.ResultColumn, ", ") + " ")
		sb.WriteString("FROM " + sel.From)
		for _, join := range sel.Join {
			translateJoin(sb, join)
		}
		return sb.String(), nil
	default:
		return "", fmt.Errorf("Unrecognized statement type: %T", s.Statement)
	}
}

func translateJoin(sb *strings.Builder, j *Join) error {
	if j.Natural {
		sb.WriteString("NATURAL ")
	}
	if j.JoinType != JoinType_INNER {
		switch j.JoinType {
		case JoinType_LEFT:
			sb.WriteString("LEFT ")
		case JoinType_LEFT_OUTER:
			sb.WriteString("LEFT OUTER ")
		case JoinType_RIGHT:
			sb.WriteString("RIGHT ")
		case JoinType_RIGHT_OUTER:
			sb.WriteString("RIGHT OUTER ")
		case JoinType_CROSS:
			sb.WriteString("CROSS ")
		}
	}
	sb.WriteString(" JOIN ")
	sb.WriteString(j.Table)
	sb.WriteString(" ON ")
	translateExpr(sb, j.Expr)
	return nil
}

func translateExpr(sb *strings.Builder, e *Expr) error {
	switch e.Expr.(type) {
	case *Expr_Col:
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
	case *Expr_UnaryExpr:
	case *Expr_BinaryExpr:
		be := e.GetBinaryExpr()
		translateExpr(sb, be.Expr1)
		switch be.Op {
		case BinaryOp_EQ:
			sb.WriteString(" = ")
		}
		translateExpr(sb, be.Expr2)
	}
	return nil
}
