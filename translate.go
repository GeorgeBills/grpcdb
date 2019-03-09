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

func translateWhere(sb *strings.Builder, e *Expr) error {
	sb.WriteString(" WHERE ")
	err := translateExpr(sb, e)
	return err
}

func translateExpr(sb *strings.Builder, e *Expr) error {
	switch e.Expr.(type) {
	case *Expr_Lit:
		lit := e.GetLit()
		sb.WriteString(lit)
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
		err := translateExpr(sb, be.Expr1)
		if err != nil {
			return err
		}
		switch be.Op {
		case BinaryOp_EQ:
			sb.WriteString(" = ")
		case BinaryOp_NE:
			sb.WriteString(" != ")
		case BinaryOp_GT:
			sb.WriteString(" > ")
		case BinaryOp_LT:
			sb.WriteString(" < ")
		case BinaryOp_LTE:
			sb.WriteString(" <= ")
		case BinaryOp_GTE:
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
