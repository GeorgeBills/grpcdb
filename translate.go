package grpcdb

import (
	"fmt"
	pb "github.com/GeorgeBills/grpcdb/api"
	"strconv"
	"strings"
)

//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/common.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/delete.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/expression.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/grpcdb.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/insert.proto
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/select.proto

// TranslateStatement takes a grpcdb.Statement and returns SQL.
func TranslateStatement(s *pb.Statement) (string, error) {
	sb := &strings.Builder{}
	switch s.Statement.(type) {
	case *pb.Statement_Select:
		sel := s.GetSelect()
		err := translateSelectStatement(sb, sel)
		if err != nil {
			return "", err
		}
		return sb.String(), nil
	case *pb.Statement_Insert:
		ins := s.GetInsert()
		err := translateInsertStatement(sb, ins)
		if err != nil {
			return "", err
		}
		return sb.String(), nil
	case *pb.Statement_Delete:
		del := s.GetDelete()
		err := translateDeleteStatement(sb, del)
		if err != nil {
			return "", err
		}
		return sb.String(), nil
	default:
		return "", fmt.Errorf("Unrecognized statement type: %T", s.Statement)
	}
}

func translateSelectStatement(sb *strings.Builder, sel *pb.Select) error {
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(sel.ResultColumn, ", ") + " ")
	sb.WriteString("FROM " + sel.From)
	for _, join := range sel.Join {
		err := translateJoin(sb, join)
		if err != nil {
			return err
		}
	}
	for _, where := range sel.Where {
		err := translateWhere(sb, where)
		if err != nil {
			return err
		}
	}
	for _, orderBy := range sel.OrderBy {
		err := translateOrderBy(sb, orderBy)
		if err != nil {
			return err
		}
	}
	if sel.Limit != 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(int(sel.Limit)))
	}
	if sel.Offset != 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(int(sel.Offset)))
	}
	return nil
}

func translateInsertStatement(sb *strings.Builder, ins *pb.Insert) error {
	switch ins.Insert {
	case pb.InsertType_INSERT:
		sb.WriteString("INSERT ")
	case pb.InsertType_REPLACE:
		sb.WriteString("REPLACE ")
	}
	sb.WriteString("INTO ")
	translateSchemaTable(sb, ins.Into)
	sb.WriteString(" (" + strings.Join(ins.Columns, ", ") + ")")
	sb.WriteString(" VALUES ")
	vals := ins.ToInsert.GetValues()
	lasti := len(vals.Rows) - 1
	for i, r := range vals.Rows {
		sb.WriteString("(")
		lastj := len(r.Values) - 1
		for j, v := range r.Values {
			translateExpr(sb, v)
			if j != lastj {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
		if i != lasti {
			sb.WriteString(", ")
		}
	}
	return nil
}

func translateDeleteStatement(sb *strings.Builder, del *pb.Delete) error {
	sb.WriteString("DELETE FROM ")
	translateSchemaTable(sb, del.From)
	for _, where := range del.Where {
		err := translateWhere(sb, where)
		if err != nil {
			return err
		}
	}
	return nil
}

func translateSchemaTable(sb *strings.Builder, table *pb.SchemaTable) {
	if table.Schema != "" {
		sb.WriteString(table.Schema)
	}
	sb.WriteString(table.Table)
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
	if j.On == nil {
		return fmt.Errorf("nil expression in join %+v", j)
	}
	translateExpr(sb, j.On)
	return nil
}

func translateWhere(sb *strings.Builder, e *pb.Expr) error {
	sb.WriteString(" WHERE ")
	err := translateExpr(sb, e)
	return err
}

func translateOrderBy(sb *strings.Builder, e *pb.OrderingTerm) error {
	sb.WriteString(" ORDER BY ")
	err := translateExpr(sb, e.By)
	if err != nil {
		return err
	}
	switch e.Dir {
	case pb.OrderingDirection_ASC:
		sb.WriteString(" ASC")
	case pb.OrderingDirection_DESC:
		sb.WriteString(" DESC")
	default:
		return fmt.Errorf("Unrecognized ordering direction: %d", e.Dir)
	}
	return nil
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
