package grpcdb

import (
	"errors"
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
//go:generate protoc -I api/ --go_out=plugins=grpc:api/ api/update.proto

type invalidStatementError struct {
	context *pb.Statement
	wrapped error
}

func (ise *invalidStatementError) Error() string {
	return fmt.Sprintf("Error translating statement %+v: %v", ise.context, ise.wrapped)
}

// TranslateStatement takes a grpcdb.Statement and returns SQL.
func TranslateStatement(s *pb.Statement) (string, error) {
	sb := &strings.Builder{}
	var err error
	switch s.Statement.(type) {
	case *pb.Statement_Select:
		err = translateSelectStatement(sb, s.GetSelect())
	case *pb.Statement_Insert:
		err = translateInsertStatement(sb, s.GetInsert())
	case *pb.Statement_Delete:
		err = translateDeleteStatement(sb, s.GetDelete())
	case *pb.Statement_Update:
		err = translateUpdateStatement(sb, s.GetUpdate())
	default:
		err = fmt.Errorf("Unrecognized statement type: %T", s.Statement)
	}
	if err != nil {
		return "", &invalidStatementError{
			context: s,
			wrapped: err,
		}
	}
	return sb.String(), nil
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
	if sel.Where != nil {
		sb.WriteString(" WHERE ")
		err := translateExpr(sb, sel.Where)
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
	if len(sel.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		lasti := len(sel.GroupBy) - 1
		for i, groupBy := range sel.GroupBy {
			err := translateExpr(sb, groupBy)
			if err != nil {
				return err
			}
			if i != lasti {
				sb.WriteString(", ")
			}
		}
	}
	if sel.Having != nil {
		sb.WriteString(" HAVING ")
		translateExpr(sb, sel.Having)
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
	sb.WriteString(" (" + strings.Join(ins.Columns, ", ") + ") ")
	var err error
	switch ins.ToInsert.Insert.(type) {
	case *pb.ToInsert_Values:
		err = translateInsertValues(sb, ins.ToInsert.GetValues())
	case *pb.ToInsert_Select:
		err = translateSelectStatement(sb, ins.ToInsert.GetSelect())
	}
	return err
}

func translateInsertValues(sb *strings.Builder, vals *pb.Values) error {
	sb.WriteString("VALUES ")
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
	if del.Where != nil {
		sb.WriteString(" WHERE ")
		err := translateExpr(sb, del.Where)
		if err != nil {
			return err
		}
	}
	return nil
}

func translateUpdateStatement(sb *strings.Builder, upd *pb.Update) error {
	sb.WriteString("UPDATE ")
	translateSchemaTable(sb, upd.Table)
	sb.WriteString(" SET ")
	lasti := len(upd.Set) - 1
	for i, set := range upd.Set {
		sb.WriteString(set.Column)
		sb.WriteString(" = ")
		err := translateExpr(sb, set.To)
		if err != nil {
			return err
		}
		if i != lasti {
			sb.WriteString(", ")
		}
	}
	if upd.Where != nil {
		sb.WriteString(" WHERE ")
		err := translateExpr(sb, upd.Where)
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
	translateExpr(sb, j.On)
	return nil
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
	if e == nil {
		return errors.New("expression was nil")
	}
	var err error
	switch e.Expr.(type) {
	case *pb.Expr_Lit:
		err = translateExprLit(sb, e.GetLit())
	case *pb.Expr_Col:
		err = translateExprCol(sb, e.GetCol())
	case *pb.Expr_UnaryExpr:
		err = translateExprUnaryExpr(sb, e.GetUnaryExpr())
	case *pb.Expr_BinaryExpr:
		err = translateExprBinaryExpr(sb, e.GetBinaryExpr())
	default:
		err = fmt.Errorf("Unrecognized expression type: %T", e.Expr)
	}
	return err
}

func translateExprLit(sb *strings.Builder, lit *pb.Lit) error {
	switch lit.Lit.(type) {
	case *pb.Lit_Str:
		sb.WriteString(lit.GetStr())
	case *pb.Lit_Num:
		sb.WriteString(strconv.FormatFloat(lit.GetNum(), 'f', -1, 64))
	case *pb.Lit_Boolean:
		sb.WriteString(strconv.FormatBool(lit.GetBoolean()))
	case *pb.Lit_Null:
		sb.WriteString("NULL")
	default:
		return fmt.Errorf("Unsupported literal type: %T", lit.Lit)
	}
	return nil
}

func translateExprCol(sb *strings.Builder, col *pb.Col) error {
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
	return nil
}

func translateExprUnaryExpr(sb *strings.Builder, ue *pb.UnaryExpr) error {
	switch ue.Op {
	case pb.UnaryOp_NOT:
		sb.WriteString("NOT ")
	case pb.UnaryOp_POS:
		sb.WriteString("+")
	case pb.UnaryOp_NEG:
		sb.WriteString("-")
	default:
		return fmt.Errorf("Unrecognized unary op: %d", ue.Op)
	}
	return translateExpr(sb, ue.Expr)
}

func translateExprBinaryExpr(sb *strings.Builder, be *pb.BinaryExpr) error {
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
	case pb.BinaryOp_AND:
		sb.WriteString(" AND ")
	case pb.BinaryOp_OR:
		sb.WriteString(" OR ")
	case pb.BinaryOp_IS:
		sb.WriteString(" IS ")
	case pb.BinaryOp_IS_NOT:
		sb.WriteString(" IS NOT ")
	default:
		return fmt.Errorf("Unrecognized binary op: %d", be.Op)
	}
	err = translateExpr(sb, be.Expr2)
	if err != nil {
		return err
	}
	return nil
}
