package grpcdb

import (
	pb "github.com/GeorgeBills/grpcdb/api"
)

// StatementBuilder supports fluent building of statements.
type StatementBuilder interface {
	Statement() (*pb.Statement, error)
}

type SelectStatementBuilder struct {
	sel *pb.Select // select is a keyword
	err error
}

type UpdateStatementBuilder struct {
	update *pb.Update
	err    error
}

type DeleteStatementBuilder struct {
	delete *pb.Delete
	err    error
}

type InsertStatementBuilder struct {
	insert *pb.Insert
	err    error
}

/*
 * A fully built statement looks something like:
 *
 *   &pb.Statement{
 *     Statement: &pb.Statement_Select{
 *       Select: &pb.Select{
 *         ResultColumn: []string{"x"},
 *         From: "t1",
 *         Join: []*pb.Join{
 *           pb.Join{
 *             Table: "t2",
 *             Expr &pb.Expr{
 *               Expr &pb.Expr_BinaryExpr{
 *                 BinaryExpr: &pb.BinaryExpr{
 *                   Expr1: &pb.Expr{Expr &pb.Expr_Col{Col: &pb.Col{Table: "t1", Column: "y"}}},
 *                   Op:  pb.BinaryOp_EQ,
 *                   Expr2: &pb.Expr{Expr &pb.Expr_Col{Col: &pb.Col{Table: "t2", Column: "z"}}},
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
func NewSelect(from string, columns ...string) *SelectStatementBuilder {
	return &SelectStatementBuilder{
		sel: &pb.Select{
			ResultColumn: columns,
			From:         from,
		},
	}
}

// NewInsert returns a new insert statement builder.
func NewInsert(into *pb.SchemaTable, toInsert *pb.ToInsert, columns ...string) *InsertStatementBuilder {
	return &InsertStatementBuilder{
		insert: &pb.Insert{
			Into:     into,
			Columns:  columns,
			ToInsert: toInsert,
		},
	}
}

// NewDelete returns a new delete statement builder.
func NewDelete(from *pb.SchemaTable) *DeleteStatementBuilder {
	return &DeleteStatementBuilder{
		delete: &pb.Delete{
			From: from,
		},
	}
}

// NewUpdate returns a new update statement builder.
func NewUpdate(table *pb.SchemaTable) *UpdateStatementBuilder {
	return &UpdateStatementBuilder{
		update: &pb.Update{
			Table: table,
		},
	}
}

// AddWhere adds a where clause.
func (sb *SelectStatementBuilder) AddWhere(expr *pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.sel.Where = And(sb.sel.Where, expr)
	return sb
}

func (sb *DeleteStatementBuilder) AddWhere(expr *pb.Expr) *DeleteStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.delete.Where = And(sb.delete.Where, expr)
	return sb
}

func (sb *UpdateStatementBuilder) AddWhere(expr *pb.Expr) *UpdateStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.update.Where = And(sb.update.Where, expr)
	return sb
}

func And(expr, and *pb.Expr) *pb.Expr {
	if expr == nil {
		return and
	}
	return NewBinaryExpression(expr, and, pb.BinaryOp_AND)
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

// AddJoinEq adds a join clause where two columns are equal.
func (sb *SelectStatementBuilder) AddJoinEq(table string, expr1, expr2 *pb.Expr) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	eq := NewBinaryExpression(expr1, expr2, pb.BinaryOp_EQ)
	return sb.AddJoin(table, eq)
}

// AddOrderBy adds an ordering clause.
func (sb *SelectStatementBuilder) AddOrderBy(expr *pb.Expr, dir pb.OrderingDirection) *SelectStatementBuilder {
	if sb.err != nil {
		return sb
	}
	sb.sel.OrderBy = append(sb.sel.OrderBy, &pb.OrderingTerm{
		By:  expr,
		Dir: dir,
	})
	return sb
}

// SetLimit sets the limit on the statement.
func (sb *SelectStatementBuilder) SetLimit(limit uint64) *SelectStatementBuilder {
	sb.sel.Limit = limit
	return sb
}

// SetOffset sets the offset on the statement.
func (sb *SelectStatementBuilder) SetOffset(offset uint64) *SelectStatementBuilder {
	sb.sel.Offset = offset
	return sb
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

func Statement(statement *pb.Statement, err error) (*pb.Statement, error) {
	if err != nil {
		return nil, err
	}
	return statement, nil
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *SelectStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Select{Select: sb.sel}}, sb.err)
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *InsertStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Insert{Insert: sb.insert}}, sb.err)
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *DeleteStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Delete{Delete: sb.delete}}, sb.err)
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *UpdateStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Update{Update: sb.update}}, sb.err)
}

// NewLiteralInsertValues returns rows of values for an insert statement.
func NewLiteralInsertValues(literals [][]string) *pb.ToInsert {
	vals := &pb.Values{}
	for _, row := range literals {
		newRow := &pb.Row{}
		for _, lit := range row {
			newVal := NewLiteral(lit)
			newRow.Values = append(newRow.Values, newVal)
		}
		vals.Rows = append(vals.Rows, newRow)
	}
	toInsert := &pb.ToInsert{
		Insert: &pb.ToInsert_Values{
			Values: vals,
		},
	}
	return toInsert
}

// NewTable returns a new schema table where only the table is set.
func NewTable(table string) *pb.SchemaTable {
	return NewSchemaTable("", table)
}

// NewSchemaTable returns a new schema table.
func NewSchemaTable(schema, table string) *pb.SchemaTable {
	return &pb.SchemaTable{
		Schema: schema,
		Table:  table,
	}
}

// NewColumn returns a new column expression where only the column is set.
func NewColumn(column string) *pb.Expr {
	return NewSchemaTableColumn("", "", column)
}

// NewTableColumn returns a new column expression where only the table and
// column are set.
func NewTableColumn(table, column string) *pb.Expr {
	return NewSchemaTableColumn("", table, column)
}

// NewSchemaTableColumn returns a new column expression where schema, table and
// column are all set.
func NewSchemaTableColumn(schema, table, column string) *pb.Expr {
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

// NewBinaryExpression returns a new binary expression.
func NewBinaryExpression(expr1, expr2 *pb.Expr, op pb.BinaryOp) *pb.Expr {
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

// NewLiteral returns a new literal.
func NewLiteral(lit string) *pb.Expr {
	return &pb.Expr{
		Expr: &pb.Expr_Lit{
			Lit: lit,
		},
	}
}
