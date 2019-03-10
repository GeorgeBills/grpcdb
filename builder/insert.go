package builder

import (
	pb "github.com/GeorgeBills/grpcdb/api"
)

type InsertStatementBuilder struct {
	insert *pb.Insert
	err    error
}

// NewInsert returns a new insert statement builder.
func NewInsert(into *pb.SchemaTable, columns ...string) *InsertStatementBuilder {
	return &InsertStatementBuilder{
		insert: &pb.Insert{
			Into:    into,
			Columns: columns,
		},
	}
}

// Statement returns either the correctly built statement or the first error
// that occurred.
func (sb *InsertStatementBuilder) Statement() (*pb.Statement, error) {
	return Statement(&pb.Statement{Statement: &pb.Statement_Insert{Insert: sb.insert}}, sb.err)
}

func (sb *InsertStatementBuilder) Values(literals [][]string) *InsertStatementBuilder {
	vals := &pb.Values{}
	for _, row := range literals {
		newRow := &pb.Row{}
		for _, lit := range row {
			newVal := Lit(lit)
			newRow.Values = append(newRow.Values, newVal)
		}
		vals.Rows = append(vals.Rows, newRow)
	}
	sb.insert.ToInsert = &pb.ToInsert{
		Insert: &pb.ToInsert_Values{
			Values: vals,
		},
	}
	return sb
}

func (sb *InsertStatementBuilder) From(ssb *SelectStatementBuilder) *InsertStatementBuilder {
	sel, err := ssb.Select()
	if err != nil {
		sb.err = err
		return sb
	}
	sb.insert.ToInsert = &pb.ToInsert{
		Insert: &pb.ToInsert_Select{
			Select: sel,
		},
	}
	return sb
}
