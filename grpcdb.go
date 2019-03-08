package grpcdb

import (
	"fmt"
	"strings"
)

//go:generate protoc -I . --go_out=plugins=grpc:. grpcdb.proto

// Translate takes a grpcdb.Statement and returns SQL.
func Translate(s *Statement) (string, error) {
	switch s.Statement.(type) {
	case *Statement_Select:
		sel := s.GetSelect()
		return fmt.Sprintf("SELECT %s FROM %s", strings.Join(sel.ResultColumn, ", "), sel.From), nil
	default:
		return "", fmt.Errorf("Unrecognized statement type: %T", s.Statement)
	}
}
