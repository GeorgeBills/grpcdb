package main

import (
	"context"
	"github.com/GeorgeBills/grpcdb"
	"github.com/GeorgeBills/grpcdb/api"
	"google.golang.org/grpc"
	"log"
	"net"
)

const (
	listen = ":1234"
)

func main() {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	handler := &handler{}
	grpcdbpb.RegisterGRPCDBServer(server, handler)
	server.Serve(lis)
}

type handler struct{}

func (h *handler) Query(ctx context.Context, statement *grpcdbpb.Statement) (*grpcdbpb.Result, error) {
	log.Printf("Received statement: %+v", statement)
	sql, err := grpcdb.TranslateStatement(statement)
	if err != nil {
		log.Printf("Error translating statement: %v", err)
		return nil, err
	}
	log.Printf("Running statement: %s", sql)
	return &grpcdbpb.Result{}, nil
}
