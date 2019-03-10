package main

import (
	"context"
	"database/sql"
	"github.com/GeorgeBills/grpcdb"
	"github.com/GeorgeBills/grpcdb/api"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"log"
	"net"
)

const (
	listen         = ":1234"
	dataSourceName = "host=127.0.0.1 port=5432 user=postgres password=chbqkWQQkgEJh2 dbname=postgres sslmode=disable"
)

func main() {
	// listen on socket
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}

	// get database connection
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// check that the database is connected
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Database connected")

	// start server
	server := grpc.NewServer()
	handler := &handler{
		db,
	}
	grpcdbpb.RegisterGRPCDBServer(server, handler)
	server.Serve(lis)
}

type handler struct {
	db *sql.DB
}

func (h *handler) Query(ctx context.Context, statement *grpcdbpb.Statement) (*grpcdbpb.Result, error) {
	log.Printf("Received statement: %+v", statement)
	sql, err := grpcdb.TranslateStatement(statement)
	if err != nil {
		log.Printf("Error translating statement: %v", err)
		return nil, err
	}
	log.Printf("Running statement: %s", sql)
	res, err := h.db.Exec(sql)
	if err != nil {
		log.Printf("Error running statement: %v", err)
	}
	log.Print(res)
	return &grpcdbpb.Result{}, nil
}
