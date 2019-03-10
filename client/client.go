package main

import (
	"context"
	"github.com/GeorgeBills/grpcdb/api"
	. "github.com/GeorgeBills/grpcdb/builder"
	"google.golang.org/grpc"
	"log"
	"time"
)

const (
	target = "localhost:1234"
)

func main() {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := grpcdbpb.NewGRPCDBClient(conn)
	statement, err := Select("person", "full_name").
		OrderBy(Col("birth"), grpcdbpb.OrderingDirection_DESC).
		Statement()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer cancel()
	result, err := client.Query(ctx, statement)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Result: %v", result)
}
