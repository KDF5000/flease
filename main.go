package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	pb "github.com/KDF5000/flease/proto"
	"google.golang.org/grpc"
)

var (
	port   = flag.Int("port", 50051, "The server port")
	expire = flag.Uint("expire", 10, "lease expire duration(second)")
	e      = flag.Uint("e", 1000, "clock diff in diffrent servers(nano second)")
	node   = flag.Uint64("node", 1, "node id")
	peers  = flag.String("peers", "127.0.0.1:50051", "lease peers")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv, err := NewServer(*node, *peers, uint32(*expire), uint32(*e))
	if err != nil {
		log.Fatal(err)
	}

	// start background task
	go func() {
		srv.Start()
	}()

	s := grpc.NewServer()
	pb.RegisterLeaseServer(s, srv)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
