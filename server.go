package main

import (
	"context"
	"log"

	pb "github.com/KDF5000/flease/proto"
)

// server is used to implement helloworld.GreeterServer.
type Server struct {
	pb.UnimplementedLeaseServer
	leaseManager *LeaseManager
}

func NewServer(node uint64, peers string, expired, e uint32) (*Server, error) {
	m, err := NewLeaseManager(expired, e, node, peers)
	if err != nil {
		return nil, err
	}
	return &Server{
		leaseManager: m,
	}, nil
}

func (s *Server) Start() {
	s.leaseManager.Start()
}

// SayHello implements helloworld.GreeterServer
func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *Server) Read(ctx context.Context, in *pb.ReadRequest) (*pb.ReadResponse, error) {
	var resp pb.ReadResponse
	res, ok := s.leaseManager.Read(in.GetK())
	if !ok {
		resp.Status = pb.Status_ABORT
		return &resp, nil
	}

	resp.Status = pb.Status_COMMIT
	resp.LastWrite = res.LastWrite
	resp.Lease = &pb.LeaseValue{
		Node:      res.Value.Node,
		LeaseTime: res.Value.LeaseTime,
	}

	return &resp, nil
}

// write lease
func (s *Server) Write(ctx context.Context, in *pb.WriteRequest) (*pb.WriteResponse, error) {
	var resp pb.WriteResponse
	lease := in.GetLease()
	logF(s.leaseManager.node, "receive write request, k: %d, lease: %+v", in.GetK(), lease)
	ok := s.leaseManager.Write(in.GetK(), LeaseValue{
		Node:      lease.GetNode(),
		LeaseTime: lease.GetLeaseTime(),
	})

	if ok {
		resp.Status = pb.Status_COMMIT
	} else {
		resp.Status = pb.Status_ABORT
	}

	return &resp, nil
}
