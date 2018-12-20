// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-11 17:21 (EDT)
// Function:

package main

import (
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func startServer(port int) *grpc.Server {

	addr := fmt.Sprintf(":%d", port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		dl.Fatal("failed to listen: %v", err)
	}

	dl.Verbose("listening on tcp/%s", addr)

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(10))

	acproto.RegisterACrpcServer(grpcServer, &myServer{})
	go grpcServer.Serve(lis)

	return grpcServer
}

func newClient() {

	var opt []grpc.DialOption
	opt = append(opt, grpc.WithInsecure())

	conn, derr := grpc.Dial(addr, opt...)

	if err != nil {
		return nil, nil, err
	}

	client := acproto.NewACrpcClient(conn)
	return client, func() { conn.Close() }, nil

}
