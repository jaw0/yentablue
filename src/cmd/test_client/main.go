// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jun-28 11:02 (EDT)
// Function: send hb

package main

import (
	//"flag"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	//"config"
	//"diag"
	"kibitz/kpproto"
	"kibitz/peer"
	"proto"
)

func main() {

	ac, err := newClient()
	if err != nil {
		fmt.Printf("error %v\n", err)
	} else {
		fmt.Printf("ok %v\n", ac)
	}

	t0 := time.Now()

	for i := 0; i < 10; i++ {
		_, _ = ac.SendHB(context.Background(), &acproto.ACPHeartBeatRequest{
			Myself: &acproto.ACPHeartBeat{
				PeerInfo: &kpproto.ACPPeerInfo{
					StatusCode: proto.Int32(2),
					Subsystem:  proto.String("test"),
					ServerId:   proto.String("client@localhost"),
				}}})
	}

	dt := time.Now().Sub(t0)

	//fmt.Printf("res %v err %v; dt %v\n", res, err, dt)
	fmt.Printf("dt %v\n", dt)
}

func newClient() (acproto.ACrpcClient, error) {

	conn, err := grpc.Dial("127.0.0.1:5301", grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := acproto.NewACrpcClient(conn)

	return client, nil
}
