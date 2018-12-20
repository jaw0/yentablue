// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jun-28 11:02 (EDT)
// Function: test get

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/jaw0/acgo/diag"

	"github.com/jaw0/yentablue/proto"
)

func main() {

	var dbname string
	var server string

	flag.StringVar(&dbname, "m", "cmdb", "database name")
	flag.StringVar(&server, "h", "127.0.0.1:5301", "server")
	flag.Parse()

	conn, err := newClient(server)
	if err != nil {
		fmt.Printf("error %v\n", err)
		diag.Fatal("%v", err)
	}

	diag.SetConfig(&diag.Config{
		Debug: map[string]bool{"all": true},
	})

	ac := acproto.NewACrpcClient(conn)

	for {
		key := fmt.Sprintf("key%d", 1)

		_, err := ac.Get(context.Background(), &acproto.ACPY2GetSet{
			Data: []*acproto.ACPY2MapDatum{{
				Map: proto.String(dbname),
				Key: proto.String(key),
			}}})

		if err == nil {
			fmt.Printf(".")
		}
		if err != nil {
			fmt.Printf("error %v\n", err)
		}

		time.Sleep(time.Second / 2)
	}

}

func newClient(server string) (*grpc.ClientConn, error) {

	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return conn, nil
}
