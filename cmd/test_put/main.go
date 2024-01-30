// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jun-28 11:02 (EDT)
// Function: send hb

package main

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/jaw0/acdiag"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/shard"
	"github.com/jaw0/yentablue/soty"
)

func main() {

	var numput int
	var concur int
	var dbname string
	var server string
	var verbose bool

	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.IntVar(&numput, "n", 10, "count")
	flag.IntVar(&concur, "c", 1, "concurrency")
	flag.StringVar(&dbname, "m", "cmdb", "database name")
	flag.StringVar(&server, "h", "127.0.0.1:5301", "server")
	flag.Parse()

	conn, err := newClient(server)
	if err != nil {
		fmt.Printf("error %v\n", err)
		return
	}

	diag.SetConfig(diag.Config{
		Debug: map[string]bool{"all": true},
	})

	var wg sync.WaitGroup
	t0 := time.Now()

	for c := 0; c < concur; c++ {
		wg.Add(1)
		go func(c int) {

			ac := acproto.NewACrpcClient(conn)

			defer wg.Done()
			num := numput / concur

			for i := 0; i < num; i++ {
				key := fmt.Sprintf("key%d", i+c*num)

				_, err := ac.Put(context.Background(), &acproto.ACPY2DistRequest{
					Hop:    0,
					Expire: soty.Now() + uint64(5*time.Second),
					Sender: "localhost",
					Data: &acproto.ACPY2MapDatum{
						Map:     dbname,
						Key:     key,
						Shard:   shard.Hash(key),
						Version: soty.Now(),
						Value:   []byte("value value value"),
					}})

				if err != nil {
					fmt.Printf("ERROR: %v\n", err)
					runtime.Goexit()
				}
				if verbose {
					diag.Verbose("put %s", key)
				}
			}
		}(c)
	}

	wg.Wait()
	dt := time.Now().Sub(t0)

	//fmt.Printf("res %v err %v; dt %v\n", res, err, dt)
	fmt.Printf("dt %v\n", dt)
}

func newClient(server string) (*grpc.ClientConn, error) {

	conn, err := grpc.Dial(server, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return conn, nil
}
