// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jun-16 14:08 (EDT)
// Function:

package main

import (
	"flag"
	"fmt"
	"time"

	"config"
	"diag"
	"store"
	"store/backend/sqlite"
	"store/database"
)

var dl = diag.Logger("main")
var configfile = ""

func main() {

	flag.StringVar(&configfile, "c", "", "config file")
	flag.Parse()

	if configfile == "" {
		dl.Fatal("no config specified!\ntry -c config\n")
	}

	config.Init(configfile, diag.Logger("config"))
	dl.Init("prog")
	dl.Verbose("starting...")

	cf := config.Cf()
	stdb := initDBs(cf)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		stdb.Put(&database.Record{
			Map:     "cmdb",
			Key:     key,
			Version: database.Now(),
			Shard:   0x1234,
			Value:   []byte("value value value"),
		})

		//time.Sleep(1000000 * time.Nanosecond)
	}

	res := database.Record{Map: "cmdb", Key: "key1234"}
	stdb.Get(&res)
	dl.Debug("=> %v; %s", &res, string(res.Value))

	stdb.Close()
	time.Sleep(time.Nanosecond)
}

//################################################################

func initDBs(cf *config.Config) *store.SDB {

	sdb := store.New()

	for _, c := range cf.Database {
		be := openBE(c)
		sdb.Add(c.Name, database.Open(be, database.Conf{
			Expire:   uint64(c.Expire),
			Replicas: c.Replicas,
			RingBits: c.RingBits,
		}))
	}

	return sdb
}

func openBE(cf *config.DBConf) database.BackEnder {

	switch cf.Backend {
	case "sqlite":
		be, err := sqlite.Open(cf.Name, cf.Pathname)

		if err != nil {
			dl.Fatal("sqlite failed: %v", err)
		}
		return be
	default:
		dl.Fatal("unknown backend '%s'", cf.Backend)
	}
	// not reached
	return nil
}
