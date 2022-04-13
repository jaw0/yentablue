// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-20 16:05 (EDT)
// Function: wire up the selected backends to the store layer

package main

import (
	"github.com/jaw0/yentablue/backend/badger"
	"github.com/jaw0/yentablue/backend/goleveldb"
	"github.com/jaw0/yentablue/backend/sqlite"
	"github.com/jaw0/yentablue/config"
	"github.com/jaw0/yentablue/database"
	"github.com/jaw0/yentablue/gclient"
	"github.com/jaw0/yentablue/store"
)

const (
	BASEDIR     = "/var/db"
	CONFBACKEND = "sqlite"
)

func initDBs(cf *config.Config, myid string, mydc string, myrack string) *store.SDB {

	sdb := store.New()

	cfidx := -1

	// make sure _conf is configured first
	for i, c := range cf.Database {
		if c.Name == "_conf" {
			cfidx = i
		}
	}

	if cfidx != -1 && cfidx != 0 {
		// swap it to the front
		cf.Database[0], cf.Database[cfidx] = cf.Database[cfidx], cf.Database[0]
	}
	if cfidx == -1 {
		// create default config for _conf

		c := &config.DBConf{
			Name:      "_conf",
			Pathname:  "_conf",
			Backend:   CONFBACKEND,
			CacheSize: -1, // no cache
		}

		cf.Database = append([]*config.DBConf{c}, cf.Database...)
	}

	basedir := BASEDIR
	if cf.Basedir != "" {
		basedir = cf.Basedir
	}

	grpcParam := &gclient.Defaults{
		TLS: tlsConfig,
	}

	for _, c := range cf.Database {
		be := openBE(c, basedir)
		sdb.Add(c.Name, database.Open(be, database.Conf{
			Name:      c.Name,
			Secret:    c.Secret,
			Expire:    uint64(c.Expire),
			MyId:      myid,
			DC:        mydc,
			Rack:      myrack,
			SDB:       sdb,
			Grpcp:     grpcParam,
			CacheSize: c.CacheSize,
		}))
	}

	return sdb
}

func openBE(cf *config.DBConf, dir string) database.BackEnder {

	var file string

	if cf.Pathname[0] == '/' {
		file = cf.Pathname
	} else {
		file = dir + "/" + cf.Pathname
	}

	switch cf.Backend {
	case "sqlite":
		be, err := sqlite.Open(cf.Name, file)

		if err != nil {
			dl.Fatal("sqlite failed: cannot open '%s': %v", file, err)
		}
		return be
	case "leveldb":
		be, err := goleveldb.Open(cf.Name, file)

		if err != nil {
			dl.Fatal("leveldb failed: cannot open '%s': %v", file, err)
		}
		return be
	case "badger":
		be, err := badger.Open(cf.Name, file)

		if err != nil {
			dl.Fatal("badger failed: cannot open '%s': %v", file, err)
		}
		return be

	default:
		dl.Fatal("unknown backend '%s'", cf.Backend)
	}
	// not reached
	return nil
}
