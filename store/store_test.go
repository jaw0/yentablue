// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-10 09:16 (EST)
// Function:

package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/jaw0/yentablue/store/backend/sqlite"
	"github.com/jaw0/yentablue/store/database"
)

func TestStore(t *testing.T) {

	db := openDB()
	fmt.Printf("> %v\n", db)

}

func openDB() *database.DB {

	sdb := New()

	be, err := sqlite.Open("test", ":memory:")
	if err != nil {
		return nil
	}
	db := database.Open(be, database.Conf{
		Name:   "test",
		Expire: uint64(10 * time.Minute),
		MyId:   "test",
		DC:     "",
		Rack:   "",
		SDB:    sdb,
		//Grpcp:     grpcParam,
		CacheSize: -1,
	})

	return db
}
