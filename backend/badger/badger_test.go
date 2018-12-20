// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-10 16:52 (EDT)
// Function:

package badger

import (
	"fmt"
	"testing"
)

const N = 100

func TestDB(t *testing.T) {

	db, err := Open("test", "/tmp/testdb")
	if err != nil {
		t.Fatalf("cannot open: %v\n", err)
	}

	for i := 0; i < N; i++ {
		key := fmt.Sprintf("key%d", i)
		db.Put("T", key, []byte("value value"))
		v, _, _ := db.Get("T", key)

		if string(v) != "value value" {
			t.Fatalf("put/get faile %v\n", v)
		}
	}

	n := 0
	db.Range("T", "", "", func(k string, v []byte) bool {
		n++
		return true
	})

	if n != N {
		t.Fatalf("only %d found\n", n)
	}
}
