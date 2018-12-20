// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-17 09:38 (EST)
// Function:

package database

import (
	"fmt"
	"testing"

	"github.com/jaw0/yentablue/crypto"
)

const (
	PASS = "password1234"
	TEXT = `Lorem ipsum dolor sit amet, consectetur adipiscing elit,
                sed do eiusmod tempor incididunt ut labore et dolore magna aliqua`
)

func TestCrypto(t *testing.T) {

	db := &DB{}
	db.enigma = crypto.New(PASS)

	w := &Record{
		Flags:   0xAAAAAAAA,
		Shard:   0x12345678,
		Version: 0x9876543210abcdef,
		Expire:  0x5555AAAA5555AAAA,
		Value:   []byte(TEXT),
	}

	b := db.record2bytes(w)
	//fmt.Printf("%x %s\n", b, b)

	r := db.bytes2record(b)
	// fmt.Printf("%+v\n", r)

	if string(r.Value) != TEXT {
		fmt.Printf("%s != %s", r.Value, TEXT)
		t.Fail()
	}

	if r.Version != w.Version {
		t.Fatalf("mismatch\n")
	}

	if false {
		fmt.Printf("ok\n")
	}
}
