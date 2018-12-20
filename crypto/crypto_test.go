// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-16 15:20 (EST)
// Function:

package crypto

import (
	"fmt"
	"testing"
)

const (
	PASS   = "password1234"
	SECRET = "squeamish ossifrage"
	CHECK  = "drink ovaltine"
)

func TestCrypto(t *testing.T) {

	b := New(PASS)

	ct, err := b.Encrypt([]byte(SECRET), nil, []byte(CHECK))
	if err != nil {
		t.Fatalf("ERROR: %v\n", err)
	}

	pt, err := b.Decrypt(ct, nil, []byte(CHECK))
	if err != nil {
		t.Fatalf("ERROR: %v\n", err)
	}

	if string(pt) != SECRET {
		t.Fatalf("FAIL: %s != %s", SECRET, pt)
	}

	_, err = b.Decrypt(ct, nil, []byte(""))
	if err == nil {
		t.Fatalf("invalid check should fail")
	}

	_, err = b.Decrypt(ct, []byte("abcd"), []byte(CHECK))
	if err == nil {
		t.Fatalf("invalid check should fail")
	}

	if false {
		fmt.Printf("%s\n", pt)
	}
}
