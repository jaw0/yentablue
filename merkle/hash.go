// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-18 11:46 (EDT)
// Function:

package merkle

import (
	"crypto/md5"
	"fmt"
)

func leafHash(leaves *LeafSaves) []byte {

	h := md5.New()

	for _, l := range leaves.Save {
		fmt.Fprintf(h, "%016X %s\n", l.Version, l.Key)
	}

	return h.Sum(nil)
}

// hash is hash of child hashes
func nodeHash(no *NodeSaves) []byte {

	h := md5.New()

	for _, n := range no.Save {
		h.Write([]byte(n.Hash[:]))
	}

	return h.Sum(nil)
}
