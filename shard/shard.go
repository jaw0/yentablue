// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-01 16:53 (EDT)
// Function:

package shard

import (
	"github.com/spaolacci/murmur3"
)

func Hash(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}
