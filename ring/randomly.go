// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-03 13:11 (EDT)
// Function:

package ring

type randString struct {
	count int
	p     string
}

func (rp *randString) maybe(p string) {

	rp.count++

	if random_n(rp.count) == 0 {
		rp.p = p
	}
}

func (rp *randString) use() string {
	return rp.p
}

func (rp *randString) len() int {
	return rp.count
}
