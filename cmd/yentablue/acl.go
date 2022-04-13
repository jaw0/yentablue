// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-14 21:02 (EDT)
// Function:

package main

import (
	"net"

	"github.com/jaw0/yentablue/config"
)

func aclAllowed(client string) bool {

	ip, _, err := net.SplitHostPort(client)
	if err != nil {
		dl.Verbose("cannot parse client addr '%s': %v", client, err)
		return false
	}

	addr := net.ParseIP(ip)

	if addr.IsLoopback() {
		return true
	}

	cf := config.Cf()

	for _, a := range cf.Allow {
		_, ipm, err := net.ParseCIDR(a)
		if err != nil {
			dl.Problem("cannot parse configured allow '%s': %v", a, err)
			continue
		}

		if ipm.Contains(addr) {
			return true
		}
	}

	return false
}
