// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-06 13:18 (EST)
// Function: export data to web

package main

import (
	"fmt"
	"net/http"

	"github.com/jaw0/kibitz"
)

// NB - access is permitted only to localhost + by 'allow' rules in the config file
func init() {
	http.HandleFunc("/status", wwwStatus)
	http.HandleFunc("/servers", wwwServers)
	http.HandleFunc("/loadave", wwwLoadAve)
	http.HandleFunc("/netreqs", wwwNetReqs)
}

// for your monitoring system to check
func wwwStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "status: OK\n")
}

func wwwLoadAve(w http.ResponseWriter, r *http.Request) {
	l := &load{}
	fmt.Fprintf(w, "loadave: %s\n", l)
}

func wwwNetReqs(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "netreqs: %s\n", requests)
}

// ################################################################

func wwwServerFmt(w http.ResponseWriter, px *kibitz.Export) {

	up := "up"
	if !px.IsUp {
		up = "dn"
	}

	fmt.Fprintf(w, "%s %-8s %-8s %-40s\n", up, px.Sys, px.Env, px.Id)
}

func wwwServers(w http.ResponseWriter, r *http.Request) {

	pwd := pdb.GetAll()

	for _, p := range pwd {

		px := p.GetExport()
		wwwServerFmt(w, px)
	}
	wwwServerFmt(w, pdb.GetExportSelf())
}
