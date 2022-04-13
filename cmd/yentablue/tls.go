// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-16 14:43 (EDT)
// Function:

package main

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/jaw0/yentablue/config"
)

func configureTLS() *tls.Config {

	cf := config.Cf()

	if cf.TLS_cert == "" || cf.TLS_key == "" {
		return nil
	}

	certificate, err := tls.LoadX509KeyPair(cf.TLS_cert, cf.TLS_key)
	if err != nil {
		dl.Fatal("tls cert error: %v", err)
	}

	tcf := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		Rand:         rand.Reader,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	if cf.TLS_root != "" {

		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(cf.TLS_root)
		if err != nil {
			dl.Fatal("cannot load root cert: %v", err)
		}

		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			dl.Fatal("cannot load root cert")
		}

		tcf.RootCAs = certPool
		tcf.ClientCAs = certPool

	}

	return tcf
}
