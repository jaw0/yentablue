// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-16 10:54 (EDT)
// Function: handle network connections

package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/soheilhy/cmux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	gpeer "google.golang.org/grpc/peer"

	"github.com/jaw0/acgo/diag"

	"github.com/jaw0/yentablue/config"
	"github.com/jaw0/yentablue/proto"
)

var dls = diag.Logger("server")

type Server struct {
	tcp    net.Listener
	grpcs  *grpc.Server
	grpcss *grpc.Server
	https  *http.Server
}

func startServer() *Server {

	cf := config.Cf()
	port := cf.Port_Server

	if port == 0 {
		dls.Fatal("no port specified in config!")
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		dls.Fatal("error: %v", err)
	}

	maxgo := cf.Net_threads
	if maxgo == 0 {
		maxgo = 20
	}

	dl.Verbose("starting network on tcp/%d as id %s (%s)", port, pdb.Id(), pdb.Env())
	return startMux(l, uint32(maxgo))

}

// grpc, https, http
func startMux(l net.Listener, maxgo uint32) *Server {

	s := &Server{tcp: l}

	// insecure grpc
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(aclInterceptor))
	acproto.RegisterACrpcServer(grpcServer, &myServer{})
	// secure
	grpcSServer := grpc.NewServer(grpc.UnaryInterceptor(statsInterceptor))
	acproto.RegisterACrpcServer(grpcSServer, &myServer{})

	httpServer := &http.Server{Handler: s}

	s.grpcs = grpcServer
	s.grpcss = grpcSServer
	s.https = httpServer

	m := cmux.New(l)

	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())
	tlspl := m.Match(cmux.Any())

	tlsl := tls.NewListener(tlspl, tlsConfig)

	mSec := cmux.New(tlsl)
	grpcSecL := mSec.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpSecL := mSec.Match(cmux.HTTP1Fast())

	go grpcServer.Serve(grpcL)
	go httpServer.Serve(httpL)
	go grpcSServer.Serve(grpcSecL)
	go httpServer.Serve(httpSecL)

	go m.Serve()
	go mSec.Serve()

	return s
}

func startNoSSL(l net.Listener, maxgo uint32) *Server {

	s := &Server{tcp: l}

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(maxgo), grpc.UnaryInterceptor(aclInterceptor))
	acproto.RegisterACrpcServer(grpcServer, &myServer{})

	httpServer := &http.Server{Handler: s}

	s.grpcs = grpcServer
	s.https = httpServer

	m := cmux.New(l)

	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	go grpcServer.Serve(grpcL)
	go httpServer.Serve(httpL)

	go m.Serve()

	return s

}

func (s *Server) Shutdown() {

	go s.grpcs.GracefulStop()
	if s.grpcss != nil {
		go s.grpcss.GracefulStop()
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	s.https.Shutdown(ctx)

	time.Sleep(5 * time.Second)
	s.grpcs.Stop()

	if s.grpcss != nil {
		s.grpcss.Stop()
	}

	s.tcp.Close()

	// NB - there is no way to shutdown cmux
}

// ################################################################

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	dls.Debug("http %s -> %v", req.RemoteAddr, req)

	if !aclAllowed(req.RemoteAddr) {
		http.Error(w, "denied. so sorry.", http.StatusForbidden)
		return
	}

	w.Header().Set("Server", SUBSYS)
	http.DefaultServeMux.ServeHTTP(w, req)
}

// ################################################################

func aclInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {

	p, _ := gpeer.FromContext(ctx)
	dls.Debug("connection from %v", p.Addr)

	if !aclAllowed(p.Addr.String()) {
		return
	}
	return statsInterceptor(ctx, req, info, handler)
}
