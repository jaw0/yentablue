// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-20 13:55 (EDT)
// Function: api

package main

import (
	"encoding/json"
	"net/http"

	"golang.org/x/net/context"

	"github.com/jaw0/kibitz"

	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/putstatus"
	"github.com/jaw0/yentablue/shard"
)

func init() {
	// simplified client api
	http.HandleFunc("/api/yb/hb", wwwApiHB)
	http.HandleFunc("/api/yb/get", wwwApiGet)
	http.HandleFunc("/api/yb/put", wwwApiPut)
	http.HandleFunc("/api/yb/range", wwwApiRange)
	http.HandleFunc("/api/yb/servers", wwwApiServers)
	http.HandleFunc("/api/yb/ringcf", wwwApiRingConf)
}

func (*myServer) SendHB(ctx context.Context, hb *acproto.ACPHeartBeatRequest) (*acproto.ACPHeartBeatReply, error) {

	if hb.Myself != nil {
		pdb.UpdateSceptical(hb.Myself)
	}

	res := &acproto.ACPHeartBeatReply{StatusCode: 200, StatusMessage: "OK"}
	pwd := pdb.GetAll()

	for _, p := range pwd {
		pd := p.GetData()
		w := pd.(*acproto.ACPHeartBeat)
		res.Hbinfo = append(res.Hbinfo, w)
	}

	res.Hbinfo = append(res.Hbinfo, Myself(pdb.MyInfo()))

	return res, nil
}

func addServer(res *acproto.ACPY2ServerReply, w *acproto.ACPHeartBeat, isUp bool) {

	ii := w.PeerInfo
	isLocal := ii.GetDatacenter() == pdb.Datacenter()

	dbName := make([]string, len(w.Database))
	for i, dr := range w.Database {
		dbName[i] = dr.Name
	}

	r := &acproto.ACPY2ServerData{
		Subsystem:      ii.Subsystem,
		Environment:    ii.Environment,
		ServerId:       ii.ServerId,
		Datacenter:     ii.Datacenter,
		Rack:           ii.Rack,
		Hostname:       ii.Hostname,
		TimeLastUp:     ii.TimeLastUp,
		TimeUpSince:    ii.TimeUpSince,
		NetInfo:        ii.NetInfo,
		IsUp:           isUp,
		IsLocal:        isLocal,
		SortMetric:     w.SortMetric,
		CpuMetric:      w.CpuMetric,
		CapacityMetric: w.CapacityMetric,
		Database:       dbName,
		Uptodate:       w.Uptodate,
	}

	res.Data = append(res.Data, r)
}

func (*myServer) Servers(ctx context.Context, req *acproto.ACPY2ServerRequest) (*acproto.ACPY2ServerReply, error) {

	pwd := pdb.GetAll()
	res := &acproto.ACPY2ServerReply{}

	for _, p := range pwd {
		pd := p.GetData()
		w := pd.(*acproto.ACPHeartBeat)
		ii := w.PeerInfo

		if req.Subsystem != "" && req.GetSubsystem() != ii.GetSubsystem() {
			continue
		}
		if req.Environment != "" && req.GetEnvironment() != ii.GetEnvironment() {
			continue
		}
		if req.Hostname != "" && req.GetHostname() != ii.GetHostname() {
			continue
		}
		if req.Datacenter != "" && req.GetDatacenter() != ii.GetDatacenter() {
			continue
		}
		if req.ServerId != "" && req.GetServerId() != ii.GetServerId() {
			continue
		}

		isUp := w.PeerInfo.GetStatusCode() == int32(kibitz.STATUS_UP)

		addServer(res, w, isUp)
	}

	addServer(res, Myself(pdb.MyInfo()), true)

	return res, nil
}

func (*myServer) GetMerkle(ctx context.Context, hb *acproto.ACPY2CheckRequest) (*acproto.ACPY2CheckReply, error) {

	mt, err := sdb.GetMerkle(hb.GetMap(), int(hb.GetLevel()), uint16(hb.GetTreeid()), hb.GetVersion())
	if err != nil {
		return nil, err
	}

	res := &acproto.ACPY2CheckReply{}

	for _, m := range mt {
		v := &acproto.ACPY2CheckValue{
			Map:      m.Map,
			Level:    int32(m.Level),
			Treeid:   uint32(m.TreeID),
			Shard:    m.Shard,
			Version:  m.Version,
			Keycount: m.KeyCount,
			Children: int32(m.Children),
			Isvalid:  m.IsValid,
		}

		if len(m.Hash) != 0 {
			v.Hash = m.Hash
		}
		if len(m.Key) != 0 {
			v.Key = m.Key
		}

		res.Check = append(res.Check, v)
	}

	return res, nil
}

func (*myServer) Get(ctx context.Context, req *acproto.ACPY2GetSet) (*acproto.ACPY2GetSet, error) {

	for _, r := range req.Data {
		if r.Shard == 0 {
			// was it left out? or actually 0?
			s := shard.Hash(r.GetKey())
			if s != 0 {
				dl.Debug("added missing shard")
			}
			r.Shard = s
		}

		_, err := sdb.Get(r)

		if err != nil {
			return req, err
		}
	}

	return req, nil
}

func (*myServer) Range(ctx context.Context, req *acproto.ACPY2GetRange) (*acproto.ACPY2GetSet, error) {

	res := &acproto.ACPY2GetSet{}

	_, err := sdb.GetRange(req, res)
	return res, err
}

func (*myServer) Put(ctx context.Context, req *acproto.ACPY2DistRequest) (*acproto.ACPY2DistReply, error) {

	// runmode.is_stopping => 500

	r := req.Data

	if r.Shard == 0 {
		// was it left out? or actually 0?
		s := shard.Hash(r.GetKey())
		if s != 0 {
			dl.Debug("added missing shard")
		}
		r.Shard = s
	}
	status, loc := sdb.Put(r)

	switch status {
	case putstatus.DONE:
		if loc.IsLocal {
			sdb.Distrib(loc, req, func(int) {})
		} else {
			// this does not belong on this server.
			// save + distribute, after others have it, delete
			sdb.Distrib(loc, req, func(status int) {
				if status == putstatus.DONE {
					sdb.Del(r)
				}
			})
			// ...
		}
		// RSN - push update to other clients, chans, ...
	case putstatus.NOTME:
		// try to handoff to the correct server
		// RSN - configurable option - handoff or return 302 redirect
		// or write to disk, or ...
		sdb.Handoff(req)
	}

	return &acproto.ACPY2DistReply{
		StatusCode:    200,
		StatusMessage: "OK",
		ResultCode:    int32(status),
	}, nil
}

func (*myServer) RingConf(ctx context.Context, req *acproto.ACPY2RingConfReq) (*acproto.ACPY2RingConfReply, error) {

	res, err := sdb.GetRingConf(req.GetMap(), req.GetDatacenter())

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (*myServer) ShutDown(ctx context.Context, req *acproto.ACPY2Empty) (*acproto.ACPHeartBeatReply, error) {

	close(shutdown)
	res := &acproto.ACPHeartBeatReply{StatusCode: 200, StatusMessage: "OK"}
	return res, nil
}

// ################################################################

// make the api available as json post api
func wwwApiConvert(w http.ResponseWriter, r *http.Request, req interface{}, f func() (interface{}, error)) {

	err := json.NewDecoder(r.Body).Decode(req)
	r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	res, err := f()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func wwwApiHB(w http.ResponseWriter, r *http.Request) {

	req := &acproto.ACPHeartBeatRequest{}
	wwwApiConvert(w, r, req, func() (interface{}, error) {
		x := &myServer{}
		return x.SendHB(nil, req)
	})
}

func wwwApiGet(w http.ResponseWriter, r *http.Request) {

	req := &acproto.ACPY2GetSet{}
	wwwApiConvert(w, r, req, func() (interface{}, error) {
		x := &myServer{}
		return x.Get(nil, req)
	})
}

func wwwApiPut(w http.ResponseWriter, r *http.Request) {

	req := &acproto.ACPY2DistRequest{}
	wwwApiConvert(w, r, req, func() (interface{}, error) {
		x := &myServer{}
		return x.Put(nil, req)
	})
}

func wwwApiRange(w http.ResponseWriter, r *http.Request) {

	req := &acproto.ACPY2GetRange{}
	wwwApiConvert(w, r, req, func() (interface{}, error) {
		x := &myServer{}
		return x.Range(nil, req)
	})
}

func wwwApiServers(w http.ResponseWriter, r *http.Request) {

	req := &acproto.ACPY2ServerRequest{}
	wwwApiConvert(w, r, req, func() (interface{}, error) {
		x := &myServer{}
		return x.Servers(nil, req)
	})
}

func wwwApiRingConf(w http.ResponseWriter, r *http.Request) {

	req := &acproto.ACPY2RingConfReq{}
	wwwApiConvert(w, r, req, func() (interface{}, error) {
		x := &myServer{}
		return x.RingConf(nil, req)
	})
}
