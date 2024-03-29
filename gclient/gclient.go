// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Aug-10 20:07 (EDT)
// Function: wip client

package gclient

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/jaw0/yentablue/info"
	"github.com/jaw0/yentablue/proto"
	"github.com/jaw0/yentablue/shard"
	"github.com/jaw0/yentablue/soty"
)

type Defaults struct {
	TLS *tls.Config
}

type GrpcConfig struct {
	Addr    []string
	Timeout time.Duration
	TLS     *tls.Config
}

type C struct {
	rpc   acproto.ACrpcClient
	close func()
}

type Datum struct {
	Map       string
	Key       string
	Value     []byte
	Version   uint64
	Expire    uint64
	IfVersion uint64
	Shard     uint32
	HaveShard bool
}

func GrpcClient(cf *GrpcConfig, def *Defaults) (acproto.ACrpcClient, func(), error) {

	err := fmt.Errorf("cannot connect")

	tlsCf := cf.TLS
	if tlsCf == nil && def != nil && def.TLS != nil {
		tlsCf = def.TLS
	}

	for _, addr := range cf.Addr {

		var opt []grpc.DialOption

		if tlsCf != nil {
			host, _, _ := net.SplitHostPort(addr)
			tls := tlsCf.Clone()
			tls.ServerName = host
			creds := credentials.NewTLS(tls)
			opt = append(opt, grpc.WithTransportCredentials(creds))
		} else {
			opt = append(opt, grpc.WithInsecure())

		}

		if cf.Timeout.Nanoseconds() != 0 {
			opt = append(opt, grpc.WithTimeout(cf.Timeout))
		}

		conn, derr := grpc.Dial(addr, opt...)

		if derr == nil {
			client := acproto.NewACrpcClient(conn)
			return client, func() { conn.Close() }, nil
		}

		err = derr
	}

	return nil, noop, err

}

func noop() {}

// ################################################################

func New(cf *GrpcConfig) (*C, error) {

	rpc, close, err := GrpcClient(cf, nil)
	if err != nil {
		return nil, err
	}

	return &C{rpc, close}, nil
}

func (c *C) Close() {
	c.close()
}

func (c *C) RPC() acproto.ACrpcClient {
	return c.rpc
}

func (c *C) Get(d *Datum) (*Datum, error) {

	if !d.HaveShard {
		d.Shard = shard.Hash(d.Key)
		d.HaveShard = true
	}

	get := &acproto.ACPY2MapDatum{
		Map:   d.Map,
		Key:   d.Key,
		Shard: d.Shard,
	}
	if d.Version != 0 {
		get.Version = d.Version
	}

	res, err := c.rpc.Get(context.Background(), &acproto.ACPY2GetSet{
		Data: []*acproto.ACPY2MapDatum{get},
	})

	if err != nil {
		return nil, err
	}

	if len(res.Data) == 0 {
		return nil, nil
	}

	r := res.Data[0]
	d.Value = r.Value
	d.Version = r.GetVersion()
	d.Expire = r.GetExpire()
	d.Shard = r.GetShard()

	return d, nil

}

func (c *C) MGet(d []*Datum) ([]*Datum, error) {

	gets := make([]*acproto.ACPY2MapDatum, len(d))

	for i, r := range d {
		g := &acproto.ACPY2MapDatum{
			Map: r.Map,
			Key: r.Key,
		}

		if r.Version != 0 {
			g.Version = r.Version
		}

		gets[i] = g
	}

	res, err := c.rpc.Get(context.Background(), &acproto.ACPY2GetSet{
		Data: gets,
	})

	if err != nil {
		return nil, err
	}

	if len(res.Data) == 0 {
		return nil, nil
	}

	for i, r := range res.Data {
		d[i].Value = r.Value
		d[i].Version = r.GetVersion()
		d[i].Expire = r.GetExpire()
		d[i].Shard = r.GetShard()
	}

	return d, nil
}

func (c *C) GetRange(mapname string, key0 string, key1 string, ver0 uint64, ver1 uint64) ([]*Datum, error) {

	req := &acproto.ACPY2GetRange{
		Map: mapname,
	}
	if key0 != "" {
		req.Key0 = key0
	}
	if key1 != "" {
		req.Key1 = key1
	}
	if ver0 != 0 {
		req.Version0 = ver0
	}
	if ver1 != 0 {
		req.Version1 = ver1
	}

	res, err := c.rpc.Range(context.Background(), req)

	if err != nil {
		return nil, err
	}

	ret := make([]*Datum, len(res.Data))

	for i, r := range res.Data {
		ret[i] = &Datum{
			Key:     r.GetKey(),
			Value:   r.Value,
			Version: r.GetVersion(),
			Expire:  r.GetExpire(),
			Shard:   r.GetShard(),
		}
	}

	return ret, nil
}

func (c *C) Put(d *Datum) (int, error) {

	if d.Version == 0 {
		d.Version = soty.Now()
	}

	if !d.HaveShard {
		d.Shard = shard.Hash(d.Key)
		d.HaveShard = true
	}

	put := &acproto.ACPY2MapDatum{
		Map:     d.Map,
		Key:     d.Key,
		Value:   d.Value,
		Shard:   d.Shard,
		Version: d.Version,
	}
	if d.IfVersion != 0 {
		put.IfVersion = d.IfVersion
	}

	res, err := c.rpc.Put(context.Background(), &acproto.ACPY2DistRequest{
		Hop:    0,
		Expire: soty.Now() + uint64(5*time.Second),
		Data:   put,
	})

	if err != nil {
		return 0, err
	}

	if res.GetStatusCode() != 200 {
		return 0, errors.New(res.GetStatusMessage())
	}

	return int(res.GetResultCode()), nil
}

// get servers(...)
// get ringcf(map)

// ################################################################

func (c *C) GetServersAll() ([]*info.Server, error) {
	return c.GetServersInfo("", "", "", "", "")
}

func (c *C) GetServersInfo(sys string, env string, host string, dc string, id string) ([]*info.Server, error) {

	// build search request
	req := &acproto.ACPY2ServerRequest{}

	if sys != "" {
		req.Subsystem = sys
	}
	if env != "" {
		req.Environment = env
	}
	if host != "" {
		req.Hostname = host
	}
	if dc != "" {
		req.Datacenter = dc
	}
	if id != "" {
		req.ServerId = id
	}

	// send request
	res, err := c.rpc.Servers(context.Background(), req)

	if err != nil {
		return nil, err
	}

	// process reply
	var inf []*info.Server

	for _, d := range res.Data {
		r := &info.Server{
			Subsystem:      d.GetSubsystem(),
			Environment:    d.GetEnvironment(),
			Hostname:       d.GetHostname(),
			Datacenter:     d.GetDatacenter(),
			Rack:           d.GetRack(),
			Id:             d.GetServerId(),
			Database:       d.Database,
			Uptodate:       d.GetUptodate(),
			IsUp:           d.GetIsUp(),
			IsLocal:        d.GetIsLocal(),
			SortMetric:     d.GetSortMetric(),
			CpuMetric:      d.GetCpuMetric(),
			CapacityMetric: d.GetCapacityMetric(),
		}

		for _, n := range d.NetInfo {
			r.NetInfo = append(r.NetInfo, info.Net{Addr: n.GetAddr(), Dom: n.GetNatdom()})
		}

		inf = append(inf, r)
	}

	return inf, nil
}

// ################################################################
// ring conf

type RingShard struct {
	Shard  uint32
	Server []string
}
type RingConf struct {
	Vers   uint64
	Stable bool
	DB     string
	DC     string
	Part   []RingShard
}

func (c *C) GetRingConf(db string, dc string) (*RingConf, error) {

	res, err := c.rpc.RingConf(context.Background(), &acproto.ACPY2RingConfReq{
		Map:        db,
		Datacenter: dc,
	})

	if err != nil {
		return nil, err
	}

	cf := &RingConf{
		DB:     db,
		DC:     dc,
		Vers:   res.GetVersion(),
		Stable: res.GetIsStable(),
	}

	for _, p := range res.Part {
		cf.Part = append(cf.Part, RingShard{p.GetShard(), p.Server})
	}

	return cf, nil
}
