// Copyright (c) 2017
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jun-29 12:10 (EDT)
// Function: load config file

package config

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jaw0/acconfig"
	"github.com/jaw0/acdiag"
)

type DBConf struct {
	Name         string
	Pathname     string
	Backend      string
	Secret       string
	Expire       int `ac/convert:"duration"`
	CacheSize    int
	FullPathName string
}

// proto:
//   ac   - AC::Yenta compat
//   yb   - yentablue native
//   http - must return 200
//   tcp  - send, expect

type Monitor struct {
	Protocol    string // ac, yb, http, [...]
	System      string
	Environment string `ac/name:"env"`
	Datacenter  string
	Rack        string
	Id          string
	Hostname    string
	Addr        string
	Port        int
	URL         string // for proto 'http'
}

type Config struct {
	Environment    string `ac/name:"env"`
	Datacenter     string
	Rack           string
	Error_mailto   string
	Error_mailfrom string
	Basedir        string
	Syslog         string
	TLS_cert       string // our cert - .crt
	TLS_key        string // our private key - .key
	TLS_root       string // root cert - .crt
	Save_status    string // filename
	Net_threads    int
	AE_threads     int
	Port_Server    int
	Seedpeer       []string `ac/validate:"ipport"`
	Allow          []string `ac/validate:"ipmask"`
	Monitor        []*Monitor
	Debug          map[string]bool
	Database       []*DBConf
}

var cf *Config = &Config{}
var cf_lock sync.RWMutex
var dl = diag.Logger("config")

func Init(file string) {

	err := readConfig(file)
	if err != nil {
		dl.Fatal("%s", err)
	}

	go manageConfig(file)

}

// continually check + reread config in background
func manageConfig(file string) {

	var lastmod = time.Now()

	for {
		mt, err := readConfigIfNewer(file, lastmod)

		if err != nil {
			dl.Verbose("cannot stat %s: %v", file, err)
		} else {
			lastmod = mt
		}

		time.Sleep(5 * time.Second)
	}

}

func readConfigIfNewer(file string, modtime time.Time) (time.Time, error) {
	s, err := os.Stat(file)
	if err != nil {
		return time.Time{}, err
	}
	if s.ModTime().After(modtime) {
		dl.Verbose("config changed. reloading")
		err = readConfig(file)
		return s.ModTime(), err
	}

	return modtime, nil
}

func Cf() *Config {
	cf_lock.RLock()
	r := cf
	cf_lock.RUnlock()
	return r
}

func readConfig(file string) error {

	newcf := &Config{
		Seedpeer: make([]string, 0),
		Allow:    make([]string, 0),
		Debug:    make(map[string]bool),
		Database: make([]*DBConf, 0),
	}

	err := acconfig.Read(file, newcf)

	if err != nil {
		return fmt.Errorf("cannot read config '%s': %v", file, err)
	}

	cf_lock.Lock()
	cf = newcf
	cf_lock.Unlock()

	diag.SetConfig(diag.Config{
		MailTo:   newcf.Error_mailto,
		MailFrom: newcf.Error_mailfrom,
		Debug:    newcf.Debug,
		Facility: newcf.Syslog,
	})

	dl.Debug("got> %#v; %#v\n", newcf, newcf.Debug)
	return nil
}
