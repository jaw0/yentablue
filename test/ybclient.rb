#!/usr/local/bin/ruby
# -*- mode: ruby; coding: utf-8 -*-
# Copyright (c) 2019
# Author: Jeff Weisberg <jaw @ tcp4me.com>
# Created: 2019-Jan-12 09:31 (EST)
# Function: test

require './lib/yentablue'

ydb = YentaBlue::Client.new(
  #server_file: "/tmp/acservers",
  servers:     ["192.168.200.2:5301"],
  copies:      1,
  environment: "dev",
  root:        'conf/out/ROOT.crt',
  cert:        'conf/out/athena.crt',
  key:         'conf/out/athena.key',
)

ydb.put('cmdb', 'key12345', 'hellow orld')
r = ydb.get('cmdb', 'key12345')
p r
r = ydb.mget('cmdb', ['key12345', 'key1', 'key2'])
p r

