# -*- mode: ruby; coding: utf-8 -*-
# Copyright (c) 2019
# Author: Jeff Weisberg <jaw @ tcp4me.com>
# Created: 2019-Jan-03 10:47 (EST)
# Function: simple yentablue client

require 'json'
require 'murmurhash3'
require 'base64'
require 'socket'
require 'net/http'
require 'openssl'

module YentaBlue

  TIMEOUT = 15
  LOCALHOST = "127.0.0.1"

  class Client

    def initialize(servers: [], server_file: nil, environment: "dev",
                   copies: 2, timeout: TIMEOUT, maxopen: 1,
                   key: nil, cert: nil, root: nil)

      @sd      = ACFile.new(server_file)
      @servers = servers
      @env     = environment
      @copies  = copies
      @timeout = timeout
      @maxopen = maxopen
      @reuse   = {}

      if root
        @root = root
      end
      if key && cert
        @cert = OpenSSL::X509::Certificate.new(File.read(cert))
        @key  = OpenSSL::PKey::RSA.new(File.read(key))
      end
    end

    def put(map, key, value)
      servers = [*@servers, *@sd.servers("yenta", @env)]
      copies  = 0

      req = {
        hop:    0,
        expire: (Time.now.to_i + 60) * 1_000_000_000,
        sender: Socket.gethostname,
        data: {
          map:     map,
          key:     key,
          shard:   shard(key),
          version: curr_version,
          value:   Base64.encode64(value), # the golang side expects base64 for bytes
        }
      }

      servers.each { |s|
        res = mkreq(s, '/api/yb/put', req)
        next unless res
        return true if ++copies >= @copies
      }

      return false
    end

    def get(map, key)
      servers = [*@servers, *@sd.servers("yenta", @env)]

      req = {
        data: [{ map:   map,
                 key:   key,
                 shard: shard(key),
               }]
      }

      res = nil
      servers.each { |s|
        res = mkreq(s, '/api/yb/get', req)
        break if res
      }

      return unless res
      d = res[:data][0]
      d[:value] = Base64.decode64(d[:value]) # the golang side encoded it
      return d
    end

    def mget(map, keys)
      servers = [*@servers, *@sd.servers("yenta", @env)]

      req = {
        data: keys.map { |k|
          { map: map, key: k, shard: shard(k) }
        }
      }

      res = nil
      servers.each { |s|
        res = mkreq(s, '/api/yb/get', req)
        break if res
      }

      return unless res
      d = res[:data]
      d.each { |v|
        v[:value] = Base64.decode64(v[:value])
      }

      return d
    end

    ################
    private

    def curr_version
      t = Time.now()
      t.to_i * 1_000_000_000 + t.nsec
    end

    def shard (s)
      MurmurHash3::V32.str_hash s
    end

    # simplified http api
    def mkreq(addr, endpoint, req)
      uri  = URI("https://#{addr}#{endpoint}")
      http = conn(uri.host, uri.port)
      hreq = Net::HTTP::Post.new(uri, 'Content-Type' => 'application/json')
      hreq.body = req.to_json
      res = http.request(hreq)
      return JSON.parse(res.read_body, {symbolize_names: true})
    rescue => e
      p e
      return nil
    end

    def conn(host, port)
      # reuse existing persistent connection?
      ep = "#{host}:#{port}"
      rc = @reuse[ep]

      if rc
        # refresh
        @reuse.delete ep
        @reuse[ep] = rc
        return rc
      end

      # evict?
      if @reuse.size >= @maxopen
        k,c = @reuse.shift
        c.finish
      end

      @reuse[ep] = new_conn(host, port)
    end

    def new_conn(host, port)
      http = Net::HTTP.new(host, port)

      http.open_timeout = @timeout
      http.read_timeout = @timeout
      http.ssl_timeout  = @timeout

      if host != LOCALHOST
        http.use_ssl     = true
        http.ca_file     = @root
        http.key         = @key
        http.cert        = @cert
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE unless @root
      end

      return http
    end

  end


  # NB - the serverfile might not exist, don't freak out
  class ACFile

    CHECKTIME = 10

    def initialize(server_file)
      @file    = server_file
      @loaded  = Time.new(0)
      @checked = Time.new(0)
      @servers = nil
    end

    # stat the file every so often, reload if changed
    def maybe_read
      now = Time.now()
      return if @checked + CHECKTIME >= now
      @checked = now
      mt  = File.mtime(@file)
      return if mt < @loaded
      read
      @loaded = now
    rescue
      # file does not exist. nbd.
    end

    # file is just a big blob of json
    def read
      content = File.read(@file)
      @data   = JSON.parse(content)
    end

    def servers(subsys, env)
      maybe_read
      return unless @data
      # filter + sort
      s = @data.select { |e|
        next if subsys != "" && e['Subsystem']   != subsys
        next if env    != "" && e['Environment'] != env
        true
      }
      return if s.empty?
      # -> semi-sorted local, semi-sorted faraway
      local = semisort s.select{ |e|  e['IsLocal'] }
      far   = semisort s.select{ |e| !e['IsLocal'] }
      # determine the best address
      s     = [*local, *far].map{ |e| bestaddr e }
      p s
      return s
    end

    # partially sorted, partially shuffled
    def semisort (a)
      a.sort! { |a,b| a['SortMetric'] < b['SortMetric'] }
      n = a.length / 3 || 0
      m = 2 * n
      left  = a.slice(0, n).shuffle!
      midl  = a.slice(n, n).shuffle!
      right = a.slice(m, a.length - m)
      [*left, *midl, *right]
    end

    def bestaddr (s)
      public = ""
      s['NetInfo'].each { |n|
        p n
        return n['Addr'] if  s['IsLocal'] && !n['Dom'].empty?	# local server  - use private addr
        return n['Addr'] if !s['IsLocal'] &&  n['Dom'].empty?	# remote server - use public addr
        public = n['Addr'] if n['Dom'].empty?
      }
      return public
    end
  end
end


