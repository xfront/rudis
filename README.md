# rudis

**English** | [中文](README_CN.md)

A Redis re-implementation in Rust.

## Features

rudis is a multi-threaded, cross-platform Redis-compatible server written in Rust. It implements the Redis protocol and a large subset of Redis commands, along with several extensions.

### Core

- **Full RESP protocol** support (Redis Serialization Protocol)
- **Multi-threaded architecture** — Dragonfly-inspired internal sharding for multi-core utilization
- **Cross-platform** — no UNIX-specific features required; runs on Linux, macOS, and Windows
- **TCP + UNIX socket** listening
- **AOF persistence** with configurable fsync policies
- **Configurable** via Redis-compatible configuration files

### Data Structures

| Type | Commands |
|------|----------|
| **String** | GET, SET, SETNX, SETEX, PSETEX, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, GETSET, GETRANGE, SETRANGE, MGET, MSET, MSETNX, GETDEL, GETEX, LCS, SUBSTR |
| **List** | LPUSH, RPUSH, LPOP, RPOP, BLPOP, BRPOP, LRANGE, LINDEX, LSET, LLEN, LREM, LINSERT, LTRIM, LPOS, LMOVE, BLMOVE, LMPOP, BLMPOP, RPOPLPUSH, BRPOPLPUSH |
| **Set** | SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS, SPOP, SRANDMEMBER, SINTER, SINTERSTORE, SINTERCARD, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SMOVE, SSCAN |
| **Sorted Set** | ZADD, ZREM, ZSCORE, ZMSCORE, ZINCRBY, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZCARD, ZCOUNT, ZLEXCOUNT, ZRANGESTORE, ZUNIONSTORE, ZINTERSTORE, ZDIFF, ZDIFFSTORE, ZINTER, ZUNION, ZINTERCARD, ZRANDMEMBER, ZMPOP, BZMPOP, BZPOPMIN, BZPOPMAX, ZSCAN |
| **Hash** | HSET, HSETNX, HGET, HMSET, HMGET, HDEL, HLEN, HEXISTS, HKEYS, HVALS, HGETALL, HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSTRLEN, HSCAN, HEXP, HPTTL, HTTL, HPEXPIREAT, HPEXPIRE, HEXPIREAT, HEXPIRE, HPERSIST |
| **HyperLogLog** | PFADD, PFCOUNT, PFMERGE |
| **Stream** | XADD, XLEN, XRANGE, XREVRANGE, XDEL, XTRIM, XREAD, XGROUP, XACK, XPENDING, XINFO, XSETID |
| **Geo** | GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE |
| **Bitmap** | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP |

### Extensions (Redis 7/8 Module Compatibility)

| Module | Commands |
|--------|----------|
| **Bloom / Cuckoo / TDigest / TopK** | BF.ADD, BF.EXISTS, CF.ADD, CF.EXISTS, TDIGEST.ADD, TOPK.ADD, etc. |
| **JSON** | JSON.SET, JSON.GET, JSON.DEL, JSON.TYPE, JSON.NUMINCRBY, etc. |
| **TimeSeries** | TS.CREATE, TS.ADD, TS.GET, TS.RANGE, TS.REVRANGE, TS.INFO, TS.QUERYINDEX, etc. |
| **Full-Text Search** | FT.CREATE, FT.SEARCH, FT.AGGREGATE, FT.INFO, FT.DROPINDEX |

### Cluster & Sentinel

- **Redis Cluster** — 16384-slot hash slot distribution with CRC16 routing
  - `CLUSTER INFO|NODES|SLOTS|MYID|MEET|ADDSLOTS|DELSLOTS|SETSLOT|KEYSLOT|REPLICATE|FAILOVER|SAVECONFIG|FORGET|FLUSHSLOTS|RESET`
  - `MOVED` / `ASK` client redirection
  - `ASKING` / `READONLY` / `READWRITE` protocol support
  - Cluster bus (port+10000) with Gossip protocol (PING/PONG/MEET/FAIL)
  - Cluster configuration persistence (`nodes.conf`)
- **Redis Sentinel** — master monitoring, failure detection, and failover
  - `SENTINEL MASTERS|MASTER|REPLICAS|SENTINELS|MONITOR|REMOVE|SET|CKQUORUM|FAILOVER|RESET|INFO|MYID|PING|GET-MASTER-ADDR-BY-NAME|IS-MASTER-DOWN-BY-ADDR`
  - Launch with `--sentinel` flag

### Additional Features

- **Pub/Sub** — channels, patterns, and sharded pub/sub (SSUBSCRIBE/SPUBLISH)
- **Transactions** — MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Lua Scripting** — EVAL, EVALSHA, FUNCTION, FCALL, SCRIPT
- **ACL** — user management with authentication and permission control
- **Key expiration** — active + passive expiry cycles
- **Keyspace notifications** — pub/sub events for key changes
- **Slow log** — command latency monitoring
- **SCAN family** — SCAN, SSCAN, HSCAN, ZSCAN cursor-based iteration
- **COPY, UNLINK, TOUCH, LPOS, LMOVE** and other Redis 6/7 commands

## Prerequisites

- **Rust** (stable or nightly)
- **Cargo** (included with Rust)

## Building

```bash
cargo build --release
```

The binary will be at `target/release/rudis`.

## Running

```bash
# Start with default settings (port 6379)
./target/release/rudis

# Start with a configuration file
./target/release/rudis rudis.conf

# Start in Sentinel mode
./target/release/rudis sentinel.conf --sentinel
```

### Configuration

rudis uses Redis-compatible configuration files. Key configuration directives:

```
# Network
port 6379
bind 127.0.0.1
tcp-backlog 511
timeout 0
tcp-keepalive 300

# General
daemonize no
loglevel notice
databases 16

# Snapshotting
save 900 1
save 300 10
save 60 10000
dbfilename dump.rdb
dir ./

# AOF
appendonly no
appendfilename "appendonly.aof"
appendfsync everysec

# Memory
maxmemory 0
maxmemory-policy noeviction

# Cluster
cluster-enabled no
cluster-config-file nodes.conf
cluster-node-timeout 15000

# Security
requirepass ""

# Performance
hz 10
```

## Testing

```bash
# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p database
cargo test -p command
cargo test -p networking
```

## Architecture

```
rudis/
├── src/            # Binary entry point (main.rs)
├── command/        # Command parsing, dispatch, and execution (~250+ commands)
├── compat/         # OS compatibility layer (getos, getpid)
├── config/         # Configuration file parsing
├── database/       # Core data structures, storage engine, and shard management
│   ├── cluster.rs  #   Redis Cluster state and slot management
│   ├── sentinel.rs #   Sentinel monitoring and failover
│   ├── shard.rs    #   Dragonfly-inspired multi-shard architecture
│   └── rdbutil/    #   RDB serialization utilities
├── logger/         # Logging subsystem
├── networking/     # TCP/UNIX server, client handling, cluster bus
│   └── cluster_bus.rs  # Gossip protocol and inter-node communication
├── parser/         # RESP protocol parser
├── persistence/    # AOF persistence
├── response/       # RESP response serialization
└── util/           # Shared utilities (CRC64, glob matching, random, etc.)
```

### Design Highlights

- **Dragonfly-inspired sharding**: Commands are routed to shards based on the hash of the first key argument, enabling parallel execution across multiple cores.
- **Zero-copy batched writes**: Client response writer uses `BytesMut` to batch multiple responses into a single `write()` syscall.
- **Per-shard databases**: Each shard maintains its own independent database instance with separate key spaces, expiration maps, and pub/sub state.

## Current Status

See [TODO.md](TODO.md) for the complete command and configuration checklist.

| Area | Status |
|------|--------|
| Core data types (String, List, Set, Hash, ZSet) | ✅ Complete |
| HyperLogLog | ✅ Complete |
| Streams | ✅ Complete |
| Geo commands | ✅ Complete |
| Pub/Sub + Sharded Pub/Sub | ✅ Complete |
| Transactions (MULTI/EXEC) | ✅ Complete |
| Lua scripting & Functions | ✅ Complete |
| ACL system | ✅ Complete |
| Bloom/JSON/TimeSeries/Search | ✅ Complete |
| Hash per-field expiration | ✅ Complete |
| Redis Cluster | ✅ Complete |
| Redis Sentinel | ✅ Complete |
| AOF persistence | ✅ Complete |
| RDB persistence | ❌ Not implemented |
| Full replication (PSYNC) | ❌ Partial (stubs only) |

## License

Copyright (c) 2015, Sebastian Waisbrot
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
