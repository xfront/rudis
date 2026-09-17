# rudis

[English](README.md) | **中文**

使用 Rust 重新实现 Redis。

## 特性

rudis 是一个多线程、跨平台的 Redis 兼容服务器，使用 Rust 编写。它实现了 Redis 协议和大部分 Redis 命令，并提供了多种扩展功能。

### 核心

- **完整的 RESP 协议** 支持（Redis 序列化协议）
- **多线程架构** — 借鉴 Dragonfly 的内部分片设计，充分利用多核性能
- **跨平台** — 不依赖 UNIX 特性；可在 Linux、macOS 和 Windows 上运行
- **TCP + UNIX Socket** 监听
- **AOF 持久化** — 可配置的 fsync 策略
- **Redis 兼容配置** — 支持 Redis 格式的配置文件

### 数据结构

| 类型 | 命令 |
|------|------|
| **字符串 (String)** | GET, SET, SETNX, SETEX, PSETEX, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, GETSET, GETRANGE, SETRANGE, MGET, MSET, MSETNX, GETDEL, GETEX, LCS, SUBSTR |
| **列表 (List)** | LPUSH, RPUSH, LPOP, RPOP, BLPOP, BRPOP, LRANGE, LINDEX, LSET, LLEN, LREM, LINSERT, LTRIM, LPOS, LMOVE, BLMOVE, LMPOP, BLMPOP, RPOPLPUSH, BRPOPLPUSH |
| **集合 (Set)** | SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS, SPOP, SRANDMEMBER, SINTER, SINTERSTORE, SINTERCARD, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SMOVE, SSCAN |
| **有序集合 (Sorted Set)** | ZADD, ZREM, ZSCORE, ZMSCORE, ZINCRBY, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZCARD, ZCOUNT, ZLEXCOUNT, ZRANGESTORE, ZUNIONSTORE, ZINTERSTORE, ZDIFF, ZDIFFSTORE, ZINTER, ZUNION, ZINTERCARD, ZRANDMEMBER, ZMPOP, BZMPOP, BZPOPMIN, BZPOPMAX, ZSCAN |
| **哈希 (Hash)** | HSET, HSETNX, HGET, HMSET, HMGET, HDEL, HLEN, HEXISTS, HKEYS, HVALS, HGETALL, HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSTRLEN, HSCAN, HEXP, HPTTL, HTTL, HPEXPIREAT, HPEXPIRE, HEXPIREAT, HEXPIRE, HPERSIST |
| **HyperLogLog** | PFADD, PFCOUNT, PFMERGE |
| **流 (Stream)** | XADD, XLEN, XRANGE, XREVRANGE, XDEL, XTRIM, XREAD, XGROUP, XACK, XPENDING, XINFO, XSETID |
| **地理位置 (Geo)** | GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE |
| **位图 (Bitmap)** | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP |

### 扩展模块（兼容 Redis 7/8 模块）

| 模块 | 命令 |
|------|------|
| **Bloom / Cuckoo / TDigest / TopK** | BF.ADD, BF.EXISTS, CF.ADD, CF.EXISTS, TDIGEST.ADD, TOPK.ADD 等 |
| **JSON** | JSON.SET, JSON.GET, JSON.DEL, JSON.TYPE, JSON.NUMINCRBY 等 |
| **TimeSeries（时序数据）** | TS.CREATE, TS.ADD, TS.GET, TS.RANGE, TS.REVRANGE, TS.INFO, TS.QUERYINDEX 等 |
| **全文搜索** | FT.CREATE, FT.SEARCH, FT.AGGREGATE, FT.INFO, FT.DROPINDEX |

### 集群与哨兵

- **Redis Cluster（集群模式）** — 16384 个哈希槽，CRC16 路由分发
  - `CLUSTER INFO|NODES|SLOTS|MYID|MEET|ADDSLOTS|DELSLOTS|SETSLOT|KEYSLOT|REPLICATE|FAILOVER|SAVECONFIG|FORGET|FLUSHSLOTS|RESET`
  - `MOVED` / `ASK` 客户端重定向
  - `ASKING` / `READONLY` / `READWRITE` 协议支持
  - 集群总线（端口+10000）+ Gossip 协议（PING/PONG/MEET/FAIL）
  - 集群配置持久化（`nodes.conf`）
- **Redis Sentinel（哨兵模式）** — 主节点监控、故障检测与自动故障转移
  - `SENTINEL MASTERS|MASTER|REPLICAS|SENTINELS|MONITOR|REMOVE|SET|CKQUORUM|FAILOVER|RESET|INFO|MYID|PING|GET-MASTER-ADDR-BY-NAME|IS-MASTER-DOWN-BY-ADDR`
  - 使用 `--sentinel` 参数启动

### 其他特性

- **发布/订阅** — 频道订阅、模式匹配订阅、分片发布/订阅（SSUBSCRIBE/SPUBLISH）
- **事务** — MULTI、EXEC、DISCARD、WATCH、UNWATCH
- **Lua 脚本** — EVAL、EVALSHA、FUNCTION、FCALL、SCRIPT
- **ACL 访问控制** — 用户管理、认证与权限控制
- **键过期** — 主动 + 被动过期淘汰机制
- **键空间通知** — 键变更时通过 Pub/Sub 发送事件
- **慢查询日志** — 命令延迟监控
- **SCAN 系列** — SCAN、SSCAN、HSCAN、ZSCAN 游标迭代
- **COPY, UNLINK, TOUCH, LPOS, LMOVE** 等 Redis 6/7 新命令

## 环境要求

- **Rust**（stable 或 nightly 均可）
- **Cargo**（随 Rust 一起安装）

## 编译

```bash
cargo build --release
```

编译产物位于 `target/release/rudis`。

## 运行

```bash
# 使用默认配置启动（端口 6379）
./target/release/rudis

# 使用配置文件启动
./target/release/rudis rudis.conf

# 以哨兵模式启动
./target/release/rudis sentinel.conf --sentinel
```

### 配置说明

rudis 使用 Redis 兼容的配置文件格式。常用配置项：

```
# 网络
port 6379
bind 127.0.0.1
tcp-backlog 511
timeout 0
tcp-keepalive 300

# 通用
daemonize no
loglevel notice
databases 16

# RDB 快照
save 900 1
save 300 10
save 60 10000
dbfilename dump.rdb
dir ./

# AOF 持久化
appendonly no
appendfilename "appendonly.aof"
appendfsync everysec

# 内存管理
maxmemory 0
maxmemory-policy noeviction

# 集群
cluster-enabled no
cluster-config-file nodes.conf
cluster-node-timeout 15000

# 安全
requirepass ""

# 性能
hz 10
```

## 测试

```bash
# 运行全部测试
cargo test

# 运行指定 crate 的测试
cargo test -p database
cargo test -p command
cargo test -p networking
```

## 项目架构

```
rudis/
├── src/            # 二进制入口 (main.rs)
├── command/        # 命令解析、分发与执行（250+ 条命令）
├── compat/         # 操作系统兼容层（getos、getpid）
├── config/         # 配置文件解析
├── database/       # 核心数据结构、存储引擎与分片管理
│   ├── cluster.rs  #   Redis Cluster 集群状态与槽位管理
│   ├── sentinel.rs #   Sentinel 哨兵监控与故障转移
│   ├── shard.rs    #   借鉴 Dragonfly 的多分片架构
│   └── rdbutil/    #   RDB 序列化工具
├── logger/         # 日志子系统
├── networking/     # TCP/UNIX 服务器、客户端连接管理、集群总线
│   └── cluster_bus.rs  # Gossip 协议与节点间通信
├── parser/         # RESP 协议解析器
├── persistence/    # AOF 持久化
├── response/       # RESP 响应序列化
└── util/           # 公共工具（CRC64、glob 匹配、随机数等）
```

### 设计亮点

- **借鉴 Dragonfly 的分片架构**：根据第一个 key 参数的哈希值将命令路由到对应分片，实现多核并行执行。
- **零拷贝批量写入**：客户端响应写入器使用 `BytesMut` 将多个响应批量合并为单次 `write()` 系统调用。
- **独立分片数据库**：每个分片维护独立的数据库实例，拥有独立的键空间、过期映射和 Pub/Sub 状态。

## 当前状态

完整的命令与配置清单请参见 [TODO.md](TODO.md)。

| 模块 | 状态 |
|------|------|
| 核心数据类型（String, List, Set, Hash, ZSet） | ✅ 完成 |
| HyperLogLog | ✅ 完成 |
| 流（Stream） | ✅ 完成 |
| 地理位置（Geo） | ✅ 完成 |
| 发布/订阅 + 分片发布/订阅 | ✅ 完成 |
| 事务（MULTI/EXEC） | ✅ 完成 |
| Lua 脚本与函数 | ✅ 完成 |
| ACL 访问控制 | ✅ 完成 |
| Bloom/JSON/TimeSeries/Search 扩展模块 | ✅ 完成 |
| Hash 字段级过期 | ✅ 完成 |
| Redis Cluster 集群 | ✅ 完成 |
| Redis Sentinel 哨兵 | ✅ 完成 |
| AOF 持久化 | ✅ 完成 |
| RDB 持久化 | ❌ 未实现 |
| 完整复制（PSYNC） | ❌ 仅桩实现 |

## 许可证

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
