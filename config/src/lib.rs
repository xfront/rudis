#[macro_use(log_and_exit)]
extern crate logger;
extern crate rand;
extern crate time;
extern crate util;

use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Error as IOError;
use std::io::Write;
use std::num::ParseIntError;
use std::path::Path;
use std::str::from_utf8;
use std::str::FromStr;
use std::str::Utf8Error;

use logger::{Level, Logger};
use util::splitargs;

pub struct Config {
    pub logger: Logger,
    pub daemonize: bool,
    pub databases: u8,
    pub pidfile: String,
    pub dir: String,
    pub bind: Vec<String>,
    pub port: u16,
    pub tcp_keepalive: u32,
    pub active_rehashing: bool,
    pub set_max_intset_entries: usize,
    pub timeout: u64,
    pub unixsocket: Option<String>,
    pub unixsocketperm: u32,
    pub rename_commands: HashMap<String, Option<String>>,
    pub requirepass: Option<String>,
    pub tcp_backlog: i32,
    pub syslog_enabled: bool,
    pub syslog_ident: String,
    pub syslog_facility: String,
    pub hz: u32,
    pub appendonly: bool,
    pub appendfilename: String,
    pub aof_load_truncated: bool,
    // RDB
    pub save: Vec<(u32, u32)>,
    pub stop_writes_on_bgsave_error: bool,
    pub rdbcompression: bool,
    pub rdbchecksum: bool,
    pub dbfilename: String,
    // Replication
    pub slaveof: Option<(String, u16)>,
    pub masterauth: Option<String>,
    pub slave_serve_stale_data: bool,
    pub slave_read_only: bool,
    pub repl_diskless_sync: bool,
    pub repl_diskless_sync_delay: u32,
    pub repl_ping_slave_period: u32,
    pub repl_timeout: u32,
    pub repl_disable_tcp_nodelay: bool,
    pub repl_backlog_size: usize,
    pub repl_backlog_ttl: u32,
    pub slave_priority: u32,
    pub min_slaves_to_write: u32,
    pub min_slaves_max_lag: u32,
    // Memory
    pub maxclients: usize,
    pub maxmemory: usize,
    pub maxmemory_policy: String,
    pub maxmemory_samples: u32,
    // AOF
    pub appendfsync: String,
    pub no_appendfsync_on_rewrite: bool,
    pub auto_aof_rewrite_percentage: u32,
    pub auto_aof_rewrite_min_size: usize,
    // Misc
    pub lua_time_limit: u32,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: u32,
    pub latency_monitor_threshold: i64,
    pub notify_keyspace_events: String,
    // Data structure encoding limits
    pub hash_max_ziplist_entries: usize,
    pub hash_max_ziplist_value: usize,
    pub list_max_ziplist_entries: usize,
    pub list_max_ziplist_value: usize,
    pub zset_max_ziplist_entries: usize,
    pub zset_max_ziplist_value: usize,
    pub hll_sparse_max_bytes: usize,
    // Cluster
    pub cluster_enabled: bool,
    pub cluster_config_file: String,
    pub cluster_node_timeout: u64,
    pub cluster_migration_barrier: u32,
    pub cluster_allow_replica_migration: bool,
    pub cluster_replica_validity_factor: u32,
    // Sentinel
    pub sentinel_mode: bool,
    // Other
    pub client_output_buffer_limit: String,
    pub aof_rewrite_incremental_fsync: bool,
}

#[derive(Debug)]
pub enum ConfigError {
    InvalidFormat,
    InvalidParameter,
    IOError(IOError),
    FileNotFound,
}

fn read_string(args: Vec<Vec<u8>>) -> Result<String, ConfigError> {
    if args.len() != 2 {
        Err(ConfigError::InvalidFormat)
    } else {
        Ok(from_utf8(&*args[1])?.to_owned())
    }
}

fn read_parse<T>(args: Vec<Vec<u8>>) -> Result<T, ConfigError>
where
    T: FromStr,
{
    let s = read_string(args)?;
    match s.parse() {
        Ok(f) => Ok(f),
        Err(_) => Err(ConfigError::InvalidParameter),
    }
}

fn read_bool(args: Vec<Vec<u8>>) -> Result<bool, ConfigError> {
    Ok(match &*read_string(args)? {
        "yes" => true,
        "no" => false,
        _ => return Err(ConfigError::InvalidFormat),
    })
}

/// Parses a size value with an optional binary unit suffix (Redis style):
/// "64mb" -> 67108864, "1kb" -> 1024, "512" -> 512.
fn parse_mem_size(s: &str) -> Option<usize> {
    let lower = s.to_ascii_lowercase();
    let (num, multiplier) = if let Some(n) = lower.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix('g') {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix('m') {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix('k') {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix('b') {
        (n, 1)
    } else {
        (lower.as_str(), 1)
    };
    num.parse::<usize>().ok().map(|n| n * multiplier)
}

impl Config {
    pub fn default(port: u16, logger: Logger) -> Config {
        Config {
            logger,
            active_rehashing: true,
            daemonize: false,
            databases: 16,
            pidfile: "/var/run/rudis.pid".to_owned(),
            dir: "./".to_owned(),
            bind: vec![],
            port,
            tcp_keepalive: 0,
            set_max_intset_entries: 512,
            timeout: 0,
            unixsocket: None,
            unixsocketperm: 0o700,
            rename_commands: HashMap::new(),
            requirepass: None,
            tcp_backlog: 511,
            syslog_enabled: false,
            syslog_ident: "rudis".to_owned(),
            syslog_facility: "local0".to_owned(),
            hz: 10,
            appendonly: false,
            appendfilename: "appendonly.aof".to_owned(),
            aof_load_truncated: false,
            // RDB
            save: vec![],
            stop_writes_on_bgsave_error: true,
            rdbcompression: true,
            rdbchecksum: true,
            dbfilename: "dump.rdb".to_owned(),
            // Replication
            slaveof: None,
            masterauth: None,
            slave_serve_stale_data: true,
            slave_read_only: true,
            repl_diskless_sync: false,
            repl_diskless_sync_delay: 5,
            repl_ping_slave_period: 10,
            repl_timeout: 60,
            repl_disable_tcp_nodelay: false,
            repl_backlog_size: 1048576,
            repl_backlog_ttl: 3600,
            slave_priority: 100,
            min_slaves_to_write: 0,
            min_slaves_max_lag: 10,
            // Memory
            maxclients: 10000,
            maxmemory: 0,
            maxmemory_policy: "noeviction".to_owned(),
            maxmemory_samples: 5,
            // AOF
            appendfsync: "everysec".to_owned(),
            no_appendfsync_on_rewrite: false,
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: 64 * 1024 * 1024,
            // Misc
            lua_time_limit: 5000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            latency_monitor_threshold: 0,
            notify_keyspace_events: String::new(),
            // Data structure encoding limits
            hash_max_ziplist_entries: 512,
            hash_max_ziplist_value: 64,
            list_max_ziplist_entries: 512,
            list_max_ziplist_value: 64,
            zset_max_ziplist_entries: 128,
            zset_max_ziplist_value: 64,
            hll_sparse_max_bytes: 3000,
            // Cluster
            cluster_enabled: false,
            cluster_config_file: "nodes.conf".to_owned(),
            cluster_node_timeout: 15000,
            cluster_migration_barrier: 1,
            cluster_allow_replica_migration: true,
            cluster_replica_validity_factor: 10,
            // Sentinel
            sentinel_mode: false,
            // Other
            client_output_buffer_limit: "normal 0 0 0 slave 256mb 64mb 60 pubsub 32mb 8mb 60"
                .to_owned(),
            aof_rewrite_incremental_fsync: true,
        }
    }

    pub fn new(logger: Logger) -> Config {
        Self::default(63799, logger)
    }

    pub fn parsefile(&mut self, fname: String) -> Result<(), ConfigError> {
        let path = Path::new(&*fname);
        let file = BufReader::new(match File::open(&path) {
            Ok(f) => f,
            Err(_) => {
                log_and_exit!(
                    self.logger,
                    Warning,
                    1,
                    "Fatal error, can't open config file '{}'",
                    fname
                );
                return Err(ConfigError::FileNotFound);
            }
        });
        let mut client_output_buffer_limit_seen = false;
        for line_iter in file.lines() {
            let lline = line_iter?;
            let line = lline.trim();
            if line.starts_with('#') {
                continue;
            }

            let args = match splitargs(line.as_bytes()) {
                Ok(args) => args,
                Err(_) => return Err(ConfigError::InvalidFormat),
            };

            if args.is_empty() {
                continue;
            }

            match &*args[0] {
                b"bind" => {
                    self.bind
                        .extend(args[1..].iter().filter(|x| !x.is_empty()).map(|x| {
                            match from_utf8(x) {
                                Ok(s) => s.to_owned(),
                                Err(_) => "".to_owned(), // TODO: return ConfigError
                            }
                        }))
                }
                b"port" => self.port = read_parse(args)?,
                b"activerehashing" => self.active_rehashing = read_bool(args)?,
                b"daemonize" => self.daemonize = read_bool(args)?,
                b"databases" => self.databases = read_parse(args)?,
                b"tcp-keepalive" => self.tcp_keepalive = read_parse(args)?,
                b"set-max-intset-entries" => self.set_max_intset_entries = read_parse(args)?,
                b"timeout" => self.timeout = read_parse(args)?,
                b"unixsocket" => self.unixsocket = Some(read_string(args)?.to_owned()),
                b"unixsocketperm" => {
                    self.unixsocketperm = u32::from_str_radix(&*read_string(args)?, 8)?
                }
                b"pidfile" => self.pidfile = read_string(args)?.to_owned(),
                b"dir" => self.dir = read_string(args)?.to_owned(),
                b"logfile" => {
                    let logfile = read_string(args)?;
                    if !logfile.is_empty() {
                        self.logger.set_logfile(&*logfile)?
                    }
                }
                b"loglevel" => self.logger.set_loglevel(match &*read_string(args)? {
                    "debug" => Level::Debug,
                    "verbose" => Level::Verbose,
                    "notice" => Level::Notice,
                    "warning" => Level::Warning,
                    _ => return Err(ConfigError::InvalidParameter),
                }),
                b"rename-command" => {
                    if args.len() != 3 {
                        return Err(ConfigError::InvalidFormat);
                    } else {
                        let command = from_utf8(&*args[1])?.to_owned();
                        let newname = from_utf8(&*args[2])?.to_owned();
                        if !newname.is_empty() {
                            self.rename_commands.insert(
                                newname.to_lowercase(),
                                Some(command.clone().to_lowercase()),
                            );
                        }
                        self.rename_commands.insert(command.to_lowercase(), None);
                    }
                }
                b"requirepass" => self.requirepass = Some(read_string(args)?.to_owned()),
                b"tcp-backlog" => self.tcp_backlog = read_parse(args)?,
                b"syslog-enabled" => self.syslog_enabled = read_bool(args)?,
                b"syslog-ident" => self.syslog_ident = read_string(args)?.to_owned(),
                b"syslog-facility" => self.syslog_facility = read_string(args)?.to_owned(),
                b"hz" => self.hz = read_parse(args)?,
                b"appendonly" => self.appendonly = read_bool(args)?,
                b"appendfilename" => self.appendfilename = read_string(args)?.to_owned(),
                b"aof-load-truncated" => self.aof_load_truncated = read_bool(args)?,
                // RDB
                b"save" => {
                    if args.len() == 3 {
                        let seconds: u32 = from_utf8(&*args[1])?.parse().map_err(|_| ConfigError::InvalidParameter)?;
                        let keys: u32 = from_utf8(&*args[2])?.parse().map_err(|_| ConfigError::InvalidParameter)?;
                        self.save.push((seconds, keys));
                    }
                }
                b"stop-writes-on-bgsave-error" => self.stop_writes_on_bgsave_error = read_bool(args)?,
                b"rdbcompression" => self.rdbcompression = read_bool(args)?,
                b"rdbchecksum" => self.rdbchecksum = read_bool(args)?,
                b"dbfilename" => self.dbfilename = read_string(args)?.to_owned(),
                // Replication
                b"slaveof" => {
                    if args.len() == 3 {
                        let host = from_utf8(&*args[1])?.to_owned();
                        let port: u16 = from_utf8(&*args[2])?.parse().map_err(|_| ConfigError::InvalidParameter)?;
                        self.slaveof = Some((host, port));
                    }
                }
                b"masterauth" => self.masterauth = Some(read_string(args)?.to_owned()),
                b"slave-serve-stale-data" => self.slave_serve_stale_data = read_bool(args)?,
                b"slave-read-only" => self.slave_read_only = read_bool(args)?,
                b"repl-diskless-sync" => self.repl_diskless_sync = read_bool(args)?,
                b"repl-diskless-sync-delay" => self.repl_diskless_sync_delay = read_parse(args)?,
                b"repl-ping-slave-period" => self.repl_ping_slave_period = read_parse(args)?,
                b"repl-timeout" => self.repl_timeout = read_parse(args)?,
                b"repl-disable-tcp-nodelay" => self.repl_disable_tcp_nodelay = read_bool(args)?,
                b"repl-backlog-size" => self.repl_backlog_size = read_parse(args)?,
                b"repl-backlog-ttl" => self.repl_backlog_ttl = read_parse(args)?,
                b"slave-priority" => self.slave_priority = read_parse(args)?,
                b"min-slaves-to-write" => self.min_slaves_to_write = read_parse(args)?,
                b"min-slaves-max-lag" => self.min_slaves_max_lag = read_parse(args)?,
                // Memory
                b"maxclients" => self.maxclients = read_parse(args)?,
                b"maxmemory" => self.maxmemory = read_parse(args)?,
                b"maxmemory-policy" => self.maxmemory_policy = read_string(args)?.to_owned(),
                b"maxmemory-samples" => self.maxmemory_samples = read_parse(args)?,
                // AOF
                b"appendfsync" => self.appendfsync = read_string(args)?.to_owned(),
                b"no-appendfsync-on-rewrite" => self.no_appendfsync_on_rewrite = read_bool(args)?,
                b"auto-aof-rewrite-percentage" => self.auto_aof_rewrite_percentage = read_parse(args)?,
                b"auto-aof-rewrite-min-size" => {
                    let s = read_string(args)?;
                    self.auto_aof_rewrite_min_size =
                        parse_mem_size(&s).ok_or(ConfigError::InvalidParameter)?;
                }
                // Misc
                b"lua-time-limit" => self.lua_time_limit = read_parse(args)?,
                b"slowlog-log-slower-than" => self.slowlog_log_slower_than = read_parse(args)?,
                b"slowlog-max-len" => self.slowlog_max_len = read_parse(args)?,
                b"latency-monitor-threshold" => self.latency_monitor_threshold = read_parse(args)?,
                b"notify-keyspace-events" => self.notify_keyspace_events = read_string(args)?.to_owned(),
                // Data structure encoding limits
                b"hash-max-ziplist-entries" => self.hash_max_ziplist_entries = read_parse(args)?,
                b"hash-max-ziplist-value" => self.hash_max_ziplist_value = read_parse(args)?,
                b"list-max-ziplist-entries" => self.list_max_ziplist_entries = read_parse(args)?,
                b"list-max-ziplist-value" => self.list_max_ziplist_value = read_parse(args)?,
                b"zset-max-ziplist-entries" => self.zset_max_ziplist_entries = read_parse(args)?,
                b"zset-max-ziplist-value" => self.zset_max_ziplist_value = read_parse(args)?,
                b"hll-sparse-max-bytes" => self.hll_sparse_max_bytes = read_parse(args)?,
                // Other
                // Multiple directives accumulate the three classes (normal,
                // slave, pubsub) into a single space-separated string. The
                // first directive replaces the built-in default value.
                b"client-output-buffer-limit" => {
                    let mut parts: Vec<String> = args[1..]
                        .iter()
                        .map(|a| from_utf8(a).map(|s| s.to_owned()))
                        .collect::<Result<_, _>>()?;
                    if client_output_buffer_limit_seen
                        && !self.client_output_buffer_limit.is_empty()
                    {
                        parts.insert(0, self.client_output_buffer_limit.clone());
                    }
                    self.client_output_buffer_limit = parts.join(" ");
                    client_output_buffer_limit_seen = true;
                }
                // Accepted for compatibility with Redis configuration files;
                // rudis does not implement upstart/systemd supervision.
                b"supervised" => {
                    read_string(args)?;
                }
                // Redis 4+ list tuning directives; rudis still exposes the
                // legacy list-max-ziplist-entries/value pair instead.
                b"list-max-ziplist-size" | b"list-compress-depth" => {
                    read_string(args)?;
                }
                b"aof-rewrite-incremental-fsync" => self.aof_rewrite_incremental_fsync = read_bool(args)?,
                // Cluster
                b"cluster-enabled" => self.cluster_enabled = read_bool(args)?,
                b"cluster-config-file" => self.cluster_config_file = read_string(args)?,
                b"cluster-node-timeout" => self.cluster_node_timeout = read_parse(args)?,
                b"cluster-migration-barrier" => self.cluster_migration_barrier = read_parse(args)?,
                b"cluster-allow-replica-migration" => self.cluster_allow_replica_migration = read_bool(args)?,
                b"cluster-replica-validity-factor" => self.cluster_replica_validity_factor = read_parse(args)?,
                b"include" => {
                    if args.len() != 2 {
                        return Err(ConfigError::InvalidFormat);
                    } else {
                        self.parsefile(from_utf8(&*args[1])?.to_owned())?;
                    }
                }
                _ => writeln!(&mut std::io::stderr(), "Unknown configuration {:?}", line).unwrap(),
            };
        }
        if self.syslog_enabled {
            self.logger
                .set_syslog(&self.syslog_ident, &self.syslog_facility);
        }

        Ok(())
    }

    pub fn addresses(&self) -> Vec<(String, u16)> {
        if self.bind.is_empty() {
            vec![("127.0.0.1".to_owned(), self.port)]
        } else {
            self.bind
                .iter()
                .map(|s| (s.clone(), self.port))
                .collect::<Vec<_>>()
        }
    }
}

impl From<IOError> for ConfigError {
    fn from(e: IOError) -> ConfigError {
        ConfigError::IOError(e)
    }
}

impl From<ParseIntError> for ConfigError {
    fn from(_: ParseIntError) -> ConfigError {
        ConfigError::InvalidParameter
    }
}

impl From<Utf8Error> for ConfigError {
    fn from(_: Utf8Error) -> ConfigError {
        ConfigError::InvalidParameter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs::create_dir;
    use std::fs::File;
    use std::io::Write;

    use rand::random;

    use logger::{Level, Logger};
    use util::mstime;

    macro_rules! config {
        ($str: expr, $logger: expr) => {{
            let dirpath = format!("tmp/{}", mstime());
            let filepath = format!("{}/{}.conf", dirpath, random::<u64>());
            match create_dir("tmp") {
                _ => (),
            }
            match create_dir(dirpath) {
                _ => (),
            }
            match File::create(filepath.clone()).unwrap().write_all($str) {
                _ => (),
            }
            let mut config = Config::new($logger);
            config.parsefile(filepath).unwrap();
            config
        }};
    }

    #[test]
    fn parse_bind() {
        let config = config!(b"bind 1.2.3.4\nbind 5.6.7.8", Logger::new(Level::Warning));
        assert_eq!(config.bind, vec!["1.2.3.4", "5.6.7.8"]);
        assert_eq!(config.port, 63799);
    }

    #[test]
    fn parse_port() {
        let config = config!(b"port 12345", Logger::new(Level::Warning));
        assert_eq!(config.port, 12345);
        assert_eq!(config.addresses(), vec![("127.0.0.1".to_owned(), 12345)]);
    }

    #[test]
    fn parse_bind_port() {
        let config = config!(b"bind 127.0.0.1\nport 12345", Logger::new(Level::Warning));
        assert_eq!(config.bind, vec!["127.0.0.1"]);
        assert_eq!(config.port, 12345);
    }

    #[test]
    fn parse_daemonize_yes() {
        let config = config!(b"daemonize yes", Logger::new(Level::Warning));
        assert!(config.daemonize);
    }

    #[test]
    fn parse_daemonize_no() {
        let config = config!(b"daemonize no", Logger::new(Level::Warning));
        assert!(!config.daemonize);
    }

    #[test]
    fn parse_active_rehashing_yes() {
        let config = config!(b"activerehashing yes", Logger::new(Level::Warning));
        assert!(config.active_rehashing);
    }

    #[test]
    fn parse_active_rehashing_no() {
        let config = config!(b"activerehashing no", Logger::new(Level::Warning));
        assert!(!config.active_rehashing);
    }

    #[test]
    fn parse_databases() {
        let config = config!(b"databases 20", Logger::new(Level::Warning));
        assert_eq!(config.databases, 20);
    }

    #[test]
    fn parse_keepalive() {
        let config = config!(b"tcp-keepalive 123", Logger::new(Level::Warning));
        assert_eq!(config.tcp_keepalive, 123);
    }

    #[test]
    fn parse_keepalive_quotes() {
        let config = config!(b"tcp-keepalive \"123\"", Logger::new(Level::Warning));
        assert_eq!(config.tcp_keepalive, 123);
    }

    #[test]
    fn parse_set_max_intset_entries() {
        let config = config!(
            b"set-max-intset-entries 123456",
            Logger::new(Level::Warning)
        );
        assert_eq!(config.set_max_intset_entries, 123456);
    }

    #[test]
    fn parse_timeout() {
        let config = config!(b"timeout 23456", Logger::new(Level::Warning));
        assert_eq!(config.timeout, 23456);
    }

    #[test]
    fn parse_unixsocket() {
        let config = config!(
            b"unixsocket /dev/null\nunixsocketperm 777",
            Logger::new(Level::Warning)
        );
        assert_eq!(config.unixsocket, Some("/dev/null".to_owned()));
        assert_eq!(config.unixsocketperm, 511);
    }

    #[test]
    fn parse_rename_commands() {
        let config = config!(
            b"rename-command C1 C2\nrename-command HELLO world",
            Logger::new(Level::Warning)
        );
        let mut h = HashMap::new();
        h.insert("c2".to_owned(), Some("c1".to_owned()));
        h.insert("c1".to_owned(), None);
        h.insert("world".to_owned(), Some("hello".to_owned()));
        h.insert("hello".to_owned(), None);
        assert_eq!(config.rename_commands, h);
    }

    #[test]
    fn parse_requirepass() {
        let config = config!(
            b"requirepass THISISASTRONGPASSWORD",
            Logger::new(Level::Warning)
        );
        assert_eq!(config.requirepass, Some("THISISASTRONGPASSWORD".to_owned()));
    }

    #[test]
    fn parse_supervised() {
        // Accepted for Redis compatibility and ignored.
        let config = config!(b"supervised no", Logger::new(Level::Warning));
        assert!(!config.daemonize);
    }

    #[test]
    fn parse_client_output_buffer_limit() {
        let config = config!(
            b"client-output-buffer-limit normal 0 0 0\
              \nclient-output-buffer-limit slave 256mb 64mb 60\
              \nclient-output-buffer-limit pubsub 32mb 8mb 60",
            Logger::new(Level::Warning)
        );
        assert_eq!(
            config.client_output_buffer_limit,
            "normal 0 0 0 slave 256mb 64mb 60 pubsub 32mb 8mb 60"
        );
    }

    #[test]
    fn parse_mem_size_units() {
        assert_eq!(parse_mem_size("512"), Some(512));
        assert_eq!(parse_mem_size("1kb"), Some(1024));
        assert_eq!(parse_mem_size("64mb"), Some(64 * 1024 * 1024));
        assert_eq!(parse_mem_size("64MB"), Some(64 * 1024 * 1024));
        assert_eq!(parse_mem_size("2gb"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_mem_size("abc"), None);
    }

    #[test]
    fn parse_sample_config_file() {
        // The shipped rudis.conf must parse without errors.
        let path = format!("{}/../rudis.conf", env!("CARGO_MANIFEST_DIR"));
        let mut config = Config::new(Logger::new(Level::Warning));
        config.parsefile(path).unwrap();
        assert_eq!(config.auto_aof_rewrite_min_size, 64 * 1024 * 1024);
        assert_eq!(config.databases, 16);
    }
}
