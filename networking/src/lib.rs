use logger::{log, sendlog};

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    process,
    sync::mpsc::{channel, Receiver, Sender},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use bytes::BytesMut;
use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};
#[cfg(unix)]
use std::{fs::File, path::Path};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

use config::Config;
use database::shard::{Shard, ShardedDatabase};
use logger::Level;
use parser::{OwnedParsedCommand, ParseError, Parser};
use response::{Response, ResponseError};

pub mod cluster_bus;

/// Global flag set by the signal handler (SIGTERM/SIGINT) to request a
/// graceful shutdown.  The main loop in `run()` polls this flag and calls
/// `stop()` when it becomes `true`.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Install POSIX signal handlers for SIGTERM and SIGINT so the server can
/// perform a graceful shutdown (save RDB, flush AOF) instead of exiting
/// immediately.
#[cfg(unix)]
fn install_signal_handlers() {
    unsafe {
        extern "C" fn handle_signal(_sig: libc::c_int) {
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        }
        libc::signal(libc::SIGTERM, handle_signal as libc::sighandler_t);
        libc::signal(libc::SIGINT, handle_signal as libc::sighandler_t);
    }
}

#[cfg(not(unix))]
fn install_signal_handlers() {}

/// A stream connection.
#[cfg(unix)]
enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

#[cfg(not(unix))]
enum Stream {
    Tcp(TcpStream),
}

/// Common Stream operations, implemented once per platform.
/// The TCP-specific logic (keepalive, etc.) is shared;
/// Unix-specific paths are no-ops where appropriate.
#[cfg(unix)]
impl Stream {
    fn try_clone(&self) -> io::Result<Stream> {
        match self {
            Stream::Tcp(s) => Ok(Stream::Tcp(s.try_clone()?)),
            Stream::Unix(s) => Ok(Stream::Unix(s.try_clone()?)),
        }
    }

    fn set_keepalive(&self, duration: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => {
                let socket = Socket::from(s.try_clone()?);
                let keepalive = TcpKeepalive::new();
                let keepalive = match duration {
                    Some(dur) => keepalive.with_time(dur),
                    None => keepalive,
                };
                socket.set_tcp_keepalive(&keepalive)
            }
            // UNIX sockets don't support TCP keepalive
            Stream::Unix(_) => Ok(()),
        }
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(dur),
            Stream::Unix(_) => Ok(()),
        }
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(dur),
            Stream::Unix(_) => Ok(()),
        }
    }
}

#[cfg(unix)]
impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            Stream::Unix(s) => s.read(buf),
        }
    }
}

#[cfg(unix)]
impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            Stream::Unix(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            Stream::Unix(s) => s.flush(),
        }
    }
}

#[cfg(not(unix))]
impl Stream {
    fn try_clone(&self) -> io::Result<Stream> {
        match self {
            Stream::Tcp(s) => Ok(Stream::Tcp(s.try_clone()?)),
        }
    }

    fn set_keepalive(&self, duration: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => {
                let socket = Socket::from(s.try_clone()?);
                let keepalive = TcpKeepalive::new();
                let keepalive = match duration {
                    Some(dur) => keepalive.with_time(dur),
                    None => keepalive,
                };
                socket.set_tcp_keepalive(&keepalive)
            }
        }
    }

    fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(dur),
        }
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(dur),
        }
    }
}

#[cfg(not(unix))]
impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
        }
    }
}

#[cfg(not(unix))]
impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
        }
    }
}

/// A client connection
struct Client {
    /// The socket connection
    stream: Stream,
    /// A reference to the sharded database
    db: Arc<ShardedDatabase>,
    /// The client unique identifier
    id: usize,
}

/// The database server
pub struct Server {
    /// A reference to the sharded database
    db: Arc<ShardedDatabase>,
    /// Server configuration
    config: Config,
    /// A list of channels listening for incoming connections
    listener_channels: Vec<Sender<u8>>,
    /// A list of threads listening for incoming connections
    listener_threads: Vec<thread::JoinHandle<()>>,
    /// An incremental id for new clients
    pub next_id: Arc<AtomicUsize>,
    /// Sender to signal hz thread to stop
    hz_stop: Option<Sender<()>>,
    /// Handle to the hz thread so we can join it before shutdown save.
    hz_handle: Option<thread::JoinHandle<()>>,
    /// Cluster bus stop flag
    cluster_bus_stop: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Cluster bus listener thread
    cluster_bus_handle: Option<thread::JoinHandle<()>>,
    /// Gossip timer stop flag
    gossip_stop: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Gossip timer thread
    gossip_handle: Option<thread::JoinHandle<()>>,
}

impl Client {
    /// Creates a new TCP socket client
    pub fn tcp(stream: TcpStream, db: Arc<ShardedDatabase>, id: usize) -> Client {
        Client {
            stream: Stream::Tcp(stream),
            db,
            id,
        }
    }

    /// Creates a new UNIX socket client
    #[cfg(unix)]
    pub fn unix(stream: UnixStream, db: Arc<ShardedDatabase>, id: usize) -> Client {
        Client {
            stream: Stream::Unix(stream),
            db,
            id,
        }
    }

    /// Creates a thread that writes into the client stream each response received.
    /// Uses BytesMut for zero-copy serialization and batches multiple pending
    /// responses into a single write() syscall (Dragonfly-inspired optimization).
    fn create_writer_thread(
        &self,
        sender: Sender<(Level, String)>,
        rx: Receiver<Option<Response>>,
    ) {
        let mut stream = self.stream.try_clone().unwrap();
        thread::spawn(move || {
            // Reusable buffer to avoid per-response allocations
            let mut buf = BytesMut::with_capacity(512);
            while let Ok(Some(msg)) = rx.recv() {
                // Serialize the first response
                msg.write_to(&mut buf);

                // Batch: drain any additional pending responses without blocking
                while let Ok(Some(msg)) = rx.try_recv() {
                    msg.write_to(&mut buf);
                }

                // Single write syscall for all batched responses
                match stream.write_all(&buf) {
                    Ok(_) => {
                        let _ = stream.flush();
                    }
                    Err(e) => {
                        let _ = sendlog!(sender, Warning, "Error writing to client: {:?}", e);
                    }
                }
                buf.clear();
            }
        });
    }

    /// Determines the shard index for a parsed command.
    /// Routes by the first key argument for data commands,
    /// and uses shard 0 for pubsub/admin commands.
    /// Multi-key commands route to the *minimum* shard of their keys so the
    /// command implementation (which already holds that shard's lock) can
    /// lock every other involved shard in ascending order, which is
    /// deadlock-free (see ShardedDatabase::with_key_shards).
    fn shard_for_command(parsed: &parser::ParsedCommand, num_shards: usize) -> usize {
        if num_shards <= 1 {
            return 0;
        }
        // Command names are case-insensitive in the protocol; lowercase them
        // before matching (otherwise "PUBSUB" et al. would fall through and
        // be routed by a non-key argument like "CHANNELS").
        let cmd = match parsed.get_str(0) {
            Ok(cmd) => cmd.to_ascii_lowercase(),
            Err(_) => return 0,
        };
        {
            // Pubsub and admin commands go to shard 0 for consistency
            match cmd.as_str() {
                "subscribe" | "unsubscribe" | "publish" | "psubscribe" | "punsubscribe"
                | "ssubscribe" | "sunsubscribe" | "spublish"
                | "pubsub" | "monitor" | "info" | "config" | "command" | "slowlog"
                | "client" | "cluster" | "sentinel" | "latency" | "slaveof" | "replconf" | "wait"
                | "sync" | "psync" | "asking" | "readonly" | "readwrite"
                // ("debug" is handled below: DEBUG OBJECT routes by its key.)
                | "flushall" | "save" | "bgsave" | "bgrewriteaof" | "shutdown"
                | "lastsave" | "role" | "select" | "auth" | "ping" | "echo" | "quit"
                | "time" | "reset" | "acl" | "function" | "script"
                // Whole-keyspace scans: their argument 1 is a pattern or a
                // cursor, not a key, so hashing it would pick a random shard.
                | "keys" | "scan" | "randomkey" | "dbsize" => return 0,
                _ => {}
            }
            // Scripting commands: route by the first declared key
            // (EVAL source numkeys key ... -> key sits at argument 3).
            // Keyless scripts stay on shard 0.
            if matches!(cmd.as_str(), "eval" | "evalsha" | "eval_ro" | "evalsha_ro"
                | "fcall" | "fcall_ro")
            {
                if parsed.argv.len() > 3 {
                    if let Ok(numkeys) = parsed.get_i64(2) {
                        if numkeys > 0 {
                            if let Ok(key) = parsed.get_vec(3) {
                                return database::shard::ShardedDatabase::shard_for_key(&key)
                                    % num_shards;
                            }
                        }
                    }
                }
                return 0;
            }
            // XREAD/XREADGROUP: the keys come after the STREAMS keyword
            // (everything before it is options), so argument 1 is not a key.
            if cmd == "xread" || cmd == "xreadgroup" {
                for i in 1..parsed.argv.len() {
                    if let Ok(s) = parsed.get_str(i) {
                        if s.eq_ignore_ascii_case("streams") {
                            if i + 1 < parsed.argv.len() {
                                if let Ok(key) = parsed.get_vec(i + 1) {
                                    return database::shard::ShardedDatabase::shard_for_key(&key)
                                        % num_shards;
                                }
                            }
                            break;
                        }
                    }
                }
                return 0;
            }
            // JSON commands exist in two layouts: "json.set key ..." (module
            // syntax, key at argument 1) and "json set key ..." (key at 2).
            if cmd == "json" || cmd.starts_with("json.") {
                let key_pos = if cmd == "json" { 2 } else { 1 };
                if parsed.argv.len() > key_pos {
                    if let Ok(key) = parsed.get_vec(key_pos) {
                        return database::shard::ShardedDatabase::shard_for_key(&key) % num_shards;
                    }
                }
                return 0;
            }
            // GEOSEARCHSTORE destination source ...: reads the source set and
            // stores into the destination via the cross-shard store helper,
            // so route by the *source* key (argument 2).
            if cmd == "geosearchstore" {
                if parsed.argv.len() > 2 {
                    if let Ok(key) = parsed.get_vec(2) {
                        return database::shard::ShardedDatabase::shard_for_key(&key) % num_shards;
                    }
                }
                return 0;
            }
            // XGROUP/XINFO/OBJECT carry a subcommand at argument 1 and the
            // key at argument 2 (HELP has no key and stays on shard 0).
            if cmd == "xgroup" || cmd == "xinfo" || cmd == "object" {
                if parsed.argv.len() > 2 {
                    if let Ok(key) = parsed.get_vec(2) {
                        return database::shard::ShardedDatabase::shard_for_key(&key) % num_shards;
                    }
                }
                return 0;
            }
            // DEBUG OBJECT key: the key sits at argument 2 (other
            // subcommands are keyless and stay on shard 0).
            if cmd == "debug" {
                if parsed.argv.len() > 2 {
                    if let Ok(sub) = parsed.get_str(1) {
                        if sub.eq_ignore_ascii_case("object") {
                            if let Ok(key) = parsed.get_vec(2) {
                                return database::shard::ShardedDatabase::shard_for_key(&key)
                                    % num_shards;
                            }
                        }
                    }
                }
                return 0;
            }
            // Bloom-filter commands: dotted "bf.add key ..." has the key at
            // argument 1; undotted "bf add key ..." has it at argument 2.
            if cmd == "bf" || cmd.starts_with("bf.") {
                let key_pos = if cmd == "bf" { 2 } else { 1 };
                if parsed.argv.len() > key_pos {
                    if let Ok(key) = parsed.get_vec(key_pos) {
                        return database::shard::ShardedDatabase::shard_for_key(&key) % num_shards;
                    }
                }
                return 0;
            }
            // Cuckoo/TDigest/TopK/TimeSeries module commands follow the same
            // two layouts as bloom filters: dotted "<mod>.add key ..." has the
            // key at argument 1; undotted "<mod> add key ..." at argument 2.
            if cmd == "cf"
                || cmd.starts_with("cf.")
                || cmd == "tdigest"
                || cmd.starts_with("tdigest.")
                || cmd == "topk"
                || cmd.starts_with("topk.")
                || cmd == "ts"
                || cmd.starts_with("ts.")
            {
                let key_pos = if cmd.contains('.') { 1 } else { 2 };
                if parsed.argv.len() > key_pos {
                    if let Ok(key) = parsed.get_vec(key_pos) {
                        return database::shard::ShardedDatabase::shard_for_key(&key) % num_shards;
                    }
                }
                return 0;
            }
            // Multi-key commands: route to the minimum shard over all their
            // key arguments. Must stay in sync with the command layer.
            if let Some(positions) = Self::multi_key_positions(&cmd, parsed.argv.len()) {
                let mut min: Option<usize> = None;
                for pos in positions {
                    if let Ok(key) = parsed.get_vec(pos) {
                        let idx =
                            database::shard::ShardedDatabase::shard_for_key(&key) % num_shards;
                        min = Some(match min {
                            Some(m) if m < idx => m,
                            _ => idx,
                        });
                    }
                }
                if let Some(m) = min {
                    return m;
                }
            }
        }
        // Route by first key (argument 1)
        if let Ok(key) = parsed.get_vec(1) {
            database::shard::ShardedDatabase::shard_for_key(&key) % num_shards
        } else {
            0
        }
    }

    /// Argument positions that are keys for every multi-key command, or
    /// `None` when the command operates on a single key. `argc` is the total
    /// argument count (including the command name).
    fn multi_key_positions(cmd: &str, argc: usize) -> Option<Vec<usize>> {
        let range = |from: usize, to: usize| -> Vec<usize> {
            (from..to.min(argc)).collect()
        };
        let numkeys_from = |name_pos: usize| -> Option<Vec<usize>> {
            // <cmd> [numkeys at name_pos] key [key ...] [options...]
            // The count is parsed by the command layer; here we assume every
            // argument after the count up to a known option keyword is a key.
            // To stay lenient, positions [name_pos+1 .. argc) are returned and
            // non-key options are tolerated (hashing them only skews the min;
            // the command layer still resolves every key individually).
            Some(range(name_pos + 1, argc))
        };
        match cmd {
            // key, value, key, value, ...
            "mset" | "msetnx" => Some((1..argc).step_by(2).collect()),
            // key, key, [key ...]
            "mget" | "del" | "unlink" | "exists" | "touch"
            | "sdiff" | "sinter" | "sunion"
            | "sdiffstore" | "sinterstore" | "sunionstore"
            | "pfmerge" | "pfcount" => Some(range(1, argc)),
            // key, key
            "rename" | "renamenx" | "copy" | "smove" | "rpoplpush" | "lmove" => {
                Some(range(1, 3))
            }
            // op, dst, src, src, ...
            "bitop" => Some(range(2, argc)),
            // dst, numkeys, key [key ...] [WEIGHTS...] [AGGREGATE...]
            "zunionstore" | "zinterstore" => Some(range(1, argc)),
            // numkeys, key [key ...] [WEIGHTS...] [AGGREGATE...]
            "zdiff" | "zunion" | "zinter" | "zintercard" => numkeys_from(1),
            // dst, numkeys, key [key ...]
            "zdiffstore" => Some(range(1, argc)),
            // numkeys, key [key ...] [LIMIT limit]
            "sintercard" => numkeys_from(1),
            _ => None,
        }
    }

    /// Runs all clients commands. The function loops until the client
    /// disconnects.
    pub fn run(&mut self, sender: Sender<(Level, String)>) {
        let (stream_tx, rx) = channel::<Option<Response>>();
        self.create_writer_thread(sender.clone(), rx);

        let mut client = command::Client::new(stream_tx.clone(), self.id);
        let mut parser = Parser::new();

        let mut this_command: Option<OwnedParsedCommand>;
        let mut next_command: Option<OwnedParsedCommand> = None;
        loop {
            // FIXME: is_incomplete parses the command a second time
            if next_command.is_none() && parser.is_incomplete() {
                parser.allocate();
                let len = {
                    let pos = parser.written;
                    let buffer = parser.get_mut();

                    // read socket
                    match self.stream.read(&mut buffer[pos..]) {
                        Ok(r) => r,
                        Err(err) => {
                            let _ = sendlog!(sender, Verbose, "Reading from client: {:?}", err);
                            break;
                        }
                    }
                };
                parser.written += len;

                // client closed connection
                if len == 0 {
                    let _ = sendlog!(sender, Verbose, "Client closed connection");
                    break;
                }
            }

            // was there an error during the execution?
            let mut error = false;

            this_command = next_command;
            next_command = None;

            // try to parse received command
            let parsed_command = match &this_command {
                Some(c) => c.get_command(),
                None => {
                    match parser.next() {
                        Ok(p) => p,
                        Err(err) => {
                            match err {
                                // if it's incomplete, keep adding to the buffer
                                ParseError::Incomplete => {
                                    continue;
                                }
                                ParseError::BadProtocol(s) => {
                                    let _ = stream_tx.send(Some(Response::Error(s)));
                                    break;
                                }
                                _ => {
                                    let _ = sendlog!(
                                        sender,
                                        Verbose,
                                        "Protocol error from client: {:?}",
                                        err
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            };

            let r = {
                // Dragonfly-inspired: route to the correct shard based on the first key
                let num_shards = self.db.num_shards();

                // Intercept SAVE/BGSAVE/BGREWRITEAOF before acquiring shard lock to avoid
                // deadlock (save_to_file / rewrite_aof needs to lock all shards).
                let cmd_name = parsed_command.get_str(0).unwrap_or("");
                if cmd_name.eq_ignore_ascii_case("save") || cmd_name.eq_ignore_ascii_case("bgsave") {
                    let rdb_path = {
                        let shard = self.db.shard_write(0, 0).unwrap();
                        format!("{}/{}", shard.db.config.dir, shard.db.config.dbfilename)
                    };
                    match database::rdb::save_to_file(&self.db, &rdb_path) {
                        Ok(n) => {
                            self.db.stats.record_save();
                            let _ = sendlog!(sender, Notice, "RDB: saved {} keys to {}", n, rdb_path);
                            Ok(Response::Status("OK".to_owned()))
                        }
                        Err(e) => {
                            Ok(Response::Error(format!("ERR saving RDB: {}", e)))
                        }
                    }
                } else if cmd_name.eq_ignore_ascii_case("bgrewriteaof") {
                    let (aof_base, num_shards) = {
                        let shard = self.db.shard_write(0, 0).unwrap();
                        (shard.db.config.appendfilename.clone(), self.db.num_shards())
                    };
                    match database::aof_rewrite::rewrite_all_aofs(&self.db, &aof_base, num_shards) {
                        Ok(results) => {
                            let total: usize = results.iter().map(|(_, n)| n).sum();
                            let _ = sendlog!(sender, Notice, "AOF rewrite: rewrote {} keys across {} shards", total, num_shards);
                            Ok(Response::Status("Background append only file rewriting started".to_owned()))
                        }
                        Err(e) => {
                            Ok(Response::Error(format!("ERR rewriting AOF: {}", e)))
                        }
                    }
                } else {
                let shard_idx = Self::shard_for_command(&parsed_command, num_shards);
                let mut shard = match self.db.shard_write(0, shard_idx) {
                    Ok(shard) => shard,
                    Err(_) => break,
                };

                // execute the command on the shard's database
                command::command(parsed_command, &mut shard.db, &mut client)
                }
            };

            // check out the response
            match r {
                // received a response, send it to the client
                Ok(response) => {
                    match stream_tx.send(Some(response)) {
                        Ok(_) => (),
                        Err(_) => error = true,
                    };
                }
                // no response
                Err(err) => {
                    match err {
                        // There is no reply to send, that's ok
                        ResponseError::NoReply => (),
                        // We have to wait until a sender signals us back and then retry
                        // (Repeating the same command is actually wrong because of the timeout)
                        ResponseError::Wait(receiver) => {
                            // if we receive a None, send a nil, otherwise execute the command
                            match receiver.recv().unwrap() {
                                Some(cmd) => next_command = Some(cmd),
                                None => match stream_tx.send(Some(Response::Nil)) {
                                    Ok(_) => (),
                                    Err(_) => error = true,
                                },
                            }
                        }
                    }
                }
            }

            // if something failed, let's shut down the client
            if error {
                // kill threads
                stream_tx.send(None).expect("TODO: Ignore this error");
                client
                    .rawsender
                    .send(None)
                    .expect("TODO: Ignore this error");
                break;
            }
        }

        // The connection is over: release the connected-clients counter.
        self.db.stats.record_connection_closed();

        {
            // Pubsub state is on shard 0
            let mut shard = match self.db.shard_write(0, 0) {
                Ok(shard) => shard,
                Err(_) => return,
            };

            for (channel_name, subscriber_id) in client.subscriptions.into_iter() {
                shard.db.unsubscribe(channel_name.clone(), subscriber_id);
            }
            for (channel_name, subscriber_id) in client.sharded_subscriptions.into_iter() {
                shard.db.sunsubscribe(channel_name.clone(), subscriber_id);
            }
        }
    }
}

macro_rules! handle_listener {
    ($logger: expr, $listener: expr, $server: expr, $rx: expr, $tcp_keepalive: expr, $timeout: expr, $t: ident) => {{
        let db = $server.db.clone();
        let sender = $logger.sender();
        let next_id = $server.next_id.clone();
        thread::spawn(move || {
            for stream in $listener.incoming() {
                if $rx.try_recv().is_ok() {
                    // any new message should break
                    break;
                }
                match stream {
                    Ok(stream) => {
                        sendlog!(sender, Verbose, "Accepted connection to {:?}", stream).unwrap();
                        // Server-wide statistics are shared across shards.
                        db.stats.record_connection_opened();
                        let db1 = db.clone();
                        let mysender = sender.clone();
                        let id = next_id.fetch_add(1, Ordering::Relaxed);

                        thread::spawn(move || {
                            let mut client = Client::$t(stream, db1, id);
                            client
                                .stream
                                .set_keepalive(if $tcp_keepalive > 0 {
                                    Some(Duration::from_secs($tcp_keepalive as u64))
                                } else {
                                    None
                                })
                                .unwrap();
                            client
                                .stream
                                .set_read_timeout(if $timeout > 0 {
                                    Some(Duration::new($timeout, 0))
                                } else {
                                    None
                                })
                                .unwrap();
                            client
                                .stream
                                .set_write_timeout(if $timeout > 0 {
                                    Some(Duration::new($timeout, 0))
                                } else {
                                    None
                                })
                                .unwrap();
                            client.run(mysender);
                        });
                    }
                    Err(e) => {
                        sendlog!(sender, Warning, "Accepting client connection: {:?}", e).unwrap()
                    }
                }
            }
        })
    }};
}

impl Server {
    /// Creates a new server
    pub fn new(config: Config) -> Server {
        let sharded_db = Arc::new(ShardedDatabase::new(&config));
        // Wire each shard back to the sharded database so admin commands
        // (INFO/DBSIZE) can aggregate keyspace data across shards.
        sharded_db.init_shard_links();
        Server {
            db: sharded_db,
            config,
            listener_channels: Vec::new(),
            listener_threads: Vec::new(),
            next_id: Arc::new(AtomicUsize::default()),
            hz_stop: None,
            hz_handle: None,
            cluster_bus_stop: None,
            cluster_bus_handle: None,
            gossip_stop: None,
            gossip_handle: None,
        }
    }

    /// Gets mutable access to shard 0's database for initialization.
    pub fn get_mut_db(&self) -> std::sync::MutexGuard<'_, Shard> {
        self.db.shard_write(0, 0).unwrap()
    }

    /// Runs the server. If `config.daemonize` is true, it forks and exits.
    #[cfg(unix)]
    pub fn run(&mut self) {
        install_signal_handlers();
        let (daemonize, pidfile) = (self.config.daemonize, self.config.pidfile.clone());
        if daemonize {
            if unsafe { libc::daemon(1, 1) } == 0 {
                if let Ok(mut fp) = File::create(Path::new(&*pidfile)) {
                    match write!(fp, "{}", process::id()) {
                        Ok(_) => (),
                        Err(e) => {
                            log!(self.config.logger, Warning, "Error writing pid: {}", e);
                        }
                    }
                }
                self.start();
                self.wait_for_shutdown();
                self.stop();
            } else {
                panic!("Fork failed");
            }
        } else {
            self.start();
            self.wait_for_shutdown();
            self.stop();
        }
    }

    #[cfg(not(unix))]
    pub fn run(&mut self) {
        let daemonize = self.config.daemonize;
        if daemonize {
            panic!("Cannot daemonize in non-unix");
        } else {
            self.start();
            self.wait_for_shutdown();
            self.stop();
        }
    }

    #[cfg(windows)]
    fn set_reuse_address(&self, _socket: &Socket) -> io::Result<()> {
        Ok(())
    }

    #[cfg(not(windows))]
    fn set_reuse_address(&self, socket: &Socket) -> io::Result<()> {
        socket.set_reuse_address(true)?;
        Ok(())
    }

    /// Join the listener threads.
    pub fn join(&mut self) {
        while !self.listener_threads.is_empty() {
            let _ = self.listener_threads.pop().unwrap().join();
        }
    }

    /// Block until a shutdown signal (SIGTERM/SIGINT) is received or the
    /// server is otherwise asked to stop.  Polls the global atomic flag
    /// installed by the signal handler.
    fn wait_for_shutdown(&self) {
        while !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(250));
        }
    }

    /// Listens to a socket address.
    fn listen<T: ToSocketAddrs>(
        &mut self,
        t: T,
        tcp_keepalive: u32,
        timeout: u64,
        tcp_backlog: i32,
    ) -> io::Result<()> {
        for addr in t.to_socket_addrs()? {
            let (tx, rx) = channel();
            let domain = match addr {
                SocketAddr::V4(_) => Domain::IPV4,
                SocketAddr::V6(_) => Domain::IPV6,
            };
            let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
            self.set_reuse_address(&socket)?;
            socket.bind(&SockAddr::from(addr))?;
            socket.listen(tcp_backlog)?;
            let listener: std::net::TcpListener = socket.into();
            self.listener_channels.push(tx);
            {
                let th = handle_listener!(
                    self.config.logger,
                    listener,
                    self,
                    rx,
                    tcp_keepalive,
                    timeout,
                    tcp
                );
                self.listener_threads.push(th);
            }
        }
        Ok(())
    }

    /// Starts threads listening to new connections.
    pub fn start(&mut self) {
        let tcp_keepalive = self.config.tcp_keepalive;
        let timeout = self.config.timeout;
        let tcp_backlog = self.config.tcp_backlog;
        let addresses = self.config.addresses();
        for (host, port) in addresses {
            match self.listen((&host[..], port), tcp_keepalive, timeout, tcp_backlog) {
                Ok(_) => {
                    log!(
                        self.config.logger,
                        Notice,
                        "The server is now ready to accept connections on port {}",
                        port
                    );
                }
                Err(err) => {
                    log!(
                        self.config.logger,
                        Warning,
                        "Creating Server TCP listening socket {}:{}: {:?}",
                        host,
                        port,
                        err
                    );
                    continue;
                }
            }
        }

        self.handle_unixsocket();

        {
            let (hz_stop_tx, hz_stop_rx) = channel();
            self.hz_stop = Some(hz_stop_tx);
            let hz = self.config.hz;
            let db_ref = self.db.clone();
            let num_shards = db_ref.num_shards();
            let save_points = self.config.save.clone();
            let rdb_path = format!("{}/{}", self.config.dir, self.config.dbfilename);
            let hz_handle = thread::spawn(move || {
                let mut shard_cursor = 0usize;
                while hz_stop_rx.try_recv().is_err() {
                    // Roll the ops/sec sampling window (like Redis's serverCron)
                    // so INFO shows a fresh value even between commands.
                    db_ref.stats.instantaneous_ops_per_sec();
                    // Run active expire cycle across all shards round-robin.
                    // Each tick processes one shard; the cursor advances so all
                    // shards get serviced within num_shards ticks.
                    if let Ok(mut shard) = db_ref.shard_write(0, shard_cursor) {
                        shard.db.active_expire_cycle(10);
                    }
                    shard_cursor = (shard_cursor + 1) % num_shards;

                    // Check if any save threshold is met and trigger auto-RDB.
                    // Skip auto-save if a graceful shutdown is in progress to
                    // avoid the hz thread's rename overwriting the shutdown save.
                    if !SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
                        && db_ref.stats.should_auto_save(&save_points)
                    {
                        match database::rdb::save_to_file(&db_ref, &rdb_path) {
                            Ok(n) => {
                                db_ref.stats.record_save();
                                eprintln!("RDB: auto-save {} keys to {}", n, rdb_path);
                            }
                            Err(e) => {
                                eprintln!("RDB: auto-save failed: {}", e);
                            }
                        }
                    }

                    thread::sleep(Duration::from_millis(10000 / hz as u64));
                }
            });
            self.hz_handle = Some(hz_handle);
        }

        // Load RDB snapshot if it exists (before AOF, so AOF can replay on top)
        {
            let rdb_path = {
                let shard = self.db.shard_write(0, 0).unwrap();
                format!("{}/{}", shard.db.config.dir, shard.db.config.dbfilename)
            };
            if database::rdb::rdb_exists(&rdb_path) {
                match database::rdb::load_from_file(&rdb_path) {
                    Ok(snapshot) => {
                        database::rdb::apply_snapshot(&self.db, snapshot);
                        self.db.stats.record_save(); // initialize last_save_time
                        log!(self.config.logger, Notice, "Loaded RDB snapshot from {}", rdb_path);
                    }
                    Err(e) => {
                        log!(self.config.logger, Warning, "Failed to load RDB from {}: {}", rdb_path, e);
                    }
                }
            }
        }

        // Load AOF if configured (on all shards, one AOF file per shard)
        if self.config.appendonly {
            let fsync_policy = persistence::aof::AofFsyncPolicy::from_str(&self.config.appendfsync);
            let num_shards = self.db.num_shards();
            for shard_idx in 0..num_shards {
                if let Ok(mut shard) = self.db.shard_write(0, shard_idx) {
                    let aof_path = if num_shards > 1 {
                        format!("{}.{}", self.config.appendfilename, shard_idx)
                    } else {
                        self.config.appendfilename.clone()
                    };
                    match persistence::aof::Aof::with_fsync_policy(&aof_path, fsync_policy.clone()) {
                        Ok(aof) => {
                            shard.db.aof = Some(aof);
                            command::aof::load(&mut shard.db);
                            log!(self.config.logger, Notice, "Loaded AOF for shard {} from {} (fsync={:?})", shard_idx, aof_path, fsync_policy);
                        }
                        Err(e) => {
                            log!(self.config.logger, Warning, "Failed to open AOF for shard {}: {:?}", shard_idx, e);
                        }
                    }
                }
            }
        }

        // Start cluster bus if cluster mode is enabled
        if self.config.cluster_enabled {
            // Register self in the cluster state
            {
                let mut shard = self.db.shard_write(0, 0).unwrap();
                shard.db.cluster.register_self();
                // Try to load existing cluster config
                let config_file = shard.db.cluster.config_file.clone();
                if std::path::Path::new(&config_file).exists() {
                    let _ = database::cluster::load_cluster_config(&mut shard.db.cluster, &config_file);
                    log!(self.config.logger, Notice, "Loaded cluster config from {}", config_file);
                }
            }

            let bus_port = self.config.port.saturating_add(10000);
            let (bus_handle, bus_stop) = cluster_bus::start_cluster_bus(
                bus_port,
                self.db.clone(),
                self.config.logger.clone(),
            );
            self.cluster_bus_handle = Some(bus_handle);
            self.cluster_bus_stop = Some(bus_stop);

            // Start gossip timer (ping every 1 second)
            let (gossip_handle, gossip_stop) = cluster_bus::start_gossip_timer(
                self.db.clone(),
                self.config.logger.clone(),
                1000,
            );
            self.gossip_handle = Some(gossip_handle);
            self.gossip_stop = Some(gossip_stop);
        }
    }

    #[cfg(unix)]
    fn handle_unixsocket(&mut self) {
        if let Some(unixsocket) = &self.config.unixsocket {
            let tcp_keepalive = self.config.tcp_keepalive;
            let timeout = self.config.timeout;

            let (tx, rx) = channel();
            self.listener_channels.push(tx);
            let listener = match UnixListener::bind(unixsocket) {
                Ok(l) => l,
                Err(err) => {
                    log!(
                        self.config.logger,
                        Warning,
                        "Creating Server Unix socket {}: {:?}",
                        unixsocket,
                        err
                    );
                    return;
                }
            };
            let th = handle_listener!(
                self.config.logger,
                listener,
                self,
                rx,
                tcp_keepalive,
                timeout,
                unix
            );
            self.listener_threads.push(th);
        }
    }

    #[cfg(not(unix))]
    fn handle_unixsocket(&mut self) {
        if self.config.unixsocket.is_some() {
            let _ = writeln!(
                &mut std::io::stderr(),
                "Ignoring unixsocket in non unix environment\n"
            );
        }
    }

    /// Sends a kill signal to the listeners and connects to the incoming
    /// connections to break the listening loop.
    pub fn stop(&mut self) {
        for sender in self.listener_channels.iter() {
            let _ = sender.send(0);
            for (host, port) in self.config.addresses() {
                for addrs in (&host[..], port).to_socket_addrs().unwrap() {
                    let _ = TcpStream::connect(addrs);
                }
            }
        }
        if let Some(t) = &self.hz_stop {
            let _ = t.send(());
        }
        // Wait for the hz thread to finish so its auto-save cannot race
        // with the shutdown RDB save below (both write the same temp file).
        if let Some(h) = self.hz_handle.take() {
            let _ = h.join();
        }
        // Stop cluster bus and gossip timer
        if let Some(stop) = &self.cluster_bus_stop {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(stop) = &self.gossip_stop {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        // Save cluster config on shutdown
        if self.config.cluster_enabled {
            if let Ok(shard) = self.db.shard_write(0, 0) {
                let _ = database::cluster::save_cluster_config(&shard.db.cluster, &shard.db.cluster.config_file.clone());
            }
        }
        // Save RDB snapshot on shutdown
        {
            let rdb_path = {
                if let Ok(shard) = self.db.shard_write(0, 0) {
                    format!("{}/{}", shard.db.config.dir, shard.db.config.dbfilename)
                } else {
                    String::new()
                }
            };
            if !rdb_path.is_empty() {
                match database::rdb::save_to_file(&self.db, &rdb_path) {
                    Ok(n) => {
                        self.db.stats.record_save();
                        eprintln!("RDB: saved {} keys to {} on shutdown", n, rdb_path);
                    }
                    Err(e) => {
                        eprintln!("RDB: failed to save on shutdown: {}", e);
                    }
                }
            }
        }
        // Flush AOF files on shutdown (one per shard)
        if self.config.appendonly {
            let num_shards = self.db.num_shards();
            for shard_idx in 0..num_shards {
                if let Ok(mut shard) = self.db.shard_write(0, shard_idx) {
                    if let Some(ref mut aof) = shard.db.aof {
                        let _ = aof.do_fsync();
                    }
                }
            }
            eprintln!("AOF: flushed {} shard AOF files on shutdown", num_shards);
        }
        self.join();
    }
}

#[cfg(test)]
mod test_networking {

    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::str::from_utf8;
    use std::sync::atomic::Ordering;
    use std::thread;

    use config::Config;
    use logger::{Level, Logger};

    use super::Server;
    use std::time::Duration;

    #[test]
    fn parse_ping() {
        let port = 16379;

        let mut server = Server::new(Config::default(port, Logger::new(Level::Warning)));
        server.start();

        let addr = format!("127.0.0.1:{}", port);
        let streamres = TcpStream::connect(&*addr);
        assert!(streamres.is_ok());
        let mut stream = streamres.unwrap();
        let message = b"*2\r\n$4\r\nping\r\n$4\r\npong\r\n";
        assert!(stream.write(message).is_ok());
        let mut h = [0u8; 4];
        assert!(stream.read(&mut h).is_ok());
        assert_eq!(from_utf8(&h).unwrap(), "$4\r\n");
        let mut c = [0u8; 6];
        assert!(stream.read(&mut c).is_ok());
        assert_eq!(from_utf8(&c).unwrap(), "pong\r\n");
        server.stop();
    }

    #[test]
    fn allow_multiwrite() {
        let port = 16380;
        let mut server = Server::new(Config::default(port, Logger::new(Level::Warning)));
        server.start();

        let addr = format!("127.0.0.1:{}", port);
        let streamres = TcpStream::connect(&*addr);
        assert!(streamres.is_ok());
        let mut stream = streamres.unwrap();
        let message = b"*2\r\n$4\r\nping\r\n";
        assert!(stream.write(message).is_ok());
        let message = b"$4\r\npong\r\n";
        assert!(stream.write(message).is_ok());
        let mut h = [0u8; 4];
        assert!(stream.read(&mut h).is_ok());
        assert_eq!(from_utf8(&h).unwrap(), "$4\r\n");
        let mut c = [0u8; 6];
        assert!(stream.read(&mut c).is_ok());
        assert_eq!(from_utf8(&c).unwrap(), "pong\r\n");
        server.stop();
    }
    #[test]
    fn allow_stop() {
        let port = 16381;
        let mut server = Server::new(Config::default(port, Logger::new(Level::Warning)));
        server.start();
        {
            let addr = format!("127.0.0.1:{}", port);
            let streamres = TcpStream::connect(&*addr);
            assert!(streamres.is_ok());
        }
        server.stop();

        {
            let addr = format!("127.0.0.1:{}", port);
            let streamres = TcpStream::connect(&*addr);
            assert!(streamres.is_err());
        }

        server.start();
        {
            let addr = format!("127.0.0.1:{}", port);
            let streamres = TcpStream::connect(&*addr);
            assert!(streamres.is_ok());
        }
        server.stop();
    }

    #[test]
    fn allow_multiple_clients() {
        let port = 16382;
        let mut server = Server::new(Config::default(port, Logger::new(Level::Warning)));
        server.start();

        let addr = format!("127.0.0.1:{}", port);
        let _ = TcpStream::connect(&*addr);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(server.next_id.load(Ordering::Relaxed), 1);
        let _ = TcpStream::connect(&*addr);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(server.next_id.load(Ordering::Relaxed), 2);
        server.stop();
    }
}
