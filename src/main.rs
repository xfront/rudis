pub mod release;

use std::env::args;
use std::process::exit;

use crate::release::*;
use compat::getpid;
use config::Config;
use logger::{Level, Logger};
use networking::Server;

fn main() {
    let mut config = Config::new(Logger::new(Level::Notice));
    if let Some(f) = args().nth(1) {
        if config.parsefile(f).is_err() {
            exit(1);
        }
    }

    let (port, daemonize) = (config.port, config.daemonize);
    let mut server = Server::new(config);
    {
        let mut shard = server.get_mut_db();
        shard.db.git_sha1 = GIT_SHA1;
        shard.db.git_dirty = GIT_DIRTY;
        shard.db.version = env!("CARGO_PKG_VERSION");
        shard.db.rustc_version = RUSTC_VERSION;
    }

    if !daemonize {
        println!("Port: {}", port);
        println!("PID: {}", getpid());
    }
    server.run();
}
