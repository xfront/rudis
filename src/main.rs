pub mod release;

use std::env::args;
use std::process::exit;

use crate::release::*;
use compat::getpid;
use config::Config;
use database::sentinel::SentinelState;
use logger::{Level, Logger};
use networking::Server;

fn main() {
    let all_args: Vec<String> = args().collect();
    let sentinel_mode = all_args.iter().any(|a| a == "--sentinel");

    let mut config = Config::new(Logger::new(Level::Notice));
    // Find config file (first non-flag argument, or after --sentinel)
    let config_file = all_args.iter()
        .skip(1)
        .find(|a| *a != "--sentinel")
        .cloned();
    if let Some(ref f) = config_file {
        if config.parsefile(f.clone()).is_err() {
            exit(1);
        }
    }

    if sentinel_mode {
        config.sentinel_mode = true;
    }

    let (port, daemonize) = (config.port, config.daemonize);
    let mut server = Server::new(config);
    {
        let mut shard = server.get_mut_db();
        shard.db.git_sha1 = GIT_SHA1;
        shard.db.git_dirty = GIT_DIRTY;
        shard.db.version = env!("CARGO_PKG_VERSION");
        shard.db.rustc_version = RUSTC_VERSION;

        // Initialize sentinel state if in sentinel mode
        if sentinel_mode {
            shard.db.sentinel = Some(SentinelState::new());
        }
    }

    if !daemonize {
        if sentinel_mode {
            println!("Sentinel mode");
        }
        println!("Port: {}", port);
        println!("PID: {}", getpid());
    }
    server.run();
}
