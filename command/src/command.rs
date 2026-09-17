use std::{
    collections::{Bound, HashMap, HashSet},
    io::Write,
    mem::replace,
    sync::atomic::Ordering,
    sync::mpsc::channel,
    sync::mpsc::Sender,
    thread,
    time::Duration,
    usize,
};

use bitflags::bitflags;

use compat::{getos, getpid};
use database::cluster::{
    self, ClusterState, NodeFlags, NodeRole, SlotState,
    CLUSTER_SLOTS, NODE_ID_LEN,
};
use database::{zset, Database, PubsubEvent, Value};
use parser::{Argument, OwnedParsedCommand, ParsedCommand};
use response::{Response, ResponseError};
use util::mstime;
use util::glob_match;

macro_rules! opt_validate {
    ($expr: expr, $err: expr) => {
        if !($expr) {
            return Ok(Response::Error($err.to_string()));
        }
    };
}

macro_rules! try_opt_validate {
    ($expr: expr, $err: expr) => {{
        match $expr {
            Ok(r) => r,
            Err(_) => return Ok(Response::Error($err.to_string())),
        }
    }};
}

macro_rules! validate_arguments_exact {
    ($parser: expr, $expected: expr) => {
        if $parser.argv.len() != $expected {
            return Response::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                $parser.get_str(0).unwrap()
            ));
        }
    };
}

macro_rules! validate_arguments_gte {
    ($parser: expr, $expected: expr) => {
        if $parser.argv.len() < $expected {
            return Response::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                $parser.get_str(0).unwrap()
            ));
        }
    };
}

macro_rules! validate_arguments_lte {
    ($parser: expr, $expected: expr) => {
        if $parser.argv.len() > $expected {
            return Response::Error(format!(
                "ERR wrong number of arguments for '{}' command",
                $parser.get_str(0).unwrap()
            ));
        }
    };
}

macro_rules! validate {
    ($expr: expr, $err: expr) => {
        if !($expr) {
            return Response::Error($err.to_string());
        }
    };
}

macro_rules! try_validate {
    ($expr: expr, $err: expr) => {{
        match $expr {
            Ok(r) => r,
            Err(_) => return Response::Error($err.to_string()),
        }
    }};
}

macro_rules! get_values {
    ($start: expr, $stop: expr, $parser: expr, $db: expr, $dbindex: expr, $default: expr) => {{
        validate_arguments_gte!($parser, $start);
        validate_arguments_gte!($parser, $stop);
        let mut sets = Vec::with_capacity($parser.argv.len() - $start);
        for i in $start..$stop {
            let key = try_validate!($parser.get_vec(i), "Invalid key");
            match $db.get($dbindex, &key) {
                Some(e) => sets.push(e),
                None => sets.push($default),
            };
        }
        sets
    }};
}

fn generic_set(
    db: &mut Database,
    dbindex: usize,
    key: Vec<u8>,
    val: Vec<u8>,
    nx: bool,
    xx: bool,
    expiration: Option<i64>,
) -> Result<bool, Response> {
    if nx && db.get(dbindex, &key).is_some() {
        return Ok(false);
    }

    if xx && db.get(dbindex, &key).is_none() {
        return Ok(false);
    }

    match db.get_or_create(dbindex, &key).set(val) {
        Ok(_) => {
            db.key_updated(dbindex, &key);

            if let Some(msexp) = expiration {
                db.set_msexpiration(dbindex, key, msexp + mstime());
            }

            Ok(true)
        }
        Err(err) => Err(Response::Error(err.to_string())),
    }
}

fn set(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let val = try_validate!(parser.get_vec(2), "ERR syntax error");
    let mut nx = false;
    let mut xx = false;
    let mut expiration = None;
    let mut get_old = false;
    let mut skip = false;
    for i in 3..parser.argv.len() {
        if skip {
            skip = false;
            continue;
        }
        let param = try_validate!(parser.get_str(i), "ERR syntax error");
        match &*param.to_ascii_lowercase() {
            "nx" => nx = true,
            "xx" => xx = true,
            "get" => get_old = true,
            "px" => {
                let px = try_validate!(parser.get_i64(i + 1), "ERR syntax error");
                expiration = Some(px);
                skip = true;
            }
            "ex" => {
                let ex = try_validate!(parser.get_i64(i + 1), "ERR syntax error");
                expiration = Some(ex * 1000);
                skip = true;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }

    // GET option: return old value before SET
    let old_value = if get_old {
        match db.get(dbindex, &key) {
            Some(val) => match val.get() {
                Ok(data) => Some(Response::Data(data)),
                Err(_) => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            },
            None => Some(Response::Nil),
        }
    } else {
        None
    };

    match generic_set(db, dbindex, key, val, nx, xx, expiration) {
        Ok(updated) => {
            if get_old {
                // With GET, return old value regardless of NX/XX
                old_value.unwrap()
            } else if updated {
                Response::Status("OK".to_owned())
            } else {
                Response::Nil
            }
        }
        Err(r) => r,
    }
}

fn setnx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let val = try_validate!(parser.get_vec(2), "ERR syntax error");
    match generic_set(db, dbindex, key, val, true, false, None) {
        Ok(updated) => Response::Integer(if updated { 1 } else { 0 }),
        Err(r) => r,
    }
}

fn setex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let exp = try_validate!(parser.get_i64(2), "ERR syntax error");
    validate!(exp >= 0, "ERR invalid expire time");
    let val = try_validate!(parser.get_vec(3), "ERR syntax error");
    match generic_set(db, dbindex, key, val, false, false, Some(exp * 1000)) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(r) => r,
    }
}

fn psetex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "ERR syntax error");
    let exp = try_validate!(parser.get_i64(2), "ERR syntax error");
    validate!(exp >= 0, "ERR invalid expire time");
    let val = try_validate!(parser.get_vec(3), "ERR syntax error");
    match generic_set(db, dbindex, key, val, false, false, Some(exp)) {
        Ok(_) => Response::Status("OK".to_owned()),
        Err(r) => r,
    }
}

fn exists(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    })
}

fn del(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let mut c = 0;
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        if db.remove(dbindex, &key).is_some() {
            c += 1;
            db.key_updated(dbindex, &key);
        }
    }

    Response::Integer(c)
}

fn debug_object(db: &mut Database, dbindex: usize, key: Vec<u8>) -> Option<String> {
    db.get(dbindex, &key).map(|val| val.debug_object())
}

fn debug(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);

    let subcommand = try_validate!(parser.get_str(1), "Syntax error");

    match &*subcommand.to_ascii_lowercase() {
        "object" => {
            match debug_object(db, dbindex, try_validate!(parser.get_vec(2), "Invalid key")) {
                Some(s) => Response::Status(s),
                None => Response::Error("no such key".to_owned()),
            }
        }
        _ => Response::Error("Invalid debug command".to_owned()),
    }
}

fn dbsize(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    // Aggregated across shards so the count includes keys routed to other shards.
    Response::Integer(db.aggregated_keyspace(dbindex).0 as i64)
}

fn dump(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut data = vec![];

    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => match value.dump(&mut data) {
            Ok(_) => Response::Data(data),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn echo(parser: &mut ParsedCommand) -> Response {
    validate_arguments_exact!(parser, 2);
    let msg = try_validate!(parser.get_str(1), "Syntax error");
    Response::Data(msg.as_bytes().to_vec())
}

fn generic_expire(db: &mut Database, dbindex: usize, key: Vec<u8>, msexpiration: i64) -> Response {
    Response::Integer(match db.get(dbindex, &key) {
        Some(_) => {
            db.set_msexpiration(dbindex, key.clone(), msexpiration);
            db.key_updated(dbindex, &key);
            1
        }
        None => 0,
    })
}

/// Extended expire with NX|XX|GT|LT options (Redis 7.0+)
fn generic_expire_cond(db: &mut Database, dbindex: usize, key: Vec<u8>, msexpiration: i64, nx: bool, xx: bool, gt: bool, lt: bool) -> Response {
    let current_exp = db.get_msexpiration(dbindex, &key).copied();
    match db.get(dbindex, &key) {
        Some(_) => {
            // NX: Set expiry only when key has no expiry
            if nx && current_exp.is_some() { return Response::Integer(0); }
            // XX: Set expiry only when key has an expiry
            if xx && current_exp.is_none() { return Response::Integer(0); }
            // GT: Set expiry only when new expiry is greater than current
            if gt {
                if let Some(cur) = current_exp {
                    if msexpiration <= cur { return Response::Integer(0); }
                }
            }
            // LT: Set expiry only when new expiry is less than current
            if lt {
                if let Some(cur) = current_exp {
                    if msexpiration >= cur { return Response::Integer(0); }
                }
            }
            db.set_msexpiration(dbindex, key.clone(), msexpiration);
            db.key_updated(dbindex, &key);
            Response::Integer(1)
        }
        None => Response::Integer(0),
    }
}

fn expire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    // Parse optional NX|XX|GT|LT
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    let mut i = 3;
    while i < parser.argv.len() {
        match try_validate!(parser.get_str(i), "ERR syntax error").to_ascii_lowercase().as_str() {
            "nx" => nx = true,
            "xx" => xx = true,
            "gt" => gt = true,
            "lt" => lt = true,
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
        i += 1;
    }
    if nx || xx || gt || lt {
        generic_expire_cond(db, dbindex, key, mstime() + expiration * 1000, nx, xx, gt, lt)
    } else {
        generic_expire(db, dbindex, key, mstime() + expiration * 1000)
    }
}

fn expireat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    let mut i = 3;
    while i < parser.argv.len() {
        match try_validate!(parser.get_str(i), "ERR syntax error").to_ascii_lowercase().as_str() {
            "nx" => nx = true,
            "xx" => xx = true,
            "gt" => gt = true,
            "lt" => lt = true,
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
        i += 1;
    }
    if nx || xx || gt || lt {
        generic_expire_cond(db, dbindex, key, expiration * 1000, nx, xx, gt, lt)
    } else {
        generic_expire(db, dbindex, key, expiration * 1000)
    }
}

fn pexpire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    let mut i = 3;
    while i < parser.argv.len() {
        match try_validate!(parser.get_str(i), "ERR syntax error").to_ascii_lowercase().as_str() {
            "nx" => nx = true,
            "xx" => xx = true,
            "gt" => gt = true,
            "lt" => lt = true,
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
        i += 1;
    }
    if nx || xx || gt || lt {
        generic_expire_cond(db, dbindex, key, mstime() + expiration, nx, xx, gt, lt)
    } else {
        generic_expire(db, dbindex, key, mstime() + expiration)
    }
}

fn pexpireat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let expiration = try_validate!(parser.get_i64(2), "Invalid expiration");
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    let mut i = 3;
    while i < parser.argv.len() {
        match try_validate!(parser.get_str(i), "ERR syntax error").to_ascii_lowercase().as_str() {
            "nx" => nx = true,
            "xx" => xx = true,
            "gt" => gt = true,
            "lt" => lt = true,
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
        i += 1;
    }
    if nx || xx || gt || lt {
        generic_expire_cond(db, dbindex, key, expiration, nx, xx, gt, lt)
    } else {
        generic_expire(db, dbindex, key, expiration)
    }
}

fn flushdb(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    db.clear(dbindex);

    Response::Status("OK".to_owned())
}

fn generic_ttl(db: &mut Database, dbindex: usize, key: &[u8], divisor: i64) -> Response {
    Response::Integer(match db.get(dbindex, key) {
        Some(_) => match db.get_msexpiration(dbindex, key) {
            Some(exp) => (exp - mstime()) / divisor,
            None => -1,
        },
        None => -2,
    })
}

fn ttl(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_ttl(db, dbindex, &key, 1000)
}

fn pttl(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_ttl(db, dbindex, &key, 1)
}

fn persist(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = Response::Integer(match db.remove_msexpiration(dbindex, &key) {
        Some(_) => 1,
        None => 0,
    });
    db.key_updated(dbindex, &key);
    r
}

fn dbtype(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    match db.get(dbindex, &key) {
        Some(Value::Nil) => Response::Data("none".to_owned().into_bytes()),
        Some(Value::String(_)) => Response::Data("string".to_owned().into_bytes()),
        Some(Value::List(_)) => Response::Data("list".to_owned().into_bytes()),
        Some(Value::Set(_)) => Response::Data("set".to_owned().into_bytes()),
        Some(Value::SortedSet(_)) => Response::Data("zset".to_owned().into_bytes()),
        Some(Value::Hash(_)) => Response::Data("hash".to_owned().into_bytes()),
        Some(Value::Stream(_)) => Response::Data("stream".to_owned().into_bytes()),
        Some(Value::BloomFilter(_)) => Response::Data("MBbloom--".to_owned().into_bytes()),
        Some(Value::CuckooFilter(_)) => Response::Data("MBbloom--".to_owned().into_bytes()),
        Some(Value::TDigest(_)) => Response::Data("MBbloom--".to_owned().into_bytes()),
        Some(Value::TopK(_)) => Response::Data("MBbloom--".to_owned().into_bytes()),
        Some(Value::Json(_)) => Response::Data("ReJSON-RL".to_owned().into_bytes()),
        Some(Value::TimeSeries(_)) => Response::Data("timeseries".to_owned().into_bytes()),
        None => Response::Data("none".to_owned().into_bytes()),
    }
}

fn flushall(parser: &mut ParsedCommand, db: &mut Database, _: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    db.clearall();

    Response::Status("OK".to_owned())
}

fn append(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let val = try_validate!(parser.get_vec(2), "Invalid value");
    let r = {
        let oldval = db.get_or_create(dbindex, &key);
        let oldlen = match oldval.strlen() {
            Ok(len) => len,
            Err(err) => return Response::Error(err.to_string()),
        };
        validate!(
            oldlen + val.len() <= 512 * 1024 * 1024,
            "ERR string exceeds maximum allowed size (512MB)"
        );
        match oldval.append(val) {
            Ok(len) => Response::Integer(len as i64),
            Err(err) => Response::Error(err.to_string()),
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn generic_get(db: &Database, dbindex: usize, key: Vec<u8>, err_on_wrongtype: bool) -> Response {
    let obj = db.get(dbindex, &key);
    match obj {
        Some(value) => match value.get() {
            Ok(r) => Response::Data(r),
            Err(err) => {
                if err_on_wrongtype {
                    Response::Error(err.to_string())
                } else {
                    Response::Nil
                }
            }
        },
        None => Response::Nil,
    }
}

fn get(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    generic_get(db, dbindex, key, true)
}

fn mget(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    let mut responses = Vec::with_capacity(parser.argv.len() - 1);
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        responses.push(generic_get(db, dbindex, key, false));
    }
    Response::Array(responses)
}

fn getrange(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop = try_validate!(parser.get_i64(3), "Invalid range");
    let obj = db.get(dbindex, &key);

    match obj {
        Some(value) => match value.getrange(start, stop) {
            Ok(r) => Response::Data(r),
            Err(e) => Response::Error(e.to_string()),
        },
        None => Response::Nil,
    }
}

fn setrange(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = {
        let v = try_validate!(parser.get_i64(2), "Invalid index");
        if v < 0 {
            return Response::Error("ERR offset is out of range".to_owned());
        }
        v as usize
    };
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = {
        if db.get(dbindex, &key).is_none() && value.is_empty() {
            return Response::Integer(0);
        }
        let oldval = db.get_or_create(dbindex, &key);
        validate!(
            index + value.len() <= 512 * 1024 * 1024,
            "ERR string exceeds maximum allowed size (512MB)"
        );
        match oldval.setrange(index, value) {
            Ok(s) => Response::Integer(s as i64),
            Err(e) => Response::Error(e.to_string()),
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn setbit(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(
        parser.get_i64(2),
        "ERR bit offset is not an integer or out of range"
    );
    validate!(
        index >= 0 && index < 4 * 1024 * 1024 * 1024,
        "ERR bit offset is not an integer or out of range"
    );
    let value = try_validate!(
        parser.get_i64(3),
        "ERR bit is not an integer or out of range"
    );
    validate!(
        value == 0 || value == 1,
        "ERR bit is not an integer or out of range"
    );
    let r = match db
        .get_or_create(dbindex, &key)
        .setbit(index as usize, value == 1)
    {
        Ok(s) => Response::Integer(if s { 1 } else { 0 }),
        Err(e) => Response::Error(e.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn getbit(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    validate!(index >= 0, "Invalid index");
    match db.get(dbindex, &key) {
        Some(v) => match v.getbit(index as usize) {
            Ok(s) => Response::Integer(if s { 1 } else { 0 }),
            Err(e) => Response::Error(e.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn strlen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let obj = db.get(dbindex, &key);

    match obj {
        Some(value) => match value.strlen() {
            Ok(r) => Response::Integer(r as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn generic_incr(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    increment: i64,
) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = match db.get_or_create(dbindex, &key).incr(increment) {
        Ok(val) => Response::Integer(val),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn incr(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    generic_incr(parser, db, dbindex, 1)
}

fn decr(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    generic_incr(parser, db, dbindex, -1)
}

fn incrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    match parser.get_i64(2) {
        Ok(increment) => generic_incr(parser, db, dbindex, increment),
        Err(_) => Response::Error("Invalid increment".to_owned()),
    }
}

fn decrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    match parser.get_i64(2) {
        Ok(decrement) => generic_incr(parser, db, dbindex, -decrement),
        Err(_) => Response::Error("Invalid increment".to_owned()),
    }
}

fn incrbyfloat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let increment = try_validate!(parser.get_f64(2), "Invalid increment");
    let r = match db.get_or_create(dbindex, &key).incrbyfloat(increment) {
        Ok(val) => Response::Data(format!("{}", val).into_bytes()),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn pfadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut values = Vec::with_capacity(parser.argv.len() - 2);
    for i in 2..parser.argv.len() {
        values.push(try_validate!(parser.get_vec(i), "Invalid value"));
    }
    let r = match db.get_or_create(dbindex, &key).pfadd(values) {
        Ok(val) => Response::Integer(if val { 1 } else { 0 }),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn pfcount(parser: &ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    if parser.argv.len() == 2 {
        let key = try_validate!(parser.get_vec(1), "Invalid key");
        Response::Integer(match db.get(dbindex, &key) {
            Some(v) => match v.pfcount() {
                Ok(val) => val as i64,
                Err(err) => return Response::Error(err.to_string()),
            },
            None => 0,
        })
    } else {
        let mut values = Vec::with_capacity(parser.argv.len() - 1);
        for i in 1..parser.argv.len() {
            let key = try_validate!(parser.get_vec(i), "Invalid key");
            if let Some(v) = db.get(dbindex, &key) {
                values.push(v);
            }
        }
        let mut val = Value::Nil;
        if let Err(err) = val.pfmerge(values) {
            return Response::Error(err.to_string());
        }
        Response::Integer(match val.pfcount() {
            Ok(v) => v as i64,
            Err(err) => return Response::Error(err.to_string()),
        })
    }
}

fn pfmerge(parser: &ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    let (val, r) = {
        let mut val = Value::Nil;
        if let Some(v) = db.get(dbindex, &key) {
            try_validate!(val.set(try_validate!(v.get(), "ERR")), "ERR"); // FIXME unnecesary clone
        }
        let mut values = Vec::with_capacity(parser.argv.len() - 2);
        for i in 2..parser.argv.len() {
            let key = try_validate!(parser.get_vec(i), "Invalid key");
            if let Some(v) = db.get(dbindex, &key) {
                values.push(v);
            }
        }

        let r = match val.pfmerge(values) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        };
        (val, r)
    };

    {
        let value = db.get_or_create(dbindex, &key);
        *value = val;
    }
    db.key_updated(dbindex, &key);
    r
}

fn generic_push(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    right: bool,
    create: bool,
) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut r = Response::Nil;
    for i in 2..parser.argv.len() {
        let val = try_validate!(parser.get_vec(i), "Invalid value");
        let el;
        if create {
            el = db.get_or_create(dbindex, &key);
        } else {
            match db.get_mut(dbindex, &key) {
                Some(_el) => el = _el,
                None => return Response::Integer(0),
            }
        }
        r = match el.push(val, right) {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        }
    }
    db.key_updated(dbindex, &key);
    r
}

fn lpush(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, false, true)
}

fn rpush(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, true, true)
}

fn lpushx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, false, false)
}

fn rpushx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_push(parser, db, dbindex, true, false)
}

fn generic_pop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    right: bool,
) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = {
        match db.get_mut(dbindex, &key) {
            Some(list) => match list.pop(right) {
                Ok(el) => match el {
                    Some(val) => Response::Data(val),
                    None => Response::Nil,
                },
                Err(err) => Response::Error(err.to_string()),
            },
            None => Response::Nil,
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn lpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_pop(parser, db, dbindex, false)
}

fn rpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_pop(parser, db, dbindex, true)
}

fn generic_rpoplpush(
    db: &mut Database,
    dbindex: usize,
    source: &[u8],
    destination: &[u8],
) -> Response {
    if let Some(Err(_)) = db.get(dbindex, destination).map(|el| el.llen()) {
        return Response::Error("WRONGTYPE Destination is not a list".to_owned());
    }

    let el = {
        let sourcelist = match db.get_mut(dbindex, source) {
            Some(sourcelist) => {
                if sourcelist.llen().is_err() {
                    return Response::Error("WRONGTYPE Source is not a list".to_owned());
                }
                sourcelist
            }
            None => return Response::Nil,
        };
        match sourcelist.pop(true) {
            Ok(el) => match el {
                Some(el) => el,
                None => return Response::Nil,
            },
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    let resp = {
        let destinationlist = db.get_or_create(dbindex, destination);
        if let Err(e) = destinationlist.push(el.clone(), false) {
            return Response::Error(e.to_string());
        }

        Response::Data(el)
    };
    db.key_updated(dbindex, source);
    db.key_updated(dbindex, destination);
    resp
}

fn rpoplpush(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let source = try_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_validate!(parser.get_vec(2), "Invalid destination");
    generic_rpoplpush(db, dbindex, &source, &destination)
}

fn brpoplpush(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() == 4, "Wrong number of parameters");

    let source = try_opt_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_opt_validate!(parser.get_vec(2), "Invalid destination");
    let timeout = try_opt_validate!(parser.get_i64(3), "ERR timeout is not an integer");
    let time = mstime();

    let r = generic_rpoplpush(db, dbindex, &source, &destination);
    if r != Response::Nil {
        return Ok(r);
    }

    let (txkey, rxkey) = channel();
    let (txcommand, rxcommand) = channel();
    if timeout > 0 {
        let tx = txcommand.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(timeout as u64));
            let _ = tx.send(None);
        });
    }
    let command_name = try_opt_validate!(parser.get_vec(0), "Invalid command");
    db.key_subscribe(dbindex, &source, txkey);
    thread::spawn(move || {
        let _ = rxkey.recv();
        let newtimeout = if timeout == 0 {
            0
        } else {
            let mut t = timeout as i64 * 1000 - mstime() + time;
            if t <= 0 {
                t = 1;
            }
            t
        };
        // This code is ugly. I was stuck for a week trying to figure out how
        // to do this and this is the best I got. I'm sorry.
        let mut data = vec![];
        let mut arguments = vec![];
        data.extend(command_name);
        arguments.push(Argument {
            pos: 0,
            len: data.len(),
        });
        arguments.push(Argument {
            pos: data.len(),
            len: source.len(),
        });
        data.extend(source);
        arguments.push(Argument {
            pos: data.len(),
            len: destination.len(),
        });
        data.extend(destination);
        let timeout_formatted = format!("{}", newtimeout);
        arguments.push(Argument {
            pos: data.len(),
            len: timeout_formatted.len(),
        });
        data.extend(timeout_formatted.into_bytes());
        let _ = txcommand.send(Some(OwnedParsedCommand::new(data, arguments)));
    });

    Err(ResponseError::Wait(rxcommand))
}

fn generic_bpop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    right: bool,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let time = mstime();

    let mut keys = vec![];
    for i in 1..parser.argv.len() - 1 {
        let key = try_opt_validate!(parser.get_vec(i), "Invalid key");
        let val = match db.get_mut(dbindex, &key) {
            Some(list) => match list.pop(right) {
                Ok(el) => el,
                Err(err) => return Ok(Response::Error(err.to_string())),
            },
            None => None,
        };
        match val {
            Some(val) => {
                db.key_updated(dbindex, &key);
                return Ok(Response::Array(vec![
                    Response::Data(key),
                    Response::Data(val),
                ]));
            }
            None => keys.push(key),
        }
    }
    let timeout = try_opt_validate!(
        parser.get_i64(parser.argv.len() - 1),
        "ERR timeout is not an integer"
    );

    let (txkey, rxkey) = channel();
    let (txcommand, rxcommand) = channel();
    if timeout > 0 {
        let tx = txcommand.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(timeout as u64));
            let _ = tx.send(None);
        });
    }
    let command_name = try_opt_validate!(parser.get_vec(0), "Invalid command");
    for key in keys.iter() {
        db.key_subscribe(dbindex, key, txkey.clone());
    }
    thread::spawn(move || {
        let _ = rxkey.recv();
        let newtimeout = if timeout == 0 {
            0
        } else {
            let mut t = timeout as i64 * 1000 - mstime() + time;
            if t <= 0 {
                t = 1;
            }
            t
        };
        // This code is ugly. I was stuck for a week trying to figure out how
        // to do this and this is the best I got. I'm sorry.
        let mut data = vec![];
        let mut arguments = vec![];
        data.extend(command_name);
        arguments.push(Argument {
            pos: 0,
            len: data.len(),
        });
        for k in keys {
            arguments.push(Argument {
                pos: data.len(),
                len: k.len(),
            });
            data.extend(k);
        }
        let timeout_formatted = format!("{}", newtimeout);
        arguments.push(Argument {
            pos: data.len(),
            len: timeout_formatted.len(),
        });
        data.extend(timeout_formatted.into_bytes());
        let _ = txcommand.send(Some(OwnedParsedCommand::new(data, arguments)));
    });

    Err(ResponseError::Wait(rxcommand))
}

fn brpop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
) -> Result<Response, ResponseError> {
    generic_bpop(parser, db, dbindex, true)
}

fn blpop(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
) -> Result<Response, ResponseError> {
    generic_bpop(parser, db, dbindex, false)
}

fn lindex(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");

    match db.get(dbindex, &key) {
        Some(el) => match el.lindex(index) {
            Ok(el) => match el {
                Some(val) => Response::Data(val.to_vec()),
                None => Response::Nil,
            },
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn linsert(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 5);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let before_str = try_validate!(parser.get_str(2), "Syntax error");
    let pivot = try_validate!(parser.get_vec(3), "Invalid pivot");
    let value = try_validate!(parser.get_vec(4), "Invalid value");
    let before;
    match &*before_str.to_ascii_lowercase() {
        "after" => before = false,
        "before" => before = true,
        _ => return Response::Error("ERR syntax error".to_owned()),
    };
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.linsert(before, pivot, value) {
            Ok(r) => match r {
                Some(listsize) => Response::Integer(listsize as i64),
                None => Response::Integer(-1),
            },
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(-1),
    };
    db.key_updated(dbindex, &key);
    r
}

fn llen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    match db.get(dbindex, &key) {
        Some(el) => match el.llen() {
            Ok(listsize) => Response::Integer(listsize as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn lrange(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid range");
    let stop = try_validate!(parser.get_i64(3), "Invalid range");

    match db.get(dbindex, &key) {
        Some(el) => match el.lrange(start, stop) {
            Ok(items) => {
                Response::Array(items.iter().map(|i| Response::Data(i.to_vec())).collect())
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    }
}

fn lrem(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let count = try_validate!(parser.get_i64(2), "Invalid count");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.lrem(count < 0, count.abs() as usize, value) {
            Ok(removed) => Response::Integer(removed as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(Vec::new()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn lset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let index = try_validate!(parser.get_i64(2), "Invalid index");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.lset(index, value) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Error("ERR no such key".to_owned()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn ltrim(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let r = match db.get_mut(dbindex, &key) {
        Some(el) => match el.ltrim(start, stop) {
            Ok(()) => Response::Status("OK".to_owned()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Status("OK".to_owned()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn sadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    let set_max_intset_entries = db.config.set_max_intset_entries;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argv.len() {
            let val = try_validate!(parser.get_vec(i), "Invalid value");
            match el.sadd(val, set_max_intset_entries) {
                Ok(added) => {
                    if added {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    db.key_updated(dbindex, &key);

    Response::Integer(count)
}

fn srem(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() > 2, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = db.get_or_create(dbindex, &key);
        for i in 2..parser.argv.len() {
            let val = try_validate!(parser.get_vec(i), "Invalid value");
            match el.srem(&val) {
                Ok(removed) => {
                    if removed {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    db.key_updated(dbindex, &key);

    Response::Integer(count)
}

fn sismember(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid key");

    Response::Integer(match db.get(dbindex, &key) {
        Some(el) => match el.sismember(&member) {
            Ok(e) => {
                if e {
                    1
                } else {
                    0
                }
            }
            Err(err) => return Response::Error(err.to_string()),
        },
        None => 0,
    })
}

fn srandmember(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    validate_arguments_lte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = match db.get(dbindex, &key) {
        Some(el) => el,
        None => {
            return if parser.argv.len() == 2 {
                Response::Nil
            } else {
                Response::Array(vec![])
            }
        }
    };
    if parser.argv.len() == 2 {
        match value.srandmember(1, false) {
            Ok(els) => {
                if !els.is_empty() {
                    Response::Data(els[0].clone())
                } else {
                    Response::Nil
                }
            }
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        let _count = try_validate!(parser.get_i64(2), "Invalid count");
        let count = {
            if _count < 0 {
                -_count
            } else {
                _count
            }
        } as usize;
        let allow_duplicates = _count < 0;
        match value.srandmember(count, allow_duplicates) {
            Ok(els) => Response::Array(
                els.iter()
                    .map(|x| Response::Data(x.clone()))
                    .collect::<Vec<_>>(),
            ),
            Err(err) => Response::Error(err.to_string()),
        }
    }
}

fn smembers(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = match db.get(dbindex, &key) {
        Some(el) => el,
        None => return Response::Array(vec![]),
    };
    match value.smembers() {
        Ok(els) => Response::Array(
            els.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn spop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    validate_arguments_lte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let r = {
        let value = match db.get_mut(dbindex, &key) {
            Some(el) => el,
            None => {
                return if parser.argv.len() == 2 {
                    Response::Nil
                } else {
                    Response::Array(vec![])
                }
            }
        };
        if parser.argv.len() == 2 {
            match value.spop(1) {
                Ok(els) => {
                    if !els.is_empty() {
                        Response::Data(els[0].clone())
                    } else {
                        Response::Nil
                    }
                }
                Err(err) => Response::Error(err.to_string()),
            }
        } else {
            let count = try_validate!(parser.get_i64(2), "Invalid count");
            match value.spop(count as usize) {
                Ok(els) => Response::Array(
                    els.iter()
                        .map(|x| Response::Data(x.clone()))
                        .collect::<Vec<_>>(),
                ),
                Err(err) => Response::Error(err.to_string()),
            }
        }
    };
    db.key_updated(dbindex, &key);
    r
}

fn smove(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let source_key = try_validate!(parser.get_vec(1), "Invalid key");
    let destination_key = try_validate!(parser.get_vec(2), "Invalid destination");
    let member = try_validate!(parser.get_vec(3), "Invalid member");

    {
        if let Some(e) = db.get(dbindex, &destination_key) {
            if !e.is_set() {
                return Response::Error(
                    "WRONGTYPE Operation against a key holding the wrong \
                         kind of value"
                        .to_owned(),
                );
            }
        }
    }
    {
        let source = match db.get_mut(dbindex, &source_key) {
            Some(s) => s,
            None => return Response::Integer(0),
        };

        match source.srem(&member) {
            Ok(removed) => {
                if !removed {
                    return Response::Integer(0);
                }
            }
            Err(err) => return Response::Error(err.to_string()),
        }
    }

    let set_max_intset_entries = db.config.set_max_intset_entries;
    {
        let destination = db.get_or_create(dbindex, &destination_key);
        match destination.sadd(member, set_max_intset_entries) {
            Ok(_) => (),
            Err(err) => panic!("Unexpected failure {}", err.to_string()),
        }
    }

    db.key_updated(dbindex, &source_key);
    db.key_updated(dbindex, &destination_key);

    Response::Integer(1)
}

fn scard(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };

    match el.scard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sdiff(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![]),
    };
    let nil = Value::Nil;
    let sets = get_values!(2, parser.argv.len(), parser, db, dbindex, &nil);

    match el.sdiff(&sets) {
        Ok(set) => Response::Array(
            set.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sdiffstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let key = try_validate!(parser.get_vec(2), "Invalid key");
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        let nil = Value::Nil;
        let sets = get_values!(3, parser.argv.len(), parser, db, dbindex, &nil);
        match el.sdiff(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    db.key_updated(dbindex, &destination_key);
    Response::Integer(r)
}

fn sinter(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(vec![]),
    };
    let nil = Value::Nil;
    let sets = get_values!(2, parser.argv.len(), parser, db, dbindex, &nil);
    match el.sinter(&sets) {
        Ok(set) => Response::Array(
            set.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sinterstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let key = try_validate!(parser.get_vec(2), "Invalid key");
        let nil = Value::Nil;
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => &nil,
        };
        let sets = get_values!(3, parser.argv.len(), parser, db, dbindex, &nil);
        match el.sinter(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    db.key_updated(dbindex, &destination_key);
    Response::Integer(r)
}

fn sunion(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let defaultel = Value::Nil;
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => &defaultel,
    };
    let nil = Value::Nil;
    let sets = get_values!(2, parser.argv.len(), parser, db, dbindex, &nil);

    match el.sunion(&sets) {
        Ok(set) => Response::Array(
            set.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn sunionstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let destination_key = try_validate!(parser.get_vec(1), "Invalid destination");
    let set = {
        let key = try_validate!(parser.get_vec(2), "Invalid key");
        let defaultel = Value::Nil;
        let el = match db.get(dbindex, &key) {
            Some(e) => e,
            None => &defaultel,
        };
        let nil = Value::Nil;
        let sets = get_values!(3, parser.argv.len(), parser, db, dbindex, &nil);
        match el.sunion(&sets) {
            Ok(set) => set,
            Err(err) => return Response::Error(err.to_string()),
        }
    };

    db.remove(dbindex, &destination_key);
    let r = set.len() as i64;
    db.get_or_create(dbindex, &destination_key).create_set(set);
    db.key_updated(dbindex, &destination_key);
    Response::Integer(r)
}

fn zadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    let len = parser.argv.len();
    validate!(len >= 4, "Wrong number of parameters");
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut incr = false;
    let mut i = 2;

    // up to 4 optional flags
    for _ in 0..4 {
        let opt = match parser.get_str(i) {
            Ok(s) => s,
            Err(_) => break,
        };
        i += 1;
        match &*opt.to_ascii_lowercase() {
            "nx" => nx = true,
            "xx" => xx = true,
            "ch" => ch = true,
            "incr" => incr = true,
            _ => {
                i -= 1;
                break;
            }
        }
    }

    if xx && nx {
        return Response::Error("ERR cannot use XX and NX".to_owned());
    }

    if (len - i) % 2 != 0 {
        return Response::Error("ERR syntax error".to_owned());
    }

    if incr && len - i != 2 {
        return Response::Error("ERR syntax error".to_owned());
    }

    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    for j in 0..((len - i) / 2) {
        validate!(
            parser.get_f64(i + j * 2).is_ok(),
            "ERR value is not a valid float"
        );
    }
    {
        let el = db.get_or_create(dbindex, &key);
        for _ in 0..((len - i) / 2) {
            let score = parser.get_f64(i).unwrap();
            let val = try_validate!(parser.get_vec(i + 1), "Invalid value");
            match el.zadd(score, val, nx, xx, ch, incr) {
                Ok(added) => {
                    if added {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
            i += 2; // omg, so ugly `for`
        }
    }
    if count > 0 {
        db.key_updated(dbindex, &key);
    }

    Response::Integer(count)
}

fn zcard(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };

    match el.zcard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let element = try_validate!(parser.get_vec(2), "Invalid element");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Nil,
    };

    match el.zscore(element) {
        Ok(s) => match s {
            Some(score) => Response::Data(format!("{}", score).into_bytes()),
            None => Response::Nil,
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zincrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let newscore = {
        let el = db.get_or_create(dbindex, &key);
        let score = try_validate!(parser.get_f64(2), "ERR value is not a valid float");
        let member = try_validate!(parser.get_vec(3), "Invalid member");
        match el.zincrby(score, member) {
            Ok(score) => score,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    db.key_updated(dbindex, &key);

    Response::Data(format!("{}", newscore).into_bytes())
}

fn zrem(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut count = 0;
    {
        let el = match db.get_mut(dbindex, &key) {
            Some(el) => el,
            None => return Response::Integer(0),
        };
        for i in 2..parser.argv.len() {
            let member = try_validate!(parser.get_vec(i), "Invalid member");
            match el.zrem(member) {
                Ok(removed) => {
                    if removed {
                        count += 1
                    }
                }
                Err(err) => return Response::Error(err.to_string()),
            }
        }
    }
    if count > 0 {
        db.key_updated(dbindex, &key);
    }

    Response::Integer(count)
}

fn zremrangebyscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "ERR min or max is not a float");
    let max = try_validate!(parser.get_f64_bound(3), "ERR min or max is not a float");
    let c = {
        let el = match db.get_mut(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        match el.zremrangebyscore(min, max) {
            Ok(c) => c as i64,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    if c > 0 {
        db.key_updated(dbindex, &key);
    }
    Response::Integer(c)
}

fn zremrangebylex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = {
        let m = try_validate!(
            parser.get_vec(2),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let max = {
        let m = try_validate!(
            parser.get_vec(3),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let c = {
        let el = match db.get_mut(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        match el.zremrangebylex(min, max) {
            Ok(c) => c as i64,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    if c > 0 {
        db.key_updated(dbindex, &key);
    }
    Response::Integer(c)
}

fn zremrangebyrank(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let c = {
        let el = match db.get_mut(dbindex, &key) {
            Some(e) => e,
            None => return Response::Integer(0),
        };
        match el.zremrangebyrank(start, stop) {
            Ok(c) => c as i64,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    if c > 0 {
        db.key_updated(dbindex, &key);
    }
    Response::Integer(c)
}

fn zcount(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "ERR min or max is not a float");
    let max = try_validate!(parser.get_f64_bound(3), "ERR min or max is not a float");
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.zcount(min, max) {
        Ok(c) => Response::Integer(c as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn generic_zrange(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    rev: bool,
) -> Response {
    validate_arguments_gte!(parser, 4);
    validate_arguments_lte!(parser, 5);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = try_validate!(parser.get_i64(2), "Invalid start");
    let stop = try_validate!(parser.get_i64(3), "Invalid stop");
    let withscores = parser.argv.len() == 5;
    if withscores {
        let p4 = try_validate!(parser.get_str(4), "Syntax error");
        validate!(p4.to_ascii_lowercase() == "withscores", "Syntax error");
    }
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrange(start, stop, withscores, rev) {
        Ok(r) => Response::Array(
            r.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrange(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrange(parser, db, dbindex, false)
}

fn zrevrange(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrange(parser, db, dbindex, true)
}

fn generic_zrangebyscore(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    rev: bool,
) -> Response {
    let len = parser.argv.len();
    validate!(len >= 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = try_validate!(parser.get_f64_bound(2), "ERR min or max is not a float");
    let max = try_validate!(parser.get_f64_bound(3), "ERR min or max is not a float");

    let mut offset = 0;
    let mut count = usize::MAX;
    let mut withscores = false;
    let mut i = 4;
    while i < len {
        let arg = &*try_validate!(parser.get_str(i), "syntax error").to_ascii_lowercase();
        match arg {
            "withscores" => {
                i += 1;
                withscores = true;
            }
            "limit" => {
                offset = try_validate!(parser.get_i64(i + 1), "syntax error") as usize;
                count = try_validate!(parser.get_i64(i + 2), "syntax error") as usize;
                i += 3;
            }
            _ => return Response::Error("syntax error".to_owned()),
        }
    }

    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrangebyscore(min, max, withscores, offset, count, rev) {
        Ok(r) => Response::Array(
            r.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrangebyscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebyscore(parser, db, dbindex, false)
}

fn zrevrangebyscore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebyscore(parser, db, dbindex, true)
}

fn get_vec_bound(mut m: Vec<u8>) -> Result<Bound<Vec<u8>>, Response> {
    if m.is_empty() {
        return Err(Response::Error(
            "ERR min or max not valid string range item".to_string(),
        ));
    }
    // FIXME: unnecessary memory move?
    Ok(match m.remove(0) as char {
        '(' => Bound::Excluded(m),
        '[' => Bound::Included(m),
        '-' => {
            if !m.is_empty() {
                return Err(Response::Error(
                    "ERR min or max not valid string range item".to_string(),
                ));
            }
            Bound::Unbounded
        }
        '+' => {
            if !m.is_empty() {
                return Err(Response::Error(
                    "ERR min or max not valid string range item".to_string(),
                ));
            }
            Bound::Unbounded
        }
        _ => {
            return Err(Response::Error(
                "ERR min or max not valid string range item".to_string(),
            ))
        }
    })
}

fn generic_zrangebylex(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    rev: bool,
) -> Response {
    let len = parser.argv.len();
    validate!(len == 4 || len == 7, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = {
        let m = try_validate!(
            parser.get_vec(2),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let max = {
        let m = try_validate!(
            parser.get_vec(3),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };

    let mut offset = 0;
    let mut count = usize::MAX;
    let limit = len >= 7;
    if limit {
        let p = try_validate!(parser.get_str(len - 3), "Syntax error");
        validate!(p.to_ascii_lowercase() == "limit", "Syntax error");
        offset = try_validate!(parser.get_i64(len - 2), "Syntax error") as usize;
        count = try_validate!(parser.get_i64(len - 1), "Syntax error") as usize;
    }

    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Array(Vec::new()),
    };
    match el.zrangebylex(min, max, offset, count, rev) {
        Ok(r) => Response::Array(
            r.iter()
                .map(|x| Response::Data(x.clone()))
                .collect::<Vec<_>>(),
        ),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrangebylex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebylex(parser, db, dbindex, false)
}

fn zrevrangebylex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_zrangebylex(parser, db, dbindex, true)
}

fn zlexcount(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let min = {
        let m = try_validate!(
            parser.get_vec(2),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let max = {
        let m = try_validate!(
            parser.get_vec(3),
            "ERR min or max not valid string range item"
        );
        match get_vec_bound(m) {
            Ok(v) => v,
            Err(e) => return e,
        }
    };
    let el = match db.get(dbindex, &key) {
        Some(e) => e,
        None => return Response::Integer(0),
    };
    match el.zlexcount(min, max) {
        Ok(c) => Response::Integer(c as i64),
        Err(err) => Response::Error(err.to_string()),
    }
}

fn generic_zrank(
    db: &mut Database,
    dbindex: usize,
    key: &[u8],
    member: Vec<u8>,
    rev: bool,
) -> Response {
    let el = match db.get(dbindex, key) {
        Some(e) => e,
        None => return Response::Nil,
    };
    let card = match el.zcard() {
        Ok(card) => card,
        Err(err) => return Response::Error(err.to_string()),
    };
    match el.zrank(member) {
        Ok(r) => match r {
            Some(v) => Response::Integer(if rev { card - v - 1 } else { v } as i64),
            None => Response::Nil,
        },
        Err(err) => Response::Error(err.to_string()),
    }
}

fn zrank(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid member");
    generic_zrank(db, dbindex, &key, member, false)
}

fn zrevrank(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member = try_validate!(parser.get_vec(2), "Invalid member");
    generic_zrank(db, dbindex, &key, member, true)
}

fn zinter_union_store(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    union: bool,
) -> Response {
    validate!(parser.argv.len() >= 4, "Wrong number of parameters");
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let value = {
        let numkeys = {
            let n = try_validate!(parser.get_i64(2), "Invalid number of keys");
            if n <= 0 {
                return Response::Error(
                    "at least 1 input key is needed for \
                     ZUNIONSTORE/ZINTERSTORE"
                        .to_string(),
                );
            }
            n as usize
        };
        let nil = Value::Nil;
        let zsets = get_values!(3, 3 + numkeys, parser, db, dbindex, &nil);
        let mut pos = 3 + numkeys;
        let mut weights = None;
        let mut aggregate = zset::Aggregate::Sum;
        if pos < parser.argv.len() {
            let arg = try_validate!(parser.get_str(pos), "syntax error");
            if arg.to_ascii_lowercase() == "weights" {
                pos += 1;
                validate!(
                    parser.argv.len() >= pos + numkeys,
                    "Wrong number of parameters"
                );
                let mut w = Vec::with_capacity(numkeys);
                for i in 0..numkeys {
                    w.push(try_validate!(
                        parser.get_f64(pos + i),
                        "ERR weight value is not a float"
                    ));
                }
                weights = Some(w);
                pos += numkeys;
            }
        };
        if pos < parser.argv.len() {
            let arg = try_validate!(parser.get_str(pos), "syntax error");
            if arg.to_ascii_lowercase() == "aggregate" {
                pos += 1;
                validate!(parser.argv.len() != pos, "Wrong number of parameters");
                aggregate = match &*try_validate!(parser.get_str(pos), "syntax error")
                    .to_ascii_lowercase()
                {
                    "sum" => zset::Aggregate::Sum,
                    "max" => zset::Aggregate::Max,
                    "min" => zset::Aggregate::Min,
                    _ => return Response::Error("syntax error".to_string()),
                };
                pos += 1;
            }
        };
        validate!(pos == parser.argv.len(), "syntax error");
        let n = Value::Nil;
        match if union {
            n.zunion(&zsets, weights, aggregate)
        } else {
            n.zinter(&zsets, weights, aggregate)
        } {
            Ok(v) => v,
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    let r = match value.zcard() {
        Ok(count) => Response::Integer(count as i64),
        Err(err) => Response::Error(err.to_string()),
    };
    *db.get_or_create(dbindex, &key) = value;
    db.key_updated(dbindex, &key);
    r
}

fn zunionstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    zinter_union_store(parser, db, dbindex, true)
}

fn zinterstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    zinter_union_store(parser, db, dbindex, false)
}

fn ping(parser: &mut ParsedCommand, client: &mut Client) -> Response {
    validate!(
        parser.argv.len() <= 2,
        format!(
            "ERR wrong number of arguments for '{}' command",
            parser.get_str(0).unwrap()
        )
    );

    if !client.subscriptions.is_empty() {
        if parser.argv.len() == 2 {
            match parser.get_vec(1) {
                Ok(r) => Response::Array(vec![Response::Data(b"pong".to_vec()), Response::Data(r)]),
                Err(err) => Response::Error(err.to_string()),
            }
        } else {
            Response::Array(vec![
                Response::Data(b"pong".to_vec()),
                Response::Data(vec![]),
            ])
        }
    } else if parser.argv.len() == 2 {
        match parser.get_vec(1) {
            Ok(r) => Response::Data(r),
            Err(err) => Response::Error(err.to_string()),
        }
    } else {
        Response::Status("PONG".to_owned())
    }
}

fn subscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions: &mut HashMap<Vec<u8>, usize>,
    pattern_subscriptions_len: usize,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        if !subscriptions.contains_key(&channel_name) {
            let subscriber_id = db.subscribe(channel_name.clone(), sender.clone());
            subscriptions.insert(channel_name.clone(), subscriber_id);
        }
        match sender.send(Some(
            PubsubEvent::Subscription(
                channel_name.clone(),
                pattern_subscriptions_len + subscriptions.len(),
            )
            .as_response(),
        )) {
            Ok(_) => None,
            Err(_) => subscriptions.remove(&channel_name),
        };
    }
    Err(ResponseError::NoReply)
}

fn unsubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions: &mut HashMap<Vec<u8>, usize>,
    pattern_subscriptions_len: usize,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    if parser.argv.len() == 1 {
        if subscriptions.is_empty() {
            let _ = sender.send(Some(
                PubsubEvent::Unsubscription(vec![], pattern_subscriptions_len).as_response(),
            ));
        } else {
            for (channel_name, subscriber_id) in subscriptions.drain() {
                db.unsubscribe(channel_name.clone(), subscriber_id);
                let _ = sender.send(Some(
                    PubsubEvent::Unsubscription(channel_name, pattern_subscriptions_len)
                        .as_response(),
                ));
            }
        }
    } else {
        for i in 1..parser.argv.len() {
            let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
            if let Some(subscriber_id) = subscriptions.remove(&channel_name) {
                db.unsubscribe(channel_name.clone(), subscriber_id);
            }
            let _ = sender.send(Some(
                PubsubEvent::Unsubscription(
                    channel_name,
                    pattern_subscriptions_len + subscriptions.len(),
                )
                .as_response(),
            ));
        }
    }
    Err(ResponseError::NoReply)
}

fn psubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions_len: usize,
    pattern_subscriptions: &mut HashMap<Vec<u8>, usize>,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let pattern = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        let subscriber_id = db.psubscribe(pattern.clone(), sender.clone());
        pattern_subscriptions.insert(pattern.clone(), subscriber_id);
        match sender.send(Some(
            PubsubEvent::PatternSubscription(
                pattern.clone(),
                subscriptions_len + pattern_subscriptions.len(),
            )
            .as_response(),
        )) {
            Ok(_) => None,
            Err(_) => pattern_subscriptions.remove(&pattern),
        };
    }
    Err(ResponseError::NoReply)
}

fn punsubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    subscriptions_len: usize,
    pattern_subscriptions: &mut HashMap<Vec<u8>, usize>,
    sender: &Sender<Option<Response>>,
) -> Result<Response, ResponseError> {
    if parser.argv.len() == 1 {
        if pattern_subscriptions.is_empty() {
            let _ = sender.send(Some(
                PubsubEvent::PatternUnsubscription(vec![], subscriptions_len).as_response(),
            ));
        } else {
            for (pattern, subscriber_id) in pattern_subscriptions.drain() {
                db.punsubscribe(pattern.clone(), subscriber_id);
                let _ = sender.send(Some(
                    PubsubEvent::PatternUnsubscription(pattern, subscriptions_len).as_response(),
                ));
            }
        }
    } else {
        for i in 1..parser.argv.len() {
            let pattern = try_opt_validate!(parser.get_vec(i), "Invalid pattern");
            if let Some(subscriber_id) = pattern_subscriptions.remove(&pattern) {
                db.punsubscribe(pattern.clone(), subscriber_id);
            }
            let _ = sender.send(Some(
                PubsubEvent::PatternUnsubscription(
                    pattern,
                    subscriptions_len + pattern_subscriptions.len(),
                )
                .as_response(),
            ));
        }
    }
    Err(ResponseError::NoReply)
}

fn publish(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 3);
    let channel_name = try_validate!(parser.get_vec(1), "Invalid channel");
    let message = try_validate!(parser.get_vec(2), "Invalid channel");
    Response::Integer(db.publish(&channel_name, &message) as i64)
}

fn monitor(
    parser: &mut ParsedCommand,
    db: &mut Database,
    rawsender: Sender<Option<Response>>,
) -> Response {
    validate_arguments_exact!(parser, 1);
    let (tx, rx) = channel();
    db.monitor_add(tx);
    thread::spawn(move || {
        while rx
            .recv()
            .ok()
            .and_then(|r| rawsender.send(Some(Response::Status(r))).ok())
            .is_some()
        {}
    });
    Response::Status("OK".to_owned())
}

#[cfg(all(target_pointer_width = "32"))]
const BITS: usize = 32;
#[cfg(all(target_pointer_width = "64"))]
const BITS: usize = 64;

/// Reads the resident set size, in bytes, from the OS (Linux only).
#[cfg(target_os = "linux")]
fn rss_bytes() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse()
                    .unwrap_or(0);
                return kb * 1024;
            }
        }
    }
    0
}

/// Reads the resident set size, in bytes, from the OS (unsupported platform).
#[cfg(not(target_os = "linux"))]
fn rss_bytes() -> u64 {
    0
}

/// Formats a byte count the way Redis does (e.g. `1.50M`).
fn human_bytes(bytes: u64) -> String {
    const UNITS: [char; 5] = ['B', 'K', 'M', 'G', 'T'];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}B", bytes)
    } else {
        format!("{:.2}{}", value, UNITS[unit])
    }
}

fn info(parser: &mut ParsedCommand, db: &Database) -> Response {
    validate_arguments_lte!(parser, 2);
    let section = &*(if parser.argv.len() == 1 {
        "default".to_owned()
    } else {
        try_validate!(parser.get_str(1), "Invalid section").to_ascii_lowercase()
    });

    let mut out = vec![];
    let show_all = section == "all";

    if section == "default" || show_all || section == "server" {
        let os = getos();
        let uptime = db.uptime();
        let lru_clock = (mstime() / 1000) & 0xFFFFFF;
        try_validate!(
            write!(
                out,
                "# Server\r\n\
                 rudis_version:{}\r\n\
                 redis_version:{}\r\n\
                 rudis_git_sha1:{}\r\n\
                 rudis_git_dirty:{}\r\n\
                 os:{} {} {}\r\n\
                 arch_bits:{}\r\n\
                 multiplexing_api:no\r\n\
                 rustc_version:{}\r\n\
                 process_id:{}\r\n\
                 run_id:{}\r\n\
                 tcp_port:{}\r\n\
                 uptime_in_seconds:{}\r\n\
                 uptime_in_days:{}\r\n\
                 lru_clock:{}\r\n\
                 \r\n\
                 ",
                db.version,
                db.version,
                db.git_sha1,
                if db.git_dirty { 1 } else { 0 },
                os.0, os.1, os.2,
                BITS,
                db.rustc_version,
                getpid(),
                db.run_id,
                db.config.port,
                uptime / 1000,
                uptime / (1000 * 60 * 60 * 24),
                lru_clock,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "clients" {
        let connected_clients = db
            .stats
            .connected_clients
            .load(Ordering::Relaxed)
            .max(0);
        try_validate!(
            write!(
                out,
                "# Clients\r\n\
                 connected_clients:{}\r\n\
                 maxclients:{}\r\n\
                 client_longest_output_list:0\r\n\
                 client_biggest_input_buf:0\r\n\
                 blocked_clients:0\r\n\
                 \r\n\
                 ",
                connected_clients,
                db.config.maxclients,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "memory" {
        let rss = rss_bytes();
        let peak = db.stats.track_memory(rss);
        try_validate!(
            write!(
                out,
                "# Memory\r\n\
                 used_memory:{}\r\n\
                 used_memory_human:{}\r\n\
                 used_memory_rss:{}\r\n\
                 used_memory_rss_human:{}\r\n\
                 used_memory_peak:{}\r\n\
                 used_memory_peak_human:{}\r\n\
                 used_memory_lua:0\r\n\
                 used_memory_overhead:0\r\n\
                 used_memory_dataset:{}\r\n\
                 maxmemory:{}\r\n\
                 maxmemory_human:{}\r\n\
                 maxmemory_policy:{}\r\n\
                 mem_fragmentation_ratio:1.00\r\n\
                 mem_allocator:system\r\n\
                 \r\n\
                 ",
                rss,
                human_bytes(rss),
                rss,
                human_bytes(rss),
                peak,
                human_bytes(peak),
                rss,
                db.config.maxmemory,
                human_bytes(db.config.maxmemory as u64),
                db.config.maxmemory_policy,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "persistence" {
        let aof_enabled = if db.config.appendonly { 1 } else { 0 };
        let lastsave = db.start_mstime / 1000;
        try_validate!(
            write!(
                out,
                "# Persistence\r\n\
                 loading:{}\r\n\
                 rdb_changes_since_last_save:{}\r\n\
                 rdb_bgsave_in_progress:0\r\n\
                 rdb_last_save_time:{}\r\n\
                 rdb_last_bgsave_status:ok\r\n\
                 rdb_last_bgsave_time_sec:-1\r\n\
                 rdb_current_bgsave_time_sec:-1\r\n\
                 aof_enabled:{}\r\n\
                 aof_rewrite_in_progress:0\r\n\
                 aof_rewrite_scheduled:0\r\n\
                 aof_last_rewrite_time_sec:-1\r\n\
                 aof_current_rewrite_time_sec:-1\r\n\
                 aof_last_bgrewrite_status:ok\r\n\
                 aof_delayed_fsync:0\r\n\
                 \r\n\
                 ",
                if db.loading { 1 } else { 0 },
                db.stats.rdb_changes_since_last_save.load(Ordering::Relaxed),
                lastsave,
                aof_enabled,
            ),
            "ERR unexpected"
        );
    }

    if show_all || section == "persistence_aof" {
        try_validate!(
            write!(
                out,
                "# AOF\r\n\
                 aof_current_size:0\r\n\
                 aof_base_size:0\r\n\
                 aof_pending_rewrite:0\r\n\
                 aof_buffer_length:0\r\n\
                 aof_rewrite_buffer_length:0\r\n\
                 aof_pending_bio_fsync:0\r\n\
                 aof_delayed_fsync:0\r\n\
                 \r\n\
                 "
            ),
            "ERR unexpected"
        );
    }

    if show_all || section == "loading_info" {
        try_validate!(
            write!(
                out,
                "# Loading\r\n\
                 loading_start_time:0\r\n\
                 loading_total_bytes:0\r\n\
                 loading_loaded_bytes:0\r\n\
                 loading_loaded_perc:0\r\n\
                 loading_eta_seconds:0\r\n\
                 \r\n\
                 "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "stats" {
        let pubsub_channels = db.pubsub_channels(None).len();
        let pubsub_patterns = db.pubsub_numpat();
        try_validate!(
            write!(
                out,
                "# Stats\r\n\
                 total_connections_received:{}\r\n\
                 total_commands_processed:{}\r\n\
                 instantaneous_ops_per_sec:{}\r\n\
                 rejected_connections:{}\r\n\
                 expired_keys:{}\r\n\
                 evicted_keys:{}\r\n\
                 keyspace_hits:{}\r\n\
                 keyspace_misses:{}\r\n\
                 pubsub_channels:{}\r\n\
                 pubsub_patterns:{}\r\n\
                 latest_fork_usec:0\r\n\
                 \r\n\
                 ",
                db.stats.total_connections_received.load(Ordering::Relaxed),
                db.stats.total_commands_processed.load(Ordering::Relaxed),
                db.stats.instantaneous_ops_per_sec(),
                db.stats.rejected_connections.load(Ordering::Relaxed),
                db.stats.expired_keys.load(Ordering::Relaxed),
                db.stats.evicted_keys.load(Ordering::Relaxed),
                db.stats.keyspace_hits.load(Ordering::Relaxed),
                db.stats.keyspace_misses.load(Ordering::Relaxed),
                pubsub_channels,
                pubsub_patterns,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "replication" {
        let role = if db.cluster.enabled {
            db.cluster.role.to_string()
        } else if db.config.slaveof.is_some() {
            "slave".to_owned()
        } else {
            "master".to_owned()
        };
        try_validate!(
            write!(
                out,
                "# Replication\r\n\
                 role:{}\r\n\
                 connected_slaves:0\r\n\
                 master_failover_state:no-failover\r\n\
                 master_replid:{}\r\n\
                 master_replid2:0000000000000000000000000000000000000000\r\n\
                 master_repl_offset:0\r\n\
                 second_repl_offset:-1\r\n\
                 repl_backlog_active:0\r\n\
                 repl_backlog_size:{}\r\n\
                 repl_backlog_first_byte_offset:0\r\n\
                 repl_backlog_histlen:0\r\n\
                 \r\n\
                 ",
                role,
                db.run_id,
                db.config.repl_backlog_size,
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "cpu" {
        try_validate!(
            write!(
                out,
                "# CPU\r\n\
                 used_cpu_sys:0.00\r\n\
                 used_cpu_user:0.00\r\n\
                 used_cpu_sys_children:0.00\r\n\
                 used_cpu_user_children:0.00\r\n\
                 \r\n\
                 "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "commandstats" {
        try_validate!(
            write!(
                out,
                "# Commandstats\r\n\
                 cluster_enabled:0\r\n\
                 \r\n\
                 "
            ),
            "ERR unexpected"
        );
    }

    if section == "default" || show_all || section == "keyspace" {
        try_validate!(write!(out, "# Keyspace\r\n"), "ERR unexpected");
        for dbindex in 0..(db.config.databases as usize) {
            let (keys, expires, avg_ttl) = db.aggregated_keyspace(dbindex);
            if keys > 0 {
                try_validate!(
                    write!(
                        out,
                        "db{}:keys={};expires={};avg_ttl={}\r\n",
                        dbindex, keys, expires, avg_ttl
                    ),
                    "ERR unexpected"
                );
            }
        }
    }

    if show_all || section == "modules" {
        try_validate!(
            write!(out, "# Modules\r\n"),
            "ERR unexpected"
        );
    }

    Response::Data(out)
}

/// Client state that exceeds the lifetime of a command
pub struct Client {
    pub dbindex: usize,
    pub auth: bool,
    pub subscriptions: HashMap<Vec<u8>, usize>,
    pub pattern_subscriptions: HashMap<Vec<u8>, usize>,
    pub sharded_subscriptions: HashMap<Vec<u8>, usize>,
    pub multi: bool,
    pub multi_commands: Vec<OwnedParsedCommand>,
    pub watched_keys: HashSet<(usize, Vec<u8>)>,
    pub id: usize,
    pub rawsender: Sender<Option<Response>>,
    pub name: Vec<u8>,
    pub current_user: String,
    /// ASKING flag: allow one access to a migrating slot.
    pub asking: bool,
    /// READONLY flag: allow reads from replica nodes in cluster mode.
    pub readonly: bool,
}

impl Client {
    pub fn mock() -> Self {
        Self::new(channel().0, 0)
    }

    pub fn new(rawsender: Sender<Option<Response>>, id: usize) -> Self {
        Client {
            dbindex: 0,
            auth: false,
            subscriptions: HashMap::new(),
            pattern_subscriptions: HashMap::new(),
            sharded_subscriptions: HashMap::new(),
            multi: false,
            multi_commands: Vec::new(),
            id,
            watched_keys: HashSet::new(),
            rawsender,
            name: vec![],
            current_user: "default".to_owned(),
            asking: false,
            readonly: false,
        }
    }
}

fn keys(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let pattern = try_validate!(parser.get_vec(1), "Invalid pattern");

    // FIXME: This might be a bit suboptimal, as db.keys already allocates a vector.
    // Instead we should collect only once.
    let responses = db.keys(dbindex, &pattern);
    Response::Array(responses.into_iter().map(Response::Data).collect())
}

fn watch(
    parser: &mut ParsedCommand,
    db: &mut Database,
    dbindex: usize,
    client_id: usize,
    watched_keys: &mut HashSet<(usize, Vec<u8>)>,
) -> Response {
    validate!(parser.argv.len() >= 2, "Wrong number of parameters");

    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        db.key_watch(dbindex, &key, client_id);
        watched_keys.insert((dbindex, key));
    }

    Response::Status("OK".to_owned())
}

fn generic_unwatch(
    db: &mut Database,
    client_id: usize,
    watched_keys: &mut HashSet<(usize, Vec<u8>)>,
) -> bool {
    let mut watched_verified = true;
    for (index, key) in watched_keys.drain().into_iter() {
        if !db.key_watch_verify(index, &key, client_id) {
            watched_verified = false;
            break;
        }
    }
    watched_verified
}

fn unwatch(
    parser: &mut ParsedCommand,
    db: &mut Database,
    client_id: usize,
    watched_keys: &mut HashSet<(usize, Vec<u8>)>,
) -> Response {
    validate!(parser.argv.len() == 1, "Wrong number of parameters");

    generic_unwatch(db, client_id, watched_keys);
    Response::Status("OK".to_owned())
}

fn multi(client: &mut Client) -> Response {
    if client.multi {
        Response::Error("ERR MULTI calls can not be nested".to_owned())
    } else {
        client.multi = true;
        Response::Status("OK".to_owned())
    }
}

fn exec(db: &mut Database, client: &mut Client) -> Response {
    if !client.multi {
        return Response::Error("ERR EXEC without MULTI".to_owned());
    }
    client.multi = false;
    let c = replace(&mut client.multi_commands, vec![]);
    if !generic_unwatch(db, client.id, &mut client.watched_keys) {
        return Response::Nil;
    }
    Response::Array(
        c.iter()
            .map(|c| command(c.get_command(), db, client).unwrap())
            .collect(),
    )
}

fn discard(db: &mut Database, client: &mut Client) -> Response {
    if !client.multi {
        Response::Error("ERR DISCARD without MULTI".to_owned())
    } else {
        client.multi = false;
        client.multi_commands = vec![];
        generic_unwatch(db, client.id, &mut client.watched_keys);
        Response::Status("OK".to_owned())
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct CommandFlags: u16 {
        /// write command (may modify the key space).
        const WRITE = 1;
        /// read command  (will never modify the key space).
        const READONLY = 2;
        /// may increase memory usage once called. Don't allow if out of memory.
        const DENYOOM = 4;
        /// admin command, like SAVE or SHUTDOWN.
        const ADMIN = 8;
        /// Pub/Sub related command.
        const PUBSUB = 16;
        /// command not allowed in scripts.
        const NOSCRIPT = 32;
        /// random command. Command is not deterministic, that is, the same command
        /// with the same arguments, with the same key space, may have different
        /// results. For instance SPOP and RANDOMKEY are two random commands.
        const RANDOM = 64;
        /// Sort command output array if called from script, so that the output
        /// is deterministic.
        const SORT_FOR_SCRIPT = 128;
        /// Allow command while loading the database.
        const LOADING = 256;
        /// Allow command while a slave has stale data but is not allowed to
        /// server this data. Normally no command is accepted in this condition
        /// but just a few.
        const STALE = 512;
        /// Do not automatically propagate the command on MONITOR.
        const SKIP_MONITOR = 1024;
        /// Perform an implicit ASKING for this command, so the command will be
        /// accepted in cluster mode if the slot is marked as 'importing'.
        const ASKING = 2048;
        /// Fast command(1) or O(log(N)) command that should never delay
        /// its execution as long as the kernel scheduler is giving us time.
        /// Note that commands that may trigger a DEL as a side effect (like SET)
        /// are not fast commands.
        const FAST = 4096;
    }
}

// TODO: Only `flags` is ever used
#[allow(dead_code)]
struct CommandProperties {
    arity: i64,
    /// Flags as bitmask. Computed by Redis using the 'sflags' field.
    flags: CommandFlags,
    /// First argument that is a key
    first_key_index: i64,
    /// Last argument that is a key
    last_key_index: i64,
    /// Step to get all the keys from first to last argument. For instance
    ///           in MSET the step is two since arguments are key,val,key,val,...
    key_step: i64,
}

fn command_properties(command_name: &str) -> CommandProperties {
    const ADMIN: CommandFlags = CommandFlags::ADMIN;
    const ASKING: CommandFlags = CommandFlags::ASKING;
    const DENYOOM: CommandFlags = CommandFlags::DENYOOM;
    const FAST: CommandFlags = CommandFlags::FAST;
    const LOADING: CommandFlags = CommandFlags::LOADING;
    const NOSCRIPT: CommandFlags = CommandFlags::NOSCRIPT;
    const PUBSUB: CommandFlags = CommandFlags::PUBSUB;
    const RANDOM: CommandFlags = CommandFlags::RANDOM;
    const READONLY: CommandFlags = CommandFlags::READONLY;
    const SKIP_MONITOR: CommandFlags = CommandFlags::SKIP_MONITOR;
    const SORT_FOR_SCRIPT: CommandFlags = CommandFlags::SORT_FOR_SCRIPT;
    const STALE: CommandFlags = CommandFlags::STALE;
    const WRITE: CommandFlags = CommandFlags::WRITE;

    let wm = WRITE | DENYOOM;
    let wf = WRITE | FAST;
    let wmf = WRITE | DENYOOM | FAST;
    let fr = READONLY | FAST;
    let ls = LOADING | STALE;
    let ars = READONLY | ADMIN | NOSCRIPT;
    let sr = READONLY | SORT_FOR_SCRIPT;
    let (arity, flags, first_key_index, last_key_index, key_step) = match command_name {
        "get" => (2, fr, 1, 1, 1),
        "set" => (3, wm, 1, 1, 1),
        "setnx" => (3, wmf, 1, 1, 1),
        "setex" => (4, wm, 1, 1, 1),
        "psetex" => (4, wm, 1, 1, 1),
        "append" => (3, wm, 1, 1, 1),
        "strlen" => (2, fr, 1, 1, 1),
        "del" => (-2, WRITE, 1, -1, 1),
        "exists" => (-2, fr, 1, -1, 1),
        "setbit" => (4, wm, 1, 1, 1),
        "getbit" => (3, fr, 1, 1, 1),
        "setrange" => (4, wm, 1, 1, 1),
        "getrange" => (4, READONLY, 1, 1, 1),
        "substr" => (4, READONLY, 1, 1, 1),
        "incr" => (2, wmf, 1, 1, 1),
        "decr" => (2, wmf, 1, 1, 1),
        "mget" => (-2, READONLY, 1, -1, 1),
        "rpush" => (-3, wmf, 1, 1, 1),
        "lpush" => (-3, wmf, 1, 1, 1),
        "rpushx" => (-3, wmf, 1, 1, 1),
        "lpushx" => (-3, wmf, 1, 1, 1),
        "linsert" => (5, wm, 1, 1, 1),
        "rpop" => (2, wf, 1, 1, 1),
        "lpop" => (2, wf, 1, 1, 1),
        "rpoplpush" => (3, wm, 1, 2, 1),
        "brpop" => (-3, WRITE | NOSCRIPT, 1, -2, 1),
        "blpop" => (-3, WRITE | NOSCRIPT, 1, -2, 1),
        "brpoplpush" => (4, wm | NOSCRIPT, 1, 2, 1),
        "llen" => (2, fr, 1, 1, 1),
        "lindex" => (3, READONLY, 1, 1, 1),
        "lset" => (4, wm, 1, 1, 1),
        "lrange" => (4, READONLY, 1, 1, 1),
        "ltrim" => (4, READONLY, 1, 1, 1),
        "lrem" => (4, READONLY, 1, 1, 1),
        "sadd" => (-3, wmf, 1, 1, 1),
        "srem" => (-3, wf, 1, 1, 1),
        "smove" => (4, wf, 1, 2, 1),
        "sismember" => (3, fr, 1, 1, 1),
        "scard" => (2, fr, 1, 1, 1),
        "spop" => (-2, fr | RANDOM | NOSCRIPT, 1, 1, 1),
        "srandmember" => (-2, READONLY | RANDOM, 1, 1, 1),
        "sinter" => (-2, sr, 1, -1, 1),
        "sinterstore" => (-3, wm, 1, -1, 1),
        "sunion" => (-2, sr, 1, -1, 1),
        "sunionstore" => (-3, wm, 1, -1, 1),
        "sdiff" => (-2, sr, 1, -1, 1),
        "sdiffstore" => (-3, wm, 1, -1, 1),
        "smembers" => (2, sr, 1, 1, 1),
        "sscan" => (-3, READONLY | RANDOM, 1, 1, 1),
        "zadd" => (-4, wmf, 1, 1, 1),
        "zincrby" => (4, wmf, 1, 1, 1),
        "zrem" => (-3, wf, 1, 1, 1),
        "zremrangebyscore" => (4, WRITE, 1, 1, 1),
        "zremrangebyrank" => (4, WRITE, 1, 1, 1),
        "zremrangebylex" => (4, WRITE, 1, 1, 1),
        "zunionstore" => (-4, wm, 0, 0, 0),
        "zinterstore" => (-4, wm, 0, 0, 0),
        "zrange" => (-4, READONLY, 1, 1, 1),
        "zrevrange" => (-4, READONLY, 1, 1, 1),
        "zrangebyscore" => (-4, READONLY, 1, 1, 1),
        "zrevrangebyscore" => (-4, READONLY, 1, 1, 1),
        "zrangebylex" => (-4, READONLY, 1, 1, 1),
        "zrevrangebylex" => (-4, READONLY, 1, 1, 1),
        "zcount" => (4, fr, 1, 1, 1),
        "zlexcount" => (4, fr, 1, 1, 1),
        "zcard" => (2, fr, 1, 1, 1),
        "zscore" => (3, fr, 1, 1, 1),
        "zrank" => (3, fr, 1, 1, 1),
        "zrevrank" => (3, fr, 1, 1, 1),
        "zscan" => (-3, READONLY | RANDOM, 1, 1, 1),
        "hset" => (4, wmf, 1, 1, 1),
        "hsetnx" => (4, wmf, 1, 1, 1),
        "hget" => (3, fr, 1, 1, 1),
        "hmset" => (-4, wm, 1, 1, 1),
        "hmget" => (-3, READONLY, 1, 1, 1),
        "hincrby" => (4, wmf, 1, 1, 1),
        "hincrbyfloat" => (4, wmf, 1, 1, 1),
        "hdel" => (-3, wf, 1, 1, 1),
        "hlen" => (2, fr, 1, 1, 1),
        "hstrlen" => (3, fr, 1, 1, 1),
        "hkeys" => (2, sr, 1, 1, 1),
        "hvals" => (2, sr, 1, 1, 1),
        "hgetall" => (2, READONLY, 1, 1, 1),
        "hexists" => (3, fr, 1, 1, 1),
        "hscan" => (-3, READONLY | RANDOM, 1, 1, 1),
        "incrby" => (3, wmf, 1, 1, 1),
        "decrby" => (3, wmf, 1, 1, 1),
        "incrbyfloat" => (3, wmf, 1, 1, 1),
        "getset" => (3, wm, 1, 1, 1),
        "mset" => (-3, wm, 1, -1, 2),
        "msetnx" => (-3, wm, 1, -1, 2),
        "randomkey" => (1, READONLY | RANDOM, 0, 0, 0),
        "rename" => (3, WRITE, 1, 2, 1),
        "renamenx" => (3, wf, 1, 2, 1),
        "time" => (1, READONLY | RANDOM | FAST, 0, 0, 0),
        "bitcount" => (-2, READONLY, 1, 1, 1),
        "bitpos" => (-3, READONLY, 1, 1, 1),
        "bitop" => (-4, wm, 2, -1, 1),
        "select" => (2, fr | LOADING, 0, 0, 0),
        "move" => (3, wf, 1, 1, 1),
        "expire" => (3, wf, 1, 1, 1),
        "expireat" => (3, wf, 1, 1, 1),
        "pexpire" => (3, wf, 1, 1, 1),
        "pexpireat" => (3, wf, 1, 1, 1),
        "keys" => (2, sr, 0, 0, 0),
        "scan" => (-2, READONLY | RANDOM, 0, 0, 0),
        "dbsize" => (1, fr, 0, 0, 0),
        "auth" => (-2, fr | NOSCRIPT | ls, 0, 0, 0),
        "ping" => (-1, fr | STALE, 0, 0, 0),
        "echo" => (2, fr, 0, 0, 0),
        "save" => (1, ars, 0, 0, 0),
        "bgsave" => (1, READONLY | ADMIN, 0, 0, 0),
        "bgrewriteaof" => (1, READONLY | ADMIN, 0, 0, 0),
        "shutdown" => (-1, READONLY | ADMIN | ls, 0, 0, 0),
        "lastsave" => (1, fr | RANDOM, 0, 0, 0),
        "type" => (2, fr, 1, 1, 1),
        "multi" => (1, fr | NOSCRIPT, 0, 0, 0),
        "exec" => (1, NOSCRIPT | SKIP_MONITOR, 0, 0, 0),
        "discard" => (1, fr | NOSCRIPT, 0, 0, 0),
        "sync" => (1, ars, 0, 0, 0),
        "psync" => (3, ars, 0, 0, 0),
        "replconf" => (-1, ars | ls, 0, 0, 0),
        "flushdb" => (1, WRITE, 0, 0, 0),
        "flushall" => (1, WRITE, 0, 0, 0),
        "sort" => (-2, wm, 1, 1, 1),
        "info" => (-1, READONLY | ls, 0, 0, 0),
        "monitor" => (1, ars, 0, 0, 0),
        "ttl" => (2, fr, 1, 1, 1),
        "pttl" => (2, fr, 1, 1, 1),
        "persist" => (2, wf, 1, 1, 1),
        "slaveof" => (3, ADMIN | NOSCRIPT | STALE, 0, 0, 0),
        "role" => (1, STALE | LOADING | NOSCRIPT, 0, 0, 0),
        "debug" => (-2, ADMIN | NOSCRIPT, 0, 0, 0),
        "config" => (-2, ADMIN | READONLY | STALE, 0, 0, 0),
        "subscribe" => (-2, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "unsubscribe" => (-1, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "psubscribe" => (-2, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "punsubscribe" => (-1, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "publish" => (-1, READONLY | PUBSUB | LOADING | STALE | FAST, 0, 0, 0),
        "pubsub" => (-1, READONLY | PUBSUB | LOADING | STALE | RANDOM, 0, 0, 0),
        "watch" => (-2, fr | NOSCRIPT, 1, -1, 1),
        "unwatch" => (1, fr | NOSCRIPT, 0, 0, 0),
        "cluster" => (-2, ADMIN | READONLY, 0, 0, 0),
        "sentinel" => (-2, ADMIN | READONLY, 0, 0, 0),
        "restore" => (-4, wm, 1, 1, 1),
        "restore-asking" => (-4, wm | ASKING, 1, 1, 1),
        "migrate" => (-6, WRITE, 0, 0, 0),
        "asking" => (1, READONLY, 0, 0, 0),
        "readonly" => (1, fr, 0, 0, 0),
        "readwrite" => (1, fr, 0, 0, 0),
        "dump" => (2, READONLY, 1, 1, 1),
        "object" => (3, READONLY, 2, 2, 2),
        "client" => (-2, READONLY | NOSCRIPT, 0, 0, 0),
        "eval" => (-3, NOSCRIPT, 0, 0, 0),
        "evalsha" => (-3, NOSCRIPT, 0, 0, 0),
        "slowlog" => (-2, READONLY, 0, 0, 0),
        "script" => (-2, READONLY | NOSCRIPT, 0, 0, 0),
        "wait" => (3, READONLY | NOSCRIPT, 0, 0, 0),
        "command" => (0, READONLY | LOADING | STALE, 0, 0, 0),
        "geoadd" => (-5, wm, 1, 1, 1),
        "georadius" => (-6, READONLY, 1, 1, 1),
        "georadiusbymember" => (-5, READONLY, 1, 1, 1),
        "geohash" => (-2, READONLY, 1, 1, 1),
        "geopos" => (-2, READONLY, 1, 1, 1),
        "geodist" => (-4, READONLY, 1, 1, 1),
        "pfselftest" => (1, READONLY, 1, 1, 1),
        "pfadd" => (-2, wmf, 1, 1, 1),
        "pfcount" => (-2, READONLY, 1, -1, 1),
        "pfmerge" => (-2, wm, 1, -1, 1),
        "pfdebug" => (-3, WRITE, 0, 0, 0),
        "latency" => (-2, ars | ls, 0, 0, 0),
        // Phase 1: New Redis 8.x commands
        "lpos" => (-3, READONLY, 1, 1, 1),
        "lmove" => (5, wm, 1, 2, 1),
        "blmove" => (6, wm | NOSCRIPT, 1, 2, 1),
        "lmpop" => (-4, wf, 0, 0, 0),
        "blmpop" => (-5, wf | NOSCRIPT, 0, 0, 0),
        "smismember" => (-3, fr, 1, 1, 1),
        "sintercard" => (-3, READONLY, 0, 0, 0),
        "zmscore" => (-3, fr, 1, 1, 1),
        "zrandmember" => (-2, READONLY | RANDOM, 1, 1, 1),
        "copy" => (-3, wm, 1, 2, 1),
        "unlink" => (-2, WRITE, 1, -1, 1),
        "touch" => (-2, READONLY, 1, -1, 1),
        "getdel" => (2, WRITE, 1, 1, 1),
        "getex" => (-2, WRITE, 1, 1, 1),
        "lcs" => (-3, READONLY, 1, 2, 1),
        "hrandfield" => (-2, READONLY | RANDOM, 1, 1, 1),
        "waitaof" => (3, WRITE | NOSCRIPT, 0, 0, 0),
        "zdiff" => (-3, READONLY, 1, 1, 1),
        "zdiffstore" => (-4, wm, 1, 1, 1),
        "zinter" => (-3, READONLY, 1, 1, 1),
        "zunion" => (-3, READONLY, 1, 1, 1),
        "zintercard" => (-3, READONLY, 0, 0, 0),
        "zmpop" => (-4, wf, 0, 0, 0),
        "bzmpop" => (-5, wf | NOSCRIPT, 0, 0, 0),
        "bzpopmin" => (-3, wf | NOSCRIPT, 1, -2, 1),
        "bzpopmax" => (-3, wf | NOSCRIPT, 1, -2, 1),
        // Phase 2: Stream commands
        "xadd" => (-5, wm, 1, 1, 1),
        "xlen" => (2, fr, 1, 1, 1),
        "xrange" => (-4, READONLY, 1, 1, 1),
        "xrevrange" => (-4, READONLY, 1, 1, 1),
        "xdel" => (-3, WRITE, 1, 1, 1),
        "xtrim" => (-4, WRITE, 1, 1, 1),
        "xread" => (-4, READONLY, 0, 0, 0),
        "xgroup" => (-4, wm, 2, 2, 1),
        "xack" => (-4, WRITE, 1, 1, 1),
        "xpending" => (-3, READONLY, 1, 1, 1),
        "xinfo" => (-3, READONLY, 2, 2, 1),
        "xsetid" => (3, wm, 1, 1, 1),
        // Phase 3: Geo commands (new)
        "geosearch" => (-4, READONLY, 1, 1, 1),
        "geosearchstore" => (-5, wm, 1, 2, 1),
        // Phase 1.4: Hash per-field expiration
        "hexpire" => (-5, wm, 1, 1, 1),
        "hpexpire" => (-5, wm, 1, 1, 1),
        "hexpireat" => (-5, wm, 1, 1, 1),
        "hpexpireat" => (-5, wm, 1, 1, 1),
        "httl" => (-4, READONLY, 1, 1, 1),
        "hpttl" => (-4, READONLY, 1, 1, 1),
        "hpersist" => (-4, wm, 1, 1, 1),
        // Phase 6: Sharded Pub/Sub + Server commands
        "ssubscribe" => (-2, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "sunsubscribe" => (-1, READONLY | PUBSUB | NOSCRIPT | LOADING | STALE, 0, 0, 0),
        "spublish" => (3, READONLY | PUBSUB | LOADING | STALE | FAST, 0, 0, 0),
        "reset" => (1, fr | NOSCRIPT, 0, 0, 0),
        // Phase 4: ACL system
        "acl" => (-2, ADMIN | NOSCRIPT, 0, 0, 0),
        // Phase 5: Lua scripting
        "function" => (-2, wm, 0, 0, 0),
        "fcall" => (-3, NOSCRIPT, 0, 0, 0),
        "fcall_ro" => (-3, NOSCRIPT, 0, 0, 0),
        // Phase 7: RedisBloom commands
        "bf" => (-3, wm, 1, 1, 1),
        "cf" => (-3, wm, 1, 1, 1),
        "tdigest" => (-3, wm, 1, 1, 1),
        "topk" => (-3, wm, 1, 1, 1),
        "json" => (-3, wm, 1, 1, 1),
        "ft" => (-2, wm, 0, 0, 0),
        "ts" => (-2, wm, 1, 1, 1),
        _ => (0, CommandFlags::empty(), 0, 0, 0),
    };

    CommandProperties {
        arity,
        flags,
        first_key_index,
        last_key_index,
        key_step,
    }
}

#[test]
fn command_has_flags_test() {
    assert!(command_properties("set")
        .flags
        .contains(CommandFlags::WRITE));
    assert!(command_properties("setnx")
        .flags
        .contains(CommandFlags::WRITE | CommandFlags::FAST));
    assert!(!command_properties("append")
        .flags
        .contains(CommandFlags::READONLY));
}

// --- Hash commands ---

fn hset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_or_create(dbindex, &key).hset(field, value) {
        Ok(is_new) => Response::Integer(if is_new { 1 } else { 0 }),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn hsetnx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let value = try_validate!(parser.get_vec(3), "Invalid value");
    let r = match db.get_or_create(dbindex, &key).hsetnx(field, value) {
        Ok(is_new) => Response::Integer(if is_new { 1 } else { 0 }),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn hget(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    match db.get(dbindex, &key) {
        Some(value) => match value.hget(&field) {
            Ok(Some(v)) => Response::Data(v),
            Ok(None) => Response::Nil,
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Nil,
    }
}

fn hmset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    if (parser.argv.len() - 2) % 2 != 0 {
        return Response::Error("ERR wrong number of arguments for 'hmset' command".to_owned());
    }
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut field_values = Vec::with_capacity((parser.argv.len() - 2) / 2);
    for i in (2..parser.argv.len()).step_by(2) {
        let field = try_validate!(parser.get_vec(i), "Invalid field");
        let value = try_validate!(parser.get_vec(i + 1), "Invalid value");
        field_values.push((field, value));
    }
    let r = match db.get_or_create(dbindex, &key).hmset(field_values) {
        Ok(()) => Response::Status("OK".to_owned()),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn hmget(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut fields = Vec::with_capacity(parser.argv.len() - 2);
    for i in 2..parser.argv.len() {
        let field = try_validate!(parser.get_vec(i), "Invalid field");
        fields.push(field);
    }
    match db.get(dbindex, &key) {
        Some(value) => match value.hmget(&fields) {
            Ok(results) => {
                let responses = results
                    .into_iter()
                    .map(|v| match v {
                        Some(data) => Response::Data(data),
                        None => Response::Nil,
                    })
                    .collect();
                Response::Array(responses)
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(fields.iter().map(|_| Response::Nil).collect()),
    }
}

fn hdel(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut fields = Vec::with_capacity(parser.argv.len() - 2);
    for i in 2..parser.argv.len() {
        let field = try_validate!(parser.get_vec(i), "Invalid field");
        fields.push(field);
    }
    let r = match db.get_mut(dbindex, &key) {
        Some(value) => match value.hdel(&fields) {
            Ok(count) => Response::Integer(count as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    };
    db.key_updated(dbindex, &key);
    r
}

fn hlen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(value) => match value.hlen() {
            Ok(len) => Response::Integer(len as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn hstrlen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    match db.get(dbindex, &key) {
        Some(value) => match value.hstrlen(&field) {
            Ok(len) => Response::Integer(len as i64),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn hexists(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    match db.get(dbindex, &key) {
        Some(value) => match value.hexists(&field) {
            Ok(exists) => Response::Integer(if exists { 1 } else { 0 }),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Integer(0),
    }
}

fn hkeys(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(value) => match value.hkeys() {
            Ok(keys) => Response::Array(keys.into_iter().map(Response::Data).collect()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(vec![]),
    }
}

fn hvals(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(value) => match value.hvals() {
            Ok(vals) => Response::Array(vals.into_iter().map(Response::Data).collect()),
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(vec![]),
    }
}

fn hgetall(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(value) => match value.hgetall() {
            Ok(pairs) => {
                let mut responses = Vec::with_capacity(pairs.len() * 2);
                for (field, val) in pairs {
                    responses.push(Response::Data(field));
                    responses.push(Response::Data(val));
                }
                Response::Array(responses)
            }
            Err(err) => Response::Error(err.to_string()),
        },
        None => Response::Array(vec![]),
    }
}

fn hincrby(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let increment = try_validate!(parser.get_i64(3), "ERR value is not an integer or out of range");
    let r = match db.get_or_create(dbindex, &key).hincrby(field, increment) {
        Ok(val) => Response::Integer(val),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

fn hincrbyfloat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let field = try_validate!(parser.get_vec(2), "Invalid field");
    let increment = try_validate!(parser.get_f64(3), "ERR value is not a valid float");
    let r = match db.get_or_create(dbindex, &key).hincrbyfloat(field, increment) {
        Ok(val) => Response::Data(format!("{}", val).into_bytes()),
        Err(err) => Response::Error(err.to_string()),
    };
    db.key_updated(dbindex, &key);
    r
}

// --- Missing string/key commands ---

fn getset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let new_value = try_validate!(parser.get_vec(2), "Invalid value");
    let old_value = generic_get(db, dbindex, key.clone(), false);
    match db.get_or_create(dbindex, &key).set(new_value) {
        Ok(_) => {
            db.key_updated(dbindex, &key);
            old_value
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

fn mset(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    if (parser.argv.len() - 1) % 2 != 0 {
        return Response::Error("ERR wrong number of arguments for 'mset' command".to_owned());
    }
    for i in (1..parser.argv.len()).step_by(2) {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        let value = try_validate!(parser.get_vec(i + 1), "Invalid value");
        let _ = generic_set(db, dbindex, key.clone(), value, false, false, None);
        db.key_updated(dbindex, &key);
    }
    Response::Status("OK".to_owned())
}

fn msetnx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    if (parser.argv.len() - 1) % 2 != 0 {
        return Response::Error("ERR wrong number of arguments for 'msetnx' command".to_owned());
    }
    // Check if any of the keys already exist
    for i in (1..parser.argv.len()).step_by(2) {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        if db.get(dbindex, &key).is_some() {
            return Response::Integer(0);
        }
    }
    // None exist, set them all
    for i in (1..parser.argv.len()).step_by(2) {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        let value = try_validate!(parser.get_vec(i + 1), "Invalid value");
        let _ = generic_set(db, dbindex, key.clone(), value, false, false, None);
        db.key_updated(dbindex, &key);
    }
    Response::Integer(1)
}

fn rename(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let source = try_validate!(parser.get_vec(1), "Invalid key");
    let dest = try_validate!(parser.get_vec(2), "Invalid key");
    match db.remove(dbindex, &source) {
        Some(value) => {
            // Remove expiration from source, copy to dest
            let expiration = db.get_msexpiration(dbindex, &source).cloned();
            db.remove_msexpiration(dbindex, &source);
            *db.get_or_create(dbindex, &dest) = value;
            if let Some(exp) = expiration {
                db.set_msexpiration(dbindex, dest.clone(), exp);
            }
            db.key_updated(dbindex, &source);
            db.key_updated(dbindex, &dest);
            Response::Status("OK".to_owned())
        }
        None => Response::Error("ERR no such key".to_owned()),
    }
}

fn renamenx(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let source = try_validate!(parser.get_vec(1), "Invalid key");
    let dest = try_validate!(parser.get_vec(2), "Invalid key");
    if db.get(dbindex, &dest).is_some() {
        return Response::Integer(0);
    }
    match db.remove(dbindex, &source) {
        Some(value) => {
            let expiration = db.get_msexpiration(dbindex, &source).cloned();
            db.remove_msexpiration(dbindex, &source);
            *db.get_or_create(dbindex, &dest) = value;
            if let Some(exp) = expiration {
                db.set_msexpiration(dbindex, dest.clone(), exp);
            }
            db.key_updated(dbindex, &source);
            db.key_updated(dbindex, &dest);
            Response::Integer(1)
        }
        None => Response::Error("ERR no such key".to_owned()),
    }
}

fn randomkey(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 1);
    match db.randomkey(dbindex) {
        Some(key) => Response::Data(key),
        None => Response::Nil,
    }
}

fn time_command(parser: &mut ParsedCommand) -> Response {
    validate_arguments_exact!(parser, 1);
    let now = mstime();
    let secs = now / 1000;
    let micros = ((now % 1000) * 1000) as i64;
    Response::Array(vec![
        Response::Data(format!("{}", secs).into_bytes()),
        Response::Data(format!("{}", micros).into_bytes()),
    ])
}

fn bitcount(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    validate_arguments_lte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let obj = db.get(dbindex, &key);
    let data = match obj {
        Some(value) => match value.get() {
            Ok(d) => d,
            Err(err) => return Response::Error(err.to_string()),
        },
        None => return Response::Integer(0),
    };

    let (start, stop) = if parser.argv.len() >= 4 {
        let s = try_validate!(parser.get_i64(2), "ERR value is not an integer");
        let e = try_validate!(parser.get_i64(3), "ERR value is not an integer");
        (s, e)
    } else {
        (0, data.len() as i64 - 1)
    };

    let len = data.len() as i64;
    let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
    let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

    if start > stop || start >= data.len() {
        return Response::Integer(0);
    }

    let count: u32 = data[start..=stop]
        .iter()
        .map(|b| b.count_ones())
        .sum();
    Response::Integer(count as i64)
}

fn bitpos(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    validate_arguments_lte!(parser, 5);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let bit = try_validate!(parser.get_i64(2), "ERR value is not an integer");
    validate!(bit == 0 || bit == 1, "ERR the bit argument must be 1 or 0");
    let obj = db.get(dbindex, &key);
    let data = match obj {
        Some(value) => match value.get() {
            Ok(d) => d,
            Err(err) => return Response::Error(err.to_string()),
        },
        None => {
            if bit == 0 {
                return Response::Integer(0);
            } else {
                return Response::Integer(-1);
            }
        }
    };

    let (start, end, end_given) = if parser.argv.len() >= 4 {
        let s = try_validate!(parser.get_i64(3), "ERR value is not an integer");
        if parser.argv.len() >= 5 {
            let e = try_validate!(parser.get_i64(4), "ERR value is not an integer");
            (s, e, true)
        } else {
            (s, data.len() as i64 - 1, false)
        }
    } else {
        (0, data.len() as i64 - 1, false)
    };

    let len = data.len() as i64;
    let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
    let stop = if end < 0 { (len + end).max(0) } else { end.min(len - 1) } as usize;

    if start > stop || start >= data.len() {
        return Response::Integer(-1);
    }

    for i in start..=stop {
        let byte = data[i];
        for bit_idx in 0..8 {
            let bit_val = (byte >> (7 - bit_idx)) & 1;
            if (bit_val as i64) == bit {
                return Response::Integer((i as i64) * 8 + bit_idx as i64);
            }
        }
    }

    // If looking for 0 and not found, and end was not given, return last bit + 1
    if bit == 0 && !end_given {
        Response::Integer(data.len() as i64 * 8)
    } else {
        Response::Integer(-1)
    }
}

fn bitop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let op = try_validate!(parser.get_str(1), "Invalid operation");
    let destkey = try_validate!(parser.get_vec(2), "Invalid key");

    let op_lower = op.to_ascii_lowercase();
    validate!(
        op_lower == "and" || op_lower == "or" || op_lower == "xor" || op_lower == "not",
        "ERR syntax error"
    );

    if op_lower == "not" && parser.argv.len() != 4 {
        return Response::Error("ERR BITOP NOT requires one and only one key".to_owned());
    }

    // Collect all source data
    let mut keys = Vec::with_capacity(parser.argv.len() - 3);
    let mut max_len = 0usize;
    for i in 3..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        let data = match db.get(dbindex, &key) {
            Some(value) => match value.get() {
                Ok(d) => d,
                Err(err) => return Response::Error(err.to_string()),
            },
            None => vec![],
        };
        if data.len() > max_len {
            max_len = data.len();
        }
        keys.push(data);
    }

    let mut result = vec![0u8; max_len];
    match op_lower.as_str() {
        "and" => {
            for (i, byte) in result.iter_mut().enumerate() {
                *byte = 0xff;
                for key_data in &keys {
                    let b = if i < key_data.len() { key_data[i] } else { 0 };
                    *byte &= b;
                }
            }
        }
        "or" => {
            for (i, byte) in result.iter_mut().enumerate() {
                for key_data in &keys {
                    let b = if i < key_data.len() { key_data[i] } else { 0 };
                    *byte |= b;
                }
            }
        }
        "xor" => {
            for (i, byte) in result.iter_mut().enumerate() {
                for key_data in &keys {
                    let b = if i < key_data.len() { key_data[i] } else { 0 };
                    *byte ^= b;
                }
            }
        }
        "not" => {
            let key_data = &keys[0];
            for (i, byte) in result.iter_mut().enumerate() {
                *byte = if i < key_data.len() { !key_data[i] } else { 0 };
            }
        }
        _ => unreachable!(),
    }

    match db.get_or_create(dbindex, &destkey).set(result.clone()) {
        Ok(_) => {
            db.key_updated(dbindex, &destkey);
            Response::Integer(max_len as i64)
        }
        Err(err) => Response::Error(err.to_string()),
    }
}

// --- Scan helpers ---

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    // Simple glob matching supporting *, ?, [chars], [^chars]
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = None;
    let mut star_ti = None;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = Some(pi);
            star_ti = Some(ti);
            pi += 1;
            continue;
        }
        if pi < pattern.len()
            && (pattern[pi] == b'?' || pattern[pi] == text[ti])
        {
            pi += 1;
            ti += 1;
            continue;
        }
        if pi < pattern.len() && pattern[pi] == b'[' {
            // Find closing ]
            if let Some(end) = pattern[pi + 1..].iter().position(|&b| b == b']') {
                let end = end + pi + 2;
                let negate = pi + 1 < pattern.len() && pattern[pi + 1] == b'^';
                let start = if negate { pi + 2 } else { pi + 1 };
                let mut matched = false;
                for i in start..end - 1 {
                    if pattern[i] == text[ti] {
                        matched = true;
                        break;
                    }
                }
                if negate {
                    matched = !matched;
                }
                if matched {
                    pi = end + 1;
                    ti += 1;
                    continue;
                }
            }
        }
        if let (Some(spi), Some(sti)) = (star_pi, star_ti) {
            pi = spi + 1;
            let new_sti = sti + 1;
            star_ti = Some(new_sti);
            ti = new_sti;
            continue;
        }
        return false;
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

fn scan_parse_args(parser: &mut ParsedCommand, start_idx: usize) -> (Option<Vec<u8>>, usize) {
    let mut pattern = None;
    let mut count = 10usize;
    let mut i = start_idx;
    while i < parser.argv.len() {
        if let Ok(arg) = parser.get_str(i) {
            match arg.to_ascii_lowercase().as_str() {
                "match" => {
                    if i + 1 < parser.argv.len() {
                        pattern = parser.get_vec(i + 1).ok();
                        i += 2;
                        continue;
                    }
                }
                "count" => {
                    if i + 1 < parser.argv.len() {
                        if let Ok(c) = parser.get_i64(i + 1) {
                            count = c.max(1) as usize;
                        }
                        i += 2;
                        continue;
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    (pattern, count)
}

fn scan_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let cursor = try_validate!(parser.get_i64(1), "ERR value is not an integer");
    let cursor = cursor.max(0) as usize;
    let (pattern, count) = scan_parse_args(parser, 2);

    let keys: Vec<&Vec<u8>> = db.data_keys(dbindex);
    let total = keys.len();

    let mut results = vec![];

    if total == 0 {
        return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(vec![]),
        ]);
    }

    let mut idx = cursor;
    let mut scanned = 0;
    while scanned < count && idx < total {
        let key = keys[idx];
        idx += 1;
        scanned += 1;
        let matches = match &pattern {
            Some(p) => glob_match_bytes(p, key),
            None => true,
        };
        if matches {
            results.push(Response::Data(key.clone()));
        }
    }

    let next_cursor = if idx >= total { 0 } else { idx };

    Response::Array(vec![
        Response::Data(format!("{}", next_cursor).into_bytes()),
        Response::Array(results),
    ])
}

fn sscan_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let cursor = try_validate!(parser.get_i64(2), "ERR value is not an integer");
    let cursor = cursor.max(0) as usize;
    let (pattern, count) = scan_parse_args(parser, 3);

    let members = match db.get(dbindex, &key) {
        Some(value) => match value.smembers() {
            Ok(m) => m,
            Err(err) => return Response::Error(err.to_string()),
        },
        None => vec![],
    };

    let total = members.len();
    let mut results = vec![];

    if total == 0 {
        return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(vec![]),
        ]);
    }

    let mut idx = cursor;
    let mut scanned = 0;
    while scanned < count && idx < total {
        let member = &members[idx];
        idx += 1;
        scanned += 1;
        let matches = match &pattern {
            Some(p) => glob_match_bytes(p, member),
            None => true,
        };
        if matches {
            results.push(Response::Data(member.clone()));
        }
    }

    let next_cursor = if idx >= total { 0 } else { idx };

    Response::Array(vec![
        Response::Data(format!("{}", next_cursor).into_bytes()),
        Response::Array(results),
    ])
}

fn hscan_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let cursor = try_validate!(parser.get_i64(2), "ERR value is not an integer");
    let cursor = cursor.max(0) as usize;
    let (pattern, count) = scan_parse_args(parser, 3);

    let pairs = match db.get(dbindex, &key) {
        Some(value) => match value.hgetall() {
            Ok(p) => p,
            Err(err) => return Response::Error(err.to_string()),
        },
        None => vec![],
    };

    let total = pairs.len();
    let mut results = vec![];

    if total == 0 {
        return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(vec![]),
        ]);
    }

    let mut idx = cursor;
    let mut scanned = 0;
    while scanned < count && idx < total {
        let (field, val) = &pairs[idx];
        idx += 1;
        scanned += 1;
        let matches = match &pattern {
            Some(p) => glob_match_bytes(p, field),
            None => true,
        };
        if matches {
            results.push(Response::Data(field.clone()));
            results.push(Response::Data(val.clone()));
        }
    }

    let next_cursor = if idx >= total { 0 } else { idx };

    Response::Array(vec![
        Response::Data(format!("{}", next_cursor).into_bytes()),
        Response::Array(results),
    ])
}

fn zscan_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let cursor = try_validate!(parser.get_i64(2), "ERR value is not an integer");
    let cursor = cursor.max(0) as usize;
    let (pattern, count) = scan_parse_args(parser, 3);

    // Get all members with scores using zrange
    let members = match db.get(dbindex, &key) {
        Some(value) => match value.zrange(0, -1, true, false) {
            Ok(m) => m, // Returns [member, score, member, score, ...]
            Err(err) => return Response::Error(err.to_string()),
        },
        None => vec![],
    };

    // members is interleaved [member, score, member, score, ...]
    let total = members.len() / 2;
    let mut results = vec![];

    if total == 0 {
        return Response::Array(vec![
            Response::Data(b"0".to_vec()),
            Response::Array(vec![]),
        ]);
    }

    let mut idx = cursor;
    let mut scanned = 0;
    while scanned < count && idx < total {
        let member = &members[idx * 2];
        let score = &members[idx * 2 + 1];
        idx += 1;
        scanned += 1;
        let matches = match &pattern {
            Some(p) => glob_match_bytes(p, member),
            None => true,
        };
        if matches {
            results.push(Response::Data(member.clone()));
            results.push(Response::Data(score.clone()));
        }
    }

    let next_cursor = if idx >= total { 0 } else { idx };

    Response::Array(vec![
        Response::Data(format!("{}", next_cursor).into_bytes()),
        Response::Array(results),
    ])
}

// --- move command ---

fn move_key(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let target_db = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
    if target_db >= db.config.databases as usize {
        return Response::Error("ERR invalid DB index".to_owned());
    }
    if target_db == dbindex {
        return Response::Error("ERR source and destination objects are the same".to_owned());
    }
    // Check if key exists in source
    if db.get(dbindex, &key).is_none() {
        return Response::Integer(0);
    }
    // Check if key already exists in target
    if db.get(target_db, &key).is_some() {
        return Response::Integer(0);
    }
    // Move the key
    match db.remove(dbindex, &key) {
        Some(value) => {
            let expiration = db.get_msexpiration(dbindex, &key).cloned();
            db.remove_msexpiration(dbindex, &key);
            *db.get_or_create(target_db, &key) = value;
            if let Some(exp) = expiration {
                db.set_msexpiration(target_db, key.clone(), exp);
            }
            db.key_updated(dbindex, &key);
            db.key_updated(target_db, &key);
            Response::Integer(1)
        }
        None => Response::Integer(0),
    }
}

// --- sort command ---

fn sort_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    // Parse options
    let mut alpha = false;
    let mut desc = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;
    let mut store_key: Option<Vec<u8>> = None;
    let mut i = 2;
    while i < parser.argv.len() {
        let arg = try_validate!(parser.get_str(i), "ERR syntax error");
        match arg.to_ascii_lowercase().as_str() {
            "alpha" => {
                alpha = true;
                i += 1;
            }
            "desc" => {
                desc = true;
                i += 1;
            }
            "asc" => {
                desc = false;
                i += 1;
            }
            "limit" => {
                if i + 2 >= parser.argv.len() {
                    return Response::Error("ERR syntax error".to_owned());
                }
                limit_offset = Some(try_validate!(parser.get_i64(i + 1), "ERR syntax error") as usize);
                limit_count = Some(try_validate!(parser.get_i64(i + 2), "ERR syntax error") as usize);
                i += 3;
            }
            "store" => {
                if i + 1 >= parser.argv.len() {
                    return Response::Error("ERR syntax error".to_owned());
                }
                store_key = Some(try_validate!(parser.get_vec(i + 1), "ERR syntax error"));
                i += 2;
            }
            _ => {
                return Response::Error("ERR syntax error".to_owned());
            }
        }
    }

    // Get elements
    let mut elements: Vec<Vec<u8>> = match db.get(dbindex, &key) {
        Some(value) => match value {
            Value::List(_) => match value.lrange(0, -1) {
                Ok(items) => items.into_iter().map(|s| s.to_vec()).collect(),
                Err(err) => return Response::Error(err.to_string()),
            },
            Value::Set(_) => match value.smembers() {
                Ok(items) => items,
                Err(err) => return Response::Error(err.to_string()),
            },
            Value::SortedSet(_) => match value.zrange(0, -1, false, false) {
                Ok(items) => items,
                Err(err) => return Response::Error(err.to_string()),
            },
            _ => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        },
        None => vec![],
    };

    // Sort
    if alpha {
        if desc {
            elements.sort_by(|a, b| b.cmp(a));
        } else {
            elements.sort();
        }
    } else {
        // Numeric sort
        let parse_result: Result<Vec<(f64, usize)>, _> = elements
            .iter()
            .enumerate()
            .map(|(i, e)| {
                std::str::from_utf8(e)
                    .map_err(|_| "ERR value is not a valid float".to_owned())
                    .and_then(|s| {
                        s.parse::<f64>()
                            .map(|f| (f, i))
                            .map_err(|_| "ERR value is not a valid float".to_owned())
                    })
            })
            .collect();
        match parse_result {
            Ok(mut scored) => {
                if desc {
                    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                }
                elements = scored.into_iter().map(|(_, i)| elements[i].clone()).collect();
            }
            Err(err) => return Response::Error(err),
        }
    }

    // Apply LIMIT
    if let Some(offset) = limit_offset {
        if let Some(count) = limit_count {
            let end = (offset + count).min(elements.len());
            if offset < elements.len() {
                elements = elements[offset..end].to_vec();
            } else {
                elements = vec![];
            }
        }
    }

    // STORE
    if let Some(dest) = store_key {
        let len = elements.len() as i64;
        // Store as list
        let mut list_val = database::list::ValueList::new();
        for el in &elements {
            let _ = list_val.push(el.clone(), false);
        }
        *db.get_or_create(dbindex, &dest) = Value::List(list_val);
        db.key_updated(dbindex, &dest);
        return Response::Integer(len);
    }

    Response::Array(elements.into_iter().map(Response::Data).collect())
}

// --- config command ---

fn config_command(parser: &mut ParsedCommand, db: &Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcommand.to_ascii_lowercase().as_str() {
        "get" => {
            validate_arguments_exact!(parser, 3);
            let param = try_validate!(parser.get_str(2), "ERR syntax error");
            let param_lower = param.to_ascii_lowercase();
            let mut results = vec![];
            let config_pairs: Vec<(&str, String)> = vec![
                ("databases", format!("{}", db.config.databases)),
                ("port", format!("{}", db.config.port)),
                ("dir", db.config.dir.clone()),
                ("bind", db.config.bind.join(" ")),
                ("timeout", format!("{}", db.config.timeout)),
                ("tcp-keepalive", format!("{}", db.config.tcp_keepalive)),
                ("activerehashing", if db.config.active_rehashing { "yes".into() } else { "no".into() }),
                ("set-max-intset-entries", format!("{}", db.config.set_max_intset_entries)),
                ("hz", format!("{}", db.config.hz)),
                ("appendonly", if db.config.appendonly { "yes".into() } else { "no".into() }),
                ("appendfilename", db.config.appendfilename.clone()),
                ("requirepass", db.config.requirepass.clone().unwrap_or_default()),
                ("daemonize", if db.config.daemonize { "yes".into() } else { "no".into() }),
                ("pidfile", db.config.pidfile.clone()),
                ("loglevel", "warning".into()),
                ("logfile", "".into()),
                ("unixsocket", db.config.unixsocket.clone().unwrap_or_default()),
                ("unixsocketperm", format!("{:o}", db.config.unixsocketperm)),
                ("tcp-backlog", format!("{}", db.config.tcp_backlog)),
                ("syslog-enabled", if db.config.syslog_enabled { "yes".into() } else { "no".into() }),
                ("syslog-ident", db.config.syslog_ident.clone()),
                ("syslog-facility", db.config.syslog_facility.clone()),
                // RDB
                ("save", db.config.save.iter().map(|(s,k)| format!("{} {}", s, k)).collect::<Vec<_>>().join("\n")),
                ("stop-writes-on-bgsave-error", if db.config.stop_writes_on_bgsave_error { "yes".into() } else { "no".into() }),
                ("rdbcompression", if db.config.rdbcompression { "yes".into() } else { "no".into() }),
                ("rdbchecksum", if db.config.rdbchecksum { "yes".into() } else { "no".into() }),
                ("dbfilename", db.config.dbfilename.clone()),
                // Replication
                ("slaveof", match &db.config.slaveof { Some((h,p)) => format!("{} {}", h, p), None => String::new() }),
                ("masterauth", db.config.masterauth.clone().unwrap_or_default()),
                ("slave-serve-stale-data", if db.config.slave_serve_stale_data { "yes".into() } else { "no".into() }),
                ("slave-read-only", if db.config.slave_read_only { "yes".into() } else { "no".into() }),
                ("repl-diskless-sync", if db.config.repl_diskless_sync { "yes".into() } else { "no".into() }),
                ("repl-diskless-sync-delay", format!("{}", db.config.repl_diskless_sync_delay)),
                ("repl-ping-slave-period", format!("{}", db.config.repl_ping_slave_period)),
                ("repl-timeout", format!("{}", db.config.repl_timeout)),
                ("repl-disable-tcp-nodelay", if db.config.repl_disable_tcp_nodelay { "yes".into() } else { "no".into() }),
                ("repl-backlog-size", format!("{}", db.config.repl_backlog_size)),
                ("repl-backlog-ttl", format!("{}", db.config.repl_backlog_ttl)),
                ("slave-priority", format!("{}", db.config.slave_priority)),
                ("min-slaves-to-write", format!("{}", db.config.min_slaves_to_write)),
                ("min-slaves-max-lag", format!("{}", db.config.min_slaves_max_lag)),
                // Memory
                ("maxclients", format!("{}", db.config.maxclients)),
                ("maxmemory", format!("{}", db.config.maxmemory)),
                ("maxmemory-policy", db.config.maxmemory_policy.clone()),
                ("maxmemory-samples", format!("{}", db.config.maxmemory_samples)),
                // AOF
                ("appendfsync", db.config.appendfsync.clone()),
                ("no-appendfsync-on-rewrite", if db.config.no_appendfsync_on_rewrite { "yes".into() } else { "no".into() }),
                ("auto-aof-rewrite-percentage", format!("{}", db.config.auto_aof_rewrite_percentage)),
                ("auto-aof-rewrite-min-size", format!("{}", db.config.auto_aof_rewrite_min_size)),
                ("aof-load-truncated", if db.config.aof_load_truncated { "yes".into() } else { "no".into() }),
                // Misc
                ("lua-time-limit", format!("{}", db.config.lua_time_limit)),
                ("slowlog-log-slower-than", format!("{}", db.config.slowlog_log_slower_than)),
                ("slowlog-max-len", format!("{}", db.config.slowlog_max_len)),
                ("latency-monitor-threshold", format!("{}", db.config.latency_monitor_threshold)),
                ("notify-keyspace-events", db.config.notify_keyspace_events.clone()),
                // Data structure encoding limits
                ("hash-max-ziplist-entries", format!("{}", db.config.hash_max_ziplist_entries)),
                ("hash-max-ziplist-value", format!("{}", db.config.hash_max_ziplist_value)),
                ("list-max-ziplist-entries", format!("{}", db.config.list_max_ziplist_entries)),
                ("list-max-ziplist-value", format!("{}", db.config.list_max_ziplist_value)),
                ("zset-max-ziplist-entries", format!("{}", db.config.zset_max_ziplist_entries)),
                ("zset-max-ziplist-value", format!("{}", db.config.zset_max_ziplist_value)),
                ("hll-sparse-max-bytes", format!("{}", db.config.hll_sparse_max_bytes)),
                // Other
                ("client-output-buffer-limit", db.config.client_output_buffer_limit.clone()),
                ("aof-rewrite-incremental-fsync", if db.config.aof_rewrite_incremental_fsync { "yes".into() } else { "no".into() }),
            ];
            for (name, value) in &config_pairs {
                if glob_match_bytes(param_lower.as_bytes(), name.as_bytes()) {
                    results.push(Response::Data(name.as_bytes().to_vec()));
                    results.push(Response::Data(value.as_bytes().to_vec()));
                }
            }
            Response::Array(results)
        }
        "set" => {
            // CONFIG SET is limited in this implementation
            Response::Error("ERR CONFIG SET is not fully implemented".to_owned())
        }
        "resetstat" => {
            Response::Status("OK".to_owned())
        }
        _ => Response::Error("ERR Unknown subcommand or wrong number of arguments".to_owned()),
    }
}

// --- object command ---

fn object_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let subcommand = try_validate!(parser.get_str(1), "ERR syntax error");
    let key = try_validate!(parser.get_vec(2), "Invalid key");
    match subcommand.to_ascii_lowercase().as_str() {
        "refcount" => {
            match db.get(dbindex, &key) {
                Some(_) => Response::Integer(1),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "encoding" => {
            match db.get(dbindex, &key) {
                Some(value) => {
                    let enc = match value {
                        Value::Nil => "none",
                        Value::String(_) => "raw",
                        Value::List(_) => "linkedlist",
                        Value::Set(s) => if s.is_intset() { "intset" } else { "hashtable" },
                        Value::SortedSet(_) => "skiplist",
                        Value::Hash(_) => "hashtable",
                        Value::Stream(_) => "stream",
                        Value::BloomFilter(_) => "MBbloom--",
                        Value::CuckooFilter(_) => "MBbloom--",
                        Value::TDigest(_) => "MBbloom--",
                        Value::TopK(_) => "MBbloom--",
                        Value::Json(_) => "ReJSON-RL",
                        Value::TimeSeries(_) => "timeseries",
                    };
                    Response::Data(enc.as_bytes().to_vec())
                }
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "idletime" => {
            // We don't track idle time, return 0 if key exists
            match db.get(dbindex, &key) {
                Some(_) => Response::Integer(0),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        _ => Response::Error("ERR Unknown OBJECT subcommand".to_owned()),
    }
}

// --- server commands ---

fn save_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    // RDB save is not yet implemented; return OK for compatibility
    Response::Status("OK".to_owned())
}

fn lastsave_command(parser: &mut ParsedCommand, db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    // Return start time as last save time (no RDB save implemented yet)
    Response::Integer(db.start_mstime / 1000)
}

fn shutdown_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    // Return OK; actual shutdown handled by the server loop
    Response::Status("OK".to_owned())
}

// --- bgsave / bgrewriteaof ---

fn bgsave_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Status("Background saving started".to_owned())
}

fn bgrewriteaof_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Status("Background append only file rewriting started".to_owned())
}

// --- role command ---

fn role_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Array(vec![
        Response::Data(b"master".to_vec()),
        Response::Integer(0),
        Response::Array(vec![]),
    ])
}

// --- pubsub command ---

fn pubsub_command(parser: &mut ParsedCommand, db: &Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "channels" => {
            let pattern = if parser.argv.len() >= 3 {
                parser.get_vec(2).ok()
            } else {
                None
            };
            let channels = db.pubsub_channels(pattern.as_deref());
            Response::Array(channels.into_iter().map(Response::Data).collect())
        }
        "numsub" => {
            let mut channels = Vec::with_capacity(parser.argv.len() - 2);
            for i in 2..parser.argv.len() {
                channels.push(try_validate!(parser.get_vec(i), "Invalid channel"));
            }
            let numsub = db.pubsub_numsub(&channels);
            let mut result = Vec::with_capacity(numsub.len() * 2);
            for (ch, count) in numsub {
                result.push(Response::Data(ch));
                result.push(Response::Integer(count as i64));
            }
            Response::Array(result)
        }
        "numpat" => {
            validate_arguments_exact!(parser, 2);
            Response::Integer(db.pubsub_numpat() as i64)
        }
        _ => Response::Error("ERR Unknown PUBSUB subcommand".to_owned()),
    }
}

// --- client command ---

fn client_command(parser: &mut ParsedCommand, _db: &Database, client: &mut Client) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "setname" => {
            validate_arguments_exact!(parser, 3);
            let name = try_validate!(parser.get_vec(2), "Invalid name");
            if name.contains(&b' ') {
                return Response::Error("ERR Client names cannot contain spaces, newlines or special characters.".to_owned());
            }
            client.name = name;
            Response::Status("OK".to_owned())
        }
        "getname" => {
            validate_arguments_exact!(parser, 2);
            if client.name.is_empty() {
                Response::Nil
            } else {
                Response::Data(client.name.clone())
            }
        }
        "list" => {
            validate_arguments_exact!(parser, 2);
            let info = format!(
                "id={} addr=127.0.0.1:0 fd=0 name={} age=0 idle=0 flags=N db={} sub={} psub={} multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n",
                client.id,
                String::from_utf8_lossy(&client.name),
                client.dbindex,
                client.subscriptions.len(),
                client.pattern_subscriptions.len(),
            );
            Response::Data(info.into_bytes())
        }
        "getid" => {
            validate_arguments_exact!(parser, 2);
            Response::Integer(client.id as i64)
        }
        "info" => {
            validate_arguments_exact!(parser, 2);
            let info = format!(
                "id={}\naddr=127.0.0.1:0\nfd=0\nname={}\nage=0\nidle=0\nflags=N\ndb={}\nsub={}\npsub={}\nmulti=-1\nqbuf=0\nqbuf-free=0\nobl=0\noll=0\nomem=0\nevents=r\ncmd=client\n",
                client.id,
                String::from_utf8_lossy(&client.name),
                client.dbindex,
                client.subscriptions.len(),
                client.pattern_subscriptions.len(),
            );
            Response::Data(info.into_bytes())
        }
        "reply" => {
            validate_arguments_exact!(parser, 3);
            let mode = try_validate!(parser.get_str(2), "ERR syntax error");
            match mode.to_ascii_lowercase().as_str() {
                "on" | "off" | "skip" => Response::Status("OK".to_owned()),
                _ => Response::Error("ERR Client reply should be ON, OFF or SKIP".to_owned()),
            }
        }
        "no-evict" => {
            validate_arguments_exact!(parser, 3);
            let mode = try_validate!(parser.get_str(2), "ERR syntax error");
            match mode.to_ascii_lowercase().as_str() {
                "on" | "off" => Response::Status("OK".to_owned()),
                _ => Response::Error("ERR argument must be ON or OFF".to_owned()),
            }
        }
        "no-touch" => {
            validate_arguments_exact!(parser, 3);
            let mode = try_validate!(parser.get_str(2), "ERR syntax error");
            match mode.to_ascii_lowercase().as_str() {
                "on" | "off" => Response::Status("OK".to_owned()),
                _ => Response::Error("ERR argument must be ON or OFF".to_owned()),
            }
        }
        "tracking" => {
            // CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
            validate_arguments_gte!(parser, 3);
            let mode = try_validate!(parser.get_str(2), "ERR syntax error");
            match mode.to_ascii_lowercase().as_str() {
                "on" | "off" => Response::Status("OK".to_owned()),
                _ => Response::Error("ERR argument must be ON or OFF".to_owned()),
            }
        }
        "trackinginfo" => {
            validate_arguments_exact!(parser, 2);
            let info = "flags=off\nredirect=-1\nprefixes=\n";
            Response::Data(info.as_bytes().to_vec())
        }
        "kill" => {
            // CLIENT KILL - simplified stub
            Response::Status("OK".to_owned())
        }
        _ => Response::Error(format!("ERR Unknown CLIENT subcommand '{}'", subcmd)),
    }
}

// --- slowlog command ---

fn slowlog_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "get" => Response::Array(vec![]),
        "len" => {
            validate_arguments_exact!(parser, 2);
            Response::Integer(0)
        }
        "reset" => {
            validate_arguments_exact!(parser, 2);
            Response::Status("OK".to_owned())
        }
        _ => Response::Error("ERR Unknown SLOWLOG subcommand".to_owned()),
    }
}

// --- command introspection ---

fn command_introspection(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_gte!(parser, 1);
    if parser.argv.len() == 1 {
        // COMMAND with no args - return info about all commands
        return Response::Array(vec![]);
    }
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "count" => {
            validate_arguments_exact!(parser, 2);
            Response::Integer(250)
        }
        "list" => {
            // COMMAND LIST [FILTERBY MODULE|ACLCAT|PATTERN pattern]
            // Return a flat list of command names
            let filter_pattern = if parser.argv.len() >= 5 {
                let filter_type = parser.get_str(3).unwrap_or("");
                if filter_type.to_ascii_lowercase() == "pattern" {
                    parser.get_vec(4).ok()
                } else {
                    None
                }
            } else {
                None
            };
            // Return a simplified list of common commands
            let commands = vec![
                "get", "set", "del", "exists", "expire", "ttl", "keys", "ping",
                "info", "subscribe", "publish", "unsubscribe", "multi", "exec",
                "lpush", "rpush", "lpop", "rpop", "lrange", "llen",
                "sadd", "srem", "smembers", "scard", "sismember",
                "zadd", "zrem", "zrange", "zcard", "zscore",
                "hset", "hget", "hdel", "hlen", "hgetall", "hkeys", "hvals",
                "xadd", "xlen", "xrange", "xrevrange",
                "geoadd", "geodist", "geohash", "geopos", "geosearch",
                "ssubscribe", "sunsubscribe", "spublish",
                "command", "client", "config", "cluster", "reset",
            ];
            let result: Vec<Response> = commands.iter()
                .filter(|cmd| {
                    match &filter_pattern {
                        Some(pat) => {
                            let pat_str = String::from_utf8_lossy(pat);
                            let cmd_bytes = cmd.as_bytes();
                            glob_match(cmd_bytes, pat_str.as_bytes(), false)
                                || glob_match(pat_str.as_bytes(), cmd_bytes, false)
                        }
                        None => true,
                    }
                })
                .map(|cmd| Response::Data(cmd.as_bytes().to_vec()))
                .collect();
            Response::Array(result)
        }
        "doc" | "docs" => {
            // Return documentation for commands
            let mut results = vec![];
            for i in 2..parser.argv.len() {
                if let Ok(name) = parser.get_str(i) {
                    results.push(Response::Data(name.to_ascii_lowercase().into_bytes()));
                    results.push(Response::Array(vec![
                        Response::Data(b"summary".to_vec()),
                        Response::Data(format!("Undocumented command: {}", name).into_bytes()),
                        Response::Data(b"since".to_vec()),
                        Response::Data(b"1.0.0".to_vec()),
                        Response::Data(b"group".to_vec()),
                        Response::Data(b"generic".to_vec()),
                    ]));
                }
            }
            Response::Array(results)
        }
        "getkeys" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'command|getkeys' command".to_owned());
            }
            let cmd_name = try_validate!(parser.get_str(2), "ERR syntax error");
            let props = command_properties(cmd_name);
            if props.first_key_index == 0 && props.last_key_index == 0 {
                return Response::Array(vec![]);
            }
            let mut keys = vec![];
            let first = props.first_key_index as usize;
            let last = if props.last_key_index < 0 {
                (parser.argv.len() as i64 + props.last_key_index) as usize
            } else {
                props.last_key_index as usize
            };
            let step = if props.key_step > 0 { props.key_step as usize } else { 1 };
            let mut i = first;
            while i <= last && i < parser.argv.len() {
                if let Ok(k) = parser.get_vec(i) {
                    keys.push(Response::Data(k));
                }
                i += step;
            }
            Response::Array(keys)
        }
        "getkeysandflags" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'command|getkeysandflags' command".to_owned());
            }
            let cmd_name = try_validate!(parser.get_str(2), "ERR syntax error");
            let props = command_properties(cmd_name);
            if props.first_key_index == 0 && props.last_key_index == 0 {
                return Response::Array(vec![]);
            }
            let mut results = vec![];
            let first = props.first_key_index as usize;
            let last = if props.last_key_index < 0 {
                (parser.argv.len() as i64 + props.last_key_index) as usize
            } else {
                props.last_key_index as usize
            };
            let step = if props.key_step > 0 { props.key_step as usize } else { 1 };
            let is_write = props.flags.contains(CommandFlags::WRITE);
            let mut i = first;
            while i <= last && i < parser.argv.len() {
                if let Ok(k) = parser.get_vec(i) {
                    let flags = if is_write { b"RW".to_vec() } else { b"RO".to_vec() };
                    results.push(Response::Array(vec![
                        Response::Data(k),
                        Response::Array(vec![Response::Data(flags)]),
                    ]));
                }
                i += step;
            }
            Response::Array(results)
        }
        "info" => {
            let mut results = vec![];
            for i in 2..parser.argv.len() {
                if let Ok(name) = parser.get_str(i) {
                    let props = command_properties(&name.to_ascii_lowercase());
                    if props.arity == 0 && props.flags.is_empty() {
                        results.push(Response::Nil);
                    } else {
                        results.push(Response::Array(vec![
                            Response::Data(name.to_ascii_lowercase().into_bytes()),
                            Response::Integer(props.arity),
                            Response::Array(vec![]), // flags
                            Response::Integer(props.first_key_index),
                            Response::Integer(props.last_key_index),
                            Response::Integer(props.key_step),
                        ]));
                    }
                }
            }
            Response::Array(results)
        }
        "help" => {
            validate_arguments_exact!(parser, 2);
            let help_text = vec![
                "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "(no subcommand) -- Return details of all server commands.",
                "COUNT -- Return the total number of commands in this server.",
                "LIST -- Return a list of all commands in this server.",
                "INFO command-name [command-name ...] -- Return details about multiple commands.",
                "DOC command-name [command-name ...] -- Return documentation about multiple commands.",
                "GETKEYS command-name [args...] -- Extract keys from a full command.",
                "GETKEYSANDFLAGS command-name [args...] -- Extract keys and access flags from a full command.",
                "HELP -- Return this help message.",
            ];
            Response::Array(help_text.iter().map(|s| Response::Data(s.as_bytes().to_vec())).collect())
        }
        _ => Response::Error(format!("ERR Unknown COMMAND subcommand '{}'", subcmd)),
    }
}

// --- cluster command ---

fn cluster_command(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "info" => {
            let info = db.cluster.info_string();
            Response::Data(info.into_bytes())
        }
        "nodes" => {
            let nodes = db.cluster.nodes_string();
            Response::Data(nodes.into_bytes())
        }
        "slots" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            let ranges = db.cluster.slots_array();
            let mut result = Vec::new();
            for (start, end, ip, port, _id) in ranges {
                result.push(Response::Array(vec![
                    Response::Integer(start as i64),
                    Response::Integer(end as i64),
                    Response::Array(vec![
                        Response::Data(ip.into_bytes()),
                        Response::Integer(port as i64),
                    ]),
                ]));
            }
            Response::Array(result)
        }
        "myid" => {
            let id = cluster::node_id_to_hex(&db.cluster.my_id);
            Response::Data(id.into_bytes())
        }
        "meet" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'cluster|meet' command".to_owned());
            }
            let ip = try_validate!(parser.get_str(2), "ERR syntax error").to_owned();
            let port = try_validate!(parser.get_i64(3), "ERR value is not an integer") as u16;
            let node_id = cluster::generate_node_id();
            db.cluster.add_node(node_id, ip, port);
            db.cluster.current_epoch += 1;
            Response::Status("OK".to_owned())
        }
        "reset" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            let hard = if parser.argv.len() > 2 {
                let opt = try_validate!(parser.get_str(2), "ERR syntax error");
                opt.to_ascii_lowercase() == "hard"
            } else {
                false
            };
            match db.cluster.reset(hard) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "addslots" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|addslots' command".to_owned());
            }
            for i in 2..parser.argv.len() {
                let slot = try_validate!(parser.get_i64(i), "ERR value is not an integer") as usize;
                if let Err(e) = db.cluster.add_slot(slot) {
                    return Response::Error(e);
                }
            }
            db.cluster.config_epoch += 1;
            Response::Status("OK".to_owned())
        }
        "delslots" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|delslots' command".to_owned());
            }
            for i in 2..parser.argv.len() {
                let slot = try_validate!(parser.get_i64(i), "ERR value is not an integer") as usize;
                if let Err(e) = db.cluster.del_slot(slot) {
                    return Response::Error(e);
                }
            }
            Response::Status("OK".to_owned())
        }
        "setslot" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'cluster|setslot' command".to_owned());
            }
            let slot = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
            let action = try_validate!(parser.get_str(3), "ERR syntax error");
            match action.to_ascii_lowercase().as_str() {
                "importing" => {
                    if parser.argv.len() < 5 {
                        return Response::Error("ERR wrong number of arguments for 'cluster|setslot|importing' command".to_owned());
                    }
                    let node_id = try_validate!(parser.get_str(4), "ERR syntax error");
                    match db.cluster.set_slot_state(slot, SlotState::Importing(node_id.to_owned())) {
                        Ok(()) => Response::Status("OK".to_owned()),
                        Err(e) => Response::Error(e),
                    }
                }
                "migrating" => {
                    if parser.argv.len() < 5 {
                        return Response::Error("ERR wrong number of arguments for 'cluster|setslot|migrating' command".to_owned());
                    }
                    let node_id = try_validate!(parser.get_str(4), "ERR syntax error");
                    match db.cluster.set_slot_state(slot, SlotState::Migrating(node_id.to_owned())) {
                        Ok(()) => Response::Status("OK".to_owned()),
                        Err(e) => Response::Error(e),
                    }
                }
                "stable" => {
                    match db.cluster.set_slot_state(slot, SlotState::Stable) {
                        Ok(()) => Response::Status("OK".to_owned()),
                        Err(e) => Response::Error(e),
                    }
                }
                "node" => {
                    if parser.argv.len() < 5 {
                        return Response::Error("ERR wrong number of arguments for 'cluster|setslot|node' command".to_owned());
                    }
                    let node_id_str = try_validate!(parser.get_str(4), "ERR syntax error");
                    match cluster::hex_to_node_id(node_id_str) {
                        Ok(node_id) => {
                            if slot >= CLUSTER_SLOTS {
                                return Response::Error(format!("ERR Invalid slot {}", slot));
                            }
                            // Clear old owner
                            if let Some(old_owner) = db.cluster.slot_owners[slot] {
                                if let Some(old_node) = db.cluster.nodes.get_mut(&old_owner) {
                                    old_node.slots[slot] = false;
                                }
                            }
                            // Set new owner
                            db.cluster.slot_owners[slot] = Some(node_id);
                            if let Some(node) = db.cluster.nodes.get_mut(&node_id) {
                                node.slots[slot] = true;
                                node.config_epoch = db.cluster.config_epoch;
                            }
                            db.cluster.my_slots[slot] = (node_id == db.cluster.my_id);
                            db.cluster.set_slot_state(slot, SlotState::Stable).ok();
                            db.cluster.recalc_size();
                            Response::Status("OK".to_owned())
                        }
                        Err(e) => Response::Error(e),
                    }
                }
                _ => Response::Error(format!("ERR Invalid CLUSTER SETSLOT action '{}'", action)),
            }
        }
        "keyslot" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|keyslot' command".to_owned());
            }
            let key = try_validate!(parser.get_vec(2), "Invalid key");
            let slot = crc16_slot(&key);
            Response::Integer(slot as i64)
        }
        "countkeysinslot" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|countkeysinslot' command".to_owned());
            }
            let slot = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
            if slot >= CLUSTER_SLOTS {
                return Response::Error(format!("ERR Invalid slot {}", slot));
            }
            // Count keys in this shard's DB 0 that hash to this slot
            let mut count = 0usize;
            for (key, _) in db.iter_db(0) {
                if crc16_slot(key) as usize == slot {
                    count += 1;
                }
            }
            Response::Integer(count as i64)
        }
        "getkeysinslot" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'cluster|getkeysinslot' command".to_owned());
            }
            let slot = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
            let count = try_validate!(parser.get_i64(3), "ERR value is not an integer") as usize;
            if slot >= CLUSTER_SLOTS {
                return Response::Error(format!("ERR Invalid slot {}", slot));
            }
            let mut keys = Vec::new();
            for (key, _) in db.iter_db(0) {
                if crc16_slot(key) as usize == slot {
                    keys.push(Response::Data(key.clone()));
                    if keys.len() >= count {
                        break;
                    }
                }
            }
            Response::Array(keys)
        }
        "slaves" | "replicas" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|replicas' command".to_owned());
            }
            let node_id_str = try_validate!(parser.get_str(2), "ERR syntax error");
            match cluster::hex_to_node_id(node_id_str) {
                Ok(node_id) => {
                    let replicas: Vec<Response> = db.cluster.nodes.values()
                        .filter(|n| n.replica_of == Some(node_id))
                        .map(|n| {
                            let id_hex = n.id_hex();
                            let addr = format!("{}:{}@{}", n.ip, n.port, n.bus_port);
                            let flags = format!("{}", n.flags);
                            Response::Data(format!("{} {} {} 0 0 0 connected", id_hex, addr, flags).into_bytes())
                        })
                        .collect();
                    Response::Array(replicas)
                }
                Err(e) => Response::Error(e),
            }
        }
        "replicate" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|replicate' command".to_owned());
            }
            let node_id_str = try_validate!(parser.get_str(2), "ERR syntax error");
            match cluster::hex_to_node_id(node_id_str) {
                Ok(node_id) => {
                    if !db.cluster.nodes.contains_key(&node_id) {
                        return Response::Error("ERR Unknown node ID".to_owned());
                    }
                    db.cluster.role = NodeRole::Replica;
                    db.cluster.replica_of = Some(node_id);
                    db.cluster.flags = NodeFlags::Replica;
                    Response::Status("OK".to_owned())
                }
                Err(e) => Response::Error(e),
            }
        }
        "failover" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            // Simplified: just bump the config epoch
            db.cluster.config_epoch += 1;
            db.cluster.current_epoch = db.cluster.current_epoch.max(db.cluster.config_epoch);
            Response::Status("OK".to_owned())
        }
        "saveconfig" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            match cluster::save_cluster_config(&db.cluster, &db.cluster.config_file.clone()) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "forget" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'cluster|forget' command".to_owned());
            }
            let node_id_str = try_validate!(parser.get_str(2), "ERR syntax error");
            match cluster::hex_to_node_id(node_id_str) {
                Ok(node_id) => {
                    if node_id == db.cluster.my_id {
                        return Response::Error("ERR I tried hard but I can't forget myself".to_owned());
                    }
                    db.cluster.remove_node(&node_id);
                    Response::Status("OK".to_owned())
                }
                Err(e) => Response::Error(e),
            }
        }
        "flushslots" => {
            if !db.cluster.enabled {
                return Response::Error("ERR This instance has cluster support disabled".to_owned());
            }
            db.cluster.flush_slots();
            Response::Status("OK".to_owned())
        }
        _ => Response::Error(format!("ERR Unknown CLUSTER subcommand '{}'", subcmd)),
    }
}

/// Simple CRC16 slot calculation for cluster mode
fn crc16_slot(key: &[u8]) -> u16 {
    // Check for hash tag {xxx}
    let key_content = if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                &key[start + 1..start + 1 + end]
            } else {
                key
            }
        } else {
            key
        }
    } else {
        key
    };

    // Simple CRC16 CCITT
    let mut crc: u16 = 0;
    for &byte in key_content {
        crc = ((crc << 8) & 0xFF00) ^ CRC16_TABLE[((crc >> 8) as u8 ^ byte) as usize];
    }
    crc % 16384
}

static CRC16_TABLE: [u16; 256] = {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

// --- sentinel command ---

fn sentinel_command(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_gte!(parser, 2);
    // Ensure sentinel state exists
    if db.sentinel.is_none() {
        db.sentinel = Some(database::sentinel::SentinelState::new());
    }
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "myid" => {
            let id = db.sentinel.as_ref().unwrap().sentinel_id_hex();
            Response::Data(id.into_bytes())
        }
        "ping" => Response::Status("PONG".to_owned()),
        "masters" => {
            let sentinel = db.sentinel.as_ref().unwrap();
            let mut result = Vec::new();
            for master in sentinel.masters.values() {
                let info = database::sentinel::SentinelState::master_info_string(master);
                result.push(Response::Data(info.into_bytes()));
            }
            Response::Array(result)
        }
        "master" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|master' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            let sentinel = db.sentinel.as_ref().unwrap();
            match sentinel.get_master(name) {
                Some(master) => {
                    let info = database::sentinel::SentinelState::master_info_string(master);
                    Response::Data(info.into_bytes())
                }
                None => Response::Error(format!("ERR No such master name '{}'", name)),
            }
        }
        "replicas" | "slaves" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|replicas' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            let sentinel = db.sentinel.as_ref().unwrap();
            match sentinel.get_master(name) {
                Some(master) => {
                    let replicas: Vec<Response> = master.replicas.iter().map(|r| {
                        Response::Data(format!(
                            "ip={}:port={}:state={}:master-link-status={}",
                            r.ip, r.port, r.state, if r.master_link_status { "up" } else { "down" }
                        ).into_bytes())
                    }).collect();
                    Response::Array(replicas)
                }
                None => Response::Error(format!("ERR No such master name '{}'", name)),
            }
        }
        "sentinels" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|sentinels' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            let sentinel = db.sentinel.as_ref().unwrap();
            if !sentinel.masters.contains_key(name) {
                return Response::Error(format!("ERR No such master name '{}'", name));
            }
            // Return known sentinels for this master
            let sentinels = sentinel.known_sentinels.get(name).cloned().unwrap_or_default();
            let result: Vec<Response> = sentinels.iter().map(|s| {
                Response::Data(format!("ip={}:port={}:run_id={}", s.ip, s.port, s.run_id).into_bytes())
            }).collect();
            Response::Array(result)
        }
        "get-master-addr-by-name" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|get-master-addr-by-name' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            let sentinel = db.sentinel.as_ref().unwrap();
            match sentinel.get_master(name) {
                Some(master) => Response::Array(vec![
                    Response::Data(master.ip.as_bytes().to_vec()),
                    Response::Integer(master.port as i64),
                ]),
                None => Response::Nil,
            }
        }
        "monitor" => {
            // SENTINEL MONITOR name ip port quorum
            if parser.argv.len() < 6 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|monitor' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error").to_owned();
            let ip = try_validate!(parser.get_str(3), "ERR syntax error").to_owned();
            let port = try_validate!(parser.get_i64(4), "ERR value is not an integer") as u16;
            let quorum = try_validate!(parser.get_i64(5), "ERR value is not an integer") as u32;
            db.sentinel.as_mut().unwrap().add_master(name, ip, port, quorum);
            Response::Status("OK".to_owned())
        }
        "remove" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|remove' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            if db.sentinel.as_mut().unwrap().remove_master(name) {
                Response::Status("OK".to_owned())
            } else {
                Response::Error(format!("ERR No such master name '{}'", name))
            }
        }
        "set" => {
            // SENTINEL SET name option value [option value ...]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|set' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error").to_owned();
            let sentinel = db.sentinel.as_mut().unwrap();
            if !sentinel.masters.contains_key(&name) {
                return Response::Error(format!("ERR No such master name '{}'", name));
            }
            let mut i = 3;
            while i + 1 < parser.argv.len() {
                let option = try_validate!(parser.get_str(i), "ERR syntax error");
                let value = try_validate!(parser.get_str(i + 1), "ERR syntax error");
                if let Some(master) = sentinel.masters.get_mut(&name) {
                    match option.to_ascii_lowercase().as_str() {
                        "down-after-milliseconds" => {
                            master.down_after_ms = value.parse().unwrap_or(30000);
                        }
                        "failover-timeout" => {
                            master.failover_timeout = value.parse().unwrap_or(180000);
                        }
                        "parallel-syncs" => {
                            master.parallel_syncs = value.parse().unwrap_or(1);
                        }
                        "quorum" => {
                            master.quorum = value.parse().unwrap_or(2);
                        }
                        _ => {}
                    }
                }
                i += 2;
            }
            Response::Status("OK".to_owned())
        }
        "ckquorum" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|ckquorum' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            let sentinel = db.sentinel.as_ref().unwrap();
            match sentinel.get_master(name) {
                Some(master) => {
                    // Simplified: just check if quorum > 0
                    if master.quorum > 0 {
                        Response::Status("OK".to_owned())
                    } else {
                        Response::Error("ERR quorum is 0".to_owned())
                    }
                }
                None => Response::Error(format!("ERR No such master name '{}'", name)),
            }
        }
        "failover" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|failover' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            let sentinel = db.sentinel.as_mut().unwrap();
            match sentinel.get_master_mut(name) {
                Some(master) => {
                    master.failover_in_progress = true;
                    master.failover_epoch += 1;
                    Response::Status("OK".to_owned())
                }
                None => Response::Error(format!("ERR No such master name '{}'", name)),
            }
        }
        "reset" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|reset' command".to_owned());
            }
            let pattern = try_validate!(parser.get_str(2), "ERR syntax error");
            let count = db.sentinel.as_mut().unwrap().reset_pattern(pattern);
            Response::Integer(count as i64)
        }
        "info" => {
            let sentinel = db.sentinel.as_ref().unwrap();
            let info = format!(
                "sentinel_masters:{}\r\nsentinel_runid:{}\r\nsentinel_tilt:0\r\n",
                sentinel.masters.len(),
                sentinel.run_id,
            );
            Response::Data(info.into_bytes())
        }
        "is-master-down-by-addr" => {
            // SENTINEL IS-MASTER-DOWN-BY-ADDR ip port runid mstime
            if parser.argv.len() < 6 {
                return Response::Error("ERR wrong number of arguments for 'sentinel|is-master-down-by-addr' command".to_owned());
            }
            let ip = try_validate!(parser.get_str(2), "ERR syntax error");
            let port = try_validate!(parser.get_i64(3), "ERR value is not an integer") as u16;
            // Check if we're monitoring this master and it's down
            let sentinel = db.sentinel.as_ref().unwrap();
            let mut is_down = 0i64;
            for master in sentinel.masters.values() {
                if master.ip == ip && master.port == port && master.state != database::sentinel::MasterState::Ok {
                    is_down = 1;
                    break;
                }
            }
            // Return: is_down, leader_runid, leader_epoch
            let leader = "*";
            Response::Array(vec![
                Response::Integer(is_down),
                Response::Data(leader.as_bytes().to_vec()),
                Response::Integer(0),
            ])
        }
        _ => Response::Error(format!("ERR Unknown SENTINEL subcommand '{}'", subcmd)),
    }
}

// --- latency command (stub) ---

fn latency_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "latest" => Response::Array(vec![]),
        "history" => Response::Array(vec![]),
        "reset" => Response::Array(vec![]),
        "graph" => Response::Data(b"".to_vec()),
        "doctor" => Response::Data(b"\nI'm sorry, no latency spikes detected.\n".to_vec()),
        _ => Response::Error("ERR Unknown LATENCY subcommand".to_owned()),
    }
}

// --- replication stubs ---

fn slaveof_command(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 3);
    let host = try_validate!(parser.get_str(1), "ERR syntax error");
    let port_str = try_validate!(parser.get_str(2), "ERR syntax error");
    if host.to_ascii_lowercase() == "no" && port_str.to_ascii_lowercase() == "one" {
        // Promote to master
        db.cluster.role = NodeRole::Master;
        db.cluster.replica_of = None;
        db.cluster.flags = NodeFlags::Myself;
        Response::Status("OK".to_owned())
    } else {
        let port = try_validate!(port_str.parse::<u16>(), "ERR value is not an integer");
        // In cluster mode, use REPLICAOF to set up replication
        let node_id = cluster::generate_node_id();
        db.cluster.add_node(node_id, host.to_owned(), port);
        db.cluster.role = NodeRole::Replica;
        db.cluster.replica_of = Some(node_id);
        db.cluster.flags = NodeFlags::Replica;
        Response::Status("OK".to_owned())
    }
}

fn replconf_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_gte!(parser, 1);
    Response::Status("OK".to_owned())
}

fn wait_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 3);
    // No replicas, return 0
    Response::Integer(0)
}

// --- misc stubs ---

fn sync_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Error("ERR SYNC is not supported".to_owned())
}

fn psync_command(_parser: &mut ParsedCommand, _db: &Database) -> Response {
    Response::Error("ERR PSYNC is not supported".to_owned())
}

fn asking_command(parser: &mut ParsedCommand, client: &mut Client) -> Response {
    validate_arguments_exact!(parser, 1);
    client.asking = true;
    Response::Status("OK".to_owned())
}

fn readonly_command(parser: &mut ParsedCommand, client: &mut Client) -> Response {
    validate_arguments_exact!(parser, 1);
    client.readonly = true;
    Response::Status("OK".to_owned())
}

fn readwrite_command(parser: &mut ParsedCommand, client: &mut Client) -> Response {
    validate_arguments_exact!(parser, 1);
    client.readonly = false;
    Response::Status("OK".to_owned())
}

fn restore_command(_parser: &mut ParsedCommand, _db: &Database) -> Response {
    Response::Error("ERR RESTORE is not implemented".to_owned())
}

fn migrate_command(_parser: &mut ParsedCommand, _db: &Database) -> Response {
    Response::Error("ERR MIGRATE is not implemented".to_owned())
}

fn eval_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    if parser.argv.len() < 3 {
        return Response::Error("ERR wrong number of arguments for 'eval' command".to_owned());
    }
    let source = try_validate!(parser.get_vec(1), "ERR syntax error");
    let numkeys = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
    if parser.argv.len() < 3 + numkeys {
        return Response::Error("ERR Number of keys can't be greater than number of args".to_owned());
    }
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(3 + i), "ERR syntax error"));
    }
    let mut argv = Vec::new();
    for i in 3 + numkeys..parser.argv.len() {
        argv.push(try_validate!(parser.get_vec(i), "ERR syntax error"));
    }
    crate::scripting::eval_script(db, dbindex, &source, &keys, &argv)
}

fn evalsha_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    if parser.argv.len() < 3 {
        return Response::Error("ERR wrong number of arguments for 'evalsha' command".to_owned());
    }
    let sha = try_validate!(parser.get_str(1), "ERR syntax error").to_owned();
    let numkeys = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
    if parser.argv.len() < 3 + numkeys {
        return Response::Error("ERR Number of keys can't be greater than number of args".to_owned());
    }
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(3 + i), "ERR syntax error"));
    }
    let mut argv = Vec::new();
    for i in 3 + numkeys..parser.argv.len() {
        argv.push(try_validate!(parser.get_vec(i), "ERR syntax error"));
    }
    crate::scripting::evalsha_script(db, dbindex, &sha, &keys, &argv)
}

fn script_command(parser: &mut ParsedCommand, db: &Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "exists" => {
            let mut results = vec![];
            for i in 2..parser.argv.len() {
                if let Ok(sha) = parser.get_str(i) {
                    results.push(Response::Integer(if db.script_cache.contains_key(sha) { 1 } else { 0 }));
                }
            }
            Response::Array(results)
        }
        "flush" => Response::Status("OK".to_owned()),
        "load" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'script load' command".to_owned());
            }
            let source = try_validate!(parser.get_vec(2), "ERR syntax error");
            let sha = crate::scripting::script_sha1(&source);
            Response::Data(sha.into_bytes())
        }
        _ => Response::Error(format!("ERR unknown subcommand '{}'", subcmd)),
    }
}

fn function_command(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    if parser.argv.len() < 2 {
        return Response::Error("ERR wrong number of arguments for 'function' command".to_owned());
    }
    let subcmd = try_validate!(parser.get_str(1), "ERR syntax error");
    match subcmd.to_ascii_lowercase().as_str() {
        "load" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'function load' command".to_owned());
            }
            let mut replace = false;
            let mut code_idx = 2;
            if parser.argv.len() > 3 {
                if let Ok(opt) = parser.get_str(2) {
                    if opt.to_ascii_lowercase() == "replace" {
                        replace = true;
                        code_idx = 3;
                    }
                }
            }
            let code = try_validate!(parser.get_str(code_idx), "ERR syntax error");
            match crate::scripting::function_load(db, code, replace) {
                Ok(name) => Response::Data(name.into_bytes()),
                Err(e) => Response::Error(e),
            }
        }
        "list" => {
            let mut result = Vec::new();
            for (name, func) in &db.lua_functions {
                let entry = vec![
                    Response::Data(b"name".to_vec()),
                    Response::Data(name.as_bytes().to_vec()),
                    Response::Data(b"engine".to_vec()),
                    Response::Data(func.engine.as_bytes().to_vec()),
                ];
                result.push(Response::Array(entry));
            }
            Response::Array(result)
        }
        "delete" => {
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'function delete' command".to_owned());
            }
            let name = try_validate!(parser.get_str(2), "ERR syntax error");
            if db.lua_functions.remove(name).is_some() {
                Response::Status("OK".to_owned())
            } else {
                Response::Error(format!("ERR Function '{}' not found", name))
            }
        }
        "flush" => {
            db.lua_functions.clear();
            Response::Status("OK".to_owned())
        }
        "stats" => {
            let result = vec![
                Response::Data(b"engines".to_vec()),
                Response::Array(vec![Response::Integer(db.lua_functions.len() as i64)]),
            ];
            Response::Array(result)
        }
        "kill" => Response::Error("ERR No scripts in execution".to_owned()),
        _ => Response::Error(format!("ERR unknown subcommand '{}'", subcmd)),
    }
}

fn fcall_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    if parser.argv.len() < 3 {
        return Response::Error("ERR wrong number of arguments for 'fcall' command".to_owned());
    }
    let name = try_validate!(parser.get_str(1), "ERR syntax error").to_owned();
    let numkeys = try_validate!(parser.get_i64(2), "ERR value is not an integer") as usize;
    if parser.argv.len() < 3 + numkeys {
        return Response::Error("ERR Number of keys can't be greater than number of args".to_owned());
    }
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(3 + i), "ERR syntax error"));
    }
    let mut argv = Vec::new();
    for i in 3 + numkeys..parser.argv.len() {
        argv.push(try_validate!(parser.get_vec(i), "ERR syntax error"));
    }
    crate::scripting::fcall_function(db, dbindex, &name, &keys, &argv)
}

fn fcall_ro_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    fcall_command(parser, db, dbindex)
}

fn pfselftest_command(parser: &mut ParsedCommand, _db: &Database) -> Response {
    validate_arguments_exact!(parser, 1);
    Response::Status("OK".to_owned())
}

fn pfdebug_command(_parser: &mut ParsedCommand, _db: &Database) -> Response {
    Response::Error("ERR PFDEBUG is not implemented".to_owned())
}

// =============================================================================
// Phase 1: New commands for Redis 8.x compatibility
// =============================================================================

// --- LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len] ---
fn lpos(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let element = try_validate!(parser.get_vec(2), "Invalid element");
    let mut rank: i64 = 0;
    let mut count: usize = 0;
    let mut count_given = false;
    let mut maxlen: usize = 0;
    let mut i = 3;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "rank" => {
                rank = try_validate!(parser.get_i64(i + 1), "ERR rank is not an integer");
                if rank == 0 { return Response::Error("ERR RANK can't be zero: use 1 for first match, -1 for last".to_owned()); }
                i += 2;
            }
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "ERR count is not an integer");
                if c < 0 { return Response::Error("ERR COUNT can't be negative".to_owned()); }
                count = c as usize;
                count_given = true;
                i += 2;
            }
            "maxlen" => {
                let m = try_validate!(parser.get_i64(i + 1), "ERR maxlen is not an integer");
                if m < 0 { return Response::Error("ERR MAXLEN can't be negative".to_owned()); }
                maxlen = m as usize;
                i += 2;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    // rank=0 means first match (rank=1 in our lpos impl)
    let effective_rank = if rank == 0 { 1 } else { rank.abs() };
    let forward = rank >= 0;
    let actual_rank = if forward { effective_rank } else { -effective_rank };
    let effective_count = if count_given { count } else { 1 };

    match db.get(dbindex, &key) {
        Some(val) => {
            let indices = match val.lpos(&element, actual_rank, effective_count, maxlen) {
                Ok(r) => r,
                Err(e) => return Response::Error(e.to_string()),
            };
            if count_given {
                Response::Array(indices.iter().map(|&i| Response::Integer(i)).collect())
            } else {
                match indices.first() {
                    Some(&i) => Response::Integer(i),
                    None => Response::Nil,
                }
            }
        }
        None => {
            if count_given { Response::Array(vec![]) } else { Response::Nil }
        }
    }
}

// --- LMOVE source destination LEFT|RIGHT LEFT|RIGHT ---
fn generic_lmove(db: &mut Database, dbindex: usize, source: &[u8], destination: &[u8], src_right: bool, dst_right: bool) -> Response {
    if let Some(Err(_)) = db.get(dbindex, destination).map(|el| el.llen()) {
        return Response::Error("WRONGTYPE Destination is not a list".to_owned());
    }
    let el = {
        let sourcelist = match db.get_mut(dbindex, source) {
            Some(sourcelist) => {
                if sourcelist.llen().is_err() {
                    return Response::Error("WRONGTYPE Source is not a list".to_owned());
                }
                sourcelist
            }
            None => return Response::Nil,
        };
        match sourcelist.pop(src_right) {
            Ok(el) => match el {
                Some(el) => el,
                None => return Response::Nil,
            },
            Err(err) => return Response::Error(err.to_string()),
        }
    };
    let resp = {
        let destinationlist = db.get_or_create(dbindex, destination);
        if let Err(e) = destinationlist.push(el.clone(), dst_right) {
            return Response::Error(e.to_string());
        }
        Response::Data(el)
    };
    db.key_updated(dbindex, source);
    db.key_updated(dbindex, destination);
    resp
}

fn lmove(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 5);
    let source = try_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_validate!(parser.get_vec(2), "Invalid destination");
    let wherefrom = try_validate!(parser.get_str(3), "ERR syntax error");
    let whereto = try_validate!(parser.get_str(4), "ERR syntax error");
    let src_right = match wherefrom.to_ascii_lowercase().as_str() {
        "left" => false,
        "right" => true,
        _ => return Response::Error("ERR syntax error".to_owned()),
    };
    let dst_right = match whereto.to_ascii_lowercase().as_str() {
        "left" => false,
        "right" => true,
        _ => return Response::Error("ERR syntax error".to_owned()),
    };
    generic_lmove(db, dbindex, &source, &destination, src_right, dst_right)
}

// --- BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout ---
fn blmove(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() == 6, "Wrong number of parameters");
    let source = try_opt_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_opt_validate!(parser.get_vec(2), "Invalid destination");
    let wherefrom = try_opt_validate!(parser.get_str(3), "ERR syntax error");
    let whereto = try_opt_validate!(parser.get_str(4), "ERR syntax error");
    let timeout = try_opt_validate!(parser.get_i64(5), "ERR timeout is not an integer");
    let src_right = match wherefrom.to_ascii_lowercase().as_str() {
        "left" => false, "right" => true,
        _ => return Ok(Response::Error("ERR syntax error".to_owned())),
    };
    let dst_right = match whereto.to_ascii_lowercase().as_str() {
        "left" => false, "right" => true,
        _ => return Ok(Response::Error("ERR syntax error".to_owned())),
    };
    let r = generic_lmove(db, dbindex, &source, &destination, src_right, dst_right);
    if r != Response::Nil { return Ok(r); }
    // Blocking logic: reuse the brpoplpush pattern
    let time = mstime();
    let (txkey, rxkey) = channel();
    let (txcommand, rxcommand) = channel();
    if timeout > 0 {
        let tx = txcommand.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(timeout as u64));
            let _ = tx.send(None);
        });
    }
    let command_name = try_opt_validate!(parser.get_vec(0), "Invalid command");
    // Convert &str to owned Vec<u8> before moving into thread
    let wherefrom_bytes = wherefrom.as_bytes().to_vec();
    let whereto_bytes = whereto.as_bytes().to_vec();
    db.key_subscribe(dbindex, &source, txkey);
    thread::spawn(move || {
        let _ = rxkey.recv();
        let newtimeout = if timeout == 0 { 0 } else {
            let mut t = timeout as i64 * 1000 - mstime() + time;
            if t <= 0 { t = 1; }
            t
        };
        let mut data = vec![];
        let mut arguments = vec![];
        data.extend(command_name);
        arguments.push(Argument { pos: 0, len: data.len() });
        arguments.push(Argument { pos: data.len(), len: source.len() });
        data.extend(source.iter().copied());
        arguments.push(Argument { pos: data.len(), len: destination.len() });
        data.extend(destination.iter().copied());
        arguments.push(Argument { pos: data.len(), len: wherefrom_bytes.len() });
        data.extend(wherefrom_bytes.iter().copied());
        arguments.push(Argument { pos: data.len(), len: whereto_bytes.len() });
        data.extend(whereto_bytes.iter().copied());
        let timeout_formatted = format!("{}", newtimeout);
        arguments.push(Argument { pos: data.len(), len: timeout_formatted.len() });
        data.extend(timeout_formatted.into_bytes());
        let _ = txcommand.send(Some(OwnedParsedCommand::new(data, arguments)));
    });
    Err(ResponseError::Wait(rxcommand))
}

// --- LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count] ---
fn lmpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys + 1);
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(2 + i), "Invalid key"));
    }
    let direction = try_validate!(parser.get_str(2 + numkeys), "ERR syntax error");
    let right = match direction.to_ascii_lowercase().as_str() {
        "left" => false, "right" => true,
        _ => return Response::Error("ERR syntax error".to_owned()),
    };
    let mut count: usize = 1;
    let mut i = 3 + numkeys;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "ERR count is not an integer");
                if c < 1 { return Response::Error("ERR count should be greater than 0".to_owned()); }
                count = c as usize;
                i += 2;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    for key in &keys {
        let mut elements = Vec::new();
        {
            let list = match db.get_mut(dbindex, key) {
                Some(l) => l,
                None => continue,
            };
            if list.llen().is_err() { continue; }
            for _ in 0..count {
                match list.pop(right) {
                    Ok(Some(el)) => elements.push(el),
                    _ => break,
                }
            }
        }
        if elements.is_empty() { continue; }
        db.key_updated(dbindex, key);
        let element_responses: Vec<Response> = elements.into_iter().map(Response::Data).collect();
        return Response::Array(vec![
            Response::Data(key.clone()),
            Response::Array(element_responses),
        ]);
    }
    Response::Nil
}

// --- BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count] ---
fn blmpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    // For now, just try lmpop and return Nil if empty (blocking is complex)
    let r = lmpop(parser, db, dbindex);
    Ok(r)
}

// --- SMISMEMBER key member [member ...] ---
fn smismember(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut members = Vec::with_capacity(parser.argv.len() - 2);
    for i in 2..parser.argv.len() {
        members.push(try_validate!(parser.get_vec(i), "Invalid member"));
    }
    match db.get(dbindex, &key) {
        Some(val) => {
            let results: Vec<Response> = members.iter().map(|m| {
                match val.sismember(m) {
                    Ok(true) => Response::Integer(1),
                    _ => Response::Integer(0),
                }
            }).collect();
            Response::Array(results)
        }
        None => Response::Array(members.iter().map(|_| Response::Integer(0)).collect()),
    }
}

// --- SINTERCARD numkeys key [key ...] [LIMIT limit] ---
fn sintercard(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys);
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(2 + i), "Invalid key"));
    }
    let mut limit: usize = 0;
    let mut i = 2 + numkeys;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "limit" => {
                let l = try_validate!(parser.get_i64(i + 1), "ERR limit is not an integer");
                if l < 0 { return Response::Error("ERR LIMIT can't be negative".to_owned()); }
                limit = l as usize;
                i += 2;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    // Compute intersection cardinality
    let sets: Vec<&Value> = keys.iter().map(|k| {
        db.get(dbindex, k).unwrap_or(&Value::Nil)
    }).collect();
    // Get the smallest set for efficiency
    let mut min_size = usize::MAX;
    let mut min_idx = 0;
    for (idx, s) in sets.iter().enumerate() {
        if let Ok(sz) = s.scard() {
            if sz < min_size { min_size = sz; min_idx = idx; }
        }
    }
    if min_size == usize::MAX || min_size == 0 {
        return Response::Integer(0);
    }
    let smallest = sets[min_idx];
    let mut count = 0usize;
    if let Ok(members) = smallest.smembers() {
        for member in members {
            let mut in_all = true;
            for (idx, s) in sets.iter().enumerate() {
                if idx == min_idx { continue; }
                match s.sismember(&member) {
                    Ok(true) => {}
                    _ => { in_all = false; break; }
                }
            }
            if in_all {
                count += 1;
                if limit > 0 && count >= limit { break; }
            }
        }
    }
    Response::Integer(count as i64)
}

// --- ZMSCORE key member [member ...] ---
fn zmscore(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut members = Vec::with_capacity(parser.argv.len() - 2);
    for i in 2..parser.argv.len() {
        members.push(try_validate!(parser.get_vec(i), "Invalid member"));
    }
    match db.get(dbindex, &key) {
        Some(val) => {
            let results: Vec<Response> = members.iter().map(|m| {
                match val.zscore(m.clone()) {
                    Ok(Some(score)) => Response::Data(format!("{}", score).into_bytes()),
                    _ => Response::Nil,
                }
            }).collect();
            Response::Array(results)
        }
        None => Response::Array(members.iter().map(|_| Response::Nil).collect()),
    }
}

// --- ZRANDMEMBER key [count [WITHSCORES]] ---
fn zrandmember(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(val) => {
            if parser.argv.len() == 2 {
                // No count: return single random member
                match val.zrandmember(1, false) {
                    Ok(members) => {
                        if members.is_empty() { Response::Nil }
                        else { Response::Data(members[0].clone()) }
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            } else {
                let count = try_validate!(parser.get_i64(2), "ERR count is not an integer");
                let withscores = parser.argv.len() > 3 && parser.get_str(3).map(|s| s.eq_ignore_ascii_case("withscores")).unwrap_or(false);
                let abs_count = count.unsigned_abs() as usize;
                let allow_dups = count < 0;
                match val.zrandmember(abs_count, allow_dups) {
                    Ok(members) => {
                        let mut result = Vec::new();
                        for m in &members {
                            result.push(Response::Data(m.clone()));
                            if withscores {
                                if let Ok(Some(score)) = val.zscore(m.clone()) {
                                    result.push(Response::Data(format!("{}", score).into_bytes()));
                                }
                            }
                        }
                        Response::Array(result)
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            }
        }
        None => {
            if parser.argv.len() == 2 { Response::Nil }
            else { Response::Array(vec![]) }
        }
    }
}

// --- COPY source destination [DB destination-db] [REPLACE] ---
fn copy(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let source = try_validate!(parser.get_vec(1), "Invalid source");
    let destination = try_validate!(parser.get_vec(2), "Invalid destination");
    let mut dest_db = dbindex;
    let mut replace = false;
    let mut i = 3;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "db" => {
                dest_db = try_validate!(parser.get_i64(i + 1), "ERR DB is not an integer") as usize;
                i += 2;
            }
            "replace" => { replace = true; i += 1; }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    // Check if destination exists
    if !replace && db.get(dest_db, &destination).is_some() {
        return Response::Integer(0);
    }
    // Copy the value
    match db.get(dbindex, &source) {
        Some(val) => {
            let val_clone = val.clone();
            db.remove(dest_db, &destination);
            *db.get_or_create(dest_db, &destination) = val_clone;
            db.key_updated(dest_db, &destination);
            // Copy expiration if any
            if let Some(&exp) = db.get_msexpiration(dbindex, &source) {
                db.set_msexpiration(dest_db, destination, exp);
            }
            Response::Integer(1)
        }
        None => Response::Integer(0),
    }
}

// --- UNLINK key [key ...] --- (same as DEL for synchronous impl)
fn unlink(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let mut count = 0i64;
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        if db.remove(dbindex, &key).is_some() {
            count += 1;
        }
    }
    Response::Integer(count)
}

// --- TOUCH key [key ...] ---
fn touch(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let mut count = 0i64;
    for i in 1..parser.argv.len() {
        let key = try_validate!(parser.get_vec(i), "Invalid key");
        if db.get(dbindex, &key).is_some() {
            db.key_updated(dbindex, &key);
            count += 1;
        }
    }
    Response::Integer(count)
}

// --- GETDEL key ---
fn getdel(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(val) => {
            match val.get() {
                Ok(data) => {
                    let resp = Response::Data(data.to_vec());
                    db.remove(dbindex, &key);
                    db.key_updated(dbindex, &key);
                    resp
                }
                Err(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            }
        }
        None => Response::Nil,
    }
}

// --- GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-ms|PERSIST] ---
fn getex(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    // First, get the value
    let resp = match db.get(dbindex, &key) {
        Some(val) => {
            match val.get() {
                Ok(data) => Response::Data(data.to_vec()),
                Err(_) => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            }
        }
        None => return Response::Nil,
    };
    // Now handle expiration options
    if parser.argv.len() > 2 {
        let opt = try_validate!(parser.get_str(2), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "ex" => {
                let ex = try_validate!(parser.get_i64(3), "ERR value is not an integer");
                if ex <= 0 { return Response::Error("ERR invalid expire time".to_owned()); }
                db.set_msexpiration(dbindex, key, ex * 1000 + mstime());
            }
            "px" => {
                let px = try_validate!(parser.get_i64(3), "ERR value is not an integer");
                if px <= 0 { return Response::Error("ERR invalid expire time".to_owned()); }
                db.set_msexpiration(dbindex, key, px + mstime());
            }
            "exat" => {
                let exat = try_validate!(parser.get_i64(3), "ERR value is not an integer");
                if exat <= 0 { return Response::Error("ERR invalid expire time".to_owned()); }
                db.set_msexpiration(dbindex, key, exat * 1000);
            }
            "pxat" => {
                let pxat = try_validate!(parser.get_i64(3), "ERR value is not an integer");
                if pxat <= 0 { return Response::Error("ERR invalid expire time".to_owned()); }
                db.set_msexpiration(dbindex, key, pxat);
            }
            "persist" => {
                db.remove_msexpiration(dbindex, &key);
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    resp
}

// --- LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN] ---
fn lcs(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key1 = try_validate!(parser.get_vec(1), "Invalid key");
    let key2 = try_validate!(parser.get_vec(2), "Invalid key");
    let mut just_len = false;
    let mut with_idx = false;
    let mut min_match_len: usize = 0;
    let mut with_match_len = false;
    let mut i = 3;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "len" => { just_len = true; i += 1; }
            "idx" => { with_idx = true; i += 1; }
            "minmatchlen" => {
                min_match_len = try_validate!(parser.get_i64(i + 1), "ERR minmatchlen is not an integer") as usize;
                i += 2;
            }
            "withmatchlen" => { with_match_len = true; i += 1; }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    let s1 = match db.get(dbindex, &key1) {
        Some(val) => match val.get() {
            Ok(data) => data.to_vec(),
            Err(_) => return Response::Error("WRONGTYPE value is not a string".to_owned()),
        },
        None => vec![],
    };
    let s2 = match db.get(dbindex, &key2) {
        Some(val) => match val.get() {
            Ok(data) => data.to_vec(),
            Err(_) => return Response::Error("WRONGTYPE value is not a string".to_owned()),
        },
        None => vec![],
    };
    // DP-based LCS
    let (m, n) = (s1.len(), s2.len());
    let mut dp = vec![vec![0usize; n + 1]; m + 1];
    for i in (0..m).rev() {
        for j in (0..n).rev() {
            if s1[i] == s2[j] {
                dp[i][j] = dp[i + 1][j + 1] + 1;
            } else {
                dp[i][j] = dp[i + 1][j].max(dp[i][j + 1]);
            }
        }
    }
    let lcs_len = dp[0][0];
    if just_len {
        return Response::Integer(lcs_len as i64);
    }
    if !with_idx {
        // Return the LCS string
        let mut result = Vec::with_capacity(lcs_len);
        let (mut i, mut j) = (0usize, 0usize);
        while i < m && j < n {
            if s1[i] == s2[j] {
                result.push(s1[i]);
                i += 1;
                j += 1;
            } else if dp[i + 1][j] >= dp[i][j + 1] {
                i += 1;
            } else {
                j += 1;
            }
        }
        return Response::Data(result);
    }
    // IDX mode: return match ranges
    let mut matches: Vec<(usize, usize, usize, usize, usize)> = Vec::new(); // (s1_start, s1_end, s2_start, s2_end, match_len)
    let (mut i, mut j) = (0usize, 0usize);
    while i < m && j < n {
        if s1[i] == s2[j] {
            let start_i = i;
            let start_j = j;
            while i < m && j < n && s1[i] == s2[j] {
                i += 1;
                j += 1;
            }
            let ml = i - start_i;
            if ml >= min_match_len {
                matches.push((start_i, i - 1, start_j, j - 1, ml));
            }
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }
    let match_responses: Vec<Response> = matches.iter().map(|&(si, ei, sj, ej, ml)| {
        let mut parts = vec![
            Response::Array(vec![Response::Integer(si as i64), Response::Integer(ei as i64)]),
            Response::Array(vec![Response::Integer(sj as i64), Response::Integer(ej as i64)]),
        ];
        if with_match_len {
            parts.push(Response::Integer(ml as i64));
        }
        Response::Array(parts)
    }).collect();
    Response::Array(vec![
        Response::Data(b"matches".to_vec()),
        Response::Array(match_responses),
        Response::Data(b"len".to_vec()),
        Response::Integer(lcs_len as i64),
    ])
}

// --- HRANDFIELD key [count [WITHVALUES]] ---
fn hrandfield(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(val) => {
            match val.hgetall() {
                Ok(pairs) => {
                    let num_fields = pairs.len();
                    if num_fields == 0 {
                        if parser.argv.len() == 2 { return Response::Nil; }
                        else { return Response::Array(vec![]); }
                    }
                    if parser.argv.len() == 2 {
                        // Single random field
                        use rand::Rng;
                        let idx = rand::thread_rng().gen_range(0..num_fields);
                        Response::Data(pairs[idx].0.clone())
                    } else {
                        let count = try_validate!(parser.get_i64(2), "ERR count is not an integer");
                        let withvalues = parser.argv.len() > 3 && parser.get_str(3).map(|s| s.eq_ignore_ascii_case("withvalues")).unwrap_or(false);
                        let mut result = Vec::new();
                        if count < 0 {
                            // Allow duplicates
                            use rand::Rng;
                            for _ in 0..(-count as usize) {
                                let idx = rand::thread_rng().gen_range(0..num_fields);
                                result.push(Response::Data(pairs[idx].0.clone()));
                                if withvalues {
                                    result.push(Response::Data(pairs[idx].1.clone()));
                                }
                            }
                        } else {
                            // No duplicates, up to count
                            let mut indices: Vec<usize> = (0..num_fields).collect();
                            use rand::seq::SliceRandom;
                            indices.shuffle(&mut rand::thread_rng());
                            let take = (count as usize).min(num_fields);
                            for idx in indices.into_iter().take(take) {
                                result.push(Response::Data(pairs[idx].0.clone()));
                                if withvalues {
                                    result.push(Response::Data(pairs[idx].1.clone()));
                                }
                            }
                        }
                        Response::Array(result)
                    }
                }
                Err(e) => Response::Error(e.to_string()),
            }
        }
        None => {
            if parser.argv.len() == 2 { Response::Nil }
            else { Response::Array(vec![]) }
        }
    }
}

// --- WAITAOF numlocal numreplicas timeout --- (stub)
fn waitaof_command(_parser: &mut ParsedCommand, _db: &Database) -> Response {
    Response::Array(vec![Response::Integer(0), Response::Integer(0)])
}

// --- ZDIFF numkeys key [key ...] ---
fn zdiff(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys);
    let mut withscores = false;
    if parser.argv.len() > 2 + numkeys {
        let opt = try_validate!(parser.get_str(2 + numkeys), "ERR syntax error");
        if opt.eq_ignore_ascii_case("withscores") { withscores = true; }
        else { return Response::Error("ERR syntax error".to_owned()); }
    }
    let first_key = try_validate!(parser.get_vec(2), "Invalid key");
    let first_val = db.get(dbindex, &first_key);
    // Get all members with scores from first set
    let first_members = match first_val {
        Some(val) => match val.zrange(0, -1, true, false) {
            Ok(m) => m, // alternating member, score pairs
            Err(e) => return Response::Error(e.to_string()),
        },
        None => return Response::Array(vec![]),
    };
    let mut result = Vec::new();
    for chunk in first_members.chunks(2) {
        if chunk.len() < 2 { break; }
        let member = &chunk[0];
        let score_str = &chunk[1];
        // Check if member exists in any other set
        let mut found = false;
        for k in 1..numkeys {
            let other_key = try_validate!(parser.get_vec(2 + k), "Invalid key");
            if let Some(other_val) = db.get(dbindex, &other_key) {
                if let Ok(Some(_)) = other_val.zscore(member.clone()) {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            result.push(Response::Data(member.clone()));
            if withscores {
                result.push(Response::Data(score_str.clone()));
            }
        }
    }
    Response::Array(result)
}

// --- ZDIFFSTORE destination numkeys key [key ...] ---
fn zdiffstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let destination = try_validate!(parser.get_vec(1), "Invalid destination");
    let numkeys = try_validate!(parser.get_i64(2), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 3 + numkeys);
    // Compute diff result first
    let first_key = try_validate!(parser.get_vec(3), "Invalid key");
    let first_val = db.get(dbindex, &first_key);
    let first_members = match first_val {
        Some(val) => match val.zrange(0, -1, true, false) {
            Ok(m) => m,
            Err(e) => return Response::Error(e.to_string()),
        },
        None => {
            db.remove(dbindex, &destination);
            return Response::Integer(0);
        }
    };
    let mut result_members: Vec<(Vec<u8>, f64)> = Vec::new();
    for chunk in first_members.chunks(2) {
        if chunk.len() < 2 { break; }
        let member = &chunk[0];
        let score: f64 = String::from_utf8_lossy(&chunk[1]).parse().unwrap_or(0.0);
        let mut found = false;
        for k in 1..numkeys {
            let other_key = try_validate!(parser.get_vec(3 + k), "Invalid key");
            if let Some(other_val) = db.get(dbindex, &other_key) {
                if let Ok(Some(_)) = other_val.zscore(member.clone()) {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            result_members.push((member.clone(), score));
        }
    }
    // Store result
    db.remove(dbindex, &destination);
    let dest_val = db.get_or_create(dbindex, &destination);
    for (member, score) in &result_members {
        if let Err(e) = dest_val.zadd(*score, member.clone(), false, false, false, false) {
            return Response::Error(e.to_string());
        }
    }
    let card = result_members.len();
    db.key_updated(dbindex, &destination);
    Response::Integer(card as i64)
}

// --- ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] ---
fn zinter_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys);
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(2 + i), "Invalid key"));
    }
    let mut weights: Option<Vec<f64>> = None;
    let mut aggregate = zset::Aggregate::Sum;
    let mut withscores = false;
    let mut i = 2 + numkeys;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "weights" => {
                let mut w = Vec::with_capacity(numkeys);
                for j in 0..numkeys {
                    w.push(try_validate!(parser.get_f64(i + 1 + j), "ERR weight is not a float"));
                }
                weights = Some(w);
                i += 1 + numkeys;
            }
            "aggregate" => {
                let agg = try_validate!(parser.get_str(i + 1), "ERR syntax error");
                aggregate = match agg.to_ascii_lowercase().as_str() {
                    "sum" => zset::Aggregate::Sum,
                    "min" => zset::Aggregate::Min,
                    "max" => zset::Aggregate::Max,
                    _ => return Response::Error("ERR syntax error".to_owned()),
                };
                i += 2;
            }
            "withscores" => { withscores = true; i += 1; }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    // Build zset values for intersection
    let vals: Vec<Value> = keys.iter().map(|k| {
        db.get(dbindex, k).cloned().unwrap_or(Value::Nil)
    }).collect();
    let val_refs: Vec<&Value> = vals.iter().collect();
    match Value::Nil.zinter(&val_refs, weights, aggregate) {
        Ok(result) => {
            let members = result.zrange(0, -1, withscores, false).unwrap_or_default();
            let responses: Vec<Response> = members.into_iter().map(Response::Data).collect();
            Response::Array(responses)
        }
        Err(e) => Response::Error(e.to_string()),
    }
}

// --- ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] ---
fn zunion_command(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys);
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(2 + i), "Invalid key"));
    }
    let mut weights: Option<Vec<f64>> = None;
    let mut aggregate = zset::Aggregate::Sum;
    let mut withscores = false;
    let mut i = 2 + numkeys;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "weights" => {
                let mut w = Vec::with_capacity(numkeys);
                for j in 0..numkeys {
                    w.push(try_validate!(parser.get_f64(i + 1 + j), "ERR weight is not a float"));
                }
                weights = Some(w);
                i += 1 + numkeys;
            }
            "aggregate" => {
                let agg = try_validate!(parser.get_str(i + 1), "ERR syntax error");
                aggregate = match agg.to_ascii_lowercase().as_str() {
                    "sum" => zset::Aggregate::Sum,
                    "min" => zset::Aggregate::Min,
                    "max" => zset::Aggregate::Max,
                    _ => return Response::Error("ERR syntax error".to_owned()),
                };
                i += 2;
            }
            "withscores" => { withscores = true; i += 1; }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    let vals: Vec<Value> = keys.iter().map(|k| {
        db.get(dbindex, k).cloned().unwrap_or(Value::Nil)
    }).collect();
    let val_refs: Vec<&Value> = vals.iter().collect();
    match Value::Nil.zunion(&val_refs, weights, aggregate) {
        Ok(result) => {
            let members = result.zrange(0, -1, withscores, false).unwrap_or_default();
            let responses: Vec<Response> = members.into_iter().map(Response::Data).collect();
            Response::Array(responses)
        }
        Err(e) => Response::Error(e.to_string()),
    }
}

// --- ZINTERCARD numkeys key [key ...] [LIMIT limit] ---
fn zintercard(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys);
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(2 + i), "Invalid key"));
    }
    let mut limit: usize = 0;
    let mut i = 2 + numkeys;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "limit" => {
                let l = try_validate!(parser.get_i64(i + 1), "ERR limit is not an integer");
                if l < 0 { return Response::Error("ERR LIMIT can't be negative".to_owned()); }
                limit = l as usize;
                i += 2;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    // Compute intersection cardinality using zinter
    let vals: Vec<Value> = keys.iter().map(|k| {
        db.get(dbindex, k).cloned().unwrap_or(Value::Nil)
    }).collect();
    let val_refs: Vec<&Value> = vals.iter().collect();
    match Value::Nil.zinter(&val_refs, None, zset::Aggregate::Sum) {
        Ok(result) => {
            let card = result.zcard().unwrap_or(0);
            if limit > 0 && card > limit {
                Response::Integer(limit as i64)
            } else {
                Response::Integer(card as i64)
            }
        }
        Err(_) => Response::Integer(0),
    }
}

// --- ZMPOP numkeys key [key ...] MIN|MAX [COUNT count] ---
fn zmpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let numkeys = try_validate!(parser.get_i64(1), "ERR numkeys is not an integer") as usize;
    if numkeys < 1 { return Response::Error("ERR numkeys should be greater than 0".to_owned()); }
    validate_arguments_gte!(parser, 2 + numkeys + 1);
    let mut keys = Vec::with_capacity(numkeys);
    for i in 0..numkeys {
        keys.push(try_validate!(parser.get_vec(2 + i), "Invalid key"));
    }
    let order = try_validate!(parser.get_str(2 + numkeys), "ERR syntax error");
    let is_max = match order.to_ascii_lowercase().as_str() {
        "min" => false,
        "max" => true,
        _ => return Response::Error("ERR syntax error".to_owned()),
    };
    let mut count: usize = 1;
    let mut i = 3 + numkeys;
    while i < parser.argv.len() {
        let opt = try_validate!(parser.get_str(i), "ERR syntax error");
        match opt.to_ascii_lowercase().as_str() {
            "count" => {
                let c = try_validate!(parser.get_i64(i + 1), "ERR count is not an integer");
                if c < 1 { return Response::Error("ERR count should be greater than 0".to_owned()); }
                count = c as usize;
                i += 2;
            }
            _ => return Response::Error("ERR syntax error".to_owned()),
        }
    }
    for key in &keys {
        let mut elements = Vec::new();
        {
            let zset = match db.get_mut(dbindex, key) {
                Some(z) => z,
                None => continue,
            };
            if zset.zcard().is_err() { continue; }
            for _ in 0..count {
                // Pop from min or max position
                let range = if is_max {
                    zset.zrange(0, 0, true, true) // get highest score
                } else {
                    zset.zrange(0, 0, true, false) // get lowest score
                };
                match range {
                    Ok(pair) if pair.len() == 2 => {
                        let member = pair[0].clone();
                        if let Err(_) = zset.zrem(member.clone()) { break; }
                        elements.push((pair[0].clone(), pair[1].clone()));
                    }
                    _ => break,
                }
            }
        }
        if elements.is_empty() { continue; }
        db.key_updated(dbindex, key);
        let element_responses: Vec<Response> = elements.into_iter().map(|(m, s)| {
            Response::Array(vec![Response::Data(m), Response::Data(s)])
        }).collect();
        return Response::Array(vec![
            Response::Data(key.clone()),
            Response::Array(element_responses),
        ]);
    }
    Response::Nil
}

// --- BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count] --- (stub: just tries zmpop)
fn bzmpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    let r = zmpop(parser, db, dbindex);
    Ok(r)
}

// --- BZPOPMIN / BZPOPMAX key [key ...] timeout ---
fn generic_bzpop(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize, is_max: bool) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 3, "Wrong number of parameters");
    let timeout = try_opt_validate!(parser.get_i64(parser.argv.len() - 1), "ERR timeout is not an integer");
    // Try each key
    for i in 1..parser.argv.len() - 1 {
        let key = try_opt_validate!(parser.get_vec(i), "Invalid key");
        let result = {
            let zset = match db.get_mut(dbindex, &key) {
                Some(z) => z,
                None => continue,
            };
            if zset.zcard().is_err() { continue; }
            let range = if is_max {
                zset.zrange(0, 0, true, true)
            } else {
                zset.zrange(0, 0, true, false)
            };
            match range {
                Ok(pair) if pair.len() == 2 => {
                    let member = pair[0].clone();
                    let score = pair[1].clone();
                    let _ = zset.zrem(member.clone());
                    Some((key, member, score))
                }
                _ => None,
            }
        };
        if let Some((key, member, score)) = result {
            db.key_updated(dbindex, &key);
            return Ok(Response::Array(vec![
                Response::Data(key),
                Response::Data(member),
                Response::Data(score),
            ]));
        }
    }
    // For now, return Nil if nothing found (blocking not fully implemented)
    if timeout == 0 {
        // Would block indefinitely, just return Nil for now
        Ok(Response::Nil)
    } else {
        Ok(Response::Nil)
    }
}

fn bzpopmin(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    generic_bzpop(parser, db, dbindex, false)
}

fn bzpopmax(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Result<Response, ResponseError> {
    generic_bzpop(parser, db, dbindex, true)
}

// ==================== Stream commands ====================

use database::stream::{StreamID, ValueStream};

/// Helper: parse a stream ID from parser argument, supporting "*" for auto
fn parse_stream_id(parser: &ParsedCommand, idx: usize) -> Result<Option<StreamID>, Response> {
    let raw = parser.get_vec(idx).map_err(|_| Response::Error("ERR invalid stream ID".to_owned()))?;
    match StreamID::parse(&raw) {
        Ok(id) => Ok(id),
        Err(_) => Err(Response::Error("ERR Invalid stream ID".to_owned())),
    }
}

/// Helper: parse "+" as max ID, "-" as min ID, or a specific ID
fn parse_stream_id_bound(parser: &ParsedCommand, idx: usize, _is_start: bool) -> Result<StreamID, Response> {
    let raw = parser.get_vec(idx).map_err(|_| Response::Error("ERR invalid stream ID".to_owned()))?;
    let s = std::str::from_utf8(&raw).map_err(|_| Response::Error("ERR invalid stream ID".to_owned()))?;
    match s {
        "-" => Ok(StreamID::zero()),
        "+" => Ok(StreamID::new(u64::MAX, u64::MAX)),
        _ => {
            let id = StreamID::parse(&raw).map_err(|_| Response::Error("ERR invalid stream ID".to_owned()))?;
            Ok(id.unwrap_or(StreamID::zero()))
        }
    }
}

/// Helper: format a stream entry as a Response array [id, [field, value, ...]]
fn format_stream_entry(entry: &database::stream::StreamEntry) -> Response {
    let mut fields = Vec::with_capacity(entry.fields.len() * 2);
    for (f, v) in &entry.fields {
        fields.push(Response::Data(f.clone()));
        fields.push(Response::Data(v.clone()));
    }
    Response::Array(vec![
        Response::Data(entry.id.to_bytes()),
        Response::Array(fields),
    ])
}

// --- XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...] ---
fn xadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut idx = 2;
    let mut nomkstream = false;
    let mut maxlen: Option<usize> = None;
    let mut minid: Option<StreamID> = None;

    // Parse optional NOMKSTREAM, MAXLEN, MINID
    while idx < parser.argv.len() {
        let opt = match parser.get_str(idx) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => break,
        };
        match opt.as_str() {
            "nomkstream" => { nomkstream = true; idx += 1; }
            "maxlen" => {
                idx += 1;
                // Skip optional = or ~
                if idx < parser.argv.len() {
                    if let Ok(s) = parser.get_str(idx) {
                        if s == "=" || s == "~" { idx += 1; }
                    }
                }
                if let Ok(n) = parser.get_i64(idx) {
                    maxlen = Some(n as usize);
                    idx += 1;
                }
                // Skip optional LIMIT
                if idx < parser.argv.len() {
                    if let Ok(s) = parser.get_str(idx) {
                        if s.to_ascii_lowercase() == "limit" { idx += 2; }
                    }
                }
            }
            "minid" => {
                idx += 1;
                if idx < parser.argv.len() {
                    if let Ok(s) = parser.get_str(idx) {
                        if s == "=" || s == "~" { idx += 1; }
                    }
                }
                if let Ok(raw) = parser.get_vec(idx) {
                    if let Ok(Some(id)) = StreamID::parse(&raw) {
                        minid = Some(id);
                    }
                }
                idx += 1;
                if idx < parser.argv.len() {
                    if let Ok(s) = parser.get_str(idx) {
                        if s.to_ascii_lowercase() == "limit" { idx += 2; }
                    }
                }
            }
            _ => break,
        }
    }

    // Parse the ID (* or explicit)
    let given_id = if idx < parser.argv.len() {
        match parse_stream_id(parser, idx) {
            Ok(id) => { idx += 1; id }
            Err(e) => return e,
        }
    } else {
        return Response::Error("ERR wrong number of arguments for 'xadd' command".to_owned());
    };

    // Parse field-value pairs
    if (parser.argv.len() - idx) % 2 != 0 {
        return Response::Error("ERR wrong number of arguments for 'xadd' command".to_owned());
    }
    let mut fields = Vec::with_capacity((parser.argv.len() - idx) / 2);
    while idx < parser.argv.len() {
        let field = try_validate!(parser.get_vec(idx), "Invalid field");
        let value = try_validate!(parser.get_vec(idx + 1), "Invalid value");
        fields.push((field, value));
        idx += 2;
    }

    // Get or create stream
    let exists = db.get(dbindex, &key).is_some();
    if nomkstream && !exists {
        return Response::Nil;
    }

    let stream = match db.get(dbindex, &key) {
        Some(Value::Stream(_)) => db.get_mut(dbindex, &key).unwrap(),
        Some(_) => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => {
            db.get_or_create(dbindex, &key);
            let val = Value::Stream(ValueStream::new());
            *db.get_or_create(dbindex, &key) = val;
            db.get_mut(dbindex, &key).unwrap()
        }
    };

    if let Value::Stream(ref mut s) = stream {
        match s.xadd(given_id, fields, util::mstime() as u64) {
            Ok(id) => {
                // Apply trimming
                if let Some(ml) = maxlen {
                    s.xtrim(Some(ml), None, false, None);
                }
                if let Some(mid) = minid {
                    s.xtrim(None, Some(mid), false, None);
                }
                db.key_updated(dbindex, &key);
                Response::Data(id.to_bytes())
            }
            Err(e) => Response::Error(e.to_string()),
        }
    } else {
        Response::Error("ERR not a stream".to_owned())
    }
}

// --- XLEN key ---
fn xlen(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_exact!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    match db.get(dbindex, &key) {
        Some(Value::Stream(s)) => Response::Integer(s.xlen() as i64),
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Integer(0),
    }
}

// --- XRANGE key start end [COUNT count] ---
fn xrange(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let start = match parse_stream_id_bound(parser, 2, true) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let end = match parse_stream_id_bound(parser, 3, false) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let mut count: Option<usize> = None;
    if parser.argv.len() > 4 {
        if let Ok(s) = parser.get_str(4) {
            if s.to_ascii_lowercase() == "count" && parser.argv.len() > 5 {
                if let Ok(c) = parser.get_i64(5) {
                    count = Some(c as usize);
                }
            }
        }
    }
    match db.get(dbindex, &key) {
        Some(Value::Stream(s)) => {
            let entries = s.xrange(start, end, count);
            Response::Array(entries.into_iter().map(|e| format_stream_entry(e)).collect())
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(vec![]),
    }
}

// --- XREVRANGE key end start [COUNT count] ---
fn xrevrange(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let end = match parse_stream_id_bound(parser, 2, false) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let start = match parse_stream_id_bound(parser, 3, true) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let mut count: Option<usize> = None;
    if parser.argv.len() > 4 {
        if let Ok(s) = parser.get_str(4) {
            if s.to_ascii_lowercase() == "count" && parser.argv.len() > 5 {
                if let Ok(c) = parser.get_i64(5) {
                    count = Some(c as usize);
                }
            }
        }
    }
    match db.get(dbindex, &key) {
        Some(Value::Stream(s)) => {
            let entries = s.xrevrange(end, start, count);
            Response::Array(entries.into_iter().map(|e| format_stream_entry(e)).collect())
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(vec![]),
    }
}

// --- XDEL key id [id ...] ---
fn xdel(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut ids = Vec::new();
    for i in 2..parser.argv.len() {
        match parse_stream_id(parser, i) {
            Ok(Some(id)) => ids.push(id),
            Ok(None) => return Response::Error("ERR Invalid stream ID".to_owned()),
            Err(e) => return e,
        }
    }
    match db.get_mut(dbindex, &key) {
        Some(Value::Stream(s)) => {
            let deleted = s.xdel(&ids);
            db.key_updated(dbindex, &key);
            Response::Integer(deleted as i64)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Integer(0),
    }
}

// --- XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count] ---
fn xtrim(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let strategy = try_validate!(parser.get_str(2), "ERR syntax error").to_ascii_lowercase();
    let mut idx = 3;
    // Skip optional = or ~
    if idx < parser.argv.len() {
        if let Ok(s) = parser.get_str(idx) {
            if s == "=" || s == "~" { idx += 1; }
        }
    }
    let mut maxlen = None;
    let mut minid = None;
    match strategy.as_str() {
        "maxlen" => {
            if let Ok(n) = parser.get_i64(idx) {
                maxlen = Some(n as usize);
            } else {
                return Response::Error("ERR MAXLEN is not an integer".to_owned());
            }
        }
        "minid" => {
            if let Ok(raw) = parser.get_vec(idx) {
                if let Ok(Some(id)) = StreamID::parse(&raw) {
                    minid = Some(id);
                }
            }
        }
        _ => return Response::Error("ERR syntax error".to_owned()),
    }
    match db.get_mut(dbindex, &key) {
        Some(Value::Stream(s)) => {
            let deleted = s.xtrim(maxlen, minid, false, None);
            db.key_updated(dbindex, &key);
            Response::Integer(deleted as i64)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Integer(0),
    }
}

// --- XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...] ---
fn xread(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let mut count: Option<usize> = None;
    let mut _block: Option<i64> = None;
    let mut idx = 1;

    // Parse options
    while idx < parser.argv.len() {
        let opt = match parser.get_str(idx) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => break,
        };
        match opt.as_str() {
            "count" => {
                idx += 1;
                if let Ok(c) = parser.get_i64(idx) { count = Some(c as usize); }
                idx += 1;
            }
            "block" => {
                idx += 1;
                if let Ok(b) = parser.get_i64(idx) { _block = Some(b); }
                idx += 1;
            }
            "streams" => { idx += 1; break; }
            _ => break,
        }
    }

    // After STREAMS: keys then IDs
    let remaining = parser.argv.len() - idx;
    if remaining < 2 || remaining % 2 != 0 {
        return Response::Error("ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified".to_owned());
    }
    let num_streams = remaining / 2;
    let mut keys = Vec::with_capacity(num_streams);
    let mut start_ids = Vec::with_capacity(num_streams);
    for i in 0..num_streams {
        keys.push(try_validate!(parser.get_vec(idx + i), "Invalid key"));
    }
    for i in 0..num_streams {
        let id_str = try_validate!(parser.get_str(idx + num_streams + i), "ERR invalid stream ID");
        if id_str == "$" {
            // "$" means only new messages; use the stream's last_id
            start_ids.push(None); // Will resolve to last_id
        } else {
            match StreamID::parse(id_str.as_bytes()) {
                Ok(Some(id)) => start_ids.push(Some(id)),
                _ => return Response::Error("ERR Invalid stream ID".to_owned()),
            }
        }
    }

    let mut result = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        match db.get(dbindex, key) {
            Some(Value::Stream(s)) => {
                let start = match start_ids[i] {
                    Some(id) => id,
                    None => s.last_id(), // "$" means start after current last
                };
                let entries = s.xread(start, count);
                if !entries.is_empty() {
                    let entry_responses: Vec<Response> = entries.into_iter().map(|e| format_stream_entry(e)).collect();
                    result.push(Response::Array(vec![
                        Response::Data(key.clone()),
                        Response::Array(entry_responses),
                    ]));
                }
            }
            Some(_) => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            None => {}
        }
    }

    if result.is_empty() {
        Response::Nil
    } else {
        Response::Array(result)
    }
}

// --- XGROUP CREATE|CREATECONSUMER|SETID|DESTROY|DELCONSUMER ---
fn xgroup(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let subcmd = try_validate!(parser.get_str(2), "ERR syntax error").to_ascii_lowercase();
    match subcmd.as_str() {
        "create" => {
            validate_arguments_gte!(parser, 5);
            let key = try_validate!(parser.get_vec(3), "Invalid key");
            let group_name = try_validate!(parser.get_vec(4), "Invalid group name");
            let id_str = try_validate!(parser.get_str(5), "ERR syntax error");
            let id = if id_str == "$" {
                None // Will use stream's last_id
            } else {
                match StreamID::parse(id_str.as_bytes()) {
                    Ok(Some(id)) => Some(id),
                    _ => return Response::Error("ERR Invalid stream ID".to_owned()),
                }
            };
            // Create stream if it doesn't exist (MKSTREAM option)
            let mkstream = parser.argv.len() > 6 && {
                if let Ok(s) = parser.get_str(6) { s.to_ascii_lowercase() == "mkstream" } else { false }
            };
            if db.get(dbindex, &key).is_none() {
                if mkstream {
                    *db.get_or_create(dbindex, &key) = Value::Stream(ValueStream::new());
                } else {
                    return Response::Error("ERR The XGROUP subcommand requires the key to exist. Note that CREATE may be called with MKSTREAM to create the stream.".to_owned());
                }
            }
            match db.get_mut(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    match s.xgroup_create(group_name, id) {
                        Ok(()) => {
                            db.key_updated(dbindex, &key);
                            Response::Status("OK".to_owned())
                        }
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "destroy" => {
            validate_arguments_exact!(parser, 4);
            // XGROUP DESTROY key groupname => argv = [xgroup, destroy, key, groupname]
            let key = try_validate!(parser.get_vec(2), "Invalid key");
            let group_name = try_validate!(parser.get_vec(3), "Invalid group name");
            match db.get_mut(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    let destroyed = s.xgroup_destroy(&group_name);
                    db.key_updated(dbindex, &key);
                    Response::Integer(if destroyed { 1 } else { 0 })
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Integer(0),
            }
        }
        "setid" => {
            validate_arguments_gte!(parser, 5);
            let key = try_validate!(parser.get_vec(2), "Invalid key");
            let group_name = try_validate!(parser.get_vec(3), "Invalid group name");
            let id_str = try_validate!(parser.get_str(4), "ERR syntax error");
            let id = if id_str == "$" {
                match db.get(dbindex, &key) {
                    Some(Value::Stream(s)) => s.last_id(),
                    _ => StreamID::zero(),
                }
            } else {
                match StreamID::parse(id_str.as_bytes()) {
                    Ok(Some(id)) => id,
                    _ => return Response::Error("ERR Invalid stream ID".to_owned()),
                }
            };
            match db.get_mut(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    match s.xgroup_setid(&group_name, id) {
                        Ok(()) => Response::Status("OK".to_owned()),
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "createconsumer" => {
            validate_arguments_gte!(parser, 5);
            let key = try_validate!(parser.get_vec(2), "Invalid key");
            let group_name = try_validate!(parser.get_vec(3), "Invalid group name");
            let consumer_name = try_validate!(parser.get_vec(4), "Invalid consumer name");
            match db.get_mut(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    match s.xgroup_createconsumer(&group_name, consumer_name, util::mstime()) {
                        Ok(created) => Response::Integer(if created { 1 } else { 0 }),
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "delconsumer" => {
            validate_arguments_gte!(parser, 5);
            let key = try_validate!(parser.get_vec(2), "Invalid key");
            let group_name = try_validate!(parser.get_vec(3), "Invalid group name");
            let consumer_name = try_validate!(parser.get_vec(4), "Invalid consumer name");
            match db.get_mut(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    match s.xgroup_delconsumer(&group_name, &consumer_name) {
                        Ok(count) => Response::Integer(count as i64),
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        _ => Response::Error("ERR Unknown XGROUP subcommand".to_owned()),
    }
}

// --- XACK key group id [id ...] ---
fn xack(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let group_name = try_validate!(parser.get_vec(2), "Invalid group name");
    let mut ids = Vec::new();
    for i in 3..parser.argv.len() {
        match parse_stream_id(parser, i) {
            Ok(Some(id)) => ids.push(id),
            Ok(None) => return Response::Error("ERR Invalid stream ID".to_owned()),
            Err(e) => return e,
        }
    }
    match db.get_mut(dbindex, &key) {
        Some(Value::Stream(s)) => {
            let count = s.xack(&group_name, &ids);
            db.key_updated(dbindex, &key);
            Response::Integer(count as i64)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Integer(0),
    }
}

// --- XPENDING key [group] [[IDLE min-idle-time] start end count [consumer]] ---
fn xpending(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let group_name = try_validate!(parser.get_vec(2), "Invalid group name");

    if parser.argv.len() == 3 {
        // Summary form: XPENDING key group
        match db.get(dbindex, &key) {
            Some(Value::Stream(s)) => {
                match s.xpending_summary(&group_name) {
                    Ok((total, min_id, max_id, consumers)) => {
                        let mut result = vec![
                            Response::Integer(total as i64),
                            match min_id {
                                Some(id) => Response::Data(id.to_bytes()),
                                None => Response::Nil,
                            },
                            match max_id {
                                Some(id) => Response::Data(id.to_bytes()),
                                None => Response::Nil,
                            },
                        ];
                        let consumer_arr: Vec<Response> = consumers.into_iter().map(|(name, count)| {
                            Response::Array(vec![Response::Data(name), Response::Integer(count as i64)])
                        }).collect();
                        result.push(Response::Array(consumer_arr));
                        Response::Array(result)
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            None => Response::Error("ERR no such key".to_owned()),
        }
    } else {
        // Extended form: XPENDING key group [IDLE min-idle-time] start end count [consumer]
        let mut idx = 3;
        let mut _min_idle: Option<i64> = None;
        // Check for IDLE
        if idx < parser.argv.len() {
            if let Ok(s) = parser.get_str(idx) {
                if s.to_ascii_lowercase() == "idle" {
                    idx += 1;
                    if let Ok(idle) = parser.get_i64(idx) { _min_idle = Some(idle); }
                    idx += 1;
                }
            }
        }
        let start = match parse_stream_id_bound(parser, idx, true) {
            Ok(id) => id,
            Err(e) => return e,
        };
        idx += 1;
        let end = match parse_stream_id_bound(parser, idx, false) {
            Ok(id) => id,
            Err(e) => return e,
        };
        idx += 1;
        let count = match parser.get_i64(idx) {
            Ok(c) => Some(c as usize),
            Err(_) => return Response::Error("ERR count is not an integer".to_owned()),
        };
        idx += 1;
        let consumer_filter = if idx < parser.argv.len() {
            parser.get_vec(idx).ok()
        } else {
            None
        };

        match db.get(dbindex, &key) {
            Some(Value::Stream(s)) => {
                match s.xpending(&group_name, Some(start), Some(end), count, consumer_filter.as_deref()) {
                    Ok(entries) => {
                        let result: Vec<Response> = entries.into_iter().map(|p| {
                            Response::Array(vec![
                                Response::Data(p.id.to_bytes()),
                                Response::Data(p.consumer.clone()),
                                Response::Integer(util::mstime() - p.last_delivery_time),
                                Response::Integer(p.delivery_count as i64),
                            ])
                        }).collect();
                        Response::Array(result)
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            None => Response::Error("ERR no such key".to_owned()),
        }
    }
}

// --- XINFO STREAM|GROUPS|CONSUMERS key [group] ---
fn xinfo(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let subcmd = try_validate!(parser.get_str(2), "ERR syntax error").to_ascii_lowercase();
    match subcmd.as_str() {
        "stream" => {
            validate_arguments_gte!(parser, 4);
            let key = try_validate!(parser.get_vec(3), "Invalid key");
            match db.get(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    let info = s.xinfo();
                    let mut result = vec![
                        Response::Data(b"length".to_vec()), Response::Integer(info.length as i64),
                        Response::Data(b"last-generated-id".to_vec()), Response::Data(info.last_id.to_bytes()),
                        Response::Data(b"entries-added".to_vec()), Response::Integer(info.entries_added as i64),
                        Response::Data(b"groups".to_vec()), Response::Integer(info.groups as i64),
                    ];
                    match info.first_id {
                        Some(id) => { result.push(Response::Data(b"first-entry".to_vec())); result.push(Response::Data(id.to_bytes())); }
                        None => { result.push(Response::Data(b"first-entry".to_vec())); result.push(Response::Nil); }
                    }
                    Response::Array(result)
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "groups" => {
            validate_arguments_gte!(parser, 4);
            let key = try_validate!(parser.get_vec(3), "Invalid key");
            match db.get(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    let groups = s.xinfo_groups();
                    let result: Vec<Response> = groups.into_iter().map(|g| {
                        Response::Array(vec![
                            Response::Data(b"name".to_vec()), Response::Data(g.name),
                            Response::Data(b"consumers".to_vec()), Response::Integer(g.consumers as i64),
                            Response::Data(b"pending".to_vec()), Response::Integer(g.pending as i64),
                            Response::Data(b"last-delivered-id".to_vec()), Response::Data(g.last_id.to_bytes()),
                            Response::Data(b"entries-added".to_vec()), Response::Integer(g.entries_added as i64),
                        ])
                    }).collect();
                    Response::Array(result)
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "consumers" => {
            validate_arguments_gte!(parser, 5);
            let key = try_validate!(parser.get_vec(3), "Invalid key");
            let group_name = try_validate!(parser.get_vec(4), "Invalid group name");
            match db.get(dbindex, &key) {
                Some(Value::Stream(s)) => {
                    match s.xinfo_consumers(&group_name) {
                        Ok(consumers) => {
                            let result: Vec<Response> = consumers.into_iter().map(|c| {
                                Response::Array(vec![
                                    Response::Data(b"name".to_vec()), Response::Data(c.name.clone()),
                                    Response::Data(b"pending".to_vec()), Response::Integer(0),
                                    Response::Data(b"idle".to_vec()), Response::Integer(util::mstime() - c.last_seen),
                                ])
                            }).collect();
                            Response::Array(result)
                        }
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        _ => Response::Error("ERR Unknown XINFO subcommand".to_owned()),
    }
}

// --- XSETID key last-id [ENTRIESADDED entries-added] ---
fn xsetid(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let id = match parse_stream_id(parser, 2) {
        Ok(Some(id)) => id,
        Ok(None) => return Response::Error("ERR Invalid stream ID".to_owned()),
        Err(e) => return e,
    };
    let mut entries_added: Option<u64> = None;
    let mut idx = 3;
    while idx < parser.argv.len() {
        if let Ok(s) = parser.get_str(idx) {
            if s.to_ascii_lowercase() == "entriesadded" {
                idx += 1;
                if let Ok(n) = parser.get_i64(idx) { entries_added = Some(n as u64); }
                idx += 1;
            } else { idx += 1; }
        } else { break; }
    }
    match db.get_mut(dbindex, &key) {
        Some(Value::Stream(s)) => {
            s.xsetid(id, entries_added);
            db.key_updated(dbindex, &key);
            Response::Status("OK".to_owned())
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Error("ERR no such key".to_owned()),
    }
}

// ==================== Geo commands ====================

use database::geo;

// --- GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...] ---
fn geoadd(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 5);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let mut idx = 2;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    // Parse options
    while idx < parser.argv.len() {
        match parser.get_str(idx).unwrap_or("").to_ascii_lowercase().as_str() {
            "nx" => { nx = true; idx += 1; }
            "xx" => { xx = true; idx += 1; }
            "ch" => { ch = true; idx += 1; }
            _ => break,
        }
    }

    // Parse lon lat member triplets
    if (parser.argv.len() - idx) % 3 != 0 {
        return Response::Error("ERR wrong number of arguments for 'geoadd' command".to_owned());
    }

    let mut added = 0;
    let mut changed = 0;

    while idx + 2 < parser.argv.len() {
        let lon = match parser.get_f64(idx) {
            Ok(v) => v,
            Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
        };
        let lat = match parser.get_f64(idx + 1) {
            Ok(v) => v,
            Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
        };
        let member = match parser.get_vec(idx + 2) {
            Ok(v) => v,
            Err(_) => return Response::Error("ERR invalid member".to_owned()),
        };
        idx += 3;

        if !geo::validate_longitude(lon) {
            return Response::Error("ERR invalid longitude".to_owned());
        }
        if !geo::validate_latitude(lat) {
            return Response::Error("ERR invalid latitude".to_owned());
        }

        let score = geo::geohash_to_score(lon, lat);

        // Check NX/XX
        let exists = match db.get(dbindex, &key) {
            Some(Value::SortedSet(s)) => s.zscore(&member).is_some(),
            Some(_) => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
            None => false,
        };

        if nx && exists { continue; }
        if xx && !exists { continue; }

        // Use zadd with the geohash score
        let was_new = match db.get_or_create(dbindex, &key).zadd(score, member, false, false, false, false) {
            Ok(is_new) => is_new,
            Err(e) => return Response::Error(e.to_string()),
        };

        if was_new {
            added += 1;
            changed += 1;
        } else {
            changed += 1;
        }
    }

    db.key_updated(dbindex, &key);
    if ch {
        Response::Integer(changed as i64)
    } else {
        Response::Integer(added as i64)
    }
}

// --- GEODIST key member1 member2 [M|KM|FT|MI] ---
fn geodist(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let member1 = try_validate!(parser.get_vec(2), "Invalid member");
    let member2 = try_validate!(parser.get_vec(3), "Invalid member");
    let unit = if parser.argv.len() > 4 {
        try_validate!(parser.get_str(4), "ERR syntax error").to_ascii_lowercase()
    } else {
        "m".to_owned()
    };

    let (lon1, lat1, lon2, lat2) = match db.get(dbindex, &key) {
        Some(Value::SortedSet(s)) => {
            let score1 = match s.zscore(&member1) {
                Some(sc) => sc,
                None => return Response::Nil,
            };
            let score2 = match s.zscore(&member2) {
                Some(sc) => sc,
                None => return Response::Nil,
            };
            let (lon1, lat1) = geo::score_to_geohash(score1);
            let (lon2, lat2) = geo::score_to_geohash(score2);
            (lon1, lat1, lon2, lat2)
        }
        Some(_) => return Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => return Response::Nil,
    };

    let dist_m = geo::haversine_distance(lon1, lat1, lon2, lat2);
    let dist = geo::convert_distance(dist_m, &unit);
    Response::Data(format!("{:.4}", dist).into_bytes())
}

// --- GEOHASH key member [member ...] ---
fn geohash_cmd(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    let mut members: Vec<Vec<u8>> = Vec::new();
    for i in 2..parser.argv.len() {
        members.push(try_validate!(parser.get_vec(i), "Invalid member"));
    }

    match db.get(dbindex, &key) {
        Some(Value::SortedSet(s)) => {
            let result: Vec<Response> = members.iter().map(|m| {
                match s.zscore(m) {
                    Some(score) => {
                        let hash = score as u64;
                        Response::Data(geo::geohash_to_string(hash).into_bytes())
                    }
                    None => Response::Nil,
                }
            }).collect();
            Response::Array(result)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(members.iter().map(|_| Response::Nil).collect()),
    }
}

// --- GEOPOS key member [member ...] ---
fn geopos(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 3);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    let mut members: Vec<Vec<u8>> = Vec::new();
    for i in 2..parser.argv.len() {
        members.push(try_validate!(parser.get_vec(i), "Invalid member"));
    }

    match db.get(dbindex, &key) {
        Some(Value::SortedSet(s)) => {
            let result: Vec<Response> = members.iter().map(|m| {
                match s.zscore(m) {
                    Some(score) => {
                        let (lon, lat) = geo::score_to_geohash(score);
                        Response::Array(vec![
                            Response::Data(format!("{:.6}", lon).into_bytes()),
                            Response::Data(format!("{:.6}", lat).into_bytes()),
                        ])
                    }
                    None => Response::Nil,
                }
            }).collect();
            Response::Array(result)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(members.iter().map(|_| Response::Nil).collect()),
    }
}

// --- GEOSEARCH key [FROMMEMBER member|FROMLONLAT lon lat] [BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI] [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH] ---
fn geosearch(parser: &mut ParsedCommand, db: &Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let key = try_validate!(parser.get_vec(1), "Invalid key");

    let mut center_lon: f64 = 0.0;
    let mut center_lat: f64 = 0.0;
    let mut has_center = false;
    let mut radius_m: Option<f64> = None;
    let mut box_width_m: Option<f64> = None;
    let mut box_height_m: Option<f64> = None;
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    let mut _any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut _withhash = false;

    let mut idx = 2;
    while idx < parser.argv.len() {
        let opt = match parser.get_str(idx) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => { idx += 1; continue; }
        };
        match opt.as_str() {
            "frommember" => {
                idx += 1;
                let member = try_validate!(parser.get_vec(idx), "Invalid member");
                match db.get(dbindex, &key) {
                    Some(Value::SortedSet(s)) => {
                        if let Some(score) = s.zscore(&member) {
                            let (lon, lat) = geo::score_to_geohash(score);
                            center_lon = lon;
                            center_lat = lat;
                            has_center = true;
                        } else {
                            return Response::Error("ERR could not decode requested member from sorted set".to_owned());
                        }
                    }
                    _ => return Response::Error("ERR could not decode requested member from sorted set".to_owned()),
                }
                idx += 1;
            }
            "fromlonlat" => {
                idx += 1;
                center_lon = match parser.get_f64(idx) {
                    Ok(v) => v,
                    Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
                };
                idx += 1;
                center_lat = match parser.get_f64(idx) {
                    Ok(v) => v,
                    Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
                };
                has_center = true;
                idx += 1;
            }
            "byradius" => {
                idx += 1;
                let r = match parser.get_f64(idx) {
                    Ok(v) => v,
                    Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
                };
                idx += 1;
                let unit = match parser.get_str(idx) {
                    Ok(s) => s.to_ascii_lowercase(),
                    Err(_) => return Response::Error("ERR syntax error".to_owned()),
                };
                radius_m = Some(r * geo::unit_to_meters(&unit));
                idx += 1;
            }
            "bybox" => {
                idx += 1;
                let w = match parser.get_f64(idx) {
                    Ok(v) => v,
                    Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
                };
                idx += 1;
                let h = match parser.get_f64(idx) {
                    Ok(v) => v,
                    Err(_) => return Response::Error("ERR value is not a valid float".to_owned()),
                };
                idx += 1;
                let unit = match parser.get_str(idx) {
                    Ok(s) => s.to_ascii_lowercase(),
                    Err(_) => return Response::Error("ERR syntax error".to_owned()),
                };
                let factor = geo::unit_to_meters(&unit);
                box_width_m = Some(w * factor);
                box_height_m = Some(h * factor);
                idx += 1;
            }
            "asc" => { asc = true; idx += 1; }
            "desc" => { desc = true; idx += 1; }
            "count" => {
                idx += 1;
                if let Ok(c) = parser.get_i64(idx) {
                    count = Some(c as usize);
                }
                idx += 1;
                if idx < parser.argv.len() {
                    if let Ok(s) = parser.get_str(idx) {
                        if s.to_ascii_lowercase() == "any" { _any = true; idx += 1; }
                    }
                }
            }
            "withcoord" => { withcoord = true; idx += 1; }
            "withdist" => { withdist = true; idx += 1; }
            "withhash" => { _withhash = true; idx += 1; }
            _ => { idx += 1; }
        }
    }

    if !has_center {
        return Response::Error("ERR must specify a center either with FROMMEMBER or FROMLONLAT".to_owned());
    }
    if radius_m.is_none() && box_width_m.is_none() {
        return Response::Error("ERR must specify a search area with either BYRADIUS or BYBOX".to_owned());
    }

    // Get all members and filter by distance
    match db.get(dbindex, &key) {
        Some(Value::SortedSet(s)) => {
            let card = s.zcard();
            let all = s.zrange(0, (card as i64) - 1, true, false);
            // all is [member, score, member, score, ...]
            let mut results: Vec<(Vec<u8>, f64, f64, f64)> = Vec::new(); // (member, lon, lat, dist_m)

            for pair in all.chunks(2) {
                if pair.len() < 2 { break; }
                let member = &pair[0];
                let score: f64 = std::str::from_utf8(&pair[1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let (lon, lat) = geo::score_to_geohash(score);
                let dist = geo::haversine_distance(center_lon, center_lat, lon, lat);

                let in_range = if let Some(r) = radius_m {
                    dist <= r
                } else if let (Some(w), Some(h)) = (box_width_m, box_height_m) {
                    let dlon = geo::haversine_distance(center_lon, center_lat, lon, center_lat);
                    let dlat = geo::haversine_distance(center_lon, center_lat, center_lon, lat);
                    dlon <= w / 2.0 && dlat <= h / 2.0
                } else {
                    false
                };

                if in_range {
                    results.push((member.clone(), lon, lat, dist));
                }
            }

            // Sort
            if desc {
                results.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
            } else if asc {
                results.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal));
            }

            // Apply count
            if let Some(c) = count {
                results.truncate(c);
            }

            // Format results
            let response: Vec<Response> = results.into_iter().map(|(member, lon, lat, dist)| {
                if !withcoord && !withdist {
                    Response::Data(member)
                } else {
                    let mut parts = vec![Response::Data(member)];
                    if withdist {
                        parts.push(Response::Data(format!("{:.4}", dist).into_bytes()));
                    }
                    if withcoord {
                        parts.push(Response::Array(vec![
                            Response::Data(format!("{:.6}", lon).into_bytes()),
                            Response::Data(format!("{:.6}", lat).into_bytes()),
                        ]));
                    }
                    Response::Array(parts)
                }
            }).collect();

            Response::Array(response)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(vec![]),
    }
}

// --- GEOSEARCHSTORE destination source [FROMMEMBER|FROMLONLAT ...] [BYRADIUS|BYBOX ...] [ASC|DESC] [COUNT ...] [STOREDIST] ---
fn geosearchstore(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    // For now, delegate to geosearch logic and store results
    validate_arguments_gte!(parser, 3);
    let destination = try_validate!(parser.get_vec(1), "Invalid key");
    let source = try_validate!(parser.get_vec(2), "Invalid key");

    // Check for STOREDIST option
    let _storedist = parser.argv.iter().any(|a| {
        parser.get_str(a.pos).map(|s| s.to_ascii_lowercase() == "storedist").unwrap_or(false)
    });

    // Simplified: just return the count of stored elements
    // A full implementation would re-run geosearch on the source and store to destination
    let result = geosearch(parser, db, dbindex);

    // Count results
    let count = match &result {
        Response::Array(arr) => arr.len(),
        _ => 0,
    };

    if count > 0 {
        // Store results as a sorted set at destination
        // For simplicity, copy the source key to destination
        match db.get(dbindex, &source) {
            Some(val) => {
                *db.get_or_create(dbindex, &destination) = val.clone();
                db.key_updated(dbindex, &destination);
            }
            None => {}
        }
    }

    Response::Integer(count as i64)
}

// --- Phase 1.4: Hash per-field expiration commands ---

/// Parse FIELDS numfields field [field ...] from the argument list starting at `start_idx`.
/// Returns (fields, next_idx) or an error response.
fn parse_hash_fields(parser: &mut ParsedCommand, start_idx: usize) -> Result<(Vec<Vec<u8>>, usize), Response> {
    if start_idx >= parser.argv.len() {
        return Err(Response::Error("ERR syntax error".to_owned()));
    }
    let keyword = parser.get_str(start_idx).map_err(|_| Response::Error("ERR syntax error".to_owned()))?;
    if keyword.to_ascii_lowercase() != "fields" {
        return Err(Response::Error("ERR syntax error".to_owned()));
    }
    if start_idx + 1 >= parser.argv.len() {
        return Err(Response::Error("ERR syntax error".to_owned()));
    }
    let numfields = parser.get_i64(start_idx + 1).map_err(|_| Response::Error("ERR syntax error".to_owned()))? as usize;
    if numfields == 0 {
        return Err(Response::Error("ERR syntax error".to_owned()));
    }
    let fields_start = start_idx + 2;
    if fields_start + numfields > parser.argv.len() {
        return Err(Response::Error("ERR syntax error".to_owned()));
    }
    let mut fields = Vec::with_capacity(numfields);
    for i in 0..numfields {
        fields.push(parser.get_vec(fields_start + i).map_err(|_| Response::Error("ERR syntax error".to_owned()))?);
    }
    Ok((fields, fields_start + numfields))
}

fn generic_hexpire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize, is_ms: bool, is_at: bool) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let time_val = try_validate!(parser.get_i64(2), "ERR syntax error");

    // Parse optional NX/XX/GT/LT
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut fields_start = 3;

    // Scan for NX/XX/GT/LT before FIELDS
    let mut i = 3;
    while i < parser.argv.len() {
        if let Ok(s) = parser.get_str(i) {
            match s.to_ascii_lowercase().as_str() {
                "nx" => { nx = true; i += 1; continue; }
                "xx" => { xx = true; i += 1; continue; }
                "gt" => { gt = true; i += 1; continue; }
                "lt" => { lt = true; i += 1; continue; }
                "fields" => { break; }
                _ => {}
            }
        }
        i += 1;
    }
    fields_start = i;

    let (fields, _) = match parse_hash_fields(parser, fields_start) {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Calculate absolute expiration time in ms
    let now = mstime();
    let msexpiration = if is_at {
        if is_ms { time_val } else { time_val * 1000 }
    } else {
        if is_ms { now + time_val } else { now + time_val * 1000 }
    };

    match db.get_mut(dbindex, &key) {
        Some(Value::Hash(h)) => {
            let results = h.hexpire_fields(&fields, msexpiration, now, nx, xx, gt, lt);
            Response::Array(results.into_iter().map(Response::Integer).collect())
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(fields.iter().map(|_| Response::Integer(0)).collect()),
    }
}

fn hexpire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_hexpire(parser, db, dbindex, false, false)
}

fn hpexpire(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_hexpire(parser, db, dbindex, true, false)
}

fn hexpireat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_hexpire(parser, db, dbindex, false, true)
}

fn hpexpireat(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    generic_hexpire(parser, db, dbindex, true, true)
}

fn httl(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let (fields, _) = match parse_hash_fields(parser, 2) {
        Ok(r) => r,
        Err(e) => return e,
    };
    let now = mstime();
    match db.get(dbindex, &key) {
        Some(Value::Hash(h)) => {
            let results = h.httl_fields(&fields, now);
            Response::Array(results.into_iter().map(Response::Integer).collect())
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(fields.iter().map(|_| Response::Integer(-2)).collect()),
    }
}

fn hpttl(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    // Same as HTTL but returns milliseconds (HTTL already returns ms)
    httl(parser, db, dbindex)
}

fn hpersist(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 4);
    let key = try_validate!(parser.get_vec(1), "Invalid key");
    let (fields, _) = match parse_hash_fields(parser, 2) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match db.get_mut(dbindex, &key) {
        Some(Value::Hash(h)) => {
            let results = h.hpersist_fields(&fields);
            Response::Array(results.into_iter().map(Response::Integer).collect())
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        None => Response::Array(fields.iter().map(|_| Response::Integer(0)).collect()),
    }
}

// --- Phase 6: Sharded Pub/Sub commands ---

fn ssubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    client: &mut Client,
) -> Result<Response, ResponseError> {
    opt_validate!(parser.argv.len() >= 2, "Wrong number of parameters");
    for i in 1..parser.argv.len() {
        let channel_name = try_opt_validate!(parser.get_vec(i), "Invalid channel");
        if !client.sharded_subscriptions.contains_key(&channel_name) {
            let subscriber_id = db.ssubscribe(channel_name.clone(), client.rawsender.clone());
            client.sharded_subscriptions.insert(channel_name.clone(), subscriber_id);
        }
        match client.rawsender.send(Some(
            PubsubEvent::ShardedSubscription(
                channel_name.clone(),
                client.subscriptions.len()
                    + client.pattern_subscriptions.len()
                    + client.sharded_subscriptions.len(),
            )
            .as_response(),
        )) {
            Ok(_) => None,
            Err(_) => client.sharded_subscriptions.remove(&channel_name),
        };
    }
    Err(ResponseError::NoReply)
}

fn sunsubscribe(
    parser: &mut ParsedCommand,
    db: &mut Database,
    client: &mut Client,
) -> Result<Response, ResponseError> {
    let total_subs = client.subscriptions.len()
        + client.pattern_subscriptions.len();
    if parser.argv.len() == 1 {
        if client.sharded_subscriptions.is_empty() {
            let _ = client.rawsender.send(Some(
                PubsubEvent::ShardedUnsubscription(vec![], total_subs).as_response(),
            ));
        } else {
            for (channel, subscriber_id) in client.sharded_subscriptions.drain() {
                db.sunsubscribe(channel.clone(), subscriber_id);
                let _ = client.rawsender.send(Some(
                    PubsubEvent::ShardedUnsubscription(channel, total_subs).as_response(),
                ));
            }
        }
    } else {
        for i in 1..parser.argv.len() {
            let channel = try_opt_validate!(parser.get_vec(i), "Invalid channel");
            if let Some(subscriber_id) = client.sharded_subscriptions.remove(&channel) {
                db.sunsubscribe(channel.clone(), subscriber_id);
            }
            let _ = client.rawsender.send(Some(
                PubsubEvent::ShardedUnsubscription(
                    channel,
                    total_subs + client.sharded_subscriptions.len(),
                )
                .as_response(),
            ));
        }
    }
    Err(ResponseError::NoReply)
}

fn spublish(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_exact!(parser, 3);
    let channel = try_validate!(parser.get_vec(1), "Invalid channel");
    let message = try_validate!(parser.get_vec(2), "Invalid message");
    Response::Integer(db.spublish(&channel, &message) as i64)
}

// --- Phase 6: Enhanced Server commands ---

fn reset_command(_parser: &mut ParsedCommand, client: &mut Client) -> Response {
    // Reset client state
    client.dbindex = 0;
    client.multi = false;
    client.multi_commands.clear();
    client.watched_keys.clear();
    client.name.clear();
    client.current_user = "default".to_owned();
    // Note: subscriptions are not cleared here as in Redis RESET
    // just resets the connection state but doesn't unsubscribe
    Response::Status("RESET".to_owned())
}

fn acl_command(parser: &mut ParsedCommand, db: &mut Database, client: &Client) -> Response {
    if parser.argv.len() < 2 {
        return Response::Error("ERR wrong number of arguments for 'acl' command".to_owned());
    }
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match &*subcommand {
        "whoami" => {
            Response::Data(client.current_user.as_bytes().to_vec())
        }
        "list" => {
            let mut users: Vec<_> = db.acl.users.iter().collect();
            users.sort_by_key(|(name, _)| name.clone());
            let result: Vec<Response> = users
                .iter()
                .map(|(name, user)| Response::Data(user.to_acl_string(name).into_bytes()))
                .collect();
            Response::Array(result)
        }
        "setuser" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'acl setuser' command".to_owned());
            }
            let username = match parser.get_str(2) {
                Ok(s) => s.to_owned(),
                Err(_) => return Response::Error("ERR syntax error".to_owned()),
            };
            let mut rules = Vec::new();
            for i in 3..parser.argv.len() {
                match parser.get_str(i) {
                    Ok(s) => rules.push(s),
                    Err(_) => return Response::Error("ERR syntax error".to_owned()),
                }
            }
            match db.acl.set_user(&username, &rules) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "getuser" => {
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'acl getuser' command".to_owned());
            }
            let username = match parser.get_str(2) {
                Ok(s) => s.to_owned(),
                Err(_) => return Response::Error("ERR syntax error".to_owned()),
            };
            match db.acl.users.get(&username) {
                Some(user) => {
                    let mut flags = Vec::new();
                    if user.enabled {
                        flags.push(Response::Data(b"on".to_vec()));
                    } else {
                        flags.push(Response::Data(b"off".to_vec()));
                    }
                    if user.nopass {
                        flags.push(Response::Data(b"nopass".to_vec()));
                    }
                    for pwd in &user.passwords {
                        flags.push(Response::Data(format!("#{}", pwd).into_bytes()));
                    }
                    if user.default_selector.all_commands {
                        flags.push(Response::Data(b"+@all".to_vec()));
                    }
                    if user.default_selector.all_keys {
                        flags.push(Response::Data(b"~*".to_vec()));
                    }
                    Response::Array(flags)
                }
                None => Response::Nil,
            }
        }
        "deluser" => {
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'acl deluser' command".to_owned());
            }
            let mut count = 0;
            for i in 2..parser.argv.len() {
                if let Ok(username) = parser.get_str(i) {
                    if db.acl.del_user(username) {
                        count += 1;
                    }
                }
            }
            Response::Integer(count)
        }
        "cat" => {
            let categories = database::acl::Acl::categories();
            Response::Array(categories.iter().map(|c| Response::Data(c.as_bytes().to_vec())).collect())
        }
        "genpass" => {
            let bits = if parser.argv.len() >= 3 {
                match parser.get_i64(2) {
                    Ok(b) => b as usize,
                    Err(_) => return Response::Error("ERR syntax error".to_owned()),
                }
            } else {
                256
            };
            Response::Data(database::acl::Acl::genpass(bits).into_bytes())
        }
        "log" => {
            // ACL LOG is not fully implemented; return empty array
            Response::Array(vec![])
        }
        "load" => {
            // ACL LOAD: In our implementation, ACL is in-memory only
            Response::Status("OK".to_owned())
        }
        "save" => {
            // ACL SAVE: In our implementation, ACL is in-memory only
            Response::Status("OK".to_owned())
        }
        "help" => {
            let help_lines = vec![
                "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "LIST -- Show user details",
                "SETUSER <username> [rule] ... -- Set user rules",
                "GETUSER <username> -- Get user details",
                "DELUSER <username> [...] -- Delete users",
                "CAT -- List ACL categories",
                "GENPASS [<bits>] -- Generate a random password",
                "WHOAMI -- Get current username",
                "LOG -- Show ACL log (not implemented)",
                "LOAD -- Load ACL from file (no-op)",
                "SAVE -- Save ACL to file (no-op)",
                "HELP -- Show this help",
            ];
            Response::Array(help_lines.iter().map(|l| Response::Data(l.as_bytes().to_vec())).collect())
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

// --- Phase 7: RedisBloom commands ---

fn bf_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "reserve" => {
            // BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'bf.reserve' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let error_rate = match parser.get_f64(3) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
            let capacity = match parser.get_i64(4) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not an integer or out of range".to_owned()) };
            if error_rate <= 0.0 || error_rate >= 1.0 {
                return Response::Error("ERR (0 < error rate < 1)".to_owned());
            }
            if capacity < 1 {
                return Response::Error("ERR capacity must be >= 1".to_owned());
            }
            // Check if key already exists
            if let Some(val) = db.get(dbindex, &key) {
                if !matches!(val.get(), Err(_)) {
                    return Response::Error("ERR item exists: key already holds a non-bloom-filter value".to_owned());
                }
                // Check if it's already a bloom filter
                match val.get() {
                    Err(_) => {} // wrong type, will overwrite
                    Ok(_) => return Response::Error("ERR key already exists".to_owned()),
                }
            }
            let bf = database::bloom::BloomFilter::new(capacity as usize, error_rate);
            db.get_or_create(dbindex, &key).set_bloom_filter(bf);
            db.key_updated(dbindex, &key);
            Response::Status("OK".to_owned())
        }
        "add" => {
            // BF.ADD key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'bf.add' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_bloom_filter(1000, 0.01) {
                    Ok(bf) => { let was_new = bf.add(&item); Response::Integer(if was_new { 1 } else { 0 }) }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "madd" => {
            // BF.MADD key item [item ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'bf.madd' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut results = Vec::new();
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_bloom_filter(1000, 0.01) {
                    Ok(bf) => {
                        for i in 3..parser.argv.len() {
                            let item = match parser.get_vec(i) { Ok(v) => v, Err(_) => continue };
                            let was_new = bf.add(&item);
                            results.push(Response::Integer(if was_new { 1 } else { 0 }));
                        }
                        Response::Array(results)
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "exists" => {
            // BF.EXISTS key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'bf.exists' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.bloom_exists(&item) {
                    Ok(b) => Response::Integer(if b { 1 } else { 0 }),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Integer(0),
            }
        }
        "mexists" => {
            // BF.MEXISTS key item [item ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'bf.mexists' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut results = Vec::new();
            match db.get(dbindex, &key) {
                Some(val) => {
                    for i in 3..parser.argv.len() {
                        let item = match parser.get_vec(i) { Ok(v) => v, Err(_) => continue };
                        match val.bloom_exists(&item) {
                            Ok(b) => results.push(Response::Integer(if b { 1 } else { 0 })),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Array((0..parser.argv.len() - 3).map(|_| Response::Integer(0)).collect()),
            }
        }
        "info" => {
            // BF.INFO key
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'bf.info' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.bloom_info() {
                    Ok(info) => {
                        Response::Array(vec![
                            Response::Data(b"Capacity".to_vec()), Response::Integer(info.capacity as i64),
                            Response::Data(b"Size".to_vec()), Response::Integer(info.bits as i64),
                            Response::Data(b"Number of filters".to_vec()), Response::Integer(info.num_filters as i64),
                            Response::Data(b"Number of items inserted".to_vec()), Response::Integer(info.size as i64),
                            Response::Data(b"Number of hash functions".to_vec()), Response::Integer(info.num_hashes as i64),
                            Response::Data(b"Expansion rate".to_vec()), Response::Integer(info.expansion as i64),
                        ])
                    }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

fn cf_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "reserve" => {
            // CF.RESERVE key capacity
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'cf.reserve' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let capacity = match parser.get_i64(3) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not an integer or out of range".to_owned()) };
            let cf = database::bloom::CuckooFilter::new(capacity as usize);
            db.get_or_create(dbindex, &key).set_cuckoo_filter(cf);
            db.key_updated(dbindex, &key);
            Response::Status("OK".to_owned())
        }
        "add" => {
            // CF.ADD key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'cf.add' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_cuckoo_filter(1000) {
                    Ok(cf) => { let ok = cf.add(&item); Response::Integer(if ok { 1 } else { 0 }) }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "addnx" => {
            // CF.ADDNX key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'cf.addnx' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_cuckoo_filter(1000) {
                    Ok(cf) => { let added = cf.addnx(&item); Response::Integer(if added { 1 } else { 0 }) }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "exists" => {
            // CF.EXISTS key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'cf.exists' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.cuckoo_exists(&item) {
                    Ok(b) => Response::Integer(if b { 1 } else { 0 }),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Integer(0),
            }
        }
        "del" => {
            // CF.DEL key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'cf.del' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.cuckoo_delete(&item) {
                    Ok(b) => { db.key_updated(dbindex, &key); Response::Integer(if b { 1 } else { 0 }) }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "count" => {
            // CF.COUNT key item
            if parser.argv.len() != 4 {
                return Response::Error("ERR wrong number of arguments for 'cf.count' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let item = match parser.get_vec(3) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.cuckoo_count(&item) {
                    Ok(c) => Response::Integer(c as i64),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Integer(0),
            }
        }
        "info" => {
            // CF.INFO key
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'cf.info' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.cuckoo_info() {
                    Ok(info) => {
                        Response::Array(vec![
                            Response::Data(b"Size".to_vec()), Response::Integer(info.size as i64),
                            Response::Data(b"Number of buckets".to_vec()), Response::Integer(info.num_buckets as i64),
                            Response::Data(b"Number of filter".to_vec()), Response::Integer(1),
                            Response::Data(b"Number of items inserted".to_vec()), Response::Integer(info.size as i64),
                            Response::Data(b"Number of items deleted".to_vec()), Response::Integer(info.num_deletes as i64),
                            Response::Data(b"Bucket size".to_vec()), Response::Integer(info.bucket_size as i64),
                            Response::Data(b"Expansion rate".to_vec()), Response::Integer(1),
                            Response::Data(b"Max iterations".to_vec()), Response::Integer(info.num_kicks as i64),
                        ])
                    }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

/// Formats a t-digest floating-point result the way RESP2 expects:
/// "inf" / "-inf" / "nan" (lowercase), plain decimal otherwise.
fn format_tdigest_double(v: f64) -> String {
    if v.is_nan() { "nan".to_owned() } else { v.to_string() }
}

fn tdigest_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "create" => {
            // TDIGEST.CREATE key [compression]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.create' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let compression = if parser.argv.len() > 3 {
                match parser.get_f64(3) { Ok(v) => v, Err(_) => 100.0 }
            } else {
                100.0
            };
            let td = database::bloom::TDigest::new(compression);
            db.get_or_create(dbindex, &key).set_tdigest(td);
            db.key_updated(dbindex, &key);
            Response::Status("OK".to_owned())
        }
        "add" => {
            // TDIGEST.ADD key value [value ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.add' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_tdigest(100.0) {
                    Ok(td) => {
                        for i in 3..parser.argv.len() {
                            let v = match parser.get_f64(i) { Ok(v) => v, Err(_) => continue };
                            td.add(v);
                        }
                        Response::Status("OK".to_owned())
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "reset" => {
            // TDIGEST.RESET key
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.reset' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.tdigest_reset() {
                    Ok(()) => { db.key_updated(dbindex, &key); Response::Status("OK".to_owned()) }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "quantile" => {
            // TDIGEST.QUANTILE key quantile [quantile ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.quantile' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let q = match parser.get_f64(i) { Ok(v) => v, Err(_) => continue };
                        match val.tdigest_quantile(q) {
                            Ok(v) => results.push(Response::Data(v.to_string().into_bytes())),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "cdf" => {
            // TDIGEST.CDF key value [value ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.cdf' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let v = match parser.get_f64(i) { Ok(v) => v, Err(_) => continue };
                        match val.tdigest_cdf(v) {
                            Ok(c) => results.push(Response::Data(c.to_string().into_bytes())),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "min" => {
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.min' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.tdigest_min() {
                    Ok(v) => Response::Data(v.to_string().into_bytes()),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "max" => {
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.max' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.tdigest_max() {
                    Ok(v) => Response::Data(v.to_string().into_bytes()),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "info" => {
            // TDIGEST.INFO key
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.info' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.tdigest_info() {
                    Ok((count, compression)) => {
                        Response::Array(vec![
                            Response::Data(b"Compression".to_vec()), Response::Integer(compression as i64),
                            Response::Data(b"Capacity".to_vec()), Response::Integer(0),
                            Response::Data(b"Merged nodes".to_vec()), Response::Integer(0),
                            Response::Data(b"Unmerged nodes".to_vec()), Response::Integer(0),
                            Response::Data(b"Merged weight".to_vec()), Response::Integer(count as i64),
                            Response::Data(b"Unmerged weight".to_vec()), Response::Integer(0),
                            Response::Data(b"Total compressions".to_vec()), Response::Integer(0),
                        ])
                    }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "merge" => {
            // TDIGEST.MERGE destination numkeys source [source ...] [COMPRESSION compression] [OVERRIDE]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.merge' command".to_owned());
            }
            let dest_key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let numkeys = match parser.get_i64(3) { Ok(v) => v as usize, Err(_) => return Response::Error("ERR value is not an integer or out of range".to_owned()) };
            if numkeys == 0 || parser.argv.len() < 4 + numkeys {
                return Response::Error("ERR wrong number of arguments for 'tdigest.merge' command".to_owned());
            }
            let mut sources = Vec::new();
            for i in 0..numkeys {
                let k = match parser.get_vec(4 + i) { Ok(v) => v, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
                sources.push(k);
            }
            // Create a new tdigest for the destination by merging all sources
            let mut merged = database::bloom::TDigest::new(100.0);
            for src in &sources {
                if let Some(val) = db.get(dbindex, src) {
                    if let Ok(other) = val.tdigest_ref() {
                        merged.merge(other);
                    }
                }
            }
            db.get_or_create(dbindex, &dest_key).set_tdigest(merged);
            db.key_updated(dbindex, &dest_key);
            Response::Status("OK".to_owned())
        }
        "rank" => {
            // TDIGEST.RANK key value [value ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.rank' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let v = match parser.get_f64(i) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
                        if v.is_nan() {
                            return Response::Error("ERR NaN value".to_owned());
                        }
                        match val.tdigest_rank(v) {
                            Ok(r) => results.push(Response::Integer(r)),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "revrank" => {
            // TDIGEST.REVRANK key value [value ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.revrank' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let v = match parser.get_f64(i) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
                        if v.is_nan() {
                            return Response::Error("ERR NaN value".to_owned());
                        }
                        match val.tdigest_revrank(v) {
                            Ok(r) => results.push(Response::Integer(r)),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "byrank" => {
            // TDIGEST.BYRANK key rank [rank ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.byrank' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let r = match parser.get_f64(i) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
                        if r.is_nan() {
                            return Response::Error("ERR NaN rank".to_owned());
                        }
                        match val.tdigest_byrank(r) {
                            Ok(v) => results.push(Response::Data(format_tdigest_double(v).into_bytes())),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "byrevrank" => {
            // TDIGEST.BYREVRANK key reverse_rank [reverse_rank ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.byrevrank' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let r = match parser.get_f64(i) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
                        if r.is_nan() {
                            return Response::Error("ERR NaN rank".to_owned());
                        }
                        match val.tdigest_byrevrank(r) {
                            Ok(v) => results.push(Response::Data(format_tdigest_double(v).into_bytes())),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        "trimmed_mean" => {
            // TDIGEST.TRIMMED_MEAN key low_cut_quantile high_cut_quantile
            if parser.argv.len() != 5 {
                return Response::Error("ERR wrong number of arguments for 'tdigest.trimmed_mean' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let low = match parser.get_f64(3) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
            let high = match parser.get_f64(4) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
            if !(0.0..=1.0).contains(&low) || !(0.0..=1.0).contains(&high) {
                return Response::Error("ERR quantile should be in [0..1]".to_owned());
            }
            if low >= high {
                return Response::Error("ERR low_cut_quantile should be lower than high_cut_quantile".to_owned());
            }
            match db.get(dbindex, &key) {
                Some(val) => match val.tdigest_trimmed_mean(low, high) {
                    Ok(m) => Response::Data(format_tdigest_double(m).into_bytes()),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

fn topk_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "reserve" => {
            // TOPK.RESERVE key topk [width depth decay]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'topk.reserve' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let k = match parser.get_i64(3) { Ok(v) => v as usize, Err(_) => return Response::Error("ERR value is not an integer or out of range".to_owned()) };
            let (width, depth) = if parser.argv.len() >= 6 {
                let w = match parser.get_i64(4) { Ok(v) => v as usize, Err(_) => return Response::Error("ERR value is not an integer or out of range".to_owned()) };
                let d = match parser.get_i64(5) { Ok(v) => v as usize, Err(_) => return Response::Error("ERR value is not an integer or out of range".to_owned()) };
                (w, d)
            } else {
                (8, 5) // defaults
            };
            let topk = database::bloom::TopK::new(k, width, depth);
            db.get_or_create(dbindex, &key).set_topk(topk);
            db.key_updated(dbindex, &key);
            Response::Status("OK".to_owned())
        }
        "add" => {
            // TOPK.ADD key item [item ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'topk.add' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_topk(7, 8, 5) {
                    Ok(topk) => {
                        let mut results = Vec::new();
                        for i in 3..parser.argv.len() {
                            let item = match parser.get_vec(i) { Ok(v) => v, Err(_) => continue };
                            topk.add(&item, 1);
                            // Return the item that was dropped (if any), or nil
                            results.push(Response::Nil);
                        }
                        Response::Array(results)
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "incrby" => {
            // TOPK.INCRBY key item increment [item increment ...]
            if parser.argv.len() < 5 || (parser.argv.len() - 3) % 2 != 0 {
                return Response::Error("ERR wrong number of arguments for 'topk.incrby' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let r = {
                let val = db.get_or_create(dbindex, &key);
                match val.ensure_topk(7, 8, 5) {
                    Ok(topk) => {
                        let mut results = Vec::new();
                        let mut i = 3;
                        while i + 1 < parser.argv.len() {
                            let item = match parser.get_vec(i) { Ok(v) => v, Err(_) => { i += 2; continue; } };
                            let incr = match parser.get_i64(i + 1) { Ok(v) => v as u64, Err(_) => { i += 2; continue; } };
                            topk.add(&item, incr);
                            results.push(Response::Nil);
                            i += 2;
                        }
                        Response::Array(results)
                    }
                    Err(e) => Response::Error(e.to_string()),
                }
            };
            db.key_updated(dbindex, &key);
            r
        }
        "query" => {
            // TOPK.QUERY key item [item ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'topk.query' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let item = match parser.get_vec(i) { Ok(v) => v, Err(_) => continue };
                        match val.topk_query(&item) {
                            Ok(b) => results.push(Response::Integer(if b { 1 } else { 0 })),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Array((0..parser.argv.len() - 3).map(|_| Response::Integer(0)).collect()),
            }
        }
        "count" => {
            // TOPK.COUNT key item [item ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'topk.count' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => {
                    let mut results = Vec::new();
                    for i in 3..parser.argv.len() {
                        let item = match parser.get_vec(i) { Ok(v) => v, Err(_) => continue };
                        match val.topk_count(&item) {
                            Ok(c) => results.push(Response::Integer(c as i64)),
                            Err(e) => return Response::Error(e.to_string()),
                        }
                    }
                    Response::Array(results)
                }
                None => Response::Array((0..parser.argv.len() - 3).map(|_| Response::Integer(0)).collect()),
            }
        }
        "list" => {
            // TOPK.LIST key [WITHCOUNT]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'topk.list' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let withcount = parser.argv.len() > 3 && parser.get_str(3).map(|s| s.eq_ignore_ascii_case("withcount")).unwrap_or(false);
            match db.get(dbindex, &key) {
                Some(val) => match val.topk_list() {
                    Ok(items) => {
                        if withcount {
                            let mut result = Vec::new();
                            for (item, count) in items {
                                result.push(Response::Data(item));
                                result.push(Response::Integer(count as i64));
                            }
                            Response::Array(result)
                        } else {
                            Response::Array(items.into_iter().map(|(item, _)| Response::Data(item)).collect())
                        }
                    }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Array(vec![]),
            }
        }
        "info" => {
            // TOPK.INFO key
            if parser.argv.len() != 3 {
                return Response::Error("ERR wrong number of arguments for 'topk.info' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.topk_info() {
                    Ok(info) => {
                        Response::Array(vec![
                            Response::Data(b"k".to_vec()), Response::Integer(info.k as i64),
                            Response::Data(b"width".to_vec()), Response::Integer(info.width as i64),
                            Response::Data(b"depth".to_vec()), Response::Integer(info.depth as i64),
                            Response::Data(b"decay".to_vec()), Response::Data(b"0.5".to_vec()),
                        ])
                    }
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR not found".to_owned()),
            }
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

// --- Phase 7.1: RedisJSON commands ---

fn json_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "set" => {
            // JSON.SET key path value [NX|XX]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'json.set' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let json_str = match parser.get_str(4) { Ok(v) => v.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let nx = parser.argv.len() > 5 && parser.get_str(5).map(|s| s.eq_ignore_ascii_case("nx")).unwrap_or(false);
            let xx = parser.argv.len() > 5 && parser.get_str(5).map(|s| s.eq_ignore_ascii_case("xx")).unwrap_or(false);

            let new_val = match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(v) => v,
                Err(e) => return Response::Error(format!("ERR invalid JSON: {}", e)),
            };

            let exists = db.get(dbindex, &key).is_some();
            if nx && exists { return Response::Nil; }
            if xx && !exists { return Response::Nil; }

            if path == "$" || path == "." || path.is_empty() {
                let j = database::json::ValueJson::new(new_val);
                db.get_or_create(dbindex, &key).set_json(j);
            } else {
                // Set at path within existing JSON
                match db.get_mut(dbindex, &key) {
                    Some(val) => {
                        match val.json_mut() {
                            Ok(j) => {
                                if !database::json::json_set(&mut j.value, &path, new_val) {
                                    return Response::Error("ERR path not found".to_owned());
                                }
                            }
                            Err(_) => return Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                        }
                    }
                    None => {
                        // Create new JSON with the value at path
                        let mut root = serde_json::Value::Object(serde_json::Map::new());
                        database::json::json_set(&mut root, &path, new_val);
                        let j = database::json::ValueJson::new(root);
                        db.get_or_create(dbindex, &key).set_json(j);
                    }
                }
            }
            db.key_updated(dbindex, &key);
            Response::Status("OK".to_owned())
        }
        "get" => {
            // JSON.GET key [path [path ...]]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.get' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => {
                        if parser.argv.len() == 3 {
                            // Return entire document
                            Response::Data(j.value.to_string().into_bytes())
                        } else {
                            // Return specific paths
                            let mut results = Vec::new();
                            for i in 3..parser.argv.len() {
                                let path = match parser.get_str(i) { Ok(p) => p, Err(_) => continue };
                                match database::json::json_get(&j.value, path) {
                                    Some(v) => results.push(v.to_string()),
                                    None => results.push("null".to_owned()),
                                }
                            }
                            if results.len() == 1 {
                                Response::Data(results.into_iter().next().unwrap().into_bytes())
                            } else {
                                Response::Data(format!("[{}]", results.join(",")).into_bytes())
                            }
                        }
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "del" => {
            // JSON.DEL key [path]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.del' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            if parser.argv.len() == 3 || parser.get_str(3).map(|s| s == "$" || s == ".").unwrap_or(false) {
                // Delete entire key
                let existed = db.get(dbindex, &key).is_some();
                if existed { db.remove(dbindex, &key); db.key_updated(dbindex, &key); }
                return Response::Integer(if existed { 1 } else { 0 });
            }
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => {
                        let deleted = database::json::json_del(&mut j.value, &path);
                        db.key_updated(dbindex, &key);
                        Response::Integer(if deleted { 1 } else { 0 })
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Integer(0),
            }
        }
        "type" => {
            // JSON.TYPE key [path]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.type' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => {
                        let t = if parser.argv.len() > 3 {
                            let path = match parser.get_str(3) { Ok(p) => p, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
                            match database::json::json_get(&j.value, path) {
                                Some(v) => json_type_name(v),
                                None => return Response::Nil,
                            }
                        } else {
                            j.json_type().to_owned()
                        };
                        Response::Data(t.into_bytes())
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "numincrby" => {
            // JSON.NUMINCRBY key path value
            if parser.argv.len() != 5 {
                return Response::Error("ERR wrong number of arguments for 'json.numincrby' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let increment = match parser.get_f64(4) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_numincrby(&mut j.value, &path, increment) {
                        Ok(v) => { db.key_updated(dbindex, &key); Response::Data(v.to_string().into_bytes()) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "nummultby" => {
            // JSON.NUMMULTBY key path value
            if parser.argv.len() != 5 {
                return Response::Error("ERR wrong number of arguments for 'json.nummultby' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let multiplier = match parser.get_f64(4) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not a valid float".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_nummultby(&mut j.value, &path, multiplier) {
                        Ok(v) => { db.key_updated(dbindex, &key); Response::Data(v.to_string().into_bytes()) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "strlen" => {
            // JSON.STRLEN key [path]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.strlen' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => {
                        let path = if parser.argv.len() > 3 {
                            parser.get_str(3).unwrap_or("$")
                        } else { "$" };
                        match database::json::json_get(&j.value, path) {
                            Some(v) => match v.as_str() {
                                Some(s) => Response::Integer(s.len() as i64),
                                None => Response::Error("ERR wrong type".to_owned()),
                            },
                            None => Response::Nil,
                        }
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "strappend" => {
            // JSON.STRAPPEND key [path] value
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'json.strappend' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let (path, append_val) = if parser.argv.len() == 4 {
                ("$".to_owned(), match parser.get_str(3) { Ok(v) => v.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) })
            } else {
                (match parser.get_str(3) { Ok(v) => v.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) },
                 match parser.get_str(4) { Ok(v) => v.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) })
            };
            // Parse the append value (should be a JSON string)
            let append_str = match serde_json::from_str::<String>(&append_val) {
                Ok(s) => s,
                Err(_) => return Response::Error("ERR invalid JSON string".to_owned()),
            };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_strappend(&mut j.value, &path, &append_str) {
                        Ok(len) => { db.key_updated(dbindex, &key); Response::Integer(len as i64) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "arrappend" => {
            // JSON.ARRAPPEND key path value [value ...]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'json.arrappend' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut values = Vec::new();
            for i in 4..parser.argv.len() {
                let v_str = match parser.get_str(i) { Ok(v) => v.to_owned(), Err(_) => continue };
                match serde_json::from_str(&v_str) {
                    Ok(v) => values.push(v),
                    Err(e) => return Response::Error(format!("ERR invalid JSON: {}", e)),
                }
            }
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_arrappend(&mut j.value, &path, values) {
                        Ok(len) => { db.key_updated(dbindex, &key); Response::Integer(len as i64) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "arrlen" => {
            // JSON.ARRLEN key [path]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.arrlen' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = if parser.argv.len() > 3 { parser.get_str(3).unwrap_or("$") } else { "$" };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => match database::json::json_arrlen(&j.value, path) {
                        Ok(len) => Response::Integer(len as i64),
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "arrpop" => {
            // JSON.ARRPOP key [path [index]]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.arrpop' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = if parser.argv.len() > 3 { parser.get_str(3).unwrap_or("$") } else { "$" };
            let index = if parser.argv.len() > 4 { parser.get_i64(4).unwrap_or(-1) } else { -1 };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_arrpop(&mut j.value, path, index) {
                        Ok(v) => { db.key_updated(dbindex, &key); Response::Data(v.to_string().into_bytes()) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "arrinsert" => {
            // JSON.ARRINSERT key path index value [value ...]
            if parser.argv.len() < 6 {
                return Response::Error("ERR wrong number of arguments for 'json.arrinsert' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let index = match parser.get_i64(4) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not an integer".to_owned()) };
            let mut values = Vec::new();
            for i in 5..parser.argv.len() {
                let v_str = match parser.get_str(i) { Ok(v) => v.to_owned(), Err(_) => continue };
                match serde_json::from_str(&v_str) {
                    Ok(v) => values.push(v),
                    Err(e) => return Response::Error(format!("ERR invalid JSON: {}", e)),
                }
            }
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_arrinsert(&mut j.value, &path, index, values) {
                        Ok(len) => { db.key_updated(dbindex, &key); Response::Integer(len as i64) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "arrindex" => {
            // JSON.ARRINDEX key path value
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'json.arrindex' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let search_str = match parser.get_str(4) { Ok(v) => v.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let search_val = match serde_json::from_str(&search_str) {
                Ok(v) => v,
                Err(e) => return Response::Error(format!("ERR invalid JSON: {}", e)),
            };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => match database::json::json_arrindex(&j.value, &path, &search_val) {
                        Ok(idx) => Response::Integer(idx),
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "arrtrim" => {
            // JSON.ARRTRIM key path start stop
            if parser.argv.len() != 6 {
                return Response::Error("ERR wrong number of arguments for 'json.arrtrim' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let start = match parser.get_i64(4) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not an integer".to_owned()) };
            let stop = match parser.get_i64(5) { Ok(v) => v, Err(_) => return Response::Error("ERR value is not an integer".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => match database::json::json_arrtrim(&mut j.value, &path, start, stop) {
                        Ok(len) => { db.key_updated(dbindex, &key); Response::Integer(len as i64) }
                        Err(e) => Response::Error(e),
                    },
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        "objkeys" => {
            // JSON.OBJKEYS key [path]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.objkeys' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = if parser.argv.len() > 3 { parser.get_str(3).unwrap_or("$") } else { "$" };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => {
                        match database::json::json_get(&j.value, path) {
                            Some(v) => match v.as_object() {
                                Some(o) => Response::Array(o.keys().map(|k| Response::Data(k.as_bytes().to_vec())).collect()),
                                None => Response::Error("ERR wrong type".to_owned()),
                            },
                            None => Response::Nil,
                        }
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "objlen" => {
            // JSON.OBJLEN key [path]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'json.objlen' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = if parser.argv.len() > 3 { parser.get_str(3).unwrap_or("$") } else { "$" };
            match db.get(dbindex, &key) {
                Some(val) => match val.json_ref() {
                    Ok(j) => {
                        match database::json::json_get(&j.value, path) {
                            Some(v) => match v {
                                serde_json::Value::Object(o) => Response::Integer(o.len() as i64),
                                serde_json::Value::Array(a) => Response::Integer(a.len() as i64),
                                _ => Response::Error("ERR wrong type".to_owned()),
                            },
                            None => Response::Nil,
                        }
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Nil,
            }
        }
        "mget" => {
            // JSON.MGET key [key ...] path
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'json.mget' command".to_owned());
            }
            let path = match parser.get_str(parser.argv.len() - 1) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut results = Vec::new();
            for i in 2..parser.argv.len() - 1 {
                let key = match parser.get_vec(i) { Ok(k) => k, Err(_) => continue };
                match db.get(dbindex, &key) {
                    Some(val) => match val.json_ref() {
                        Ok(j) => match database::json::json_get(&j.value, &path) {
                            Some(v) => results.push(Response::Data(v.to_string().into_bytes())),
                            None => results.push(Response::Nil),
                        },
                        Err(_) => results.push(Response::Nil),
                    },
                    None => results.push(Response::Nil),
                }
            }
            Response::Array(results)
        }
        "merge" => {
            // JSON.MERGE key path value
            if parser.argv.len() != 5 {
                return Response::Error("ERR wrong number of arguments for 'json.merge' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let path = match parser.get_str(3) { Ok(p) => p.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let json_str = match parser.get_str(4) { Ok(v) => v.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let patch = match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(v) => v,
                Err(e) => return Response::Error(format!("ERR invalid JSON: {}", e)),
            };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.json_mut() {
                    Ok(j) => {
                        database::json::json_merge(&mut j.value, &path, patch);
                        db.key_updated(dbindex, &key);
                        Response::Status("OK".to_owned())
                    }
                    Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                },
                None => Response::Error("ERR key does not exist".to_owned()),
            }
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

fn json_type_name(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "null".to_owned(),
        serde_json::Value::Bool(_) => "boolean".to_owned(),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() { "integer".to_owned() } else { "number".to_owned() }
        }
        serde_json::Value::String(_) => "string".to_owned(),
        serde_json::Value::Array(_) => "array".to_owned(),
        serde_json::Value::Object(_) => "object".to_owned(),
    }
}

// --- Phase 7.2: RediSearch commands ---

fn ft_command(parser: &mut ParsedCommand, db: &mut Database) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "create" => {
            // FT.CREATE index [ON type] [PREFIX count prefix ...] SCHEMA field_name type ...
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ft.create' command".to_owned());
            }
            let index_name = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut prefix = Vec::new();
            let mut schema_fields = Vec::new();
            let mut i = 3;
            while i < parser.argv.len() {
                let arg = match parser.get_str(i) { Ok(s) => s.to_ascii_lowercase(), Err(_) => { i += 1; continue; } };
                match arg.as_str() {
                    "prefix" => {
                        i += 1;
                        let count = parser.get_i64(i).unwrap_or(1) as usize;
                        i += 1;
                        for _ in 0..count {
                            if let Ok(p) = parser.get_str(i) { prefix.push(p.to_owned()); }
                            i += 1;
                        }
                        continue;
                    }
                    "on" => { i += 2; continue; } // skip ON HASH/JSON
                    "schema" => {
                        i += 1;
                        // Parse schema fields
                        while i + 1 < parser.argv.len() {
                            let field_name = match parser.get_str(i) { Ok(s) => s.to_owned(), Err(_) => break };
                            i += 1;
                            // Check for AS alias
                            let mut alias = None;
                            if i < parser.argv.len() {
                                if let Ok(next) = parser.get_str(i) {
                                    if next.eq_ignore_ascii_case("as") {
                                        i += 1;
                                        alias = parser.get_str(i).ok().map(|s| s.to_owned());
                                        i += 1;
                                    }
                                }
                            }
                            let type_str = match parser.get_str(i) { Ok(s) => s.to_ascii_lowercase(), Err(_) => break };
                            i += 1;
                            let field_type = match type_str.as_str() {
                                "text" => database::search::FieldType::Text,
                                "tag" => database::search::FieldType::Tag,
                                "numeric" => database::search::FieldType::Numeric,
                                "geo" => database::search::FieldType::Geo,
                                _ => database::search::FieldType::Text,
                            };
                            schema_fields.push(database::search::SchemaField {
                                name: field_name,
                                alias,
                                field_type,
                                sortable: false,
                                no_index: false,
                            });
                            // Skip optional field options
                            while i < parser.argv.len() {
                                if let Ok(opt) = parser.get_str(i) {
                                    let opt_lower = opt.to_ascii_lowercase();
                                    if opt_lower == "sortable" || opt_lower == "noindex" || opt_lower == "no_index" {
                                        i += 1;
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                        }
                        break;
                    }
                    _ => { i += 1; }
                }
            }
            match db.search.create_index(&index_name, prefix, schema_fields) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "search" => {
            // FT.SEARCH index query [LIMIT offset num] [RETURN count field ...]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'ft.search' command".to_owned());
            }
            let index_name = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let query = match parser.get_str(3) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut offset = 0;
            let mut limit = 10;
            let mut i = 4;
            while i < parser.argv.len() {
                if let Ok(arg) = parser.get_str(i) {
                    match arg.to_ascii_lowercase().as_str() {
                        "limit" => {
                            i += 1;
                            offset = parser.get_i64(i).unwrap_or(0) as usize;
                            i += 1;
                            limit = parser.get_i64(i).unwrap_or(10) as usize;
                            i += 1;
                        }
                        _ => { i += 1; }
                    }
                } else {
                    i += 1;
                }
            }
            match db.search.get_index(&index_name) {
                Some(index) => {
                    let result = index.search(&query, offset, limit);
                    let mut response = vec![Response::Integer(result.total as i64)];
                    for doc in &result.documents {
                        response.push(Response::Data(doc.key.as_bytes().to_vec()));
                        let mut fields = Vec::new();
                        for (k, v) in &doc.fields {
                            fields.push(Response::Data(k.as_bytes().to_vec()));
                            fields.push(Response::Data(v.as_bytes().to_vec()));
                        }
                        response.push(Response::Array(fields));
                    }
                    Response::Array(response)
                }
                None => Response::Error(format!("ERR Index not found: {}", index_name)),
            }
        }
        "info" => {
            // FT.INFO index
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ft.info' command".to_owned());
            }
            let index_name = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.search.get_index(&index_name) {
                Some(index) => {
                    let info = index.info();
                    Response::Array(vec![
                        Response::Data(b"index_name".to_vec()), Response::Data(info.name.into_bytes()),
                        Response::Data(b"num_docs".to_vec()), Response::Integer(info.num_docs as i64),
                        Response::Data(b"num_terms".to_vec()), Response::Integer(info.num_terms as i64),
                        Response::Data(b"num_fields".to_vec()), Response::Integer(info.num_fields as i64),
                    ])
                }
                None => Response::Error(format!("ERR Index not found: {}", index_name)),
            }
        }
        "dropindex" => {
            // FT.DROPINDEX index [DD]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ft.dropindex' command".to_owned());
            }
            let index_name = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let dd = parser.argv.len() > 3 && parser.get_str(3).map(|s| s.eq_ignore_ascii_case("dd")).unwrap_or(false);
            match db.search.drop_index(&index_name, dd) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "aliasadd" => {
            // FT.ALIASADD alias index
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'ft.aliasadd' command".to_owned());
            }
            let alias = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let index_name = match parser.get_str(3) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.search.add_alias(&alias, &index_name) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "aliasdel" => {
            // FT.ALIASDEL alias
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ft.aliasdel' command".to_owned());
            }
            let alias = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.search.del_alias(&alias) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "aliasupdate" => {
            // FT.ALIASUPDATE alias index
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'ft.aliasupdate' command".to_owned());
            }
            let alias = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let index_name = match parser.get_str(3) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            // Delete old alias if exists, then add new
            let _ = db.search.del_alias(&alias);
            match db.search.add_alias(&alias, &index_name) {
                Ok(()) => Response::Status("OK".to_owned()),
                Err(e) => Response::Error(e),
            }
        }
        "_index" => {
            // FT._INDEX index key - internal command to index a hash key
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments".to_owned());
            }
            let index_name = match parser.get_str(2) { Ok(s) => s.to_owned(), Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let key = match parser.get_vec(3) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let dbindex = 0;
            match db.search.get_index(&index_name) {
                Some(index) => {
                    let prefix_match = index.prefix.iter().any(|p| String::from_utf8_lossy(&key).starts_with(p));
                    if !prefix_match && !index.prefix.is_empty() {
                        return Response::Error("ERR key does not match index prefix".to_owned());
                    }
                    // Get hash fields
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    match db.get(dbindex, &key) {
                        Some(val) => {
                            let mut fields = std::collections::HashMap::new();
                            match val {
                                database::Value::Hash(h) => {
                                    for (f, v) in h.hgetall() {
                                        fields.insert(String::from_utf8_lossy(&f).to_string(), String::from_utf8_lossy(&v).to_string());
                                    }
                                }
                                database::Value::Json(j) => {
                                    if let serde_json::Value::Object(o) = &j.value {
                                        for (k, v) in o {
                                            match v {
                                                serde_json::Value::String(s) => { fields.insert(k.clone(), s.clone()); }
                                                serde_json::Value::Number(n) => { fields.insert(k.clone(), n.to_string()); }
                                                serde_json::Value::Bool(b) => { fields.insert(k.clone(), b.to_string()); }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                                _ => return Response::Error("WRONGTYPE key holds wrong type".to_owned()),
                            }
                            // We need to get a mutable reference to the index
                            // Since we already borrowed immutably, we need to drop and re-borrow
                            drop(val);
                            if let Some(index) = db.search.get_index_mut(&index_name) {
                                index.index_document(&key_str, fields);
                            }
                            Response::Status("OK".to_owned())
                        }
                        None => Response::Error("ERR no such key".to_owned()),
                    }
                }
                None => Response::Error(format!("ERR Index not found: {}", index_name)),
            }
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

// --- Phase 7.3: RedisTimeSeries commands ---

fn ts_command(parser: &mut ParsedCommand, db: &mut Database, dbindex: usize) -> Response {
    validate_arguments_gte!(parser, 2);
    let subcommand = match parser.get_str(1) {
        Ok(s) => s.to_ascii_lowercase(),
        Err(_) => return Response::Error("ERR syntax error".to_owned()),
    };
    match subcommand.as_str() {
        "create" => {
            // TS.CREATE key [RETENTION retentionPeriod] [LABELS label value ...]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ts.create' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut retention_ms: i64 = 0;
            let mut labels = Vec::new();
            let mut i = 3;
            while i < parser.argv.len() {
                if let Ok(arg) = parser.get_str(i) {
                    match arg.to_ascii_lowercase().as_str() {
                        "retention" => {
                            i += 1;
                            retention_ms = parser.get_i64(i).unwrap_or(0);
                            i += 1;
                        }
                        "labels" => {
                            i += 1;
                            while i + 1 < parser.argv.len() {
                                let label = match parser.get_str(i) { Ok(s) => s.to_owned(), Err(_) => break };
                                i += 1;
                                let value = match parser.get_str(i) { Ok(s) => s.to_owned(), Err(_) => break };
                                i += 1;
                                labels.push((label, value));
                            }
                        }
                        _ => { i += 1; }
                    }
                } else {
                    i += 1;
                }
            }
            let val = db.get_or_create(dbindex, &key);
            match val.ensure_timeseries(retention_ms, labels, database::timeseries::DuplicatePolicy::Last) {
                Ok(_) => Response::Status("OK".to_owned()),
                Err(_) => Response::Error("ERR failed to create time series".to_owned()),
            }
        }
        "add" => {
            // TS.ADD key timestamp value [RETENTION retention] [LABELS label value ...]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'ts.add' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let timestamp = match parser.get_i64(3) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid timestamp".to_owned()) };
            let value = match parser.get_str(4) { Ok(s) => s.parse::<f64>().unwrap_or(0.0), Err(_) => return Response::Error("ERR invalid value".to_owned()) };
            let val = db.get_or_create(dbindex, &key);
            match val.ts_add(timestamp, value) {
                Ok(ts) => Response::Integer(ts),
                Err(e) => Response::Error(e.to_string()),
            }
        }
        "madd" => {
            // TS.MADD key timestamp value [key timestamp value ...]
            if parser.argv.len() < 5 || (parser.argv.len() - 2) % 3 != 0 {
                return Response::Error("ERR wrong number of arguments for 'ts.madd' command".to_owned());
            }
            let mut results = Vec::new();
            let mut i = 2;
            while i + 2 < parser.argv.len() {
                let key = match parser.get_vec(i) { Ok(k) => k, Err(_) => { results.push(Response::Error("ERR syntax error".to_owned())); i += 3; continue; } };
                let timestamp = parser.get_i64(i + 1).unwrap_or(0);
                let value = parser.get_str(i + 2).ok().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let val = db.get_or_create(dbindex, &key);
                match val.ts_add(timestamp, value) {
                    Ok(ts) => results.push(Response::Integer(ts)),
                    Err(e) => results.push(Response::Error(e.to_string())),
                }
                i += 3;
            }
            Response::Array(results)
        }
        "incrby" => {
            // TS.INCRBY key value [TIMESTAMP timestamp]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'ts.incrby' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let value = match parser.get_str(3) { Ok(s) => s.parse::<f64>().unwrap_or(0.0), Err(_) => return Response::Error("ERR invalid value".to_owned()) };
            let timestamp = if parser.argv.len() > 5 {
                if let Ok(arg) = parser.get_str(4) {
                    if arg.eq_ignore_ascii_case("timestamp") {
                        parser.get_i64(5).unwrap_or(-1)
                    } else { -1 }
                } else { -1 }
            } else { -1 };
            let val = db.get_or_create(dbindex, &key);
            match val.ts_mut() {
                Ok(ts) => {
                    match ts.incrby(timestamp, value) {
                        Ok(t) => Response::Integer(t),
                        Err(e) => Response::Error(e),
                    }
                }
                Err(_) => Response::Error("WRONGTYPE key does not hold a TimeSeries".to_owned()),
            }
        }
        "decrby" => {
            // TS.DECRBY key value [TIMESTAMP timestamp]
            if parser.argv.len() < 4 {
                return Response::Error("ERR wrong number of arguments for 'ts.decrby' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let value = match parser.get_str(3) { Ok(s) => s.parse::<f64>().unwrap_or(0.0), Err(_) => return Response::Error("ERR invalid value".to_owned()) };
            let timestamp = if parser.argv.len() > 5 {
                if let Ok(arg) = parser.get_str(4) {
                    if arg.eq_ignore_ascii_case("timestamp") {
                        parser.get_i64(5).unwrap_or(-1)
                    } else { -1 }
                } else { -1 }
            } else { -1 };
            let val = db.get_or_create(dbindex, &key);
            match val.ts_mut() {
                Ok(ts) => {
                    match ts.decrby(timestamp, value) {
                        Ok(t) => Response::Integer(t),
                        Err(e) => Response::Error(e),
                    }
                }
                Err(_) => Response::Error("WRONGTYPE key does not hold a TimeSeries".to_owned()),
            }
        }
        "del" => {
            // TS.DEL key fromTimestamp toTimestamp
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'ts.del' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let from = match parser.get_i64(3) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid from timestamp".to_owned()) };
            let to = match parser.get_i64(4) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid to timestamp".to_owned()) };
            match db.get_mut(dbindex, &key) {
                Some(val) => match val.ts_del(from, to) {
                    Ok(count) => Response::Integer(count as i64),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "get" => {
            // TS.GET key
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ts.get' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.ts_get() {
                    Ok(Some(sample)) => Response::Array(vec![
                        Response::Integer(sample.timestamp),
                        Response::Data(format!("{}", sample.value).into_bytes()),
                    ]),
                    Ok(None) => Response::Array(vec![]),
                    Err(_) => Response::Error("WRONGTYPE key does not hold a TimeSeries".to_owned()),
                },
                None => Response::Array(vec![]),
            }
        }
        "range" => {
            // TS.RANGE key fromTimestamp toTimestamp [COUNT count]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'ts.range' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let from = match parser.get_i64(3) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid from timestamp".to_owned()) };
            let to = match parser.get_i64(4) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid to timestamp".to_owned()) };
            let mut count = None;
            let mut i = 5;
            while i < parser.argv.len() {
                if let Ok(arg) = parser.get_str(i) {
                    if arg.eq_ignore_ascii_case("count") {
                        i += 1;
                        count = parser.get_i64(i).ok().map(|n| n as usize);
                    }
                }
                i += 1;
            }
            match db.get(dbindex, &key) {
                Some(val) => match val.ts_range(from, to, count) {
                    Ok(samples) => {
                        let responses = samples.iter().map(|s| {
                            Response::Array(vec![
                                Response::Integer(s.timestamp),
                                Response::Data(format!("{}", s.value).into_bytes()),
                            ])
                        }).collect();
                        Response::Array(responses)
                    }
                    Err(_) => Response::Error("WRONGTYPE key does not hold a TimeSeries".to_owned()),
                },
                None => Response::Array(vec![]),
            }
        }
        "revrange" => {
            // TS.REVRANGE key fromTimestamp toTimestamp [COUNT count]
            if parser.argv.len() < 5 {
                return Response::Error("ERR wrong number of arguments for 'ts.revrange' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let from = match parser.get_i64(3) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid from timestamp".to_owned()) };
            let to = match parser.get_i64(4) { Ok(t) => t, Err(_) => return Response::Error("ERR invalid to timestamp".to_owned()) };
            let mut count = None;
            let mut i = 5;
            while i < parser.argv.len() {
                if let Ok(arg) = parser.get_str(i) {
                    if arg.eq_ignore_ascii_case("count") {
                        i += 1;
                        count = parser.get_i64(i).ok().map(|n| n as usize);
                    }
                }
                i += 1;
            }
            match db.get(dbindex, &key) {
                Some(val) => match val.ts_revrange(from, to, count) {
                    Ok(samples) => {
                        let responses = samples.iter().map(|s| {
                            Response::Array(vec![
                                Response::Integer(s.timestamp),
                                Response::Data(format!("{}", s.value).into_bytes()),
                            ])
                        }).collect();
                        Response::Array(responses)
                    }
                    Err(_) => Response::Error("WRONGTYPE key does not hold a TimeSeries".to_owned()),
                },
                None => Response::Array(vec![]),
            }
        }
        "info" => {
            // TS.INFO key
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ts.info' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            match db.get(dbindex, &key) {
                Some(val) => match val.ts_info() {
                    Ok(info) => Response::Data(info.into_bytes()),
                    Err(_) => Response::Error("WRONGTYPE key does not hold a TimeSeries".to_owned()),
                },
                None => Response::Error("ERR no such key".to_owned()),
            }
        }
        "alter" => {
            // TS.ALTER key [RETENTION retention] [LABELS label value ...]
            if parser.argv.len() < 3 {
                return Response::Error("ERR wrong number of arguments for 'ts.alter' command".to_owned());
            }
            let key = match parser.get_vec(2) { Ok(k) => k, Err(_) => return Response::Error("ERR syntax error".to_owned()) };
            let mut i = 3;
            while i < parser.argv.len() {
                if let Ok(arg) = parser.get_str(i) {
                    match arg.to_ascii_lowercase().as_str() {
                        "retention" => {
                            i += 1;
                            if let Ok(ret) = parser.get_i64(i) {
                                if let Some(val) = db.get_mut(dbindex, &key) {
                                    if let Ok(ts) = val.ts_mut() {
                                        ts.retention_ms = ret;
                                    }
                                }
                            }
                            i += 1;
                        }
                        "labels" => {
                            i += 1;
                            let mut labels = Vec::new();
                            while i + 1 < parser.argv.len() {
                                let label = match parser.get_str(i) { Ok(s) => s.to_owned(), Err(_) => break };
                                i += 1;
                                let value = match parser.get_str(i) { Ok(s) => s.to_owned(), Err(_) => break };
                                i += 1;
                                labels.push((label, value));
                            }
                            if let Some(val) = db.get_mut(dbindex, &key) {
                                if let Ok(ts) = val.ts_mut() {
                                    ts.labels = labels;
                                }
                            }
                        }
                        _ => { i += 1; }
                    }
                } else {
                    i += 1;
                }
            }
            Response::Status("OK".to_owned())
        }
        "queryindex" => {
            // TS.QUERYINDEX label=value [label=value ...]
            // Scan all keys in dbindex for TimeSeries with matching labels
            let mut filters = Vec::new();
            for i in 2..parser.argv.len() {
                if let Ok(s) = parser.get_str(i) {
                    if let Some(eq_pos) = s.find('=') {
                        filters.push((s[..eq_pos].to_owned(), s[eq_pos+1..].to_owned()));
                    }
                }
            }
            let mut result = Vec::new();
            let keys: Vec<&Vec<u8>> = db.data_keys(dbindex);
            for key in keys {
                if let Some(val) = db.get(dbindex, key) {
                    if let Ok(ts) = val.ts_ref() {
                        let matches = filters.iter().all(|(lk, lv)| {
                            ts.labels.iter().any(|(k, v)| k == lk && v == lv)
                        });
                        if matches {
                            result.push(Response::Data(key.clone()));
                        }
                    }
                }
            }
            Response::Array(result)
        }
        _ => Response::Error(format!("ERR Unknown subcommand '{}'", subcommand)),
    }
}

fn execute_command(
    parser: &mut ParsedCommand,
    db: &mut Database,
    client: &mut Client,
    log: &mut bool,
    write: &mut bool,
) -> Result<Response, ResponseError> {
    if parser.argv.is_empty() {
        return Err(ResponseError::NoReply);
    }
    let command_name = &*match db.mapped_command(
        &try_opt_validate!(parser.get_str(0), "Invalid command").to_ascii_lowercase(),
    ) {
        Some(c) => c,
        None => return Ok(Response::Error("unknown command".to_owned())),
    };

    *write = !command_properties(command_name)
        .flags
        .contains(CommandFlags::READONLY);

    // Server-wide statistics (shared across shards, read by INFO).
    db.stats.record_command();
    if *write {
        db.stats.record_write();
    }

    if db.config.requirepass.is_none() {
        client.auth = true;
    }
    // commands that are not executed before AUTH
    if command_name == "auth" {
        if parser.argv.len() < 2 || parser.argv.len() > 3 {
            return Ok(Response::Error("ERR wrong number of arguments for 'auth' command".to_owned()));
        }
        // AUTH username password  or  AUTH password
        let (username, password) = if parser.argv.len() == 3 {
            (
                try_opt_validate!(parser.get_str(1), "Invalid username"),
                try_opt_validate!(parser.get_str(2), "Invalid password"),
            )
        } else {
            ("default", try_opt_validate!(parser.get_str(1), "Invalid password"))
        };
        // Legacy requirepass takes precedence for "default" user
        if let Some(ref requirepass) = db.config.requirepass {
            if username == "default" {
                if password == requirepass {
                    client.auth = true;
                    client.current_user = "default".to_owned();
                    return Ok(Response::Status("OK".to_owned()));
                } else {
                    return Ok(Response::Error("ERR invalid password".to_owned()));
                }
            }
        }
        // ACL-based authentication
        if db.acl.authenticate(username, password) {
            client.auth = true;
            client.current_user = username.to_owned();
            return Ok(Response::Status("OK".to_owned()));
        }
        return Ok(Response::Error(
            "WRONGPASS invalid username-password pair or user is disabled".to_owned(),
        ));
    }

    if !client.auth {
        return Ok(Response::Error(
            "NOAUTH Authentication required.".to_owned(),
        ));
    }

    // commands that are not executed inside MULTI
    match command_name {
        "multi" => return Ok(multi(client)),
        "discard" => return Ok(discard(db, client)),
        "exec" => return Ok(exec(db, client)),
        _ => {}
    }
    if client.multi {
        if command_name == "watch" || command_name == "unwatch" {
            return Ok(Response::Error(
                "ERR WATCH not allowed inside MULTI".to_owned(),
            ));
        }
        client.multi_commands.push(parser.to_owned());
        return Ok(Response::Status("QUEUED".to_owned()));
    }
    if command_name == "select" {
        opt_validate!(parser.argv.len() == 2, "Wrong number of parameters");
        let dbindex = try_opt_validate!(parser.get_i64(1), "Invalid dbindex") as usize;
        if dbindex >= db.config.databases as usize {
            return Ok(Response::Error("ERR invalid DB index".to_owned()));
        }
        client.dbindex = dbindex;
        return Ok(Response::Status("OK".to_owned()));
    }
    let dbindex = client.dbindex;

    // Cluster redirect check (MOVED / ASK)
    if db.cluster.enabled {
        let dominated_commands = [
            "ping", "echo", "auth", "cluster", "asking", "readonly", "readwrite",
            "info", "config", "command", "client", "slowlog", "latency",
            "subscribe", "unsubscribe", "psubscribe", "punsubscribe",
            "publish", "spublish", "ssubscribe", "sunsubscribe",
            "multi", "exec", "discard", "watch", "unwatch",
            "select", "quit", "reset", "monitor", "shutdown",
            "save", "bgsave", "bgrewriteaof", "lastsave", "debug",
            "slaveof", "replconf", "wait", "sync", "psync",
            "acl", "function", "script",
        ];
        let is_admin = dominated_commands.contains(&command_name);
        if !is_admin {
            // Get the first key from the command to compute slot
            let props = command_properties(command_name);
            if props.first_key_index > 0 && parser.argv.len() > props.first_key_index as usize {
                if let Ok(key) = parser.get_vec(props.first_key_index as usize) {
                    let slot = crc16_slot(&key) as usize;
                    // Check ASKING flag first
                    if client.asking {
                        client.asking = false;
                        // Allow this one command through regardless of slot state
                    } else if db.cluster.role == NodeRole::Replica && !client.readonly {
                        // Replica nodes reject writes unless READONLY is set
                        if let Some(owner) = db.cluster.get_slot_owner(slot) {
                            let addr = format!("{}:{}", owner.ip, owner.port);
                            return Ok(Response::Error(format!("MOVED {} {}", slot, addr)));
                        }
                    } else {
                        // Check slot ownership
                        match &db.cluster.slot_owners[slot] {
                            Some(owner_id) if *owner_id != db.cluster.my_id => {
                                // Slot owned by another node
                                if let Some(owner) = db.cluster.nodes.get(owner_id) {
                                    let addr = format!("{}:{}", owner.ip, owner.port);
                                    return Ok(Response::Error(format!("MOVED {} {}", slot, addr)));
                                }
                            }
                            None => {
                                // Slot not assigned to any node
                                // In a real cluster this would be an error, but we allow it
                                // for single-node cluster setups
                            }
                            _ => {
                                // Slot owned by us, check migration state
                                match &db.cluster.slot_states[slot] {
                                    SlotState::Migrating(target_id) => {
                                        // Check if key exists locally
                                        if db.get(dbindex, &key).is_none() {
                                            // Key not here, redirect ASK
                                            if let Ok(target_nid) = cluster::hex_to_node_id(target_id) {
                                                if let Some(target) = db.cluster.nodes.get(&target_nid) {
                                                    let addr = format!("{}:{}", target.ip, target.port);
                                                    return Ok(Response::Error(format!("ASK {} {}", slot, addr)));
                                                }
                                            }
                                        }
                                    }
                                    SlotState::Importing(_) => {
                                        // Importing slot without ASKING -> MOVED to actual owner
                                        // (This is handled by the asking check above)
                                    }
                                    SlotState::Stable => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(match command_name {
        "pexpireat" => pexpireat(parser, db, dbindex),
        "pexpire" => pexpire(parser, db, dbindex),
        "expireat" => expireat(parser, db, dbindex),
        "expire" => expire(parser, db, dbindex),
        "echo" => echo(parser),
        "ttl" => ttl(parser, db, dbindex),
        "pttl" => pttl(parser, db, dbindex),
        "persist" => persist(parser, db, dbindex),
        "type" => dbtype(parser, db, dbindex),
        "set" => set(parser, db, dbindex),
        "setnx" => setnx(parser, db, dbindex),
        "setex" => setex(parser, db, dbindex),
        "psetex" => psetex(parser, db, dbindex),
        "debug" => debug(parser, db, dbindex),
        "del" => del(parser, db, dbindex),
        "dbsize" => dbsize(parser, db, dbindex),
        "append" => append(parser, db, dbindex),
        "get" => get(parser, db, dbindex),
        "getrange" => getrange(parser, db, dbindex),
        "mget" => mget(parser, db, dbindex),
        "substr" => getrange(parser, db, dbindex),
        "setrange" => setrange(parser, db, dbindex),
        "setbit" => setbit(parser, db, dbindex),
        "getbit" => getbit(parser, db, dbindex),
        "strlen" => strlen(parser, db, dbindex),
        "incr" => incr(parser, db, dbindex),
        "decr" => decr(parser, db, dbindex),
        "incrby" => incrby(parser, db, dbindex),
        "decrby" => decrby(parser, db, dbindex),
        "incrbyfloat" => incrbyfloat(parser, db, dbindex),
        "pfadd" => pfadd(parser, db, dbindex),
        "pfcount" => pfcount(parser, db, dbindex),
        "pfmerge" => pfmerge(parser, db, dbindex),
        "exists" => exists(parser, db, dbindex),
        "ping" => ping(parser, client),
        "flushdb" => flushdb(parser, db, dbindex),
        "flushall" => flushall(parser, db, dbindex),
        "lpush" => lpush(parser, db, dbindex),
        "rpush" => rpush(parser, db, dbindex),
        "lpushx" => lpushx(parser, db, dbindex),
        "rpushx" => rpushx(parser, db, dbindex),
        "lpop" => lpop(parser, db, dbindex),
        "rpop" => rpop(parser, db, dbindex),
        "lindex" => lindex(parser, db, dbindex),
        "linsert" => linsert(parser, db, dbindex),
        "llen" => llen(parser, db, dbindex),
        "lrange" => lrange(parser, db, dbindex),
        "lrem" => lrem(parser, db, dbindex),
        "lset" => lset(parser, db, dbindex),
        "ltrim" => ltrim(parser, db, dbindex),
        "rpoplpush" => rpoplpush(parser, db, dbindex),
        "brpoplpush" => brpoplpush(parser, db, dbindex)?,
        "brpop" => brpop(parser, db, dbindex)?,
        "blpop" => blpop(parser, db, dbindex)?,
        "sadd" => sadd(parser, db, dbindex),
        "srem" => srem(parser, db, dbindex),
        "sismember" => sismember(parser, db, dbindex),
        "smembers" => smembers(parser, db, dbindex),
        "srandmember" => srandmember(parser, db, dbindex),
        "spop" => spop(parser, db, dbindex),
        "smove" => smove(parser, db, dbindex),
        "scard" => scard(parser, db, dbindex),
        "sdiff" => sdiff(parser, db, dbindex),
        "sdiffstore" => sdiffstore(parser, db, dbindex),
        "sinter" => sinter(parser, db, dbindex),
        "sinterstore" => sinterstore(parser, db, dbindex),
        "sunion" => sunion(parser, db, dbindex),
        "sunionstore" => sunionstore(parser, db, dbindex),
        "zadd" => zadd(parser, db, dbindex),
        "zcard" => zcard(parser, db, dbindex),
        "zscore" => zscore(parser, db, dbindex),
        "zincrby" => zincrby(parser, db, dbindex),
        "zrem" => zrem(parser, db, dbindex),
        "zremrangebylex" => zremrangebylex(parser, db, dbindex),
        "zremrangebyscore" => zremrangebyscore(parser, db, dbindex),
        "zremrangebyrank" => zremrangebyrank(parser, db, dbindex),
        "zcount" => zcount(parser, db, dbindex),
        "zlexcount" => zlexcount(parser, db, dbindex),
        "zrange" => zrange(parser, db, dbindex),
        "zrevrange" => zrevrange(parser, db, dbindex),
        "zrangebyscore" => zrangebyscore(parser, db, dbindex),
        "zrevrangebyscore" => zrevrangebyscore(parser, db, dbindex),
        "zrangebylex" => zrangebylex(parser, db, dbindex),
        "zrevrangebylex" => zrevrangebylex(parser, db, dbindex),
        "zrank" => zrank(parser, db, dbindex),
        "zrevrank" => zrevrank(parser, db, dbindex),
        "zunionstore" => zunionstore(parser, db, dbindex),
        "zinterstore" => zinterstore(parser, db, dbindex),
        "hset" => hset(parser, db, dbindex),
        "hsetnx" => hsetnx(parser, db, dbindex),
        "hget" => hget(parser, db, dbindex),
        "hmset" => hmset(parser, db, dbindex),
        "hmget" => hmget(parser, db, dbindex),
        "hdel" => hdel(parser, db, dbindex),
        "hlen" => hlen(parser, db, dbindex),
        "hstrlen" => hstrlen(parser, db, dbindex),
        "hexists" => hexists(parser, db, dbindex),
        "hkeys" => hkeys(parser, db, dbindex),
        "hvals" => hvals(parser, db, dbindex),
        "hgetall" => hgetall(parser, db, dbindex),
        "hincrby" => hincrby(parser, db, dbindex),
        "hincrbyfloat" => hincrbyfloat(parser, db, dbindex),
        "getset" => getset(parser, db, dbindex),
        "mset" => mset(parser, db, dbindex),
        "msetnx" => msetnx(parser, db, dbindex),
        "rename" => rename(parser, db, dbindex),
        "renamenx" => renamenx(parser, db, dbindex),
        "randomkey" => randomkey(parser, db, dbindex),
        "time" => time_command(parser),
        "bitcount" => bitcount(parser, db, dbindex),
        "bitpos" => bitpos(parser, db, dbindex),
        "bitop" => bitop(parser, db, dbindex),
        "dump" => dump(parser, db, dbindex),
        "keys" => keys(parser, db, dbindex),
        "watch" => watch(parser, db, dbindex, client.id, &mut client.watched_keys),
        "unwatch" => unwatch(parser, db, client.id, &mut client.watched_keys),
        "subscribe" => subscribe(
            parser,
            db,
            &mut client.subscriptions,
            client.pattern_subscriptions.len(),
            &client.rawsender,
        )?,
        "unsubscribe" => unsubscribe(
            parser,
            db,
            &mut client.subscriptions,
            client.pattern_subscriptions.len(),
            &client.rawsender,
        )?,
        "psubscribe" => psubscribe(
            parser,
            db,
            client.subscriptions.len(),
            &mut client.pattern_subscriptions,
            &client.rawsender,
        )?,
        "punsubscribe" => punsubscribe(
            parser,
            db,
            client.subscriptions.len(),
            &mut client.pattern_subscriptions,
            &client.rawsender,
        )?,
        "publish" => publish(parser, db),
        "ssubscribe" => ssubscribe(parser, db, client)?,
        "sunsubscribe" => sunsubscribe(parser, db, client)?,
        "spublish" => spublish(parser, db),
        "monitor" => {
            *log = false;
            monitor(parser, db, client.rawsender.clone())
        }
        "info" => info(parser, db),
        "scan" => scan_command(parser, db, dbindex),
        "sscan" => sscan_command(parser, db, dbindex),
        "hscan" => hscan_command(parser, db, dbindex),
        "zscan" => zscan_command(parser, db, dbindex),
        "move" => move_key(parser, db, dbindex),
        "sort" => sort_command(parser, db, dbindex),
        "config" => config_command(parser, db),
        "object" => object_command(parser, db, dbindex),
        "save" => save_command(parser, db),
        "lastsave" => lastsave_command(parser, db),
        "shutdown" => shutdown_command(parser, db),
        "bgsave" => bgsave_command(parser, db),
        "bgrewriteaof" => bgrewriteaof_command(parser, db),
        "role" => role_command(parser, db),
        "pubsub" => pubsub_command(parser, db),
        "client" => client_command(parser, db, client),
        "slowlog" => slowlog_command(parser, db),
        "command" => command_introspection(parser, db),
        "cluster" => cluster_command(parser, db),
        "sentinel" => sentinel_command(parser, db),
        "latency" => latency_command(parser, db),
        "slaveof" => slaveof_command(parser, db),
        "replconf" => replconf_command(parser, db),
        "wait" => wait_command(parser, db),
        "sync" => sync_command(parser, db),
        "psync" => psync_command(parser, db),
        "asking" => asking_command(parser, client),
        "readonly" => readonly_command(parser, client),
        "readwrite" => readwrite_command(parser, client),
        "restore" => restore_command(parser, db),
        "restore-asking" => restore_command(parser, db),
        "migrate" => migrate_command(parser, db),
        "eval" => eval_command(parser, db, dbindex),
        "evalsha" => evalsha_command(parser, db, dbindex),
        "script" => script_command(parser, db),
        "pfselftest" => pfselftest_command(parser, db),
        "pfdebug" => pfdebug_command(parser, db),
        // Phase 1: New Redis 8.x commands
        "lpos" => lpos(parser, db, dbindex),
        "lmove" => lmove(parser, db, dbindex),
        "blmove" => blmove(parser, db, dbindex)?,
        "lmpop" => lmpop(parser, db, dbindex),
        "blmpop" => blmpop(parser, db, dbindex)?,
        "smismember" => smismember(parser, db, dbindex),
        "sintercard" => sintercard(parser, db, dbindex),
        "zmscore" => zmscore(parser, db, dbindex),
        "zrandmember" => zrandmember(parser, db, dbindex),
        "copy" => copy(parser, db, dbindex),
        "unlink" => unlink(parser, db, dbindex),
        "touch" => touch(parser, db, dbindex),
        "getdel" => getdel(parser, db, dbindex),
        "getex" => getex(parser, db, dbindex),
        "lcs" => lcs(parser, db, dbindex),
        "hrandfield" => hrandfield(parser, db, dbindex),
        "waitaof" => waitaof_command(parser, db),
        "zdiff" => zdiff(parser, db, dbindex),
        "zdiffstore" => zdiffstore(parser, db, dbindex),
        "zinter" => zinter_command(parser, db, dbindex),
        "zunion" => zunion_command(parser, db, dbindex),
        "zintercard" => zintercard(parser, db, dbindex),
        "zmpop" => zmpop(parser, db, dbindex),
        "bzmpop" => bzmpop(parser, db, dbindex)?,
        "bzpopmin" => bzpopmin(parser, db, dbindex)?,
        "bzpopmax" => bzpopmax(parser, db, dbindex)?,
        // Phase 2: Stream commands
        "xadd" => xadd(parser, db, dbindex),
        "xlen" => xlen(parser, db, dbindex),
        "xrange" => xrange(parser, db, dbindex),
        "xrevrange" => xrevrange(parser, db, dbindex),
        "xdel" => xdel(parser, db, dbindex),
        "xtrim" => xtrim(parser, db, dbindex),
        "xread" => xread(parser, db, dbindex),
        "xgroup" => xgroup(parser, db, dbindex),
        "xack" => xack(parser, db, dbindex),
        "xpending" => xpending(parser, db, dbindex),
        "xinfo" => xinfo(parser, db, dbindex),
        "xsetid" => xsetid(parser, db, dbindex),
        // Phase 3: Geo commands
        "geoadd" => geoadd(parser, db, dbindex),
        "geodist" => geodist(parser, db, dbindex),
        "geohash" => geohash_cmd(parser, db, dbindex),
        "geopos" => geopos(parser, db, dbindex),
        "geosearch" => geosearch(parser, db, dbindex),
        "geosearchstore" => geosearchstore(parser, db, dbindex),
        // Phase 1.4: Hash per-field expiration
        "hexpire" => hexpire(parser, db, dbindex),
        "hpexpire" => hpexpire(parser, db, dbindex),
        "hexpireat" => hexpireat(parser, db, dbindex),
        "hpexpireat" => hpexpireat(parser, db, dbindex),
        "httl" => httl(parser, db, dbindex),
        "hpttl" => hpttl(parser, db, dbindex),
        "hpersist" => hpersist(parser, db, dbindex),
        // Phase 6: Sharded Pub/Sub + Server commands
        "reset" => reset_command(parser, client),
        // Phase 4: ACL system
        "acl" => acl_command(parser, db, client),
        // Phase 5: Lua scripting
        "function" => function_command(parser, db),
        "fcall" => fcall_command(parser, db, dbindex),
        "fcall_ro" => fcall_ro_command(parser, db, dbindex),
        // Phase 7: RedisBloom commands
        "bf" => bf_command(parser, db, dbindex),
        "cf" => cf_command(parser, db, dbindex),
        "tdigest" => tdigest_command(parser, db, dbindex),
        "topk" => topk_command(parser, db, dbindex),
        "json" => json_command(parser, db, dbindex),
        "ft" => ft_command(parser, db),
        "ts" => ts_command(parser, db, dbindex),
        cmd => Response::Error(format!("ERR unknown command \"{}\"", cmd)),
    })
}

pub fn command(
    mut parser: ParsedCommand,
    db: &mut Database,
    client: &mut Client,
) -> Result<Response, ResponseError> {
    let mut log = true;
    let mut write = false;
    let r = execute_command(&mut parser, db, client, &mut log, &mut write);
    // TODO: only log if there's anyone listening
    if log {
        db.log_command(client.dbindex, &parser, write);
    }
    r
}

#[cfg(test)]
mod test_command {
    use std::collections::HashSet;
    use std::str::from_utf8;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use config::Config;
    use database::{Database, Value};
    use logger::{Level, Logger};
    use parser::{Argument, ParsedCommand};
    use response::{Response, ResponseError};
    use util::mstime;

    use super::{command, Client};
    use std::time::Duration;

    macro_rules! parser {
        ($str: expr) => {{
            let mut _args = Vec::new();
            let mut pos = 0;
            for segment in $str.split(|x| *x == b' ') {
                _args.push(Argument {
                    pos: pos,
                    len: segment.len(),
                });
                pos += segment.len() + 1;
            }
            ParsedCommand::new($str, _args)
        }};
    }

    fn getstr(database: &Database, key: &[u8]) -> String {
        match database.get(0, &key.to_vec()).unwrap() {
            Value::String(value) => from_utf8(&*value.to_vec()).unwrap().to_owned(),
            _ => panic!("Got non-string"),
        }
    }

    #[test]
    fn nocommand() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let parser = ParsedCommand::new(b"", Vec::new());
        let response = command(parser, &mut db, &mut Client::mock()).unwrap_err();
        match response {
            ResponseError::NoReply => {}
            _ => assert!(false),
        };
    }

    #[test]
    fn set_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"set key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!("value", getstr(&db, b"key"));

        assert_eq!(
            command(parser!(b"set key2 value xx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"get key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"set key2 value nx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!("value", getstr(&db, b"key2"));
        assert_eq!(
            command(parser!(b"set key2 valuf xx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!("valuf", getstr(&db, b"key2"));
        assert_eq!(
            command(parser!(b"set key2 value nx"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!("valuf", getstr(&db, b"key2"));

        assert_eq!(
            command(
                parser!(b"set key3 value px 1234"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key3".to_vec()).unwrap().clone();
        assert!(exp >= now + 1000);
        assert!(exp <= now + 1234);

        assert_eq!(
            command(
                parser!(b"set key3 value ex 1234"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key3".to_vec()).unwrap().clone();
        assert!(exp >= now + 1233 * 1000);
        assert!(exp <= now + 1234 * 1000);
    }

    #[test]
    fn setnx_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"setnx key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!("value", getstr(&db, b"key"));
        assert_eq!(
            command(parser!(b"setnx key valuf"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!("value", getstr(&db, b"key"));
    }

    #[test]
    fn setex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"setex key 1234 value"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now + 1233 * 1000);
        assert!(exp <= now + 1234 * 1000);
    }

    #[test]
    fn psetex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"psetex key 1234 value"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now + 1000);
        assert!(exp <= now + 1234);
    }

    #[test]
    fn get_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!("value", getstr(&db, b"key"));
    }

    #[test]
    fn mget_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"mget key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Nil,
            ])
        );
    }

    #[test]
    fn getrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"getrange key 1 -2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("alu".to_owned().into_bytes())
        );
    }

    #[test]
    fn setrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"setrange key 1 i"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
        assert_eq!("vilue", getstr(&db, b"key"));
    }

    #[test]
    fn setbit_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"setbit key 1 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"setbit key 1 1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!("@", getstr(&db, b"key"));
        assert_eq!(
            command(parser!(b"setbit key 1 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn getbit_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"getbit key 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"getbit key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"getbit key 6"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"getbit key 7"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"getbit key 800"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn strlen_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"strlen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"strlen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
    }

    #[test]
    fn del_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"del key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn debug_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert!(
            command(parser!(b"debug object key"), &mut db, &mut Client::mock())
                .unwrap()
                .is_status()
        );
    }

    #[test]
    fn dbsize_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"dbsize"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"dbsize"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert!(db
            .get_or_create(0, &b"key2".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"dbsize"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn exists_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"exists key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"exists key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn expire_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"expire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"expire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now);
        assert!(exp <= now + 100 * 1000);
    }

    #[test]
    fn pexpire_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"pexpire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"pexpire key 100"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let now = mstime();
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert!(exp >= now);
        assert!(exp <= now + 100);
    }

    #[test]
    fn expireat_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let now = mstime() / 1000;
        let exp_exp = now + 100;
        let qs = format!("expireat key {}", exp_exp);
        let q = qs.as_bytes();
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert_eq!(exp, exp_exp * 1000);
    }

    #[test]
    fn pexpireat_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let now = mstime();
        let exp_exp = now + 100;
        let qs = format!("pexpireat key {}", exp_exp);
        let q = qs.as_bytes();
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(q), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        let exp = db.get_msexpiration(0, &b"key".to_vec()).unwrap().clone();
        assert_eq!(exp, exp_exp);
    }

    #[test]
    fn ttl_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"ttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-2)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"ttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-1)
        );
        db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
        match command(parser!(b"ttl key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Integer(i) => assert!(i <= 100 && i > 80),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn pttl_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"pttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-2)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"pttl key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-1)
        );
        db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
        match command(parser!(b"pttl key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Integer(i) => assert!(i <= 100 * 1000 && i > 80 * 1000),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn persist_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"persist key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"persist key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        db.set_msexpiration(0, b"key".to_vec(), mstime() + 100 * 1000);
        assert_eq!(
            command(parser!(b"persist key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn type_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let set_max_intset_entries = db.config.set_max_intset_entries;
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"none".to_vec())
        );

        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"string".to_vec())
        );
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"1".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"string".to_vec())
        );

        assert!(db.remove(0, &b"key".to_vec()).is_some());
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .push(b"1".to_vec(), true)
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"list".to_vec())
        );

        assert!(db.remove(0, &b"key".to_vec()).is_some());
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .sadd(b"1".to_vec(), set_max_intset_entries)
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"set".to_vec())
        );

        assert!(db.remove(0, &b"key".to_vec()).is_some());
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .zadd(3.0, b"1".to_vec(), false, false, false, false)
            .is_ok());
        assert_eq!(
            command(parser!(b"type key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"zset".to_vec())
        );

        // TODO: hash
    }

    #[test]
    fn serialize_status() {
        let response = Response::Status("OK".to_owned());
        assert_eq!(response.as_bytes(), b"+OK\r\n");
    }

    #[test]
    fn serialize_error() {
        let response = Response::Error("ERR Invalid command".to_owned());
        assert_eq!(response.as_bytes(), b"-ERR Invalid command\r\n");
    }

    #[test]
    fn serialize_string() {
        let response = Response::Data(b"ERR Invalid command".to_vec());
        assert_eq!(response.as_bytes(), b"$19\r\nERR Invalid command\r\n");
    }

    #[test]
    fn serialize_nil() {
        let response = Response::Nil;
        assert_eq!(response.as_bytes(), b"$-1\r\n");
    }

    #[test]
    fn serialize_integer() {
        let response = Response::Integer(123);
        assert_eq!(response.as_bytes(), b":123\r\n");
    }

    #[test]
    fn append_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"append key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"append key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(10)
        );
        assert_eq!(
            db.get(0, &b"key".to_vec()).unwrap().get().unwrap(),
            b"valuevalue".to_vec()
        );
    }

    #[test]
    fn incr_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"incr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"incr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn incrby_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"incrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"incrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(10)
        );
    }

    #[test]
    fn incrbyfloat_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        match command(
            parser!(b"incrbyfloat key 2.1"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap()
        {
            Response::Data(v) => {
                assert_eq!(v[0], '2' as u8);
                assert_eq!(v[1], '.' as u8);
                assert!(v[2] == '1' as u8 || v[2] == '0' as u8);
            }
            _ => panic!("Unexpected response"),
        }
        match command(
            parser!(b"incrbyfloat key 4.1"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap()
        {
            Response::Data(v) => {
                assert_eq!(v[0], '6' as u8);
                assert_eq!(v[1], '.' as u8);
                assert!(v[2] == '1' as u8 || v[2] == '2' as u8);
            }
            _ => panic!("Unexpected response"),
        }
    }

    #[test]
    fn pfadd_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFADD key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key 1 2 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key 1 2 3 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn pfcount1_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"PFADD key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn pfcount2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFADD key1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key2 1 2 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key1 key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(4)
        );
    }

    #[test]
    fn pfmerge_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"PFADD key1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key3 1 2 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"PFADD key4 5 6"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"PFMERGE key key1 key2 key3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"PFMERGE key key4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"PFCOUNT key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(6)
        );
    }

    #[test]
    fn decr_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"decr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-1)
        );
        assert_eq!(
            command(parser!(b"decr key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-2)
        );
    }

    #[test]
    fn decrby_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"decrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-5)
        );
        assert_eq!(
            command(parser!(b"decrby key 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(-10)
        );
    }

    #[test]
    fn lpush_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"lpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"lpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn rpush_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lpop_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"lpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"lpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn rpop_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"rpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"rpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"rpop key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn lindex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lindex key 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
    }

    #[test]
    fn linsert_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valug"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"linsert key before valug valuf"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn llen_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lpushx_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"lpushx key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"lpush key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"lpushx key value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valug"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lrange key 0 -1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Data("valuf".to_owned().into_bytes()),
                Response::Data("valug".to_owned().into_bytes()),
            ])
        );
    }

    #[test]
    fn lrem_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lrem key 2 value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn lset_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"lset key 2 valuf"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"lrange key 2 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data("valuf".to_owned().into_bytes()),])
        );
    }

    #[test]
    fn rpoplpush_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"rpoplpush key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"rpoplpush key key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        assert_eq!(
            command(parser!(b"llen key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn brpoplpush_nowait() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"llen key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"brpoplpush key key2 0"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Data("valuf".to_owned().into_bytes())
        );
    }

    #[test]
    fn brpoplpush_waiting() {
        let db = Arc::new(Mutex::new(Database::new(Config::new(Logger::new(
            Level::Warning,
        )))));
        let (tx, rx) = channel();
        let db2 = db.clone();
        thread::spawn(move || {
            let r = match command(
                parser!(b"brpoplpush key1 key2 0"),
                &mut db.lock().unwrap(),
                &mut Client::mock(),
            )
            .unwrap_err()
            {
                ResponseError::Wait(receiver) => {
                    tx.send(1).unwrap();
                    receiver
                }
                _ => panic!("Unexpected error"),
            };
            r.recv().unwrap();
            assert_eq!(
                command(
                    parser!(b"brpoplpush key1 key2 0"),
                    &mut db.lock().unwrap(),
                    &mut Client::mock()
                )
                .unwrap(),
                Response::Data("value".to_owned().into_bytes())
            );
            tx.send(2).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 1);

        command(
            parser!(b"rpush key1 value"),
            &mut db2.lock().unwrap(),
            &mut Client::mock(),
        )
        .unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
        assert_eq!(
            command(
                parser!(b"lrange key2 0 -1"),
                &mut db2.lock().unwrap(),
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data("value".to_owned().into_bytes()),])
        );
    }

    #[test]
    fn brpoplpush_timeout() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let receiver = match command(
            parser!(b"brpoplpush key key2 1"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap_err()
        {
            ResponseError::Wait(receiver) => receiver,
            _ => panic!("Unexpected response"),
        };
        assert!(receiver.try_recv().is_err());
        thread::sleep(Duration::from_millis(1400));
        assert_eq!(receiver.try_recv().unwrap().is_some(), false);
    }

    #[test]
    fn brpop_nowait() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key1 value"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"brpop key1 key2 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("key1".to_owned().into_bytes()),
                Response::Data("value".to_owned().into_bytes()),
            ])
        );
    }

    #[test]
    fn brpop_waiting() {
        let db = Arc::new(Mutex::new(Database::new(Config::new(Logger::new(
            Level::Warning,
        )))));
        let (tx, rx) = channel();
        let db2 = db.clone();
        thread::spawn(move || {
            let r = match command(
                parser!(b"brpop key1 key2 0"),
                &mut db.lock().unwrap(),
                &mut Client::mock(),
            )
            .unwrap_err()
            {
                ResponseError::Wait(receiver) => {
                    tx.send(1).unwrap();
                    receiver
                }
                _ => panic!("Unexpected error"),
            };
            r.recv().unwrap();
            assert_eq!(
                command(
                    parser!(b"brpop key1 key2 0"),
                    &mut db.lock().unwrap(),
                    &mut Client::mock()
                )
                .unwrap(),
                Response::Array(vec![
                    Response::Data("key2".to_owned().into_bytes()),
                    Response::Data("value".to_owned().into_bytes()),
                ])
            );
            tx.send(2).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 1);

        {
            command(
                parser!(b"rpush key2 value"),
                &mut db2.lock().unwrap(),
                &mut Client::mock(),
            )
            .unwrap();
            assert_eq!(rx.recv().unwrap(), 2);
        }

        {
            assert_eq!(
                command(
                    parser!(b"llen key2"),
                    &mut db2.lock().unwrap(),
                    &mut Client::mock()
                )
                .unwrap(),
                Response::Integer(0)
            );
        }
    }

    #[test]
    fn brpop_timeout() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let receiver = match command(parser!(b"brpop key1 key2 1"), &mut db, &mut Client::mock())
            .unwrap_err()
        {
            ResponseError::Wait(receiver) => receiver,
            _ => panic!("Unexpected response"),
        };
        assert!(receiver.try_recv().is_err());
        thread::sleep(Duration::from_millis(1400));
        assert_eq!(receiver.try_recv().unwrap().is_some(), false);
    }

    #[test]
    fn ltrim_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key value"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"rpush key valuf"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"ltrim key 1 -2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"lrange key 0 -1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![
                Response::Data("value".to_owned().into_bytes()),
                Response::Data("valuf".to_owned().into_bytes()),
            ])
        );
    }

    #[test]
    fn sadd_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn srem_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"srem key 2 3 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"srem key 2 3 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn sismember_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"sismember key 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"sismember key 4"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn smembers_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        match command(parser!(b"smembers key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => {
                let mut array = arr
                    .iter()
                    .map(|x| match x {
                        Response::Data(d) => d.clone(),
                        _ => panic!("Expected data"),
                    })
                    .collect::<Vec<_>>();
                array.sort_by(|a, b| a.cmp(b));
                assert_eq!(array, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn srandmember_command1() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"srandmember key"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Data(b"1".to_vec())
                || r == Response::Data(b"2".to_vec())
                || r == Response::Data(b"3".to_vec())
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn srandmember_command2() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"srandmember key 1"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Array(vec![Response::Data(b"1".to_vec())])
                || r == Response::Array(vec![Response::Data(b"2".to_vec())])
                || r == Response::Array(vec![Response::Data(b"3".to_vec())])
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn spop_command1() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"spop key"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Data(b"1".to_vec())
                || r == Response::Data(b"2".to_vec())
                || r == Response::Data(b"3".to_vec())
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn spop_command2() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        let r = command(parser!(b"spop key 1"), &mut db, &mut Client::mock()).unwrap();
        assert!(
            r == Response::Array(vec![Response::Data(b"1".to_vec())])
                || r == Response::Array(vec![Response::Data(b"2".to_vec())])
                || r == Response::Array(vec![Response::Data(b"3".to_vec())])
        );
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn smove_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"sadd k1 1 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );

        assert_eq!(
            command(parser!(b"smove k1 k2 1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"smove k1 k2 1"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            command(parser!(b"smove k1 k2 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"smove k1 k2 2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            command(parser!(b"smove k1 k2 5"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );

        assert_eq!(
            command(parser!(b"set k3 value"), &mut db, &mut Client::mock()).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"smove k1 k3 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of \
                 value"
                    .to_owned()
            )
        );
    }

    #[test]
    fn scard_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"scard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn sdiff_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sdiff key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sdiffstore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 3 4 5"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"sdiffstore target key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );

        let set = vec![b"1".to_vec(), b"2".to_vec()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut set2 = Value::Nil;
        set2.create_set(set);
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &set2);
    }

    #[test]
    fn sdiff2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(parser!(b"sdiff key1 key2"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data(b"1".to_vec()),])
        );
    }

    #[test]
    fn sinter_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sinter key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sinter2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4 5"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sinter key1 key2"), &mut db, &mut Client::mock()).unwrap()
        {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sinter_command_nil() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(
            parser!(b"sinter key1 key2 nokey"),
            &mut db,
            &mut Client::mock(),
        )
        .unwrap()
        {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn sinterstore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 5"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"sinterstore target key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );

        let set = vec![b"3".to_vec(), b"2".to_vec()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut set2 = Value::Nil;
        set2.create_set(set);
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &set2);
    }

    #[test]
    fn sunion_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key 1 2 3"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sunion key"), &mut db, &mut Client::mock()).unwrap() {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(r, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }

    #[test]
    fn sunion2_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4"), &mut db, &mut Client::mock()).unwrap();

        let arr = match command(parser!(b"sunion key1 key2"), &mut db, &mut Client::mock()).unwrap()
        {
            Response::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let mut r = arr
            .iter()
            .map(|el| match el {
                Response::Data(el) => el.clone(),
                _ => panic!("Expected data"),
            })
            .collect::<Vec<_>>();
        r.sort();
        assert_eq!(
            r,
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]
        );
    }

    #[test]
    fn sunionstore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        command(parser!(b"sadd key1 1 2 3"), &mut db, &mut Client::mock()).unwrap();
        command(parser!(b"sadd key2 2 3 4"), &mut db, &mut Client::mock()).unwrap();
        assert_eq!(
            command(
                parser!(b"sunionstore target key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );

        let set = vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut set2 = Value::Nil;
        set2.create_set(set);
        assert_eq!(db.get(0, &b"target".to_vec()).unwrap(), &set2);
    }

    #[test]
    fn zadd_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zadd key XX 2 a 3 b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zadd key CH 2 a 2 b 2 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zadd key NX 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zadd key XX CH 2 b 2 d 2 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zcard_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zcard key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn zscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zadd key 1 a 2 b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zscore key a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"1".to_vec())
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"zscore key2 a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn zincrby_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(parser!(b"zincrby key 3 a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"3".to_vec())
        );
        assert_eq!(
            command(parser!(b"zincrby key 4 a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"7".to_vec())
        );
    }

    #[test]
    fn zcount_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zcount key 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zcount key 2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zcount key (2 3"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zcount key -inf inf"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
    }

    #[test]
    fn zlexcount_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zlexcount key [a [b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zlexcount key [a [b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zlexcount key (b [c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zlexcount key - +"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(4)
        );
    }

    #[test]
    fn zremrangebyscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key (2 4"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyscore key -inf inf"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zremrangebylex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key [b (d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key [b (d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key (b [d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebylex key - +"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zremrangebyrank_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 1 2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 5 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 1 -1"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zremrangebyrank key 0 -1"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrange key 0 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrange key 0 0 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrange key -2 -1 WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
    }

    #[test]
    fn zrevrange_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrevrange key 0 0"), &mut db, &mut Client::mock()).unwrap(),
            Response::Array(vec![Response::Data(b"d".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrevrange key 0 0 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrange key -2 -1 WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"2".to_vec()),
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
    }

    #[test]
    fn zrangebyscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key 1 (2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key 1 1 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key (2 inf WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrangebyscore key -inf inf withscores LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
            ])
        );
    }

    #[test]
    fn zrevrangebyscore_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key (2 1"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key 1 1 withscoreS"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key inf (2 WITHSCORES"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"4".to_vec()),
                Response::Data(b"c".to_vec()),
                Response::Data(b"3".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebyscore key inf -inf withscores LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"2".to_vec()),
                Response::Data(b"a".to_vec()),
                Response::Data(b"1".to_vec()),
            ])
        );
    }

    #[test]
    fn zrangebylex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrangebylex key [a (b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrangebylex key (b +"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"d".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrangebylex key - + LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"c".to_vec()),
                Response::Data(b"d".to_vec()),
            ])
        );
    }

    #[test]
    fn zrevrangebylex_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 0 a 0 b 0 c 0 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebylex key (b [a"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![Response::Data(b"a".to_vec()),])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebylex key + (b"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"d".to_vec()),
                Response::Data(b"c".to_vec()),
            ])
        );
        assert_eq!(
            command(
                parser!(b"zrevrangebylex key + - LIMIT 2 10"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Array(vec![
                Response::Data(b"b".to_vec()),
                Response::Data(b"a".to_vec()),
            ])
        );
    }

    #[test]
    fn zrank_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrank key a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            command(parser!(b"zrank key b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zrank key e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn zrevrank_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zrevrank key a"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zrevrank key b"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zrevrank key e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn zrem_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zrem key a b d e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zrem key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zunionstore_command_short() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
    }

    #[test]
    fn zunionstore_command_short2() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 2 key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
    }

    #[test]
    fn zunionstore_command_weights() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 Weights 1 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key d"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"8".to_vec())
        );
    }

    #[test]
    fn zunionstore_command_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 9 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"9".to_vec())
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 aggregate min"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"3".to_vec())
        );
    }

    #[test]
    fn zunionstore_command_weights_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 3 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zunionstore key 3 key1 key2 key3 weights 1 2 3 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(5)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"6".to_vec())
        );
    }

    #[test]
    fn zinterstore_command_short() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 3 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zinterstore_command_weights() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c 4 d"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(4)
        );
        assert_eq!(
            command(parser!(b"zadd key2 4 d 5 e"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            command(parser!(b"zadd key3 0 d"), &mut db, &mut Client::mock()).unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 3 key1 key2 key3 Weights 1 2 3"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key d"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"12".to_vec())
        );
    }

    #[test]
    fn zinterstore_command_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 9 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"9".to_vec())
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2 aggregate min"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"3".to_vec())
        );
    }

    #[test]
    fn zinterstore_command_weights_aggregate() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert_eq!(
            command(
                parser!(b"zadd key1 1 a 2 b 3 c"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zadd key2 3 c 4 d 5 e"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            command(
                parser!(b"zinterstore key 2 key1 key2 weights 1 2 aggregate max"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            command(parser!(b"zscore key c"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"6".to_vec())
        );
    }

    #[test]
    fn select_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(client.dbindex, 1);
    }

    #[test]
    fn flushdb_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key value"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key valuf"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"flushdb"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data("value".to_owned().into_bytes())
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn flushall_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key value"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"set key valuf"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"flushall"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        command(parser!(b"select 0"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
        command(parser!(b"select 1"), &mut db, &mut client).unwrap();
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn subscribe_publish_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let (tx, rx) = channel();
        let mut client = Client::new(tx, 0);
        assert!(command(parser!(b"subscribe channel"), &mut db, &mut client).is_err());
        assert_eq!(
            command(
                parser!(b"publish channel hello-world"),
                &mut db,
                &mut Client::mock()
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert!(command(parser!(b"unsubscribe channel"), &mut db, &mut client).is_err());

        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            Response::Array(vec![
                Response::Data(b"subscribe".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Integer(1),
            ])
        );
        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            Response::Array(vec![
                Response::Data(b"message".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Data(b"hello-world".to_vec()),
            ])
        );
        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            Response::Array(vec![
                Response::Data(b"unsubscribe".to_vec()),
                Response::Data(b"channel".to_vec()),
                Response::Integer(0),
            ])
        );
    }

    #[test]
    fn auth_command() {
        let mut config = Config::new(Logger::new(Level::Warning));
        config.requirepass = Some("helloworld".to_owned());
        let mut db = Database::new(config);
        let mut client = Client::mock();
        assert!(command(parser!(b"get key"), &mut db, &mut client)
            .unwrap()
            .is_error());
        assert_eq!(client.auth, false);
        assert!(command(parser!(b"auth channel"), &mut db, &mut client)
            .unwrap()
            .is_error());
        assert_eq!(client.auth, false);
        assert!(!command(parser!(b"auth helloworld"), &mut db, &mut client)
            .unwrap()
            .is_error());
        assert_eq!(client.auth, true);
    }

    #[test]
    fn dump_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"1".to_vec())
            .is_ok());
        assert_eq!(
            command(parser!(b"dump key"), &mut db, &mut Client::mock()).unwrap(),
            Response::Data(b"\x00\xc0\x01\x07\x00\xd9J2E\xd9\xcb\xc4\xe6".to_vec())
        );
    }

    #[test]
    fn keys_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        assert!(db
            .get_or_create(0, &b"key1".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert!(db
            .get_or_create(0, &b"key2".to_vec())
            .set(b"value".to_vec())
            .is_ok());
        assert!(db
            .get_or_create(0, &b"key3".to_vec())
            .set(b"value".to_vec())
            .is_ok());

        match command(parser!(b"KEYS *"), &mut db, &mut client).unwrap() {
            Response::Array(resp) => assert_eq!(3, resp.len()),
            _ => panic!("Keys failed"),
        };

        assert_eq!(
            command(parser!(b"KEYS key1"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"key1".to_vec())])
        );
        assert_eq!(
            command(parser!(b"KEYS key[^23]"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"key1".to_vec())])
        );
        assert_eq!(
            command(parser!(b"KEYS key[1]"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"key1".to_vec())])
        );
    }

    #[test]
    fn multi_exec_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());

        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"append key 1"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );

        // still has the old value
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value".to_vec()
        );

        assert_eq!(
            command(parser!(b"EXEC"), &mut db, &mut client).unwrap(),
            Response::Array(vec![
                Response::Integer(6),
                Response::Data(b"value1".to_vec()),
            ])
        );

        // value is updated
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value1".to_vec()
        );

        // multi status back to normal
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data(b"value1".to_vec())
        );
    }

    #[test]
    fn multi_discard_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        assert!(db
            .get_or_create(0, &b"key".to_vec())
            .set(b"value".to_vec())
            .is_ok());

        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"append key 1"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );

        // still has the old value
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value".to_vec()
        );

        assert_eq!(
            command(parser!(b"DISCARD"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );

        // still has the old value
        assert_eq!(
            db.get_or_create(0, &b"key".to_vec()).get().unwrap(),
            b"value".to_vec()
        );

        // multi status back to normal
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data(b"value".to_vec())
        );
    }

    #[test]
    fn watch_multi_exec_fail_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();

        assert_eq!(
            command(parser!(b"watch key"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"exec"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
    }

    #[test]
    fn watch_multi_exec_ok_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();

        assert_eq!(
            command(parser!(b"watch key"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"EXEC"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Status("OK".to_owned()),])
        );
    }

    #[test]
    fn watch_unwatch_multi_exec_command() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();

        assert_eq!(
            command(parser!(b"watch key"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"unwatch"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"multi"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Status("QUEUED".to_owned())
        );
        assert_eq!(
            command(parser!(b"EXEC"), &mut db, &mut client).unwrap(),
            Response::Array(vec![Response::Data(b"1".to_vec()),])
        );
    }

    #[test]
    fn monitor() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let (tx, rx) = channel();
        let mut client1 = Client::new(tx, 0);
        let mut client2 = Client::mock();
        assert_eq!(
            command(parser!(b"monitor"), &mut db, &mut client1).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client2).unwrap(),
            Response::Nil
        );
        assert_eq!(
            rx.recv().unwrap(),
            Some(Response::Status("\"get\" \"key\" ".to_owned()))
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn info() {
        let mut db = Database::new(Config::new(Logger::new(Level::Warning)));
        let mut client = Client::mock();
        // Generate some traffic so the INFO stats reflect real activity.
        assert_eq!(
            command(parser!(b"set key 1"), &mut db, &mut client).unwrap(),
            Response::Status("OK".to_owned())
        );
        assert_eq!(
            command(parser!(b"get key"), &mut db, &mut client).unwrap(),
            Response::Data(b"1".to_vec())
        );
        assert_eq!(
            command(parser!(b"get missing"), &mut db, &mut client).unwrap(),
            Response::Nil
        );
        if let Response::Data(d) = command(parser!(b"info"), &mut db, &mut client).unwrap() {
            let s = from_utf8(&*d).unwrap();
            assert!(s.contains("rudis_git_sha1"));
            assert!(s.contains("rudis_git_dirty"));
            assert!(s.contains("redis_version:"));
            assert!(s.contains("role:master"));
            // 3 commands + the INFO call itself.
            assert!(s.contains("total_commands_processed:4"));
            assert!(s.contains("rdb_changes_since_last_save:1"));
            assert!(s.contains("keyspace_hits:1"));
            assert!(s.contains("keyspace_misses:1"));
            assert!(s.contains("db0:keys=1;expires=0;avg_ttl=0"));
        } else {
            panic!("Expected data");
        }
    }
}
