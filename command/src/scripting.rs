//! Lua scripting engine for rudis.
//!
//! Implements EVAL, EVALSHA, FUNCTION, FCALL, FCALL_RO using mlua.

use mlua::prelude::*;
use response::Response;
use database::{Database, LuaFunctionInfo};

/// Compute a hex hash string for script caching.
/// Uses a simple hash (not cryptographic SHA1) but sufficient for script caching.
pub fn script_sha1(data: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    let h = hasher.finish();
    format!("{:040x}", h)
}

/// Execute a Lua script (EVAL).
pub fn eval_script(
    db: &mut Database,
    dbindex: usize,
    source: &[u8],
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
) -> Response {
    let sha = script_sha1(source);
    let source_str = String::from_utf8_lossy(source).to_string();
    db.script_cache.insert(sha.clone(), source_str);
    let source_cached = db.script_cache.get(&sha).unwrap().clone();
    run_lua_script(&source_cached, keys, argv, db, dbindex)
}

/// Execute a cached script by SHA (EVALSHA).
pub fn evalsha_script(
    db: &mut Database,
    dbindex: usize,
    sha: &str,
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
) -> Response {
    match db.script_cache.get(sha) {
        Some(source) => {
            let source = source.clone();
            run_lua_script(&source, keys, argv, db, dbindex)
        }
        None => Response::Error("NOSCRIPT No matching script. Use EVAL.".to_owned()),
    }
}

/// Load a Lua function (FUNCTION LOAD).
pub fn function_load(db: &mut Database, code: &str, replace: bool) -> Result<String, String> {
    let name = parse_function_name(code).unwrap_or_else(|| "unknown".to_owned());
    if db.lua_functions.contains_key(&name) && !replace {
        return Err(format!("ERR Function '{}' already exists", name));
    }
    let func = LuaFunctionInfo {
        name: name.clone(),
        code: code.to_owned(),
        engine: "lua".to_owned(),
        description: String::new(),
        flags: Vec::new(),
    };
    db.lua_functions.insert(name.clone(), func);
    Ok(name)
}

/// Call a stored function (FCALL/FCALL_RO).
pub fn fcall_function(
    db: &mut Database,
    dbindex: usize,
    name: &str,
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
) -> Response {
    match db.lua_functions.get(name) {
        Some(func) => {
            let code = strip_shebang(&func.code);
            run_lua_script(&code, keys, argv, db, dbindex)
        }
        None => Response::Error(format!("ERR Function not found: {}", name)),
    }
}

/// Parse function name from Lua code header: #!lua name=xxx
fn parse_function_name(code: &str) -> Option<String> {
    for line in code.lines() {
        let line = line.trim();
        if line.starts_with("#!") {
            let rest = &line[2..];
            for part in rest.split_whitespace() {
                if let Some(name) = part.strip_prefix("name=") {
                    return Some(name.to_owned());
                }
            }
        }
    }
    None
}

/// Strip the #!lua shebang line from function code before execution.
fn strip_shebang(code: &str) -> String {
    if code.starts_with("#!") {
        if let Some(pos) = code.find('\n') {
            return code[pos + 1..].to_owned();
        }
        return String::new();
    }
    code.to_owned()
}

/// Core Lua script execution.
fn run_lua_script(
    source: &str,
    keys: &[Vec<u8>],
    argv: &[Vec<u8>],
    db: &mut Database,
    dbindex: usize,
) -> Response {
    let lua = Lua::new();

    // Set up KEYS table
    let keys_table = lua.create_table().unwrap();
    for (i, key) in keys.iter().enumerate() {
        let s = lua.create_string(key).unwrap();
        let _ = keys_table.set(i + 1, s);
    }
    let _ = lua.globals().set("KEYS", keys_table);

    // Set up ARGV table
    let argv_table = lua.create_table().unwrap();
    for (i, arg) in argv.iter().enumerate() {
        let s = lua.create_string(arg).unwrap();
        let _ = argv_table.set(i + 1, s);
    }
    let _ = lua.globals().set("ARGV", argv_table);

    // Set up redis table
    let redis_table = lua.create_table().unwrap();

    // Use raw pointer for db access in callbacks (valid for this function's duration)
    let db_ptr = db as *mut Database;
    let dbindex_copy = dbindex;

    // redis.call
    let call_fn = lua.create_function(move |lua, args: LuaMultiValue| {
        let args_vec = lua_multi_to_vec(&args);
        if args_vec.is_empty() {
            return Err(LuaError::RuntimeError(
                "Please specify at least one argument for redis.call".to_string(),
            ));
        }
        let db = unsafe { &mut *db_ptr };
        let result = lua_execute_command(db, dbindex_copy, &args_vec);
        response_to_lua(lua, &result)
    }).unwrap();

    // redis.pcall
    let pcall_fn = lua.create_function(move |lua, args: LuaMultiValue| {
        let args_vec = lua_multi_to_vec(&args);
        if args_vec.is_empty() {
            return Err(LuaError::RuntimeError(
                "Please specify at least one argument for redis.pcall".to_string(),
            ));
        }
        let db = unsafe { &mut *db_ptr };
        let result = lua_execute_command(db, dbindex_copy, &args_vec);
        match response_to_lua(lua, &result) {
            Ok(v) => Ok(v),
            Err(e) => {
                let err_table = lua.create_table()?;
                let _ = err_table.set("err", e.to_string());
                Ok(LuaValue::Table(err_table))
            }
        }
    }).unwrap();

    // redis.log (no-op)
    let log_fn = lua.create_function(|_, _: LuaMultiValue| Ok(())).unwrap();

    // redis.error_reply
    let error_reply_fn = lua.create_function(|lua, msg: String| {
        let t = lua.create_table()?;
        t.set("err", msg)?;
        Ok(t)
    }).unwrap();

    // redis.status_reply
    let status_reply_fn = lua.create_function(|lua, msg: String| {
        let t = lua.create_table()?;
        t.set("ok", msg)?;
        Ok(t)
    }).unwrap();

    let _ = redis_table.set("call", call_fn);
    let _ = redis_table.set("pcall", pcall_fn);
    let _ = redis_table.set("log", log_fn);
    let _ = redis_table.set("error_reply", error_reply_fn);
    let _ = redis_table.set("status_reply", status_reply_fn);
    let _ = redis_table.set("LOG_DEBUG", 0);
    let _ = redis_table.set("LOG_VERBOSE", 1);
    let _ = redis_table.set("LOG_NOTICE", 2);
    let _ = redis_table.set("LOG_WARNING", 3);
    let _ = lua.globals().set("redis", redis_table);

    // Execute the script and convert result immediately
    let response = match lua.load(source).eval::<LuaValue<'_>>() {
        Ok(lua_val) => lua_value_to_response(lua_val),
        Err(e) => Response::Error(format!("ERR {}", e)),
    };
    response
}

/// Convert Lua multi-value args to Vec<Vec<u8>>.
fn lua_multi_to_vec(args: &LuaMultiValue) -> Vec<Vec<u8>> {
    args.iter()
        .map(|v| match v {
            LuaValue::String(s) => s.as_bytes().to_vec(),
            LuaValue::Integer(i) => i.to_string().into_bytes(),
            LuaValue::Number(n) => n.to_string().into_bytes(),
            LuaValue::Boolean(b) => {
                if *b { b"1".to_vec() } else { b"0".to_vec() }
            }
            _ => b"".to_vec(),
        })
        .collect()
}

/// Convert a Lua value to a Response.
fn lua_value_to_response(val: LuaValue) -> Response {
    match val {
        LuaValue::Nil => Response::Nil,
        LuaValue::Boolean(b) => Response::Integer(if b { 1 } else { 0 }),
        LuaValue::Integer(n) => Response::Integer(n),
        LuaValue::Number(n) => {
            if n == (n as i64) as f64 {
                Response::Integer(n as i64)
            } else {
                Response::Data(n.to_string().into_bytes())
            }
        }
        LuaValue::String(s) => Response::Data(s.as_bytes().to_vec()),
        LuaValue::Table(t) => {
            // Check if it's an error or status reply
            if let Ok(err) = t.get::<_, String>("err") {
                return Response::Error(err);
            }
            if let Ok(ok) = t.get::<_, String>("ok") {
                return Response::Status(ok);
            }
            // Array response
            let mut arr = Vec::new();
            let len = t.raw_len();
            for i in 1..=len {
                match t.get::<_, LuaValue>(i) {
                    Ok(v) => arr.push(lua_value_to_response(v)),
                    Err(_) => break,
                }
            }
            Response::Array(arr)
        }
        _ => Response::Nil,
    }
}

/// Convert a Response to a Lua value.
fn response_to_lua<'lua>(lua: &'lua Lua, resp: &Response) -> LuaResult<LuaValue<'lua>> {
    match resp {
        Response::Data(data) => Ok(LuaValue::String(lua.create_string(data)?)),
        Response::Integer(n) => Ok(LuaValue::Integer(*n)),
        Response::Status(s) => {
            let t = lua.create_table()?;
            t.set("ok", s.clone())?;
            Ok(LuaValue::Table(t))
        }
        Response::Error(e) => Err(LuaError::RuntimeError(e.clone())),
        Response::Nil => Ok(LuaValue::Nil),
        Response::Array(arr) => {
            let t = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() {
                let lua_val = response_to_lua(lua, item)?;
                t.set(i + 1, lua_val)?;
            }
            Ok(LuaValue::Table(t))
        }
    }
}

/// Execute a Redis command from within a Lua script.
fn lua_execute_command(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.is_empty() {
        return Response::Error("ERR no command".to_owned());
    }
    let cmd = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    match cmd.as_str() {
        "get" => lua_cmd_get(db, dbindex, args),
        "set" => lua_cmd_set(db, dbindex, args),
        "del" => lua_cmd_del(db, dbindex, args),
        "exists" => lua_cmd_exists(db, dbindex, args),
        "incr" => lua_cmd_incr(db, dbindex, args),
        "decr" => lua_cmd_decr(db, dbindex, args),
        "incrby" => lua_cmd_incrby(db, dbindex, args),
        "decrby" => lua_cmd_decrby(db, dbindex, args),
        "expire" => lua_cmd_expire(db, dbindex, args),
        "ttl" => lua_cmd_ttl(db, dbindex, args),
        "type" => lua_cmd_type(db, dbindex, args),
        "llen" => lua_cmd_llen(db, dbindex, args),
        "rpush" => lua_cmd_rpush(db, dbindex, args),
        "lpush" => lua_cmd_lpush(db, dbindex, args),
        "hset" => lua_cmd_hset(db, dbindex, args),
        "hget" => lua_cmd_hget(db, dbindex, args),
        "sadd" => lua_cmd_sadd(db, dbindex, args),
        "sismember" => lua_cmd_sismember(db, dbindex, args),
        "keys" => lua_cmd_keys(db, dbindex, args),
        _ => Response::Error(format!("ERR unknown command '{}'", cmd)),
    }
}

// Helper functions for Lua script command execution

fn lua_arg_bytes(args: &[Vec<u8>], index: usize) -> Result<Vec<u8>, Response> {
    args.get(index).cloned().ok_or_else(|| Response::Error("ERR wrong number of arguments".to_owned()))
}

fn lua_arg_str(args: &[Vec<u8>], index: usize) -> Result<String, Response> {
    let bytes = lua_arg_bytes(args, index)?;
    String::from_utf8(bytes).map_err(|_| Response::Error("ERR invalid string".to_owned()))
}

fn lua_arg_i64(args: &[Vec<u8>], index: usize) -> Result<i64, Response> {
    let s = lua_arg_str(args, index)?;
    s.parse::<i64>().map_err(|_| Response::Error("ERR value is not an integer or out of range".to_owned()))
}

fn lua_cmd_get(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'get' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    match db.get(dbindex, &key) {
        Some(val) => match val.get() {
            Ok(data) => Response::Data(data),
            Err(_) => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()),
        },
        None => Response::Nil,
    }
}

fn lua_cmd_set(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() < 3 { return Response::Error("ERR wrong number of arguments for 'set' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let val = match lua_arg_bytes(args, 2) { Ok(v) => v, Err(e) => return e };
    match db.get_or_create(dbindex, &key).set(val) {
        Ok(_) => { db.key_updated(dbindex, &key); Response::Status("OK".to_owned()) }
        Err(e) => Response::Error(e.to_string()),
    }
}

fn lua_cmd_del(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    let mut count = 0;
    for i in 1..args.len() {
        if let Ok(key) = lua_arg_bytes(args, i) {
            if db.remove(dbindex, &key).is_some() { count += 1; }
        }
    }
    Response::Integer(count)
}

fn lua_cmd_exists(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    let mut count = 0;
    for i in 1..args.len() {
        if let Ok(key) = lua_arg_bytes(args, i) {
            if db.get(dbindex, &key).is_some() { count += 1; }
        }
    }
    Response::Integer(count)
}

fn lua_cmd_incr(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'incr' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    match db.get_or_create(dbindex, &key).incr(1) {
        Ok(val) => { db.key_updated(dbindex, &key); Response::Integer(val) }
        Err(e) => Response::Error(e.to_string()),
    }
}

fn lua_cmd_decr(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'decr' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    match db.get_or_create(dbindex, &key).incr(-1) {
        Ok(val) => { db.key_updated(dbindex, &key); Response::Integer(val) }
        Err(e) => Response::Error(e.to_string()),
    }
}

fn lua_cmd_incrby(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 3 { return Response::Error("ERR wrong number of arguments for 'incrby' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let increment = match lua_arg_i64(args, 2) { Ok(v) => v, Err(e) => return e };
    match db.get_or_create(dbindex, &key).incr(increment) {
        Ok(val) => { db.key_updated(dbindex, &key); Response::Integer(val) }
        Err(e) => Response::Error(e.to_string()),
    }
}

fn lua_cmd_decrby(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 3 { return Response::Error("ERR wrong number of arguments for 'decrby' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let decrement = match lua_arg_i64(args, 2) { Ok(v) => v, Err(e) => return e };
    match db.get_or_create(dbindex, &key).incr(-decrement) {
        Ok(val) => { db.key_updated(dbindex, &key); Response::Integer(val) }
        Err(e) => Response::Error(e.to_string()),
    }
}

fn lua_cmd_expire(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 3 { return Response::Error("ERR wrong number of arguments for 'expire' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let seconds = match lua_arg_i64(args, 2) { Ok(v) => v, Err(e) => return e };
    if db.get(dbindex, &key).is_some() {
        db.set_msexpiration(dbindex, key, seconds * 1000 + util::mstime());
        Response::Integer(1)
    } else {
        Response::Integer(0)
    }
}

fn lua_cmd_ttl(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'ttl' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    match db.get_msexpiration(dbindex, &key) {
        Some(exp) => {
            let ttl = (exp - util::mstime()) / 1000;
            if ttl < 0 { Response::Integer(-1) } else { Response::Integer(ttl) }
        }
        None => {
            if db.get(dbindex, &key).is_some() { Response::Integer(-1) }
            else { Response::Integer(-2) }
        }
    }
}

fn lua_cmd_type(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'type' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    match db.get(dbindex, &key) {
        Some(val) => {
            let t = match val {
                database::Value::Nil => "none",
                database::Value::String(_) => "string",
                database::Value::List(_) => "list",
                database::Value::Set(_) => "set",
                database::Value::SortedSet(_) => "zset",
                database::Value::Hash(_) => "hash",
                database::Value::Stream(_) => "stream",
                database::Value::BloomFilter(_) => "MBbloom--",
                database::Value::CuckooFilter(_) => "MBbloom--",
                database::Value::TDigest(_) => "MBbloom--",
                database::Value::TopK(_) => "MBbloom--",
                database::Value::Json(_) => "ReJSON-RL",
                database::Value::TimeSeries(_) => "timeseries",
            };
            Response::Status(t.to_owned())
        }
        None => Response::Status("none".to_owned()),
    }
}

fn lua_cmd_llen(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'llen' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    match db.get(dbindex, &key) {
        Some(val) => match val.llen() {
            Ok(len) => Response::Integer(len as i64),
            Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
        },
        None => Response::Integer(0),
    }
}

fn lua_cmd_rpush(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() < 3 { return Response::Error("ERR wrong number of arguments for 'rpush' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    for i in 2..args.len() {
        let val = match lua_arg_bytes(args, i) { Ok(v) => v, Err(_) => continue };
        if let Err(e) = db.get_or_create(dbindex, &key).push(val, true) {
            return Response::Error(e.to_string());
        }
    }
    db.key_updated(dbindex, &key);
    let len = db.get(dbindex, &key).and_then(|v| v.llen().ok()).unwrap_or(0);
    Response::Integer(len as i64)
}

fn lua_cmd_lpush(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() < 3 { return Response::Error("ERR wrong number of arguments for 'lpush' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    for i in 2..args.len() {
        let val = match lua_arg_bytes(args, i) { Ok(v) => v, Err(_) => continue };
        if let Err(e) = db.get_or_create(dbindex, &key).push(val, false) {
            return Response::Error(e.to_string());
        }
    }
    db.key_updated(dbindex, &key);
    let len = db.get(dbindex, &key).and_then(|v| v.llen().ok()).unwrap_or(0);
    Response::Integer(len as i64)
}

fn lua_cmd_hset(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return Response::Error("ERR wrong number of arguments for 'hset' command".to_owned());
    }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let mut count = 0;
    for i in (2..args.len()).step_by(2) {
        let field = match lua_arg_bytes(args, i) { Ok(f) => f, Err(_) => continue };
        let value = match lua_arg_bytes(args, i + 1) { Ok(v) => v, Err(_) => continue };
        match db.get_or_create(dbindex, &key).hset(field, value) {
            Ok(is_new) => { if is_new { count += 1; } }
            Err(e) => return Response::Error(e.to_string()),
        }
    }
    db.key_updated(dbindex, &key);
    Response::Integer(count)
}

fn lua_cmd_hget(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 3 { return Response::Error("ERR wrong number of arguments for 'hget' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let field = match lua_arg_bytes(args, 2) { Ok(f) => f, Err(e) => return e };
    match db.get(dbindex, &key) {
        Some(val) => match val.hget(&field) {
            Ok(Some(v)) => Response::Data(v),
            Ok(None) => Response::Nil,
            Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
        },
        None => Response::Nil,
    }
}

fn lua_cmd_sadd(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() < 3 { return Response::Error("ERR wrong number of arguments for 'sadd' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let mut count = 0;
    for i in 2..args.len() {
        let member = match lua_arg_bytes(args, i) { Ok(m) => m, Err(_) => continue };
        match db.get_or_create(dbindex, &key).sadd(member, 128) {
            Ok(true) => count += 1,
            Ok(false) => {}
            Err(e) => return Response::Error(e.to_string()),
        }
    }
    db.key_updated(dbindex, &key);
    Response::Integer(count)
}

fn lua_cmd_sismember(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 3 { return Response::Error("ERR wrong number of arguments for 'sismember' command".to_owned()); }
    let key = match lua_arg_bytes(args, 1) { Ok(k) => k, Err(e) => return e };
    let member = match lua_arg_bytes(args, 2) { Ok(m) => m, Err(e) => return e };
    match db.get(dbindex, &key) {
        Some(val) => match val.sismember(&member) {
            Ok(b) => Response::Integer(if b { 1 } else { 0 }),
            Err(_) => Response::Error("WRONGTYPE key holds wrong type".to_owned()),
        },
        None => Response::Integer(0),
    }
}

fn lua_cmd_keys(db: &mut Database, dbindex: usize, args: &[Vec<u8>]) -> Response {
    if args.len() != 2 { return Response::Error("ERR wrong number of arguments for 'keys' command".to_owned()); }
    let pattern = match lua_arg_bytes(args, 1) { Ok(p) => p, Err(e) => return e };
    let keys = db.keys(dbindex, &pattern);
    Response::Array(keys.into_iter().map(Response::Data).collect())
}

#[cfg(test)]
mod test_scripting {
    use super::*;

    #[test]
    fn test_script_sha1() {
        let sha = script_sha1(b"return 1");
        assert_eq!(sha.len(), 40);
    }

    #[test]
    fn test_eval_simple() {
        let mut db = Database::mock();
        let result = eval_script(&mut db, 0, b"return 42", &[], &[]);
        assert_eq!(result, Response::Integer(42));
    }

    #[test]
    fn test_eval_string() {
        let mut db = Database::mock();
        let result = eval_script(&mut db, 0, b"return 'hello'", &[], &[]);
        assert_eq!(result, Response::Data(b"hello".to_vec()));
    }

    #[test]
    fn test_eval_keys() {
        let mut db = Database::mock();
        let result = eval_script(&mut db, 0, b"return KEYS[1]", &[b"mykey".to_vec()], &[]);
        assert_eq!(result, Response::Data(b"mykey".to_vec()));
    }

    #[test]
    fn test_eval_argv() {
        let mut db = Database::mock();
        let result = eval_script(&mut db, 0, b"return ARGV[1]", &[], &[b"hello".to_vec()]);
        assert_eq!(result, Response::Data(b"hello".to_vec()));
    }

    #[test]
    fn test_eval_set_get() {
        let mut db = Database::mock();
        let r1 = eval_script(&mut db, 0, b"redis.call('set', 'foo', 'bar'); return redis.call('get', 'foo')", &[], &[]);
        assert_eq!(r1, Response::Data(b"bar".to_vec()));
    }

    #[test]
    fn test_evalsha_noscript() {
        let mut db = Database::mock();
        let result = evalsha_script(&mut db, 0, "nonexistent", &[], &[]);
        match result {
            Response::Error(e) => assert!(e.contains("NOSCRIPT")),
            _ => panic!("Expected NOSCRIPT error"),
        }
    }

    #[test]
    fn test_evalsha_cached() {
        let mut db = Database::mock();
        let source = b"return 123";
        let sha = script_sha1(source);
        // First eval caches the script
        eval_script(&mut db, 0, source, &[], &[]);
        // Then evalsha finds it
        let result = evalsha_script(&mut db, 0, &sha, &[], &[]);
        assert_eq!(result, Response::Integer(123));
    }

    #[test]
    fn test_parse_function_name() {
        assert_eq!(
            parse_function_name("#!lua name=mylib\nreturn 1"),
            Some("mylib".to_owned())
        );
        assert_eq!(parse_function_name("return 1"), None);
    }

    #[test]
    fn test_function_load_and_call() {
        let mut db = Database::mock();
        let name = function_load(&mut db, "#!lua name=testlib\nreturn 'hello'", false).unwrap();
        assert_eq!(name, "testlib");
        let result = fcall_function(&mut db, 0, "testlib", &[], &[]);
        assert_eq!(result, Response::Data(b"hello".to_vec()));
    }

    #[test]
    fn test_function_not_found() {
        let mut db = Database::mock();
        let result = fcall_function(&mut db, 0, "nonexistent", &[], &[]);
        match result {
            Response::Error(e) => assert!(e.contains("not found")),
            _ => panic!("Expected error"),
        }
    }
}
