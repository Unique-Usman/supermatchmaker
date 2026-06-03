// Redis layer. The message bus uses two Redis Streams (with offset/consumer-
// group recovery), plus short-lived caches:
//   mm:ingest:stream   Backend -> ingestion thread (offset replay)
//   match:sync:stream  match threads -> write-behind DB poller (consumer group)
//   mm:status:{id}     per-player poll status cache (TTL)
//   mm:player:{id}     player rating cache (TTL)

use r2d2::Pool;
use redis::{Client, Commands};
use serde::{Deserialize, Serialize};

pub type RedisPool = Pool<Client>;

// Per-user match status, keyed mm:status:{user_id}. Written when a player is
// queued and when the poller commits a match; read by the /status poll
// endpoint. The cache TTL (config: STATUS_TTL_SECS env) auto-expires stale
// entries so polling never has to fall back to Postgres.
const STATUS_PREFIX: &str = "mm:status:";

// Player cache, keyed mm:player:{user_id}. Holds the player's data while they
// are active in the system so the hot path (enqueue) can avoid a Postgres
// read. Entries auto-expire after the configured TTL.
const PLAYER_PREFIX: &str = "mm:player:";

fn player_key(user_id: i64) -> String {
    format!("{PLAYER_PREFIX}{user_id}")
}

// Cache a player's data with a time-to-live (seconds). Called whenever the
// player enters the queue; the TTL refreshes on each call.
pub fn cache_player(
    pool: &RedisPool,
    user_id: i64,
    rating: i32,
    ttl_secs: u64,
) -> redis::RedisResult<()> {
    let mut conn = pool.get().map_err(redis_pool_err)?;
    let payload = serde_json::json!({ "id": user_id, "rating": rating }).to_string();
    let _: () = conn.set_ex(player_key(user_id), payload, ttl_secs)?;
    Ok(())
}

// Read a cached player's rating, if the entry is still live.
pub fn cached_rating(pool: &RedisPool, user_id: i64) -> Option<i32> {
    let mut conn = pool.get().ok()?;
    let raw: Option<String> = conn.get(player_key(user_id)).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw?).ok()?;
    v.get("rating").and_then(|r| r.as_i64()).map(|r| r as i32)
}

fn status_key(user_id: i64) -> String {
    format!("{STATUS_PREFIX}{user_id}")
}

// Mark a user as queued (set right when they enter the queue).
pub fn set_queued(pool: &RedisPool, user_id: i64, ttl_secs: u64) -> redis::RedisResult<()> {
    let mut conn = pool.get().map_err(redis_pool_err)?;
    let payload = serde_json::json!({ "status": "queued" }).to_string();
    let _: () = conn.set_ex(status_key(user_id), payload, ttl_secs)?;
    Ok(())
}

// Read a user's current status payload (raw JSON string), if any.
pub fn get_status(pool: &RedisPool, user_id: i64) -> Option<String> {
    let mut conn = pool.get().ok()?;
    let res: Option<String> = conn.get(status_key(user_id)).ok()?;
    res
}

pub fn build_pool(redis_url: &str, max_size: u32) -> RedisPool {
    let client = Client::open(redis_url).expect("invalid redis url");
    Pool::builder()
        .max_size(max_size)
        .build(client)
        .expect("failed to build Redis pool")
}

// Event published by the Backend when a user enters matchmaking.
#[derive(Serialize, Deserialize)]
pub struct IngestEvent {
    pub id: i64,
    pub rating: i32,
}

// Result published by a match thread once a game is formed. Teams carry
// (user_id, rating) pairs so the poller can assign positions by skill rank.
#[derive(Serialize, Deserialize)]
pub struct MatchResult {
    pub match_id: u64,
    pub team_a: Vec<(i64, i32)>,
    pub team_b: Vec<(i64, i32)>,
    pub spread: u32,
}

// --- Ingest stream (Backend -> ingestion thread) ---
//
// Players entering matchmaking go onto a Redis Stream. Recovery uses the
// snapshot+replay model (Kafka-style): the ingestion thread tracks the id of
// the last message it read, the snapshotter persists that id alongside the
// pool, and on restart the pool is restored from the newest snapshot and the
// stream is replayed from just after the snapshot's stored id. The stream is
// trimmed on an interval (2x the snapshot interval) so it never grows
// unbounded while still always covering the newest snapshot's offset.
pub const INGEST_STREAM: &str = "mm:ingest:stream";

// Backend: append a player to the ingest stream (XADD).
pub fn push_ingest(pool: &RedisPool, ev: &IngestEvent) -> redis::RedisResult<()> {
    let mut conn = pool.get().map_err(redis_pool_err)?;
    let payload = serde_json::to_string(ev).unwrap();
    let _: String = redis::cmd("XADD")
        .arg(INGEST_STREAM)
        .arg("*")
        .arg("data")
        .arg(payload)
        .query(&mut conn)?;
    Ok(())
}

// A stream entry: its id (the offset to remember) and the decoded event.
pub struct IngestEntry {
    pub id: String,
    pub event: IngestEvent,
}

// Ingestion thread: read up to `count` messages with id strictly greater than
// `last_id`, blocking up to `block_ms`. Pass "$" as last_id to read only new
// messages from now; pass a stored snapshot id to replay forward from it.
// Returns the entries (caller updates its cursor to the last id seen).
pub fn read_ingest_after(
    pool: &RedisPool,
    last_id: &str,
    count: usize,
    block_ms: usize,
) -> Vec<IngestEntry> {
    let mut conn = match pool.get() {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    let reply: redis::RedisResult<redis::Value> = redis::cmd("XREAD")
        .arg("COUNT")
        .arg(count)
        .arg("BLOCK")
        .arg(block_ms)
        .arg("STREAMS")
        .arg(INGEST_STREAM)
        .arg(last_id)
        .query(&mut conn);
    parse_ingest_reply(reply.ok())
}

// Trim the stream to drop entries older than `max_age_ms`. Uses XTRIM MINID
// with a millisecond-timestamp id floor (stream ids are "<ms>-<seq>"), so all
// entries older than the cutoff are removed. Run on an interval = 2x snapshot
// interval, guaranteeing the newest snapshot's offset is never trimmed away.
pub fn trim_ingest(pool: &RedisPool, max_age_ms: u64) -> redis::RedisResult<()> {
    let mut conn = pool.get().map_err(redis_pool_err)?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let cutoff = now_ms.saturating_sub(max_age_ms);
    // MINID with "~" lets Redis trim efficiently (approximate but bounded).
    let _: i64 = redis::cmd("XTRIM")
        .arg(INGEST_STREAM)
        .arg("MINID")
        .arg("~")
        .arg(cutoff)
        .query(&mut conn)?;
    Ok(())
}

fn parse_ingest_reply(reply: Option<redis::Value>) -> Vec<IngestEntry> {
    use redis::Value;
    let mut out = Vec::new();
    let Some(Value::Array(streams)) = reply else {
        return out;
    };
    for stream in streams {
        let Value::Array(pair) = stream else { continue };
        if pair.len() != 2 {
            continue;
        }
        let Value::Array(entries) = &pair[1] else {
            continue;
        };
        for entry in entries {
            let Value::Array(id_fields) = entry else {
                continue;
            };
            if id_fields.len() != 2 {
                continue;
            }
            let id = match &id_fields[0] {
                Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };
            let Value::Array(fields) = &id_fields[1] else {
                continue;
            };
            let mut payload: Option<String> = None;
            let mut i = 0;
            while i + 1 < fields.len() {
                if let (Value::BulkString(k), Value::BulkString(v)) = (&fields[i], &fields[i + 1]) {
                    if k == b"data" {
                        payload = Some(String::from_utf8_lossy(v).to_string());
                    }
                }
                i += 2;
            }
            if let Some(p) = payload {
                if let Ok(ev) = serde_json::from_str::<IngestEvent>(&p) {
                    out.push(IngestEntry { id, event: ev });
                }
            }
        }
    }
    out
}

// --- Write-behind sync stream (match threads -> DB poller) ---
//
// Match results go onto a Redis Stream (not a list) so a crash of the DB
// poller leaves unacknowledged entries in the stream to be re-read on
// recovery. A consumer group tracks delivery; the poller XACKs after a
// successful bulk insert.
pub const SYNC_STREAM: &str = "match:sync:stream";
pub const SYNC_GROUP: &str = "db-writers";

// Create the consumer group if it does not already exist. MKSTREAM creates
// the stream too. Idempotent: a BUSYGROUP error means it already exists.
pub fn ensure_sync_group(pool: &RedisPool) -> redis::RedisResult<()> {
    let mut conn = pool.get().map_err(redis_pool_err)?;
    let res: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(SYNC_STREAM)
        .arg(SYNC_GROUP)
        .arg("$")
        .arg("MKSTREAM")
        .query(&mut conn);
    match res {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()), // already created
        Err(e) => Err(e),
    }
}

// Pipelined commit: bundle ALL post-match Redis writes — the 10 player status
// tickets (SET ... EX) plus the single sync-stream XADD — into one pipeline,
// so the match thread makes a single network round-trip instead of 11. Redis
// runs them sequentially in RAM and returns one combined reply.
pub fn commit_match(pool: &RedisPool, m: &MatchResult, ttl_secs: u64) -> redis::RedisResult<()> {
    let mut conn = pool.get().map_err(redis_pool_err)?;

    // Shared "matched" payload (team rosters) reused for every player ticket.
    let a_ids: Vec<i64> = m.team_a.iter().map(|(id, _)| *id).collect();
    let b_ids: Vec<i64> = m.team_b.iter().map(|(id, _)| *id).collect();
    let status_payload = serde_json::json!({
        "status": "matched",
        "match_id": m.match_id,
        "team_a": a_ids,
        "team_b": b_ids,
    })
    .to_string();
    let sync_payload = serde_json::to_string(m).unwrap();

    // Build one pipeline: 10 SET EX (one per player) + 1 XADD.
    let mut pipe = redis::pipe();
    for (uid, _) in m.team_a.iter().chain(m.team_b.iter()) {
        pipe.cmd("SET")
            .arg(status_key(*uid))
            .arg(&status_payload)
            .arg("EX")
            .arg(ttl_secs)
            .ignore();
    }
    pipe.cmd("XADD")
        .arg(SYNC_STREAM)
        .arg("*")
        .arg("data")
        .arg(&sync_payload)
        .ignore();

    // One round-trip for all 11 commands.
    pipe.query(&mut conn)
}

// A claimed stream entry: its id (for XACK) and the decoded match.
pub struct SyncEntry {
    pub id: String,
    pub match_result: MatchResult,
}

// DB poller: read up to `count` new entries for this consumer, blocking up to
// `block_ms`. Uses ">" to get never-before-delivered messages.
pub fn read_sync_batch(
    pool: &RedisPool,
    consumer: &str,
    count: usize,
    block_ms: usize,
) -> Vec<SyncEntry> {
    let mut conn = match pool.get() {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    // XREADGROUP GROUP <grp> <consumer> COUNT <n> BLOCK <ms> STREAMS <stream> >
    let reply: redis::RedisResult<redis::Value> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(SYNC_GROUP)
        .arg(consumer)
        .arg("COUNT")
        .arg(count)
        .arg("BLOCK")
        .arg(block_ms)
        .arg("STREAMS")
        .arg(SYNC_STREAM)
        .arg(">")
        .query(&mut conn);
    parse_stream_reply(reply.ok())
}

// On recovery, reclaim entries delivered to a dead consumer but never ACKed,
// so no match is lost if the poller crashed mid-batch. Reads the pending list
// from id "0".
pub fn read_pending(pool: &RedisPool, consumer: &str, count: usize) -> Vec<SyncEntry> {
    let mut conn = match pool.get() {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    let reply: redis::RedisResult<redis::Value> = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(SYNC_GROUP)
        .arg(consumer)
        .arg("COUNT")
        .arg(count)
        .arg("STREAMS")
        .arg(SYNC_STREAM)
        .arg("0")
        .query(&mut conn);
    parse_stream_reply(reply.ok())
}

// Acknowledge a batch of stream ids after they are safely in Postgres.
pub fn ack_sync(pool: &RedisPool, ids: &[String]) -> redis::RedisResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut conn = pool.get().map_err(redis_pool_err)?;
    let mut cmd = redis::cmd("XACK");
    cmd.arg(SYNC_STREAM).arg(SYNC_GROUP);
    for id in ids {
        cmd.arg(id);
    }
    let _: i64 = cmd.query(&mut conn)?;
    Ok(())
}

// Decode the nested XREADGROUP reply into SyncEntry values. The shape is:
// [ [ stream_name, [ [id, [field, value, ...]], ... ] ] ]
fn parse_stream_reply(reply: Option<redis::Value>) -> Vec<SyncEntry> {
    use redis::Value;
    let mut out = Vec::new();
    let Some(Value::Array(streams)) = reply else {
        return out;
    };
    for stream in streams {
        let Value::Array(pair) = stream else { continue };
        // pair = [stream_name, entries]
        if pair.len() != 2 {
            continue;
        }
        let Value::Array(entries) = &pair[1] else {
            continue;
        };
        for entry in entries {
            let Value::Array(id_fields) = entry else {
                continue;
            };
            if id_fields.len() != 2 {
                continue;
            }
            let id = match &id_fields[0] {
                Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };
            let Value::Array(fields) = &id_fields[1] else {
                continue;
            };
            // fields = [ "data", "<json>" ]
            let mut payload: Option<String> = None;
            let mut i = 0;
            while i + 1 < fields.len() {
                if let (Value::BulkString(k), Value::BulkString(v)) = (&fields[i], &fields[i + 1]) {
                    if k == b"data" {
                        payload = Some(String::from_utf8_lossy(v).to_string());
                    }
                }
                i += 2;
            }
            if let Some(p) = payload {
                if let Ok(mr) = serde_json::from_str::<MatchResult>(&p) {
                    out.push(SyncEntry {
                        id,
                        match_result: mr,
                    });
                }
            }
        }
    }
    out
}

fn redis_pool_err(_: r2d2::Error) -> redis::RedisError {
    redis::RedisError::from((redis::ErrorKind::IoError, "redis pool exhausted"))
}
