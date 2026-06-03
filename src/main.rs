// Actix-Web matchmaking service.
//
//   POST /signup      {name, rating}  -> Diesel INSERT, returns id   (Backend -> DB)
//   POST /signin/{id}                 -> Diesel SELECT rating         (Backend -> DB)
//   POST /queue/{id}                  -> LPUSH ingest event to Redis  (Backend -> Queue)
//   GET  /metrics                     -> lock-free counters
//   GET  /health                      -> liveness
//
// Background threads (spawned at startup):
//   ingestion : BRPOP Redis ingest  -> Pool.enqueue            (Thread 1)
//   workers   : scan Pool -> match -> LPUSH Redis results       (Match Threads)
//   poller    : BRPOP Redis results -> Diesel UPDATE            (A poller)

mod auth;
mod db;
mod matcher;
mod metrics;
mod models;
mod queue;
mod schema;
mod snapshot;

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use matcher::{new_retry_queue, run_retry_worker, run_worker, Player, Pool};
use metrics::Metrics;
use queue::{push_ingest, IngestEvent, RedisPool};
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// --- Configuration via environment variables (fail-closed) ---
//
// Every variable is REQUIRED. There are no hardcoded defaults: if a variable
// is missing or cannot be parsed, the service refuses to start and reports
// exactly what is wrong. Config is loaded from a .env file (see dotenv() in
// main) or the real environment.

// Read a required string var, or record a clear error.
fn require(key: &str, errors: &mut Vec<String>) -> String {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v,
        Ok(_) => {
            errors.push(format!("{key} is set but empty"));
            String::new()
        }
        Err(_) => {
            errors.push(format!("{key} is not set"));
            String::new()
        }
    }
}

// Read a required var parsed to T, or record a clear error.
fn require_parse<T: std::str::FromStr>(key: &str, errors: &mut Vec<String>) -> T
where
    T: Default,
{
    match std::env::var(key) {
        Ok(v) => match v.trim().parse::<T>() {
            Ok(parsed) => parsed,
            Err(_) => {
                errors.push(format!(
                    "{key}='{v}' is not a valid {}",
                    std::any::type_name::<T>()
                ));
                T::default()
            }
        },
        Err(_) => {
            errors.push(format!("{key} is not set"));
            T::default()
        }
    }
}

struct AppState {
    pg: db::PgPool,
    redis: RedisPool,
    metrics: Arc<Metrics>,
    #[allow(dead_code)]
    pool: Arc<Pool>,
    jwt_secret: Vec<u8>,
    jwt_ttl_secs: u64,
    player_cache_ttl_secs: u64,
    status_ttl_secs: u64,
}

#[derive(Deserialize)]
struct SignupReq {
    name: String,
    rating: i32,
}

async fn signup(state: web::Data<AppState>, body: web::Json<SignupReq>) -> impl Responder {
    match db::create_user(&state.pg, &body.name, body.rating) {
        Ok(new_id) => {
            state.metrics.signups.fetch_add(1, Ordering::Relaxed);
            let token = auth::issue(new_id, &state.jwt_secret, state.jwt_ttl_secs);
            HttpResponse::Ok().json(serde_json::json!({
                "id": new_id, "rating": body.rating, "token": token
            }))
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("signup failed: {e}")),
    }
}

async fn signin(state: web::Data<AppState>, path: web::Path<i64>) -> impl Responder {
    let user_id = path.into_inner();
    match db::rating_of(&state.pg, user_id) {
        Ok(r) => {
            let token = auth::issue(user_id, &state.jwt_secret, state.jwt_ttl_secs);
            HttpResponse::Ok()
                .json(serde_json::json!({ "id": user_id, "rating": r, "token": token }))
        }
        Err(_) => HttpResponse::NotFound().body("user not found"),
    }
}

// Enter matchmaking. Protected: requires a valid bearer JWT whose subject
// matches the path id (a user can only queue themselves).
async fn enter_queue(
    state: web::Data<AppState>,
    req: HttpRequest,
    path: web::Path<i64>,
) -> impl Responder {
    let user_id = path.into_inner();

    // Extract and verify the bearer token.
    let token = match req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(auth::bearer)
    {
        Some(t) => t,
        None => return HttpResponse::Unauthorized().body("missing bearer token"),
    };
    let claims = match auth::verify(token, &state.jwt_secret) {
        Ok(c) => c,
        Err(auth::AuthError::Expired) => return HttpResponse::Unauthorized().body("token expired"),
        Err(_) => return HttpResponse::Unauthorized().body("invalid token"),
    };
    if claims.sub != user_id {
        return HttpResponse::Forbidden().body("token does not match user");
    }

    // Rating lookup: try the Redis cache first, fall back to Postgres on a
    // miss. Either way, (re)cache the player with a TTL so subsequent queue
    // calls stay off the database. The entry auto-expires when they go idle.
    let rating = match queue::cached_rating(&state.redis, user_id) {
        Some(r) => r,
        None => match db::rating_of(&state.pg, user_id) {
            Ok(r) => r,
            Err(_) => return HttpResponse::NotFound().body("user not found"),
        },
    };
    let _ = queue::cache_player(&state.redis, user_id, rating, state.player_cache_ttl_secs);
    match push_ingest(
        &state.redis,
        &IngestEvent {
            id: user_id,
            rating,
        },
    ) {
        Ok(_) => {
            state
                .metrics
                .events_published
                .fetch_add(1, Ordering::Relaxed);
            // Record initial status so the client can poll immediately.
            let _ = queue::set_queued(&state.redis, user_id, state.status_ttl_secs);
            // Return right away: the request is accepted and being processed.
            HttpResponse::Accepted().json(serde_json::json!({
                "status": "processing",
                "user_id": user_id,
                "poll": format!("/status/{user_id}"),
                "poll_interval_ms": 500
            }))
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("enqueue failed: {e}")),
    }
}

// Poll endpoint: returns the user's current matchmaking status. The client
// calls this every ~500ms after queueing. Requires the same bearer token.
async fn status(
    state: web::Data<AppState>,
    req: HttpRequest,
    path: web::Path<i64>,
) -> impl Responder {
    let user_id = path.into_inner();

    let token = match req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(auth::bearer)
    {
        Some(t) => t,
        None => return HttpResponse::Unauthorized().body("missing bearer token"),
    };
    match auth::verify(token, &state.jwt_secret) {
        Ok(c) if c.sub == user_id => {}
        Ok(_) => return HttpResponse::Forbidden().body("token does not match user"),
        Err(auth::AuthError::Expired) => return HttpResponse::Unauthorized().body("token expired"),
        Err(_) => return HttpResponse::Unauthorized().body("invalid token"),
    }

    match queue::get_status(&state.redis, user_id) {
        // Status JSON is already well-formed; pass it through.
        Some(payload) => HttpResponse::Ok()
            .content_type("application/json")
            .body(payload),
        // No status key: either never queued or it expired.
        None => HttpResponse::Ok().json(serde_json::json!({ "status": "not_found" })),
    }
}

async fn get_metrics(state: web::Data<AppState>) -> impl Responder {
    // Fully lock-free: pool depth comes from the `waiting` atomic (incremented
    // on enqueue, decremented when a match consumes 10), not from locking and
    // summing every bucket. A monitoring scrape never touches a matcher lock.
    let waiting = state
        .metrics
        .waiting
        .load(std::sync::atomic::Ordering::Relaxed);
    let body = format!(
        "{{\"pool_waiting\":{},\"counters\":{}}}",
        waiting,
        state.metrics.as_json()
    );
    HttpResponse::Ok()
        .content_type("application/json")
        .body(body)
}

async fn health() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

// Write-behind flush: bulk-insert a batch of sync-stream entries into Postgres
// in one transaction, then XACK them. If the DB write fails, the entries are
// left un-ACKed so they are retried on the next pass (at-least-once delivery).
fn flush_batch(
    pg: &db::PgPool,
    redis: &RedisPool,
    metrics: &Arc<Metrics>,
    entries: Vec<queue::SyncEntry>,
) {
    if entries.is_empty() {
        return;
    }
    let batch: Vec<(i64, i32, Vec<(i64, i32)>, Vec<(i64, i32)>)> = entries
        .iter()
        .map(|e| {
            let m = &e.match_result;
            (
                m.match_id as i64,
                m.spread as i32,
                m.team_a.clone(),
                m.team_b.clone(),
            )
        })
        .collect();

    match db::insert_match_batch(pg, &batch) {
        Ok(()) => {
            let ids: Vec<String> = entries.iter().map(|e| e.id.clone()).collect();
            let _ = queue::ack_sync(redis, &ids);
            metrics
                .matches_committed
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        Err(_) => {
            // Leave entries un-ACKed; they stay in the pending list and are
            // retried on the next recovery/read pass.
        }
    }
}

// Group a flat list of match_players rows into nested 5v5 JSON by match.
// `statuses` maps match_id -> status string so each match shows active/ended.
fn group_rosters(
    rows: Vec<crate::models::MatchPlayer>,
    statuses: &std::collections::BTreeMap<i64, String>,
) -> serde_json::Value {
    use std::collections::BTreeMap;
    let mut by_match: BTreeMap<i64, (Vec<serde_json::Value>, Vec<serde_json::Value>)> =
        BTreeMap::new();
    for r in rows {
        let player = serde_json::json!({ "user_id": r.user_id, "rating": r.rating });
        let entry = by_match.entry(r.match_id).or_default();
        if r.team == "A" {
            entry.0.push(player);
        } else {
            entry.1.push(player);
        }
    }
    let matches: Vec<serde_json::Value> = by_match
        .into_iter()
        .rev() // most recent match ids first
        .map(|(mid, (a, b))| {
            let status = statuses
                .get(&mid)
                .cloned()
                .unwrap_or_else(|| "active".into());
            serde_json::json!({
                "match_id": mid,
                "status": status,
                "team_a": a,
                "team_b": b
            })
        })
        .collect();
    serde_json::json!({ "matches": matches })
}

// GET /matches — all merged 5v5 pairings (most recent first), each with status.
async fn list_matches(state: web::Data<AppState>) -> impl Responder {
    let listed = match db::list_matches(&state.pg, 200) {
        Ok(v) => v,
        Err(e) => return HttpResponse::InternalServerError().body(format!("db error: {e}")),
    };
    let statuses: std::collections::BTreeMap<i64, String> = listed.iter().cloned().collect();
    let mut all_rows = Vec::new();
    for (mid, _) in &listed {
        if let Ok(mut rows) = db::match_roster(&state.pg, *mid) {
            all_rows.append(&mut rows);
        }
    }
    HttpResponse::Ok().json(group_rosters(all_rows, &statuses))
}

// GET /players/{id}/matches — every match a player was merged into, plus the
// id of their current active match (their "current position"), if any.
async fn player_matches(state: web::Data<AppState>, path: web::Path<i64>) -> impl Responder {
    let player_id = path.into_inner();
    let rows = match db::matches_for_player(&state.pg, player_id) {
        Ok(r) => r,
        Err(e) => return HttpResponse::InternalServerError().body(format!("db error: {e}")),
    };
    // Per-match status for this player's matches.
    let mut statuses = std::collections::BTreeMap::new();
    for r in &rows {
        statuses.entry(r.match_id).or_insert_with(|| {
            db::match_status(&state.pg, r.match_id)
                .map(|(s, _)| s)
                .unwrap_or_else(|_| "active".into())
        });
    }
    let active = db::active_match_of(&state.pg, player_id).ok().flatten();
    let mut body = group_rosters(rows, &statuses);
    body["current_match"] = serde_json::json!(active);
    HttpResponse::Ok().json(body)
}

// POST /matches/{id}/end — end an active match, freeing its players.
async fn end_match(state: web::Data<AppState>, path: web::Path<i64>) -> impl Responder {
    let match_id = path.into_inner();
    match db::end_match(&state.pg, match_id) {
        Ok(freed) => HttpResponse::Ok().json(serde_json::json!({
            "match_id": match_id, "status": "ended", "players_freed": freed
        })),
        Err(_) => HttpResponse::NotFound().body("match not found or already ended"),
    }
}

// GET /matches/{id} — a single match: its status, spread, and both teams with
// every player. 404 if the match id does not exist.
async fn get_match(state: web::Data<AppState>, path: web::Path<i64>) -> impl Responder {
    let match_id = path.into_inner();
    let (status, spread) = match db::match_status(&state.pg, match_id) {
        Ok(v) => v,
        Err(_) => return HttpResponse::NotFound().body("match not found"),
    };
    let rows = match db::match_roster(&state.pg, match_id) {
        Ok(r) => r,
        Err(e) => return HttpResponse::InternalServerError().body(format!("db error: {e}")),
    };
    let mut statuses = std::collections::BTreeMap::new();
    statuses.insert(match_id, status.clone());
    let body = group_rosters(rows, &statuses);
    // group_rosters wraps in {"matches":[...]}; unwrap to a single match object.
    let mut out = body["matches"]
        .get(0)
        .cloned()
        .unwrap_or(serde_json::json!({}));
    out["spread"] = serde_json::json!(spread);
    HttpResponse::Ok().json(out)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load variables from a .env file. Real environment variables already set
    // take precedence. If there is no .env AND the variables are not in the
    // environment, startup will fail below (fail-closed) — the service refuses
    // to run on implicit defaults.
    let dotenv_loaded = dotenvy::dotenv().is_ok();

    // Read every required variable, accumulating any problems.
    let mut errors: Vec<String> = Vec::new();

    let database_url = require("DATABASE_URL", &mut errors);
    let redis_url = require("REDIS_URL", &mut errors);
    let bind = require("BIND", &mut errors);
    let jwt_secret_str = require("JWT_SECRET", &mut errors);

    let jwt_ttl_secs: u64 = require_parse("JWT_TTL_SECS", &mut errors);
    let player_cache_ttl_secs: u64 = require_parse("PLAYER_CACHE_TTL_SECS", &mut errors);
    let status_ttl_secs: u64 = require_parse("STATUS_TTL_SECS", &mut errors);

    let num_buckets: usize = require_parse("NUM_BUCKETS", &mut errors);
    let bucket_width: u32 = require_parse("BUCKET_WIDTH", &mut errors);
    let num_workers: usize = require_parse("NUM_WORKERS", &mut errors);
    let stride: usize = require_parse("STRIDE", &mut errors);

    let http_workers: usize = require_parse("HTTP_WORKERS", &mut errors);
    let pg_pool_size: u32 = require_parse("PG_POOL_SIZE", &mut errors);
    let redis_pool_size: u32 = require_parse("REDIS_POOL_SIZE", &mut errors);

    let snapshot_dir = require("SNAPSHOT_DIR", &mut errors);
    let snapshot_secs: u64 = require_parse("SNAPSHOT_INTERVAL_SECS", &mut errors);
    let snapshots_kept: usize = require_parse("SNAPSHOTS_KEPT", &mut errors);

    // A few sanity constraints beyond mere presence.
    if num_buckets == 0 {
        errors.push("NUM_BUCKETS must be > 0".into());
    }
    if bucket_width == 0 {
        errors.push("BUCKET_WIDTH must be > 0".into());
    }
    if num_workers == 0 {
        errors.push("NUM_WORKERS must be > 0".into());
    }
    if stride == 0 {
        errors.push("STRIDE must be > 0".into());
    }

    // Fail-closed: if any required variable is missing/invalid, do not start.
    if !errors.is_empty() {
        eprintln!("FATAL: configuration error — the service will not start.");
        if !dotenv_loaded {
            eprintln!("  (no .env file was found; copy .env.example to .env, or set these in the environment)");
        }
        for e in &errors {
            eprintln!("  - {e}");
        }
        std::process::exit(1);
    }

    let jwt_secret = jwt_secret_str.into_bytes();
    // Stream is trimmed to 2x the snapshot interval, so the newest snapshot's
    // offset is always still present in the stream for replay.
    let stream_retain_ms: u64 = snapshot_secs * 2 * 1000;

    let pg = db::build_pool(&database_url, pg_pool_size);
    db::bootstrap(&pg);
    let redis = queue::build_pool(&redis_url, redis_pool_size);
    let metrics = Arc::new(Metrics::default());
    let pool = Arc::new(Pool::new(num_buckets, bucket_width));
    let stop = Arc::new(AtomicBool::new(false));
    let next_match_id = Arc::new(AtomicU64::new(1));
    let ticket = Arc::new(AtomicU64::new(0)); // shared carousel counter

    // The ingestion thread's read cursor (last stream id it consumed), shared
    // with the snapshotter so each snapshot records the exact offset. Seeded
    // from the newest snapshot on boot; "$" means "only new messages".
    let read_cursor = Arc::new(Mutex::new(String::from("$")));

    // --- Boot recovery: restore the newest snapshot, then replay forward. ---
    if let Some(snap) = snapshot::load_latest(&snapshot_dir) {
        let n = snap.players.len();
        pool.restore_players(&snap.players);
        metrics
            .players_enqueued
            .fetch_add(n as u64, Ordering::Relaxed);
        metrics.waiting.fetch_add(n as u64, Ordering::Relaxed);
        // Replay the stream from just after the snapshot's recorded offset so
        // any players that arrived between the snapshot and the crash are not
        // lost. Set the cursor to the snapshot id; the ingestion loop's first
        // read picks up everything after it.
        *read_cursor.lock().unwrap() = snap.last_stream_id.clone();
        println!(
            "recovered {} players from snapshot (created_ms={}); replaying stream after id {}",
            n, snap.created_ms, snap.last_stream_id
        );
    }

    // Thread 1: ingestion (Redis ingest STREAM -> in-memory pool), offset-based.
    // It reads messages strictly after its cursor, places them in the pool, and
    // advances the cursor. The cursor is what the snapshotter persists, so on a
    // crash the pool is restored from the newest snapshot and the stream is
    // replayed from that snapshot's offset — no message lost.
    {
        let (pool, metrics, redis, stop, cursor) = (
            Arc::clone(&pool),
            Arc::clone(&metrics),
            redis.clone(),
            Arc::clone(&stop),
            Arc::clone(&read_cursor),
        );
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let last = cursor.lock().unwrap().clone();
                let batch = queue::read_ingest_after(&redis, &last, 100, 1000);
                if batch.is_empty() {
                    continue;
                }
                let mut newest = last;
                for e in batch {
                    pool.enqueue(Player {
                        id: e.event.id,
                        rating: e.event.rating.max(0) as u32,
                        enqueued_at: Instant::now(),
                    });
                    metrics.players_enqueued.fetch_add(1, Ordering::Relaxed);
                    metrics.waiting.fetch_add(1, Ordering::Relaxed);
                    newest = e.id; // advance to the last id seen
                }
                *cursor.lock().unwrap() = newest;
            }
        });
    }

    // Snapshotter (dedicated thread): every SNAPSHOT_INTERVAL_SECS it copies the
    // pool's (id,rating) pairs out fast (brief per-bucket locks only) and then
    // does ALL slow work — JSON serialization, file write+rename, stream trim —
    // with NO pool lock held, so it never stalls matching. Keeps the last
    // SNAPSHOTS_KEPT files; trims the stream to 2x the interval so the newest
    // snapshot's offset is always still present for replay.
    {
        let (pool, redis, stop, cursor) = (
            Arc::clone(&pool),
            redis.clone(),
            Arc::clone(&stop),
            Arc::clone(&read_cursor),
        );
        let dir = snapshot_dir.clone();
        thread::spawn(move || {
            // Coarse sleep loop so shutdown is responsive.
            let mut elapsed = 0u64;
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                elapsed += 1;
                if elapsed < snapshot_secs {
                    continue;
                }
                elapsed = 0;
                // Read cursor first, then pool: the cursor is never ahead of
                // the captured players, so replay can only ever re-add a
                // player (harmless), never skip one.
                let last_id = cursor.lock().unwrap().clone();
                let players = pool.snapshot_players(); // brief per-bucket locks
                                                       // --- everything below holds NO pool lock ---
                if let Err(e) = snapshot::write_snapshot(&dir, &last_id, players, snapshots_kept) {
                    eprintln!("snapshot write failed: {e}");
                }
                let _ = queue::trim_ingest(&redis, stream_retain_ms);
            }
        });
    }

    // Shared pending-commit retry queue (matches awaiting a successful Redis
    // write). Formed matches are never discarded — only their write is retried.
    let retry_q = new_retry_queue();

    // Match threads: all share one atomic ticket counter (the carousel).
    for _ in 0..num_workers {
        let (pool, metrics, redis, stop, nmid, tk) = (
            Arc::clone(&pool),
            Arc::clone(&metrics),
            redis.clone(),
            Arc::clone(&stop),
            Arc::clone(&next_match_id),
            Arc::clone(&ticket),
        );
        let sttl = status_ttl_secs;
        let rq = retry_q.clone();
        let strd = stride;
        thread::spawn(move || run_worker(pool, metrics, redis, stop, nmid, tk, strd, sttl, rq));
    }

    // Retry thread: owns the pending-commit queue and re-attempts the Redis
    // write (with backoff) until each deferred match lands. Never drops a match.
    {
        let (rq, redis, metrics, stop) = (
            retry_q.clone(),
            redis.clone(),
            Arc::clone(&metrics),
            Arc::clone(&stop),
        );
        let sttl = status_ttl_secs;
        thread::spawn(move || run_retry_worker(rq, redis, metrics, stop, sttl));
    }

    // Write-behind DB poller: drains the sync stream in micro-batches and
    // bulk-inserts to Postgres. Reading the DB is fully decoupled from the
    // match threads, which only write Redis. Unacked entries survive a crash.
    {
        let (pg, metrics, redis, stop) = (
            pg.clone(),
            Arc::clone(&metrics),
            redis.clone(),
            Arc::clone(&stop),
        );
        // Make sure the consumer group exists before anyone reads.
        let _ = queue::ensure_sync_group(&redis);
        thread::spawn(move || {
            let consumer = "db-writer-1";

            // Recovery pass: reclaim any entries delivered to this consumer
            // before a previous crash that were never acknowledged.
            let pending = queue::read_pending(&redis, consumer, 500);
            if !pending.is_empty() {
                flush_batch(&pg, &redis, &metrics, pending);
            }

            while !stop.load(Ordering::Relaxed) {
                // Pull up to 50 new entries, blocking up to 1s when idle.
                let batch = queue::read_sync_batch(&redis, consumer, 50, 1000);
                if batch.is_empty() {
                    continue;
                }
                flush_batch(&pg, &redis, &metrics, batch);
            }
        });
    }

    let state = web::Data::new(AppState {
        pg,
        redis,
        metrics,
        pool,
        jwt_secret,
        jwt_ttl_secs,
        player_cache_ttl_secs,
        status_ttl_secs,
    });
    println!("matchmaker listening on http://{bind}");

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/health", web::get().to(health))
            .route("/metrics", web::get().to(get_metrics))
            .route("/signup", web::post().to(signup))
            .route("/signin/{id}", web::post().to(signin))
            .route("/queue/{id}", web::post().to(enter_queue))
            .route("/status/{id}", web::get().to(status))
            .route("/matches", web::get().to(list_matches))
            .route("/matches/{id}", web::get().to(get_match))
            .route("/matches/{id}/end", web::post().to(end_match))
            .route("/players/{id}/matches", web::get().to(player_matches))
    })
    .bind(&bind)?
    .workers(http_workers)
    .run()
    .await
}
