# Setup & Deployment Guide

How to configure PostgreSQL and Redis, then build and run the matchmaker.

## 1. Prerequisites

- Rust 1.80+ (`rustc --version`). The code uses the `jsonwebtoken` crate,
  whose crypto deps require a current toolchain.
- PostgreSQL 14+
- Redis 6+ (Streams are used)

On Arch Linux:
```bash
sudo pacman -S postgresql redis
```

## 2. PostgreSQL

### Initialize and start (first time)
```bash
# Arch: init the data dir as the postgres user
sudo -iu postgres initdb -D /var/lib/postgres/data
sudo systemctl enable --now postgresql
```

### Create the database and a user
```bash
sudo -iu postgres psql <<'SQL'
CREATE DATABASE matchmaker;
CREATE USER mm WITH PASSWORD 'changeme';
GRANT ALL PRIVILEGES ON DATABASE matchmaker TO mm;
SQL
```
The service creates its tables automatically on first start (users, matches,
match_players, plus indexes), so no manual migration is needed.

Your `DATABASE_URL` is then:
```
postgres://mm:changeme@127.0.0.1:5432/matchmaker
```

## 3. Redis

### Start
```bash
sudo systemctl enable --now redis
redis-cli ping   # -> PONG
```

### Production durability 
For local development and load testing you do NOT need this. Plain Redis works
fine and the service runs without it. It only matters in production: if the
Redis process itself crashes, persistence lets it reload the in flight streams
on restart.

A fresh Redis install does not have these settings; you add them. Find your
config path first (it varies by distro, and on Arch it may be `/etc/redis/redis.conf`):
```bash
systemctl status redis        # shows the config file in use
redis-cli CONFIG GET dir      # where Redis stores its data
```
Then add to that file and restart Redis:
```
appendonly yes
appendfsync everysec
```
Or enable it live with no file editing (resets on restart unless also in the file):
```bash
redis-cli CONFIG SET appendonly yes
```
Without persistence, a Redis crash loses the in flight streams (the matchmaker
crashing is still covered by snapshots; this is specifically about Redis dying).

Your `REDIS_URL`:
```
redis://127.0.0.1:6379
```

## 4. Configure the service (.env)

All config is required, and the service refuses to start without it (fail closed).
```bash
cp .env.example .env
```
Edit `.env` and set every value. Minimum to change: `DATABASE_URL`, `REDIS_URL`,
and `JWT_SECRET` (generate a strong one):
```bash
# generate a secret
openssl rand -hex 32
```

`.env` lives in the project root (next to Cargo.toml), NOT in src/. It is
gitignored so secrets are never committed.

## 5. Build and run

```bash
cargo build --release
cargo run --release
# or run the binary directly:
./target/release/mm_service
```
On a misconfigured/missing .env it prints exactly what's wrong and exits 1.
On success it logs: `matchmaker listening on http://<BIND>`.

## 6. Smoke test

```bash
# health
curl localhost:8080/health           # -> ok

# sign up (returns id + JWT)
TOKEN=$(curl -s -XPOST localhost:8080/signup \
  -H 'Content-Type: application/json' \
  -d '{"name":"alice","rating":1500}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')

# enter the queue (needs the token)
curl -XPOST localhost:8080/queue/1 -H "Authorization: Bearer $TOKEN"

# poll status every ~500ms
curl localhost:8080/status/1 -H "Authorization: Bearer $TOKEN"

# once matched, view it
curl localhost:8080/matches
curl localhost:8080/matches/1
curl localhost:8080/players/1/matches

# metrics
curl localhost:8080/metrics
```

## 7. Load test
```bash
python3 simulate.py 100000 500     # 100000 players, 500 concurrent clients
```


## Operations & useful commands

Set your URLs first (match your `.env`):
```bash
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/matchmaker"
export REDIS_URL="redis://127.0.0.1:6379"
```

### Watch it live
```bash
watch -n1 'curl -s localhost:8080/metrics | python3 -m json.tool'
```
Healthy signs: `matches_formed` and `matches_committed` rise together (nothing
lost); `pool_waiting` climbs during injection then drains; `avg_gap` stays ~1-2.

### Inspect: API
```bash
curl localhost:8080/metrics
curl localhost:8080/matches
curl localhost:8080/matches/1
curl localhost:8080/players/1/matches
```

### Inspect: Postgres
```bash
psql "$DATABASE_URL" -c "SELECT count(*) FROM users;"
psql "$DATABASE_URL" -c "SELECT count(*) FROM matches;"
psql "$DATABASE_URL" -c "SELECT count(*) FROM match_players;"
psql "$DATABASE_URL" -c "SELECT status, count(*) FROM matches GROUP BY status;"
# the 10 players of match #1, by team
psql "$DATABASE_URL" -c "SELECT match_id, team, user_id, rating FROM match_players WHERE match_id = 1 ORDER BY team;"
```

### Inspect: Redis
```bash
redis-cli XLEN mm:ingest:stream     # players waiting in the ingest stream
redis-cli XLEN match:sync:stream    # matches pending DB write
redis-cli --scan --pattern 'mm:status:*' | wc -l   # active poll status keys
redis-cli GET mm:status:1           # one player's poll status
redis-cli XPENDING match:sync:stream db-writers   # un acked (retrying) matches
```

### Inspect: snapshots
```bash
ls -la snapshots/                                       # SNAPSHOT_DIR from .env
cat snapshots/snapshot-*.json | python3 -m json.tool    # newest content
```

### Re initialize everything (full reset)
Stop the server (Ctrl-C), then wipe all three stores together so leftover
stream entries or a stale snapshot can't replay into a fresh DB:
```bash
# 1. Postgres: drop tables (auto recreated on next startup)
psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS match_players, matches, users CASCADE;"

# 2. Redis: flush this app's keys
redis-cli DEL mm:ingest:stream match:sync:stream
redis-cli --scan --pattern 'mm:status:*' | xargs -r redis-cli DEL
redis-cli --scan --pattern 'mm:player:*' | xargs -r redis-cli DEL
# (or, if Redis is dedicated to this app:  redis-cli FLUSHALL)

# 3. Snapshots
rm -rf snapshots/

# 4. restart
cargo run --release
```


## Endpoints

| Method | Path                     | Auth   | Purpose                              |
|--------|--------------------------|--------|--------------------------------------|
| GET    | /health                  | no     | liveness                             |
| GET    | /metrics                 | no     | lock free counters                   |
| POST   | /signup                  | no     | create user, returns id + JWT        |
| POST   | /signin/{id}             | no     | returns rating + JWT                 |
| POST   | /queue/{id}              | bearer | enter matchmaking (202, then poll)   |
| GET    | /status/{id}             | bearer | poll: queued / matched / not_found   |
| GET    | /matches                 | no     | all matches (most recent first)      |
| GET    | /matches/{id}            | no     | a single match with both teams       |
| POST   | /matches/{id}/end        | no     | end an active match, free its players|
| GET    | /players/{id}/matches    | no     | a player's match history             |

## Implementation details (deep dive)

These expand on the design summary in the README.

### Bucket carousel (work distribution)
Workers share one atomic ticket counter; each round a worker claims a window of
`STRIDE` buckets via `fetch_add(STRIDE) % NUM_BUCKETS`. Before locking it checks
the window's (and neighbouring buckets') player count and skips sparse windows
(density fast fail), so threads swarm dense mid MMR brackets. With more buckets
than workers, this self balances load.

### Harvest + atomic eviction
Buckets are FIFO, so the oldest player is at the front (O(1) peek). The worker
anchors on the oldest across its window, drains the anchor's bucket first then
neighbours, and may cross into a border bucket via non blocking `try_lock` (in
ascending index order, deadlock free). If it can't fill 10, it rolls every
taken player back to its queue front.

### Team split (hybrid 252)
All C(10,5)=252 splits are scored by `macro_gap + sanction_score` and the
minimum is committed only if its gap <= `t_max` (the relaxation gate). On a
tolerance fail the group returns to the pool.

### Async request / poll
`POST /queue/{id}` returns 202 immediately and writes a `queued` status to a
Redis cache key (`mm:status:{id}`, TTL `STATUS_TTL_SECS`). The client polls
`GET /status/{id}` every ~500ms; it reads only Redis, never Postgres. When a
match commits, the worker flips the key to `matched` with both rosters.

### Pipelined Redis commit
On a match, the worker bundles the 10 status ticket writes plus the sync stream
`XADD` into a single Redis pipeline, one network round trip instead of 11.

### Write behind persistence
Match results go onto a Redis stream (`match:sync:stream`). A poller consumes
via a consumer group (`XREADGROUP`), bulk inserts up to 50 matches per Postgres
transaction, then `XACK`s. Failed inserts stay un acked and are retried (and
replayed on restart from the pending list), at least once, with match id
idempotency absorbing duplicates.

### Commit retry (never discard a formed match)
If the pipelined Redis write fails, the worker makes 2 fast inline retries
(5ms/10ms); still failing, it hands the fully formed match to an in memory retry
queue owned by a dedicated thread that re attempts with backoff until it lands.
The match is never re formed and players are never returned to the pool.

### Crash recovery (snapshot + stream replay)
The ingestion thread reads the ingest stream with `XREAD` from a cursor (last id
read). Every `SNAPSHOT_INTERVAL_SECS` the snapshotter writes the pool's players
plus that cursor to a file (last `SNAPSHOTS_KEPT` kept; the stream is trimmed to
2x the interval so the newest snapshot's offset always survives). On boot it
restores the newest snapshot and replays the stream from that offset, with no
waiting player lost. The snapshot copies players under brief per bucket locks and
does all serialization/IO with no lock held, so it never stalls matching.

### Fully lock free metrics
All `/metrics` fields are atomic loads, including `pool_waiting` (an atomic
incremented on enqueue, decremented on match), so monitoring never takes a pool
lock.

### Configuration is fail closed
Every variable is required; a missing/invalid one prints exactly what's wrong
and exits 1 before any connection or thread is created. No hardcoded defaults.
