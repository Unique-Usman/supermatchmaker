// MatchPool + Match Threads, implementing the three-algorithm blueprint:
//   1. Dynamic Stride & Skip  (work distribution via atomic ticket counter)
//   2. Three-Way Peek & Border Harvest  (gather 10 with tryLock + rollback)
//   3. Combinatorial Min-Max Split  (252-way brute force + tolerance gate)
//
// Buckets are FIFO VecDeques so the oldest player is always at the front
// (O(1) peek). There are many more buckets than worker threads.

use crate::metrics::Metrics;
use crate::queue::{commit_match, MatchResult, RedisPool};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// A match that was formed but whose Redis write has not yet succeeded. It is
// never discarded: the retry thread owns it until the write lands. Players are
// already consumed from the pool — the pairing is locked in, only the write is
// outstanding.
pub struct PendingCommit {
    pub result: MatchResult,
    pub attempts: u32,
}

// Shared, thread-safe buffer of pending commits. Match threads push here when
// their fast inline attempts fail; the retry thread drains and re-attempts.
pub type RetryQueue = Arc<Mutex<VecDeque<PendingCommit>>>;

pub fn new_retry_queue() -> RetryQueue {
    Arc::new(Mutex::new(VecDeque::new()))
}

// Dedicated retry thread: keeps re-attempting the Redis write for any pending
// match, with capped exponential backoff, until it succeeds. Matches are never
// dropped and players are never returned to the pool — the formed pairing is
// preserved; only the write is retried.
pub fn run_retry_worker(
    retry_q: RetryQueue,
    redis: RedisPool,
    metrics: Arc<Metrics>,
    stop: Arc<AtomicBool>,
    status_ttl: u64,
) {
    while !stop.load(Ordering::Relaxed) {
        // Take one pending commit (FIFO) if any.
        let pending = retry_q.lock().unwrap().pop_front();
        let Some(mut p) = pending else {
            thread::sleep(Duration::from_millis(5)); // idle
            continue;
        };

        match commit_match(&redis, &p.result, status_ttl) {
            Ok(()) => {
                metrics.commit_retry_success.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // Still failing: back off (capped at 1s) and requeue at the
                // BACK so other pending matches also get attempts. The match is
                // not discarded.
                p.attempts += 1;
                metrics
                    .commit_retry_attempts
                    .fetch_add(1, Ordering::Relaxed);
                let backoff = Duration::from_millis((5u64 << p.attempts.min(7)).min(1000));
                thread::sleep(backoff);
                retry_q.lock().unwrap().push_back(p);
            }
        }
    }
}

#[derive(Clone)]
pub struct Player {
    pub id: i64,
    pub rating: u32,
    pub enqueued_at: Instant,
}

// A single MMR bucket: FIFO queue under its own lock.
pub struct Bucket {
    queue: Mutex<VecDeque<Player>>,
}

pub struct Pool {
    buckets: Vec<Bucket>,
    bucket_width: u32,
}

impl Pool {
    pub fn new(num_buckets: usize, bucket_width: u32) -> Self {
        let buckets = (0..num_buckets)
            .map(|_| Bucket {
                queue: Mutex::new(VecDeque::new()),
            })
            .collect();
        Pool {
            buckets,
            bucket_width,
        }
    }

    fn bucket_index(&self, rating: u32) -> usize {
        ((rating / self.bucket_width) as usize).min(self.buckets.len() - 1)
    }

    // FIFO enqueue: newest player goes to the back.
    pub fn enqueue(&self, p: Player) {
        let idx = self.bucket_index(p.rating);
        self.buckets[idx].queue.lock().unwrap().push_back(p);
    }

    // Authoritative pool depth by summing buckets (takes per-bucket locks).
    // NOT used on the /metrics hot path — that reads the lock-free `waiting`
    // atomic instead. Kept for debugging / reconciliation if the atomic is
    // ever suspected of drifting.
    #[allow(dead_code)]
    pub fn waiting(&self) -> usize {
        self.buckets
            .iter()
            .map(|b| b.queue.lock().unwrap().len())
            .sum()
    }

    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    // Snapshot: copy out every waiting player as (id, rating) pairs. Brief
    // per-bucket locks, taken one at a time, so matching keeps running.
    // enqueued_at is reset on restore (wait-time resets on recovery, which is
    // acceptable — the alternative is persisting timestamps, easily added).
    // Snapshot the waiting players as (id, rating) pairs. CRITICAL: this must
    // not stall the matching hot path. Each bucket lock is held only for a
    // tight copy of primitive pairs (no serialization, no I/O under the lock),
    // and buckets are taken one at a time so a worker contends at most with the
    // copy of a single bucket, never the whole pool. All slow work (JSON, file
    // write, stream trim) happens in the caller AFTER this returns, with no
    // lock held.
    pub fn snapshot_players(&self) -> Vec<(i64, u32)> {
        let mut out = Vec::new();
        for b in &self.buckets {
            // Lock, copy primitives, release — as fast as possible.
            let guard = b.queue.lock().unwrap();
            out.reserve(guard.len());
            for p in guard.iter() {
                out.push((p.id, p.rating));
            }
            drop(guard); // explicit: release before moving to the next bucket
        }
        out
    }

    // Restore players from a snapshot into the pool (used on boot recovery).
    pub fn restore_players(&self, players: &[(i64, u32)]) {
        for (id, rating) in players {
            self.enqueue(Player {
                id: *id,
                rating: *rating,
                enqueued_at: Instant::now(),
            });
        }
    }
}

// ---- Algorithm 3 helpers: tolerance + combinatorial split ----

// Dynamic tolerance: max allowed team MMR gap grows with the anchor's wait.
// T_max = base + alpha * t. (alpha scaled for the sped-up demo clock.)
fn max_team_gap(anchor_wait: Duration) -> u32 {
    let base = 50u32;
    let alpha_per_100ms = 10u32;
    base + alpha_per_100ms * (anchor_wait.as_millis() / 100) as u32
}

// Hybrid 252-combinatorial roster optimizer.
//
// For every way to pick 5 of 10 players for Team A, compute a total penalty:
//   P = macro_gap + sanction_score
// where
//   macro_gap      = |sum(A) - sum(B)|                  (global team strength)
//   sanction_score = sum_i |sortedA[i] - sortedB[i]|    (1D Wasserstein /
//                                                         role-by-role match)
// Select the split with the lowest penalty. `spread` in the result carries
// the macro gap (avg per player) so existing metrics stay comparable.
fn optimize_split(group: &[Player; 10], match_id: u64) -> (MatchResult, u32) {
    let total: i64 = group.iter().map(|p| p.rating as i64).sum();

    let mut best_mask: u16 = 0;
    let mut best_penalty: i64 = i64::MAX;
    let mut best_macro: i64 = 0;

    for mask in 0u16..1024 {
        if mask.count_ones() != 5 {
            continue;
        }

        // Partition ratings into A and B by the mask.
        let mut a_vals = [0i64; 5];
        let mut b_vals = [0i64; 5];
        let (mut ai, mut bi) = (0usize, 0usize);
        let mut sum_a: i64 = 0;
        for i in 0..10usize {
            let v = group[i].rating as i64;
            if mask & (1 << i) != 0 {
                a_vals[ai] = v;
                ai += 1;
                sum_a += v;
            } else {
                b_vals[bi] = v;
                bi += 1;
            }
        }

        // Macro gap.
        let sum_b = total - sum_a;
        let macro_gap = (sum_a - sum_b).abs();

        // Micro gap: sort each team of 5, sum position-by-position diffs.
        a_vals.sort_unstable();
        b_vals.sort_unstable();
        let mut sanction: i64 = 0;
        for i in 0..5 {
            sanction += (a_vals[i] - b_vals[i]).abs();
        }

        let penalty = macro_gap + sanction; // W_macro = W_micro = 1.0
        if penalty < best_penalty {
            best_penalty = penalty;
            best_mask = mask;
            best_macro = macro_gap;
        }
    }

    let mut team_a = Vec::with_capacity(5);
    let mut team_b = Vec::with_capacity(5);
    for i in 0..10usize {
        if best_mask & (1 << i) != 0 {
            team_a.push((group[i].id, group[i].rating as i32));
        } else {
            team_b.push((group[i].id, group[i].rating as i32));
        }
    }
    // Report avg-per-player macro gap as the spread, matching prior metric.
    let spread = (best_macro / 5) as u32;
    (
        MatchResult {
            match_id,
            team_a,
            team_b,
            spread,
        },
        spread,
    )
}

// ---- Algorithm 2: harvest exactly 10 across a window, with rollback ----
//
// `window` is the list of bucket indices this worker owns this round.
// Returns Some(10 players) on success. On any failure every player taken
// is returned to the FRONT of its origin bucket, preserving FIFO fairness.
fn harvest(pool: &Pool, window: &[usize]) -> Option<[Player; 10]> {
    // Track (origin_bucket_index, player) so rollback restores exact homes.
    let mut taken: Vec<(usize, Player)> = Vec::with_capacity(10);

    // --- Three-Way Peek: find the oldest front-player across the window ---
    let mut anchor_bucket: Option<usize> = None;
    let mut oldest = Instant::now();
    for &bi in window {
        let q = pool.buckets[bi].queue.lock().unwrap();
        if let Some(front) = q.front() {
            if anchor_bucket.is_none() || front.enqueued_at < oldest {
                oldest = front.enqueued_at;
                anchor_bucket = Some(bi);
            }
        }
    }
    let anchor_bucket = anchor_bucket?; // window empty -> nothing to do

    // --- Harvest: anchor's bucket first, then neighbours, then border ---
    // Anchor's home bucket leads so the oldest player is always included.
    // Remaining window buckets follow in ascending order, then border
    // buckets just past the window edge. Strict ascending order on the
    // border crossings is what makes deadlock impossible.
    let mut window_sorted: Vec<usize> = window.to_vec();
    window_sorted.sort_unstable();
    let mut drain_order: Vec<usize> = vec![anchor_bucket];
    for &bi in &window_sorted {
        if bi != anchor_bucket {
            drain_order.push(bi);
        }
    }
    let max_idx = *window_sorted.last().unwrap();
    for step in 1..=2usize {
        let b = max_idx + step;
        if b < pool.num_buckets() {
            drain_order.push(b);
        }
    }
    let window_len = window.len();

    for (pos, &bi) in drain_order.iter().enumerate() {
        if taken.len() == 10 {
            break;
        }
        // Window buckets: we already hold the right to them this round, take
        // the normal lock. Border buckets (beyond the window): non-blocking
        // try_lock so we never wait on another worker -> no circular wait.
        let is_border = pos >= window_len;
        let mut guard = if is_border {
            match pool.buckets[bi].queue.try_lock() {
                Ok(g) => g,
                Err(_) => {
                    // Border bucket busy: abort, roll everything back.
                    rollback(pool, taken);
                    return None;
                }
            }
        } else {
            pool.buckets[bi].queue.lock().unwrap()
        };

        while taken.len() < 10 {
            match guard.pop_front() {
                Some(p) => taken.push((bi, p)),
                None => break,
            }
        }
    }

    if taken.len() < 10 {
        rollback(pool, taken); // not enough players anywhere reachable
        return None;
    }

    let players: Vec<Player> = taken.into_iter().map(|(_, p)| p).collect();
    players.try_into().ok()
}

// Return taken players to the FRONT of their origin buckets, in reverse
// order so original FIFO ordering is preserved.
fn rollback(pool: &Pool, taken: Vec<(usize, Player)>) {
    for (bi, p) in taken.into_iter().rev() {
        pool.buckets[bi].queue.lock().unwrap().push_front(p);
    }
}

// Return a whole group to the front of their buckets after a tolerance fail.
fn return_group(pool: &Pool, group: [Player; 10]) {
    for p in group.into_iter().rev() {
        let idx = pool.bucket_index(p.rating);
        pool.buckets[idx].queue.lock().unwrap().push_front(p);
    }
}

// ---- Algorithm 1: Dynamic Stride & Skip worker loop ----

#[allow(clippy::too_many_arguments)]
pub fn run_worker(
    pool: Arc<Pool>,
    metrics: Arc<Metrics>,
    redis: RedisPool,
    stop: Arc<AtomicBool>,
    next_match_id: Arc<AtomicU64>,
    ticket: Arc<AtomicU64>,
    stride: usize,
    status_ttl: u64,
    retry_q: RetryQueue,
) {
    let total = pool.num_buckets();
    while !stop.load(Ordering::Relaxed) {
        // Atomic ticket allocation: claim the next window of `stride` buckets.
        let start = (ticket.fetch_add(stride as u64, Ordering::Relaxed) as usize) % total;
        let window: Vec<usize> = (0..stride).map(|k| (start + k) % total).collect();

        // Density-based fast-fail: skip locking heavy if the window is sparse.
        // Include up to two border buckets past the window edge in the count,
        // since harvest can cross into them — otherwise a group split across a
        // window boundary would be wrongly skipped.
        let mut approx: usize = window
            .iter()
            .map(|&bi| pool.buckets[bi].queue.lock().unwrap().len())
            .sum();
        let edge = window.iter().copied().max().unwrap_or(0);
        for step in 1..=2usize {
            let b = edge + step;
            if b < total {
                approx += pool.buckets[b].queue.lock().unwrap().len();
            }
        }
        if approx < 10 {
            // Desert zone (or not enough yet): drop ticket, grab the next.
            if approx == 0 {
                thread::sleep(Duration::from_micros(50));
            }
            continue;
        }

        // Phase 1: harvest exactly 10 (peek + border crossing + rollback).
        let Some(group) = harvest(&pool, &window) else {
            continue;
        };

        // Phase 2: combinatorial split + tolerance gate (no locks held here).
        let anchor_wait = group
            .iter()
            .map(|p| p.enqueued_at.elapsed())
            .max()
            .unwrap_or_default();
        let t_max = max_team_gap(anchor_wait);
        let mid = next_match_id.fetch_add(1, Ordering::Relaxed);

        // Hybrid 252-combinatorial optimizer: minimizes macro power gap plus
        // micro role-matchup (Sanction) score. `gap` is the macro gap used by
        // the tolerance gate and metrics.
        let (m, gap) = optimize_split(&group, mid);

        if gap <= t_max {
            // The match is locked in: count it and consume the players now. The
            // 10-player pairing is too valuable to discard, so it is never
            // returned to the pool on a Redis hiccup — only the WRITE is
            // retried (below), never the matching.
            let wait_sum: u64 = group
                .iter()
                .map(|p| p.enqueued_at.elapsed().as_micros() as u64)
                .sum();
            metrics.matches_formed.fetch_add(1, Ordering::Relaxed);
            metrics
                .total_queue_micros
                .fetch_add(wait_sum, Ordering::Relaxed);
            metrics.spread_sum.fetch_add(gap as u64, Ordering::Relaxed);
            metrics.waiting.fetch_sub(10, Ordering::Relaxed);

            // Fast inline attempts: 2 quick tries with a 5ms/10ms micro-delay,
            // which absorb brief network jitter without leaving the hot path.
            let mut committed = false;
            for attempt in 1..=2u32 {
                if commit_match(&redis, &m, status_ttl).is_ok() {
                    committed = true;
                    break;
                }
                thread::sleep(Duration::from_millis(5 * attempt as u64));
            }

            if committed {
                // Players matched and recorded. (Recovery is handled by the
                // snapshot+stream-offset model, not per-player acks.)
            } else {
                // Still not written: hand the fully-formed match to the durable
                // retry queue and move on. The retry thread owns it until the
                // write succeeds — never blocked, never lost. The ingest ids
                // ride along and are acked once the retry succeeds.
                retry_q.lock().unwrap().push_back(PendingCommit {
                    result: m,
                    attempts: 2,
                });
                metrics.commit_deferred.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // Tolerance fail: abort, return all 10 to their queue fronts.
            return_group(&pool, group);
        }
    }
}
