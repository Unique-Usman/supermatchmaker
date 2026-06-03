// Low-latency health metrics: plain atomics, read without locking the pool.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub signups: AtomicU64,
    pub events_published: AtomicU64,
    pub players_enqueued: AtomicU64,
    pub waiting: AtomicU64,
    pub matches_formed: AtomicU64,
    pub matches_committed: AtomicU64,
    pub total_queue_micros: AtomicU64,
    pub spread_sum: AtomicU64,
    pub commit_deferred: AtomicU64, // matches handed to the retry queue
    pub commit_retry_attempts: AtomicU64, // failed retry attempts
    pub commit_retry_success: AtomicU64, // matches finally written by retry
}

impl Metrics {
    pub fn as_json(&self) -> String {
        let mf = self.matches_formed.load(Ordering::Relaxed).max(1);
        let players = (self.matches_formed.load(Ordering::Relaxed) * 10).max(1);
        format!(
            "{{\"signups\":{},\"events_published\":{},\"players_enqueued\":{},\
\"waiting\":{},\"matches_formed\":{},\"matches_committed\":{},\
\"avg_queue_ms\":{:.2},\"avg_team_gap\":{:.2},\
\"commit_deferred\":{},\"commit_retry_attempts\":{},\"commit_retry_success\":{}}}",
            self.signups.load(Ordering::Relaxed),
            self.events_published.load(Ordering::Relaxed),
            self.players_enqueued.load(Ordering::Relaxed),
            self.waiting.load(Ordering::Relaxed),
            self.matches_formed.load(Ordering::Relaxed),
            self.matches_committed.load(Ordering::Relaxed),
            (self.total_queue_micros.load(Ordering::Relaxed) as f64 / players as f64) / 1000.0,
            self.spread_sum.load(Ordering::Relaxed) as f64 / mf as f64,
            self.commit_deferred.load(Ordering::Relaxed),
            self.commit_retry_attempts.load(Ordering::Relaxed),
            self.commit_retry_success.load(Ordering::Relaxed),
        )
    }
}
