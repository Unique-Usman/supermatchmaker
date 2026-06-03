// Match-pool snapshots for crash recovery (snapshot + stream-replay model).
//
// Every snapshot captures (a) all waiting players in the pool and (b) the id
// of the last ingest-stream message the ingestion thread had read. On boot the
// newest snapshot is loaded: players are restored into the pool and the stream
// is replayed from just after the stored id, so no message is lost.
//
// The last N snapshots are kept (older ones pruned). Snapshots are written to
// `SNAPSHOT_DIR` (env, default ./snapshots) as JSON files named
// snapshot-<unixmillis>.json.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug)]
pub struct Snapshot {
    pub created_ms: u64,
    pub last_stream_id: String,   // ingest-stream offset at snapshot time
    pub players: Vec<(i64, u32)>, // (user_id, rating)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// Write a new snapshot file and prune to the most recent `keep` files.
pub fn write_snapshot(
    dir: &str,
    last_stream_id: &str,
    players: Vec<(i64, u32)>,
    keep: usize,
) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let snap = Snapshot {
        created_ms: now_ms(),
        last_stream_id: last_stream_id.to_string(),
        players,
    };
    let path = Path::new(dir).join(format!("snapshot-{}.json", snap.created_ms));
    let tmp = path.with_extension("json.tmp");
    // Write to a temp file then rename, so a crash mid-write never leaves a
    // half-written snapshot that would be loaded as "newest".
    fs::write(&tmp, serde_json::to_vec(&snap).unwrap())?;
    fs::rename(&tmp, &path)?;
    prune(dir, keep);
    Ok(())
}

// List snapshot files newest-first.
fn list_snapshots(dir: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("snapshot-") && n.ends_with(".json"))
                    .unwrap_or(false)
            })
            .collect(),
        Err(_) => return vec![],
    };
    // Filenames embed the millis timestamp, so lexical sort == time order.
    files.sort();
    files.reverse(); // newest first
    files
}

fn prune(dir: &str, keep: usize) {
    let files = list_snapshots(dir);
    for old in files.into_iter().skip(keep) {
        let _ = fs::remove_file(old);
    }
}

// Load the newest valid snapshot, if any. Tries newest-first and skips any
// that fail to parse (e.g. a torn write that somehow survived).
pub fn load_latest(dir: &str) -> Option<Snapshot> {
    for path in list_snapshots(dir) {
        if let Ok(bytes) = fs::read(&path) {
            if let Ok(snap) = serde_json::from_slice::<Snapshot>(&bytes) {
                return Some(snap);
            }
        }
    }
    None
}
