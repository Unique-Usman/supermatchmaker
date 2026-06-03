// Database -> "User and Rating" box. Diesel + Postgres behind an r2d2 pool.

use crate::models::{MatchPlayer, NewMatch, NewMatchPlayer, NewUser};
use crate::schema::users::dsl::*;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::sql_query;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

pub fn build_pool(database_url: &str, max_size: u32) -> PgPool {
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    Pool::builder()
        .max_size(max_size)
        .build(manager)
        .expect("failed to build Postgres pool")
}

// Create the tables if they do not exist (stands in for migrations).
pub fn bootstrap(pool: &PgPool) {
    let mut conn = pool.get().expect("db conn");
    sql_query(
        "CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            rating INT NOT NULL,
            in_match BOOL NOT NULL DEFAULT FALSE,
            matches_played INT NOT NULL DEFAULT 0
        )",
    )
    .execute(&mut conn)
    .expect("create users table");

    sql_query(
        "CREATE TABLE IF NOT EXISTS matches (
            id BIGINT PRIMARY KEY,
            spread INT NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ended_at TIMESTAMPTZ
        )",
    )
    .execute(&mut conn)
    .expect("create matches table");

    sql_query(
        "CREATE TABLE IF NOT EXISTS match_players (
            id BIGSERIAL PRIMARY KEY,
            match_id BIGINT NOT NULL REFERENCES matches(id),
            user_id BIGINT NOT NULL,
            team VARCHAR NOT NULL,
            rating INT NOT NULL,
            active BOOL NOT NULL DEFAULT TRUE
        )",
    )
    .execute(&mut conn)
    .expect("create match_players table");

    // A player can be in at most ONE active match at a time. Partial unique
    // index: enforced only over rows where active = TRUE.
    sql_query(
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_player
         ON match_players(user_id) WHERE active",
    )
    .execute(&mut conn)
    .expect("create active-player unique index");

    sql_query("CREATE INDEX IF NOT EXISTS idx_mp_user ON match_players(user_id)")
        .execute(&mut conn)
        .expect("create match_players user index");
}

// Backend: user_signup. Returns the new user's id.
pub fn create_user(pool: &PgPool, user_name: &str, user_rating: i32) -> QueryResult<i64> {
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    diesel::insert_into(users)
        .values(NewUser {
            name: user_name,
            rating: user_rating,
        })
        .returning(id)
        .get_result(&mut conn)
}

// Backend: user_signin. Read a user's stored rating.
pub fn rating_of(pool: &PgPool, user_id: i64) -> QueryResult<i32> {
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    users.find(user_id).select(rating).first(&mut conn)
}

// Poller: (record_match removed — replaced by insert_match_batch which flags
// players as part of the bulk write-behind transaction.)

// (fetch_user removed — not used by any endpoint.)

// Persist a completed match: the matches row plus one match_players row per
// player. Positions 1..5 are assigned by skill rank within each team
// (position 1 = highest-rated). One player per position per team is enforced
// by the UNIQUE (match_id, team, position) constraint. Idempotent: if the
// match id already exists (duplicate result), it is skipped.
// Micro-batch write-behind: persist a batch of matches in ONE transaction.
// `batch` items are (match_id, spread, team_a, team_b). This protects the DB
// from per-match sequential I/O and connection-pool churn. Returns Ok(()) if
// the whole batch committed; the caller then XACKs the corresponding stream
// ids. Any error rolls the whole transaction back and leaves them un-ACKed.
#[allow(clippy::type_complexity)]
pub fn insert_match_batch(
    pool: &PgPool,
    batch: &[(i64, i32, Vec<(i64, i32)>, Vec<(i64, i32)>)],
) -> QueryResult<()> {
    use crate::schema::match_players::dsl as mp;
    use crate::schema::matches::dsl as mt;

    if batch.is_empty() {
        return Ok(());
    }
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;

    conn.transaction(|c| {
        // Collect all match rows and all player rows, then do two bulk inserts.
        let mut match_rows: Vec<NewMatch> = Vec::with_capacity(batch.len());
        let mut player_rows: Vec<NewMatchPlayer> = Vec::with_capacity(batch.len() * 10);
        let mut user_ids: Vec<i64> = Vec::with_capacity(batch.len() * 10);

        for (mid, spread_val, team_a, team_b) in batch {
            match_rows.push(NewMatch {
                id: *mid,
                spread: *spread_val,
                status: "active".to_string(),
            });
            for (team_label, team) in [("A", team_a), ("B", team_b)] {
                for (uid, rat) in team {
                    player_rows.push(NewMatchPlayer {
                        match_id: *mid,
                        user_id: *uid,
                        team: team_label.to_string(),
                        rating: *rat,
                        active: true,
                    });
                    user_ids.push(*uid);
                }
            }
        }

        // Bulk insert matches; skip any whose id was already stored (idempotent
        // on redelivery from the stream).
        diesel::insert_into(mt::matches)
            .values(&match_rows)
            .on_conflict(mt::id)
            .do_nothing()
            .execute(c)?;

        // Bulk insert the player rows.
        diesel::insert_into(mp::match_players)
            .values(&player_rows)
            .execute(c)?;

        // Flag all players in_match and bump their counts in one statement.
        diesel::update(users.filter(id.eq_any(&user_ids)))
            .set((in_match.eq(true), matches_played.eq(matches_played + 1)))
            .execute(c)?;

        Ok(())
    })
}

// End a match: flip status to "ended", stamp ended_at, and clear the active
// flag on its players so they become free to be matched again. Returns the
// number of players freed, or NotFound if the match does not exist / is ended.
pub fn end_match(pool: &PgPool, match_id_val: i64) -> QueryResult<usize> {
    use crate::schema::match_players::dsl as mp;
    use crate::schema::matches::dsl as mt;

    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    conn.transaction(|c| {
        let updated = diesel::update(
            mt::matches
                .filter(mt::id.eq(match_id_val))
                .filter(mt::status.eq("active")),
        )
        .set((mt::status.eq("ended"), mt::ended_at.eq(diesel::dsl::now)))
        .execute(c)?;
        if updated == 0 {
            return Err(diesel::result::Error::NotFound); // unknown or already ended
        }
        let freed = diesel::update(mp::match_players.filter(mp::match_id.eq(match_id_val)))
            .set(mp::active.eq(false))
            .execute(c)?;
        Ok(freed)
    })
}

// Status of a single match: ("active"|"ended", spread).
pub fn match_status(pool: &PgPool, match_id_val: i64) -> QueryResult<(String, i32)> {
    use crate::schema::matches::dsl as mt;
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    mt::matches
        .find(match_id_val)
        .select((mt::status, mt::spread))
        .first(&mut conn)
}

// All match_players rows for a given match, ordered by team.
pub fn match_roster(pool: &PgPool, match_id_val: i64) -> QueryResult<Vec<MatchPlayer>> {
    use crate::schema::match_players::dsl as mp;
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    mp::match_players
        .filter(mp::match_id.eq(match_id_val))
        .order(mp::team.asc())
        .select(MatchPlayer::as_select())
        .load(&mut conn)
}

// Recent match ids with their status (most recent first), capped by `limit`.
pub fn list_matches(pool: &PgPool, limit: i64) -> QueryResult<Vec<(i64, String)>> {
    use crate::schema::matches::dsl as mt;
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    mt::matches
        .order(mt::created_at.desc())
        .limit(limit)
        .select((mt::id, mt::status))
        .load(&mut conn)
}

// The player's current active match id, if any (their "current position").
pub fn active_match_of(pool: &PgPool, player_id: i64) -> QueryResult<Option<i64>> {
    use crate::schema::match_players::dsl as mp;
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    mp::match_players
        .filter(mp::user_id.eq(player_id))
        .filter(mp::active.eq(true))
        .select(mp::match_id)
        .first(&mut conn)
        .optional()
}

// All match_players rows for matches a given user participated in.
pub fn matches_for_player(pool: &PgPool, player_id: i64) -> QueryResult<Vec<MatchPlayer>> {
    use crate::schema::match_players::dsl as mp;
    let mut conn = pool.get().map_err(|_| diesel::result::Error::NotFound)?;
    let ids: Vec<i64> = mp::match_players
        .filter(mp::user_id.eq(player_id))
        .select(mp::match_id)
        .load(&mut conn)?;
    if ids.is_empty() {
        return Ok(vec![]);
    }
    mp::match_players
        .filter(mp::match_id.eq_any(ids))
        .order((mp::match_id.desc(), mp::team.asc()))
        .select(MatchPlayer::as_select())
        .load(&mut conn)
}
