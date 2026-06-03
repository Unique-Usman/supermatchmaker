// Diesel schema. Tables are created at startup (see db::bootstrap).

diesel::table! {
    users (id) {
        id -> Int8,
        name -> Varchar,
        rating -> Int4,
        in_match -> Bool,
        matches_played -> Int4,
    }
}

diesel::table! {
    // A match has a lifecycle: status is "active" when formed, "ended" when
    // the game finishes.
    matches (id) {
        id -> Int8,
        spread -> Int4,
        status -> Varchar,        // "active" | "ended"
        created_at -> Timestamptz,
        ended_at -> Nullable<Timestamptz>,
    }
}

diesel::table! {
    // One row per player per match. `active` mirrors the match status and is
    // used to enforce "a player can be in at most one active match at a time"
    // via a partial unique index on (user_id) WHERE active.
    match_players (id) {
        id -> Int8,
        match_id -> Int8,
        user_id -> Int8,
        team -> Varchar,          // "A" or "B"
        rating -> Int4,
        active -> Bool,
    }
}

diesel::allow_tables_to_appear_in_same_query!(matches, match_players, users);
