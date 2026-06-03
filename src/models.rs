// Diesel models.

use crate::schema::{match_players, matches, users};
use diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = users)]
pub struct NewUser<'a> {
    pub name: &'a str,
    pub rating: i32,
}

#[derive(Insertable)]
#[diesel(table_name = matches)]
pub struct NewMatch {
    pub id: i64,
    pub spread: i32,
    pub status: String,
}

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = match_players)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct MatchPlayer {
    #[allow(dead_code)]
    pub id: i64,
    pub match_id: i64,
    pub user_id: i64,
    pub team: String,
    pub rating: i32,
    #[allow(dead_code)]
    pub active: bool,
}

#[derive(Insertable)]
#[diesel(table_name = match_players)]
pub struct NewMatchPlayer {
    pub match_id: i64,
    pub user_id: i64,
    pub team: String,
    pub rating: i32,
    pub active: bool,
}
