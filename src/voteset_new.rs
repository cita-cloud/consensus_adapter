use crate::interface::{VIEW, SOURCE, TIME_INTERNAL, DATA, JSON_DATA, MaterializeOperator};
use r2d2_postgres::r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use postgres::{NoTls, Row};
use std::fs::File;
use crate::util::{append_file, create_or_truncate_file, drop_sql, pool, select_sql, json_file};
use std::path::PathBuf;
use std::io::Write;
use cita_types::{Address, H256};
use crate::message::{Step, SignedFollowerVote, FollowerVote};
use serde_derive::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;
use crate::voteset::VoteSet;
use serde::Deserialize as Deserialize1;

#[derive(Debug, Deserialize, Serialize)]
pub struct VoteEntity {
    address: Address,
    vote: SignedFollowerVote,
}

#[derive(Debug)]
pub struct VoteCollector {
    pub pool: Pool<PostgresConnectionManager<NoTls>>,
    pub aof_file: File,
}

impl MaterializeOperator<VoteEntity> for VoteCollector {
    fn name() -> String {
        "voteset".to_string()
    }

    fn client(&self) -> PooledConnection<PostgresConnectionManager<NoTls>> {
        self.pool.get().unwrap()
    }

    fn write_json(&mut self, entity: &VoteEntity) {
        self.aof_file.write(serde_json::to_vec(entity).unwrap().as_ref());
        self.aof_file.write_all("\n".as_bytes());
    }

    fn create_view_sql(&self, name: &str) -> String {
        format!(r#"CREATE MATERIALIZED VIEW IF NOT EXISTS {0}_{1} AS
            SELECT DISTINCT ON (
                CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'height' as integer),
                CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'round' as integer),
                replace(CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'step' as string), '"', ''),
                replace(CAST(CAST({2} AS jsonb)->'address' as string), '"', '')
            )
            CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'height' as integer) as height,
            CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'round' as integer) as round,
            replace(CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'step' as string), '"', '') as step,
            replace(CAST(CAST({2} AS jsonb)->'address' as string), '"', '') as address,
            replace(CAST(CAST(CAST(CAST({2} AS jsonb) -> 'vote' as jsonb) -> 'vote' as jsonb)->'hash' as string), '"', '') as hash,
            CAST({2} AS jsonb) as {2}
            FROM (
                SELECT CONVERT_FROM({3}, 'utf8') AS {2}
                FROM {0}_{4}
                order by data asc
            )
            ;
            "#, Self::name(), VIEW, DATA, JSON_DATA, SOURCE)
    }
}

impl VoteCollector {
    pub fn new(pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        let name = Self::name();
        let name = name.as_str();
        let file_name = json_file(name);

        let vote_collector = VoteCollector {
            pool,
            aof_file: append_file(&file_name),
        };
        vote_collector.clear(name, &file_name);
        vote_collector.create(name, &file_name);
        vote_collector
    }

    pub fn add(&mut self, sender: Address, sign_vote: &SignedFollowerVote) -> bool {
        self.write_json(&VoteEntity {
            address: sender,
            vote: sign_vote.clone(),
        });
        true
    }

    pub fn get_voteset(&mut self, height: u64, round: u64, step: Step) -> Option<VoteSet> {
        if let Ok(rows) = self.client().query(
            format!("SELECT {0} FROM {1}_{2} where height = $1 and round = $2 and step = $3;", DATA, Self::name(), VIEW).as_str(),
            &[&(height as i32), &(round as i32), &step.to_string()],
        ) {
            let mut voteset: VoteSet = VoteSet::new();
            for row in rows {
                let vote: VoteEntity = serde_json::from_value(row.get(DATA)).unwrap();
                voteset.add(vote.address, &vote.vote);
            }
            Some(voteset)
        } else {
            None
        }
    }
}

#[test]
fn test() {
    let mut votes = VoteCollector::new(pool());
    let (h, r, s) = (1, 2, Step::Propose);
    let count = 4;
    for i in 0..1000 {
        votes.add(Address::from_low_u64_le(i % count), &SignedFollowerVote {
            sig: Vec::new(),
            vote: FollowerVote {
                height: h,
                round: r,
                step: s,
                hash: Some(H256::from_low_u64_le(i)),
            },
        });
    }
    votes.aof_file.sync_data();
    thread::sleep(Duration::from_millis(3 * TIME_INTERNAL));
    let voteset = votes.get_voteset(h, r, s).unwrap();
    for (sender, sign_vote) in voteset.votes_by_sender {
        println!("address: {:?}, sign_vote: {:?}", sender, sign_vote);
    }
    for (hash, count) in voteset.votes_by_proposal {
        println!("hash: {:?}, count: {}", hash, count);
    }
    assert_eq!(count, voteset.count);
}
