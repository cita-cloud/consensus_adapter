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
use consensus_adapter::{Proposal};


#[derive(Debug, Deserialize, Serialize)]
pub struct ProposalEntity {
    pub height: u64,
    pub round: u64,
    pub proposal: Proposal,
}

#[derive(Debug)]
pub struct ProposalCollector {
    pub pool: Pool<PostgresConnectionManager<NoTls>>,
    pub aof_file: File,
}

impl MaterializeOperator<ProposalEntity> for ProposalCollector {
    fn name() -> String {
        "proposal".to_string()
    }

    fn client(&self) -> PooledConnection<PostgresConnectionManager<NoTls>> {
        self.pool.get().unwrap()
    }

    fn write_json(&mut self, entity: &ProposalEntity) {
        self.aof_file.write(serde_json::to_vec(entity).unwrap().as_ref());
        self.aof_file.write_all("\n".as_bytes());
    }

    fn create_view_sql(&self, name: &str) -> String {
        format!(r#"CREATE MATERIALIZED VIEW IF NOT EXISTS {0}_{1} AS
            SELECT
            CAST(CAST({2} AS jsonb) -> 'height' as integer) as height,
            CAST(CAST({2} AS jsonb) -> 'round' as integer) as round,
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

impl ProposalCollector {
    pub fn new(pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        let name = Self::name();
        let name = name.as_str();
        let file_name = json_file(name);
        create_or_truncate_file(&file_name);

        let vote_collector = ProposalCollector {
            pool,
            aof_file: append_file(&file_name),
        };
        vote_collector.clear(name, &file_name);
        vote_collector.create(name, &file_name);
        vote_collector
    }

    pub fn add(&mut self, height: u64, round: u64, proposal: Proposal) -> bool {
        self.write_json(&ProposalEntity {
            height,
            round,
            proposal
        });
        true
    }

    pub fn get_proposal(&mut self, height: u64, round: u64) -> Option<Vec<ProposalEntity>> {
        if let Ok(rows) = self.client().query(
            format!("SELECT {0} FROM {1}_{2} where height = $1 and round = $2;", DATA, Self::name(), VIEW).as_str(),
            &[&(height as i32), &(round as i32)],
        ) {
            let mut list: Vec<ProposalEntity> = Vec::new();
            for row in rows {
                let proposal: ProposalEntity = serde_json::from_value(row.get(DATA)).unwrap();
                list.push(proposal);
            }
            Some(list)
        } else {
            None
        }
    }

}

#[test]
fn test() {
    let mut proposals = ProposalCollector::new(pool());
    let (h, r) = (1, 2);
    let count = 4;
    let length = 1000;
    for i in 0..length {
        proposals.add(h, r,  Proposal::new(H256::from_low_u64_le(i % count)));
    }
    votes.aof_file.sync_data();
    thread::sleep(Duration::from_millis(3 * TIME_INTERNAL));
    let list = votes.get_proposal(1, 2).unwrap();
    assert_eq!(list.len(), length as usize);
}
