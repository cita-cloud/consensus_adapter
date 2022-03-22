use cita_types::Address;
use crate::util::{create_or_truncate_file, create_source_sql, create_view_sql, pool, append_file, drop_sql, select_sql, json_file};
use crate::params::BftParams;
use postgres::{Row, NoTls};
use std::io::Write;
use serde_derive::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;
use r2d2_postgres::r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use crate::interface::{VIEW, SOURCE, DATA, TIME_INTERNAL, MaterializeOperator};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct AuthorityEntity {
    pub authorities: Vec<Address>,
    pub validators: Vec<Address>,
    pub height: usize,
}

#[derive(Debug)]
pub struct AuthorityManage {
    pub pool: Pool<PostgresConnectionManager<NoTls>>,
    pub aof_file: File,
}

impl MaterializeOperator<AuthorityEntity> for AuthorityManage {
    fn name() -> String {
        "authority".to_string()
    }

    fn client(&self) -> PooledConnection<PostgresConnectionManager<NoTls>> {
        self.pool.get().unwrap()
    }

    fn write_json(&mut self, entity: &AuthorityEntity) {
        self.aof_file.write(serde_json::to_vec(entity).unwrap().as_ref());
        self.aof_file.write_all("\n".as_bytes());
    }

}

impl AuthorityManage {
    pub fn new(pool: Pool<PostgresConnectionManager<NoTls>>) -> Self {
        let name = Self::name();
        let name = name.as_str();
        let file_name = json_file(name);

        let mut auth_manage = AuthorityManage {
            pool,
            aof_file: append_file(&file_name),
        };

        auth_manage.clear();

        auth_manage.create(&file_name);

        auth_manage
    }

    fn select_sql() -> String {
        format!("SELECT {0} FROM {1}_{2} order by {0} desc limit 1;", DATA, Self::name(), VIEW)
    }


    pub fn get_authority_entity(&self) -> AuthorityEntity {
        let mut client = self.client();
        if let Ok(row)  = client.query_one(Self::select_sql().as_str(), &[]) {
            serde_json::from_value(row.get(0)).unwrap()
        } else {
            AuthorityEntity {
                authorities: Vec::new(),
                validators: Vec::new(),
                height: 0,
            }
        }
    }

    pub fn validator_n(&self) -> usize {
        self.get_authority_entity().validators.len()
    }

    pub fn receive_authorities_list(
        &mut self,
        height: usize,
        authorities: &[Address],
        validators: &[Address],
    ) {
        let inner = self.get_authority_entity();

        if inner.validators != validators || inner.authorities != authorities {
            self.write_json(&AuthorityEntity {
                authorities: Vec::from(authorities),
                validators: Vec::from(validators),
                height,
            });
        }
    }
}

#[test]
fn test() {
    let pool = pool();
    let mut au = AuthorityManage::new(pool);
    au.receive_authorities_list(1, &[Address::from_low_u64_le(1)], &[Address::from_low_u64_le(1)]);
    au.aof_file.sync_data();
    thread::sleep(Duration::from_millis(2 * TIME_INTERNAL));
    assert_eq!(au.validator_n(), 1);
    au.receive_authorities_list(2, &[Address::from_low_u64_le(1)], &[Address::from_low_u64_le(1), Address::from_low_u64_le(2)]);
    au.aof_file.sync_data();
    thread::sleep(Duration::from_millis(2 * TIME_INTERNAL));
    assert_eq!(au.validator_n(), 2);
}
