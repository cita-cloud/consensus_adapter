use std::path::PathBuf;
use crate::util::{create_or_truncate_file, drop_sql, create_source_sql, create_view_sql, append_file};
use r2d2_postgres::r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use postgres::{NoTls, Client};
use std::fs::File;
use crate::json_file;
use std::io::Write;
use serde::Serialize;

pub const TIME_INTERNAL: u64 = 50;
pub const VIEW: &str = "view";
pub const SOURCE: &str = "source";
pub const DATA: &str = "data";
pub const JSON_DATA: &str = "json_data";

pub trait MaterializeOperator<T: Serialize> {

    fn name() -> String;

    fn client(&self) -> PooledConnection<PostgresConnectionManager<NoTls>>;

    fn clear(&self, name: &str, file_name: &PathBuf) {
        create_or_truncate_file(file_name);
        let mut client = self.client();
        client.batch_execute(drop_sql(VIEW, name).as_str()).unwrap();
        client.batch_execute(drop_sql(SOURCE, name).as_str()).unwrap();
    }

    fn create(&self, name: &str, file_name: &PathBuf) {
        let mut client = self.client();
        client.batch_execute(self.create_source_sql(name, &file_name.display().to_string(), TIME_INTERNAL).as_str()).unwrap();
        client.batch_execute(self.create_view_sql(name).as_str()).unwrap();
    }

    fn write_json(&mut self, entity: &T);

    fn create_view_sql(&self, name: &str) -> String {
        format!(r#"
            CREATE MATERIALIZED VIEW IF NOT EXISTS {0}_{1} AS
            SELECT CAST({2} AS JSONB) AS {2}
            FROM (
                SELECT CONVERT_FROM({3}, 'utf8') AS {2}
                FROM {0}_{4}
            );
        "#,  name, VIEW, DATA, JSON_DATA, SOURCE)
    }


    fn create_source_sql(&self, name: &str, file_name: &str, time_internal: u64) -> String {
        format!(r#"
            CREATE SOURCE IF NOT EXISTS {1}_{2} ({0})
            FROM FILE '{3}'
            WITH (tail = true, timestamp_frequency_ms = {4})
            FORMAT BYTES;
        "#, JSON_DATA, name, SOURCE, file_name, time_internal)
    }

}