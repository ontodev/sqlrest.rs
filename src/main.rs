mod perf_test;

use crate::perf_test::time_json_fetch;

use argparse::{ArgumentParser, StoreTrue};
use futures::executor::block_on;
use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use std::{str::FromStr};

fn main() {
    let mut perf_test = false;
    {
        // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description(
            r#"SQLRest.rs"#,
        );
        ap.refer(&mut perf_test).add_option(
            &["--perf_test"],
            StoreTrue,
            r#"Run a number of performance tests using SQLite and PostgreSQL."#,
        );

        ap.parse_args_or_exit()
    }

    if perf_test {
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let postgresql_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
            .unwrap();
        let sq_connection_options =
            AnyConnectOptions::from_str("sqlite://test.db?mode=rwc").unwrap();
        let sqlite_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
            .unwrap();

        time_json_fetch(&postgresql_pool);
        time_json_fetch(&sqlite_pool);
    }

}
