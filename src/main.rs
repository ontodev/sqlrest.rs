mod perf_test;

use crate::perf_test::{time_json_fetch, time_window_select};

use argparse::{ArgumentParser, StoreTrue};
use futures::executor::block_on;
use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use std::{env, process, str::FromStr};

fn main() {
    let mut json_fetch = false;
    let mut window_select = false;
    {
        // this block limits scope of borrows by ap.refer() method
        let mut ap = ArgumentParser::new();
        ap.set_description(r#"SQLRest.rs"#);
        ap.refer(&mut json_fetch).add_option(
            &["--json_fetch"],
            StoreTrue,
            r#"Test the performance of the fetch_rows_as_json() function."#,
        );
        ap.refer(&mut window_select).add_option(
            &["--window_select"],
            StoreTrue,
            r#"Test the performance of window vs. two-query select."#,
        );

        ap.parse_args_or_exit()
    }

    let args: Vec<String> = env::args().collect();
    let program_name = &args[0];

    if !(json_fetch || window_select) {
        eprintln!("To see command-line usage, run {} --help", program_name);
        process::exit(1);
    }

    let pg_connection_options =
        AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
    let postgresql_pool =
        block_on(AnyPoolOptions::new().max_connections(1).connect_with(pg_connection_options))
            .unwrap();
    let sq_connection_options = AnyConnectOptions::from_str("sqlite://test.db?mode=rwc").unwrap();
    let sqlite_pool =
        block_on(AnyPoolOptions::new().max_connections(1).connect_with(sq_connection_options))
            .unwrap();

    if json_fetch {
        time_json_fetch(&postgresql_pool);
        time_json_fetch(&sqlite_pool);
    }
    if window_select {
        time_window_select(&postgresql_pool);
        time_window_select(&sqlite_pool);
    }
}
