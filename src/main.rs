mod perf_test;

use crate::perf_test::{perf_test_postgresql, perf_test_sqlite};

use argparse::{ArgumentParser, StoreTrue};

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
        perf_test_postgresql();
        perf_test_sqlite();
    }

}
