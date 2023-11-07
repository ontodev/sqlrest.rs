use ontodev_sqlrest::{CountStrategy, Filter, Select};

use futures::executor::block_on;
use serde_json::json;
use sqlx::{any::AnyPool, query as sqlx_query, Row};
use std::{char, collections::HashMap, time::Instant};

pub const NUM_ROWS: isize = 250000;
pub const NUM_COLUMNS: isize = 5;

pub fn get_setup_sql() -> (String, String, String) {
    let drop = r#"DROP TABLE IF EXISTS "my_table""#;
    let create = r#"CREATE TABLE "my_table" (
                          "row_number" BIGINT,
                          "prefix" TEXT,
                          "base" TEXT,
                          "ontology IRI" TEXT,
                          "version IRI" TEXT
                        )"#;

    fn col_to_a1(column_index: isize) -> String {
        fn divrem(dividend: isize, divisor: isize) -> (isize, isize) {
            (dividend / divisor, dividend % divisor)
        }
        let mut div = column_index;
        let mut rem;
        let mut column_id = String::from("");
        while div > 0 {
            (div, rem) = divrem(div, 26);
            if rem == 0 {
                rem = 26;
                div = -1;
            }
            column_id = format!(
                "{}{}",
                char::from_u32((rem + 64).try_into().unwrap()).unwrap(),
                column_id
            );
        }

        column_id
    }

    let mut insert = String::from(r#"INSERT INTO "my_table" VALUES "#);
    for i in 1..NUM_ROWS {
        insert.push_str(&format!("({}, ", i));
        let mut cell_ids = vec![];
        for j in 1..NUM_COLUMNS {
            let cell_id = format!("'{}{}'", col_to_a1(j), i);
            cell_ids.push(cell_id);
        }
        insert.push_str(&format!("{})", cell_ids.join(", ")));
        if i != (NUM_ROWS - 1) {
            insert.push_str(",");
        }
    }

    (drop.to_string(), create.to_string(), insert.to_string())
}

pub fn time_window_select(pool: &AnyPool) {
    println!(
        "Checking performance of fetch_as_json() with and without a window function for {:?}.",
        pool.any_kind()
    );
    println!("====");
    let (drop, create, insert) = get_setup_sql();
    let num_iterations = 5;
    for i in 1..(num_iterations + 1) {
        println!(
            "Running performance test #{} of {} for {:?}",
            i,
            num_iterations,
            pool.any_kind()
        );
        for sql in vec![&drop, &create, &insert] {
            let query = sqlx_query(sql);
            block_on(query.execute(pool)).unwrap();
        }

        // Run the VACUUM command to clear the cache:
        let query = sqlx_query("VACUUM");
        block_on(query.execute(pool)).unwrap();

        let select = Select::new("my_table");

        // Time the query:
        let start = Instant::now();
        let _rows = select.fetch_as_json(pool, &HashMap::new()).unwrap();
        println!("Elapsed time for two query fetch: {:.2?}", start.elapsed());

        // Run the VACUUM command to clear the cache:
        let query = sqlx_query("VACUUM");
        block_on(query.execute(pool)).unwrap();

        // Time the query:
        let start = Instant::now();
        let _rows = select
            .fetch_as_json_with_count_strategy(pool, &HashMap::new(), &CountStrategy::Window)
            .unwrap();
        println!("Elapsed time for window fetch: {:.2?}", start.elapsed());
    }

    println!(
        "Done performance check of fetch_as_json() with and without a window function for {:?}.",
        pool.any_kind()
    );
    println!("====");
}

pub fn time_json_fetch(pool: &AnyPool) {
    println!(
        "Checking performance of fetch_rows_as_json() for {:?}.",
        pool.any_kind()
    );
    println!("====");

    let (drop, create, insert) = get_setup_sql();
    let num_iterations = 5;
    for i in 1..(num_iterations + 1) {
        println!(
            "Running performance test #{} of {} for {:?}",
            i,
            num_iterations,
            pool.any_kind()
        );
        for sql in vec![&drop, &create, &insert] {
            let query = sqlx_query(sql);
            block_on(query.execute(pool)).unwrap();
        }

        let mut select = Select::new("my_table");
        select
            .select(vec![
                "row_number",
                "prefix",
                "base",
                "\"ontology IRI\"",
                "\"version IRI\"",
            ])
            .filter(vec![
                Filter::new("row_number", "lt", json!(NUM_ROWS / 2)).unwrap()
            ]);

        // Run the VACUUM command to clear the cache:
        let query = sqlx_query("VACUUM");
        block_on(query.execute(pool)).unwrap();
        // Time the query:
        let start = Instant::now();
        let rows = select.fetch_rows(pool, &HashMap::new());
        println!(
            "Elapsed time for Select::fetch_rows(): {:.2?}",
            start.elapsed()
        );
        for row in rows.unwrap() {
            let _: &str = row.try_get("prefix").unwrap();
        }
        println!("Elapsed time after iterating: {:.2?}", start.elapsed());

        // Run the VACUUM command to clear the cache:
        let query = sqlx_query("VACUUM");
        block_on(query.execute(pool)).unwrap();
        // Time the query:
        let start = Instant::now();
        let json_rows = select.fetch_rows_as_json(pool, &HashMap::new());
        println!(
            "Elapsed time for Select::fetch_rows_as_json(): {:.2?}",
            start.elapsed()
        );
        for row in json_rows.unwrap() {
            let _ = row.get("prefix").unwrap();
        }
        println!("Elapsed time after iterating: {:.2?}", start.elapsed());
        println!("----------");
    }
    println!(
        "Done checking performance of fetch_rows_as_json() for {:?}.",
        pool.any_kind()
    );
    println!("====");
}
