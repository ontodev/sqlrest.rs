//! <!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
//!      in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
//!      `cargo readme > README.md` -->
//!
//! ## Working with Select structs
//! ```rust
//! use ontodev_sqlrest::{
//!     bind_sql, construct_query, get_db_type, fetch_rows_from_selects,
//!     fetch_rows_as_json_from_selects, interpolate_sql, local_sql_syntax, DbType, Direction,
//!     Filter, OrderByColumn, Select, SelectColumn, selects_to_sql,
//! };
//! use futures::executor::block_on;
//! use indoc::indoc;
//! use serde_json::{json, Value as SerdeValue};
//! use sqlx::{
//!     any::AnyKind,
//! #     any::{AnyConnectOptions, AnyPool, AnyPoolOptions, AnyRow},
//!     query as sqlx_query, Row,
//! };
//! use std::{
//!     collections::HashMap,
//! #   str::FromStr
//! };
//! # let connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
//! # let sqlite_pool =
//! #     block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
//! #         .unwrap();
//!
//! # let connection_options = AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
//! # let postgresql_pool =
//! #     block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
//! #         .unwrap();
//! /*
//!  * Use the local_sql_syntax() function to convert a SQL string with the given placeholder
//!  * to the syntax appropriate to the given database connection pool.
//!  */
//! let generic_sql = r#"SELECT "foo" FROM "bar" WHERE "xyzzy" IN (VAL, VAL, VAL)"#;
//! let local_sql = local_sql_syntax(&sqlite_pool, "VAL", generic_sql);
//! assert_eq!("SELECT \"foo\" FROM \"bar\" WHERE \"xyzzy\" IN (?, ?, ?)", local_sql);
//! let local_sql = local_sql_syntax(&postgresql_pool, "VAL", generic_sql);
//! assert_eq!("SELECT \"foo\" FROM \"bar\" WHERE \"xyzzy\" IN ($1, $2, $3)", local_sql);
//!
//! # for pool in vec![&sqlite_pool, &postgresql_pool] {
//! # let drop_query = sqlx_query(r#"DROP TABLE IF EXISTS "test""#);
//! # block_on(drop_query.execute(pool)).unwrap();
//! # let create_query = sqlx_query(
//! #     r#"CREATE TABLE "test" ("table" TEXT, "row_num" BIGINT, "col1" TEXT, "col2" TEXT)"#,
//! # );
//! # block_on(create_query.execute(pool)).unwrap();
//! # let insert_query = sqlx_query(r#"INSERT INTO "test" VALUES ('foo', 1, 'bar', 'bar')"#);
//! # block_on(insert_query.execute(pool)).unwrap();
//! /*
//!  * Use the bind_sql() function to bind a given parameter map to a given SQL string, then call
//!  * interpolate_sql() on the bound SQL and parameter vector that are returned. Finally, create a
//!  * sqlx_query with the bound SQL and parameter vector and execute it.
//!  */
//! let test_sql = indoc! {r#"
//!        SELECT "table", "row_num", "col1", "col2"
//!        FROM "test"
//!        WHERE "table" = {table}
//!          AND "row_num" = {row_num}
//!          AND "col1" = {column}
//!          AND "col2" = {column}
//!     "#};
//! let mut test_params = HashMap::new();
//! test_params.insert("table", json!("foo"));
//! test_params.insert("row_num", json!(1));
//! test_params.insert("column", json!("bar"));
//! let (bound_sql, test_params) = bind_sql(pool, test_sql, &test_params).unwrap();
//!
//! let interpolated_sql = interpolate_sql(pool, &bound_sql, &test_params, None).unwrap();
//! assert_eq!(
//!     indoc! {r#"
//!        SELECT "table", "row_num", "col1", "col2"
//!        FROM "test"
//!        WHERE "table" = 'foo'
//!          AND "row_num" = 1
//!          AND "col1" = 'bar'
//!          AND "col2" = 'bar'
//!     "#},
//!     interpolated_sql
//! );
//!
//! let test_query = construct_query(&bound_sql, &test_params).unwrap();
//! let row = block_on(test_query.fetch_one(pool)).unwrap();
//! # let table: &str = row.get("table");
//! # assert_eq!(table, "foo");
//! # let row_num: i64 = row.get("row_num");
//! # assert_eq!(row_num, 1);
//! # let col1: &str = row.get("col1");
//! # assert_eq!(col1, "bar");
//! # let col2: &str = row.get("col2");
//! # assert_eq!(col2, "bar");
//! # }
//! # fn setup_for_select_test() -> (AnyPool, AnyPool) {
//! #     // Setup database connections and create the needed tables:
//! #     let pg_connection_options =
//! #         AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
//! #     let postgresql_pool =
//! #         block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
//! #             .unwrap();
//! #     let sq_connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
//! #     let sqlite_pool =
//! #         block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
//! #             .unwrap();
//! #     let drop_table1 = r#"DROP TABLE IF EXISTS "my_table""#;
//! #     let create_table1 = r#"CREATE TABLE "my_table" (
//! #                              "row_number" BIGINT,
//! #                              "prefix" TEXT,
//! #                              "base" TEXT,
//! #                              "ontology IRI" TEXT,
//! #                              "version IRI" TEXT
//! #                            )"#;
//! #     let insert_table1 = r#"INSERT INTO "my_table" VALUES
//! #                             (1, 'p1', 'b1', 'o1', 'v1'),
//! #                             (2, 'p2', 'b2', 'o2', 'v2'),
//! #                             (3, 'p3', 'b3', 'o3', 'v3'),
//! #                             (4, 'p4', 'b4', 'o1', 'v4')"#;
//! #     let drop_table2 = r#"DROP TABLE IF EXISTS "a table name with spaces""#;
//! #     let create_table2 = r#"CREATE TABLE "a table name with spaces" (
//! #                              "foo" TEXT,
//! #                              "a column name with spaces" TEXT,
//! #                              "bar" TEXT
//! #                            )"#;
//! #     let insert_table2 = r#"INSERT INTO "a table name with spaces" VALUES
//! #                             ('f1', 's1', 'b1'),
//! #                             ('f2', 's2', 'b2'),
//! #                             ('f3', 's3', 'b3'),
//! #                             ('f4', 's4', 'b4'),
//! #                             ('f5', 's5', 'b5'),
//! #                             ('f6', 's6', 'b6')"#;
//! #     for pool in vec![&sqlite_pool, &postgresql_pool] {
//! #         for sql in &vec![
//! #             drop_table1,
//! #             create_table1,
//! #             insert_table1,
//! #             drop_table2,
//! #             create_table2,
//! #             insert_table2,
//! #         ] {
//! #             let query = sqlx_query(sql);
//! #             block_on(query.execute(pool)).unwrap();
//! #         }
//! #     }
//! #     (sqlite_pool, postgresql_pool)
//! # }
//!
//! /*
//!  * Create a new Select struct by calling new() and progressively adding fields.
//!  */
//! # let (sqlite_pool, postgresql_pool) = setup_for_select_test();
//! let mut select = Select::new(r#""a table name with spaces""#);
//! select.explicit_select(vec![
//!     &SelectColumn::new("foo", Some("foo"), None),
//!     &SelectColumn::new(r#""a column name with spaces""#, Some("C"), None)]);
//! select.add_select("bar");
//! select.add_explicit_select(&SelectColumn::new("COUNT(1)", Some("count"), None));
//! select.filter(vec![Filter::new("foo", "is", json!("{foo}")).unwrap()]);
//! select.add_filter(Filter::new("bar", "in", json!(["{val1}", "{val2}"])).unwrap());
//! select.order_by(vec!["foo", "bar"]);
//! select.group_by(vec!["foo"]);
//! select.add_group_by("C");
//! select.add_group_by("bar");
//! select.having(vec![Filter::new("COUNT(1)", "gt", json!(1)).unwrap()]);
//! select.limit(11);
//! select.offset(50);
//!
//! /*
//!  * Convert the Select defined above to SQL for both Postgres and Sqlite, bind the SQL using
//!  * bind_sql(), and then create and execure a sqlx_query using the bound SQL and parameter
//!  * vector that are returned.
//!  */
//! for pool in vec![&postgresql_pool, &sqlite_pool] {
//!     let (expected_is_clause, placeholder1, placeholder2, placeholder3);
//!     if pool.any_kind() == AnyKind::Postgres {
//!         expected_is_clause = "IS NOT DISTINCT FROM";
//!         placeholder1 = "$1";
//!         placeholder2 = "$2";
//!         placeholder3 = "$3";
//!     } else {
//!         expected_is_clause = "IS";
//!         placeholder1 = "?";
//!         placeholder2 = "?";
//!         placeholder3 = "?";
//!     }
//!
//!     let expected_sql_with_mapvars = format!(
//!         "SELECT foo AS foo, \"a column name with spaces\" AS C, bar, COUNT(1) AS count \
//!          FROM \"a table name with spaces\" \
//!          WHERE foo {} {{foo}} AND bar IN ({{val1}}, {{val2}}) \
//!          GROUP BY foo, C, bar \
//!          HAVING COUNT(1) > 1 \
//!          ORDER BY foo ASC, bar ASC \
//!          LIMIT 11 OFFSET 50",
//!         expected_is_clause,
//!     );
//!     let dbtype = get_db_type(&pool).unwrap();
//!     let sql = select.to_sql(&dbtype).unwrap();
//!     assert_eq!(expected_sql_with_mapvars, sql);
//!
//!     let mut param_map = HashMap::new();
//!     param_map.insert("foo", json!("foo_val"));
//!     param_map.insert("val1", json!("bar_val1"));
//!     param_map.insert("val2", json!("bar_val2"));
//!
//!     let expected_sql_with_listvars = format!(
//!         "SELECT foo AS foo, \"a column name with spaces\" AS C, bar, COUNT(1) AS count \
//!          FROM \"a table name with spaces\" \
//!          WHERE foo {} {} AND bar IN ({}, {}) \
//!          GROUP BY foo, C, bar \
//!          HAVING COUNT(1) > 1 \
//!          ORDER BY foo ASC, bar ASC \
//!          LIMIT 11 OFFSET 50",
//!         expected_is_clause, placeholder1, placeholder2, placeholder3,
//!     );
//!     let (sql, params) = bind_sql(pool, &sql, &param_map).unwrap();
//!     assert_eq!(expected_sql_with_listvars, sql);
//! #     match params[0] {
//! #         SerdeValue::String(s) if s == "foo_val" => assert!(true),
//! #         _ => assert!(false, "{} != 'foo_val'", params[0]),
//! #     };
//! #     match params[1] {
//! #         SerdeValue::String(s) if s == "bar_val1" => assert!(true),
//! #         _ => assert!(false, "{} != 'bar_val1'", params[1]),
//! #     };
//! #     match params[2] {
//! #         SerdeValue::String(s) if s == "bar_val2" => assert!(true),
//! #         _ => assert!(false, "{} != 'bar_val2'", params[2]),
//! #     };
//!     let query = construct_query(&sql, &params).unwrap();
//!     block_on(query.execute(pool)).unwrap();
//! }
//!
//! /*
//!  * Use a window function:
//!  */
//!
//! let mut select = Select::new("my_table");
//! select.window("COUNT", "row_number", None);
//! let expected_sql = "SELECT *, COUNT(row_number) OVER() count_row_number FROM my_table";
//! assert_eq!(select.to_sqlite().unwrap(), expected_sql);
//! assert_eq!(select.to_postgres().unwrap(), expected_sql);
//!
//! /*
//!  * Generate the SQL for a simple combined query.
//!  */
//! let mut cte = Select::new("my_table");
//! cte.select(vec!["prefix"]);
//! // Note: When building a Select struct, chaining is possible but you must first create
//! // a mutable struct in a separate statement using new() before before chaining:
//! let mut main_select = Select::new("cte");
//! main_select.select(vec!["prefix"]).limit(10).offset(20);
//!
//! let sql = selects_to_sql(&cte, &main_select, &DbType::Postgres).unwrap();
//! assert_eq!(
//!     sql,
//!     "WITH cte AS (SELECT prefix FROM my_table) SELECT prefix FROM cte LIMIT 10 OFFSET 20",
//! );
//! block_on(sqlx_query(&sql).execute(&sqlite_pool)).unwrap();
//! block_on(sqlx_query(&sql).execute(&postgresql_pool)).unwrap();
//! # fn validate_rows(rows: &Vec<AnyRow>) {
//! #     let num_rows = rows.len();
//! #     for (i, row) in rows.iter().enumerate() {
//! #         let foo: &str = row.get("foo");
//! #         let spaces: &str = row.get("a column name with spaces");
//! #         let bar: &str = row.get("bar");
//! #         assert_eq!(foo.to_string(), format!("f{}", num_rows - i));
//! #         assert_eq!(spaces.to_string(), format!("s{}", num_rows - i));
//! #         assert_eq!(bar.to_string(), format!("b{}", num_rows - i));
//! #     }
//! # }
//! # let (sqlite_pool, postgresql_pool) = setup_for_select_test();
//! /*
//!  * Fetch database rows using Select::fetch_rows().
//!  */
//! let mut select = Select::new(r#""a table name with spaces""#);
//! select
//!     .select_all(&sqlite_pool)
//!     .expect("")
//!     .filter(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
//!     .explicit_order_by(vec![&OrderByColumn::new("foo", &Direction::Descending)]);
//!
//! let mut param_map = HashMap::new();
//! param_map.insert("foo1", json!("f5"));
//! param_map.insert("foo2", json!("f6"));
//! let sqlite_rows = select.fetch_rows(&sqlite_pool, &param_map).unwrap();
//! # validate_rows(&sqlite_rows);
//! let postgresql_rows = select.fetch_rows(&postgresql_pool, &param_map).unwrap();
//! # validate_rows(&postgresql_rows);
//! # let (sqlite_pool, postgresql_pool) = setup_for_select_test();
//!
//! /*
//!  * Fetch database rows in JSON format using Select::fetch_rows_as_json().
//!  */
//! let mut select = Select::new(r#""a table name with spaces""#);
//! select
//!     .select(vec!["foo", r#""a column name with spaces""#, "bar"])
//!     .add_explicit_select(&SelectColumn::new("COUNT(1)", Some("count"), None))
//!     .filter(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
//!     .explicit_order_by(vec![&OrderByColumn::new("foo", &Direction::Ascending),
//!                             &OrderByColumn::new("bar", &Direction::Descending)])
//!     .group_by(vec!["foo", r#""a column name with spaces""#, "bar"])
//!     .having(vec![Filter::new("COUNT(1)", "gte", json!(1)).unwrap()])
//!     .limit(10)
//!     .offset(1);
//!
//! let mut param_map = HashMap::new();
//! param_map.insert("foo1", json!("f5"));
//! param_map.insert("foo2", json!("f6"));
//! for pool in vec![postgresql_pool, sqlite_pool] {
//!     let json_rows = select.fetch_rows_as_json(&pool, &param_map).unwrap();
//! #     for (i, row) in json_rows.iter().enumerate() {
//! #         let i = i + 2;
//! #         let expected_row = format!(
//! #             r#"{{"foo":"f{}","a column name with spaces":"s{}","bar":"b{}","count":1}}"#,
//! #             i, i, i
//! #         );
//! #         let row = SerdeValue::Object(row.clone());
//! #         assert_eq!(format!("{}", row), expected_row)
//! #     }
//! }
//! # let (sqlite_pool, postgresql_pool) = setup_for_select_test();
//! /*
//!  * Fetch rows from a combined query.
//!  */
//! let mut cte = Select::new("my_table");
//! cte.select(vec!["prefix"]);
//! let mut main_select = Select::new("cte");
//! main_select
//!     .select(vec!["prefix"])
//!     .limit(10)
//!     .offset(0)
//!     .add_explicit_order_by(&OrderByColumn::new("prefix", &Direction::Ascending));
//! for pool in vec![sqlite_pool, postgresql_pool] {
//!     let rows = fetch_rows_from_selects(&cte, &main_select, &pool, &HashMap::new()).unwrap();
//! #     for (i, row) in rows.iter().enumerate() {
//! #         let prefix: &str = row.get("prefix");
//! #         assert_eq!(prefix.to_string(), format!("p{}", i + 1));
//! #     }
//! }
//! # let (sqlite_pool, postgresql_pool) = setup_for_select_test();
//! /*
//!  * Fetch rows as json from a combined query.
//!  */
//! let mut cte = Select::new("my_table");
//! cte.select(vec!["prefix"]);
//! let mut main_select = Select::new("cte");
//! main_select
//!     .select(vec!["prefix"])
//!     .limit(10)
//!     .offset(0)
//!     .add_explicit_order_by(&OrderByColumn::new("prefix", &Direction::Ascending));
//! for pool in vec![sqlite_pool, postgresql_pool] {
//!     let json_rows =
//!         fetch_rows_as_json_from_selects(&cte, &main_select, &pool, &HashMap::new())
//!             .unwrap();
//! #     for (i, row) in json_rows.iter().enumerate() {
//! #         let i = i + 1;
//! #         let expected_row = format!(r#"{{"prefix":"p{}"}}"#, i);
//! #         let row = SerdeValue::Object(row.clone());
//! #         assert_eq!(format!("{}", row), expected_row)
//! #     }
//! }
//! # let (sqlite_pool, postgresql_pool) = setup_for_select_test();
//! /*
//!  * Call fetch_as_json() which returns results suitable for paging as part of a web application.
//!  */
//! let mut select = Select::new("my_table");
//! select.limit(2).offset(1);
//! let rows = select.fetch_as_json(&postgresql_pool, &HashMap::new()).unwrap();
//! assert_eq!(
//!     format!("{}", json!(rows)),
//!     "{\"status\":206,\"unit\":\"items\",\"start\":1,\"end\":3,\"count\":4,\"rows\":\
//!      [{\"row_number\":2,\"prefix\":\"p2\",\"base\":\"b2\",\"ontology IRI\":\"o2\",\
//!      \"version IRI\":\"v2\"},{\"row_number\":3,\"prefix\":\"p3\",\"base\":\"b3\",\
//!      \"ontology IRI\":\"o3\",\"version IRI\":\"v3\"}]}"
//! );
//! ```
//! ## Parsing Selects from URLs and vice versa.
//! ### Select all columns from the table "bar", with no filtering.
//! ```rust
//! use ontodev_sqlrest::parse;
//! use urlencoding::{decode, encode};
//!
//! let from_url = "bar";
//! // Table names and column names are always rendered in double quotes in SQL:
//! let expected_sql = "SELECT * FROM \"bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(from_url, select.to_url().unwrap());
//!
//! // Table names with spaces can be specified in a URL:
//! let from_url = "a bar";
//! let expected_sql = "SELECT * FROM \"a bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(encode(from_url), select.to_url().unwrap());
//!
//! // Alternately, spaces can in table names can be specified in a URL using percent encoding:
//! let from_url = "a%20bar";
//! let expected_sql = "SELECT * FROM \"a bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(&from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // Quoted strings in table names are not allowed:
//! let from_url = "\"a bar\"";
//! let result = parse(from_url);
//! assert!(result.is_err());
//!
//! // Quoted strings in table names are not allowed, even if the quotes are encoded:
//! let from_url = "%22a%20bar%22";
//! let result = parse(from_url);
//! assert!(result.is_err());
//! ```
//!
//! ### Select all columns from the table, bar, with filtering.
//! ```rust
//! # use ontodev_sqlrest::parse;
//! # use urlencoding::{decode, encode};
//! // Column names in filters are handled similarly to column names in select clauses.
//! let from_url = "bar?\
//!                 column 1=eq.5\
//!                 &column_2=eq.10\
//!                 &column%203=eq.30";
//! let expected_sql = "SELECT * FROM \"bar\" \
//!                     WHERE \"column 1\" = 5 \
//!                     AND \"column_2\" = 10 \
//!                     AND \"column 3\" = 30";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//! ```
//!
//! ### Select specific columns from the table "bar".
//! ```rust
//! # use ontodev_sqlrest::parse;
//! # use urlencoding::{decode, encode};
//! let from_url = "bar?select=foo,goo";
//! let expected_sql = "SELECT \"foo\", \"goo\" FROM \"bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(encode(from_url), select.to_url().unwrap());
//!
//! // Column names with spaces are allowed in a URL:
//! let from_url = "bar?select=foo moo,goo";
//! let expected_sql = "SELECT \"foo moo\", \"goo\" FROM \"bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(encode(from_url), select.to_url().unwrap());
//!
//! // Column names with percent-encoded spaces are allowed in a URL:
//! let from_url = "bar?select=foo%20moo,goo%20hoo";
//! let expected_sql = "SELECT \"foo moo\", \"goo hoo\" FROM \"bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // Quoted strings in column names are not allowed in a URL.
//! let from_url = "bar?select=\"foo moo\",goo";
//! let result = parse(from_url);
//! assert!(result.is_err());
//!
//! // Quoted strings in column names are not allowed in a URL, not even when the quotes are
//! // percent-encoded.
//! let from_url = "bar?select=%22foo%20moo%22,goo";
//! let result = parse(from_url);
//! assert!(result.is_err());
//!
//! // Aliasing and casting of columns is supported (but optional) using the syntax:
//! // [ALIAS:]COLUMN[::CAST]
//! let from_url = "bar?select=a bar:a foo::text,goo::text,loop:goop";
//! let expected_postgres_sql =
//!     "SELECT \"a foo\"::TEXT AS \"a bar\", \"goo\"::TEXT, \"goop\" AS \"loop\" FROM \"bar\"";
//! let expected_sqlite_sql =
//!     "SELECT CAST(\"a foo\" AS TEXT) AS \"a bar\", CAST(\"goo\" AS TEXT), \
//!      \"goop\" AS \"loop\" FROM \"bar\"";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sqlite_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_postgres_sql, select.to_postgres().unwrap());
//! assert_eq!(encode(from_url), select.to_url().unwrap());
//!
//! // Wildcards in LIKE clauses are indicated using '*'.
//! let from_url = "bar?foo=like.*yogi*";
//! let expected_sql = "SELECT * FROM \"bar\" WHERE \"foo\" LIKE '%yogi%'";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // Quotes are not allowed in column names in URLs.
//! let from_url = "bar?\"column 1\"=eq.5";
//! let result = parse(from_url);
//! assert!(result.is_err());
//!
//! // Quotes are not allowed in column names in URLs, even when they are encoded.
//! let from_url = "bar?%22column 1%22=eq.5";
//! let result = parse(from_url);
//! assert!(result.is_err());
//!
//! // Unicode is supported.
//! let from_url = "épée?universität=like.*münchen";
//! let expected_sql = "SELECT * FROM \"épée\" WHERE \"universität\" LIKE '%münchen'";
//! let select = parse(&from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//! ```
//!
//! ### Literals and NULLs
//! <i>Double quotes may be used when specifying literal string values. This is mandatory if
//! a number is required to be interpreted as a string, e.g., 'foo=eq.\"10\"' (otherwise, in
//! 'foo=eq.10', 10 is interpreted as a number). Note that all literal string values will be
//! rendered within single quotes in SQL. When converting the parsed Select struct back to a
//! URL, these values will never be enclosed in double-quotes in the URL except for the case
//! of a numeric string or a string containing one of the reserved chars
//! (see ontodev_sqlrest::RESERVED).</i>
//! ```rust
//! # use ontodev_sqlrest::parse;
//! # use urlencoding::{decode, encode};
//! let from_url = "bar?c1=eq.Henry%20Kissinger\
//!                 &c2=in.(\"McMahon, Jim\",William Perry,\"72\",Nancy,NULL)\
//!                 &c3=eq.Fred";
//! let expected_sql = "SELECT * FROM \"bar\" WHERE \"c1\" = 'Henry Kissinger' \
//!                     AND \"c2\" IN ('McMahon, Jim', 'William Perry', '72', 'Nancy', NULL) \
//!                     AND \"c3\" = 'Fred'";
//! let select = parse(&from_url).unwrap();
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // NULL values are treated differently from literal values (i.e., they are not rendered in
//! // quotes). Note also that they are not converted to uppercase in the generated SQL:
//! let from_url = "bar?select=c1,c2&c1=not_eq.null";
//! let expected_sql = "SELECT \"c1\", \"c2\" FROM \"bar\" WHERE \"c1\" <> null";
//! let select = parse(&from_url).unwrap();
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(from_url, decode(&select.to_url().unwrap()).unwrap());
//! ```
//!
//! ### ORDER BY, LIMIT, and OFFSET
//! ```rust
//! # use ontodev_sqlrest::parse;
//! # use urlencoding::{decode, encode};
//! // Columns in order_by clauses are handled similarly to table and column names:
//! let from_url = "bar?order=foo1.asc,foo2.desc,foo3.asc";
//! let expected_sql =
//!     "SELECT * FROM \"bar\" ORDER BY \"foo1\" ASC, \"foo2\" DESC, \"foo3\" ASC";
//! let select = parse(&from_url).unwrap();
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(from_url, decode(&select.to_url().unwrap()).unwrap());
//!
//! let from_url = "bar?order=foo%201.asc,foo 2.desc,foo3.asc";
//! let expected_sql =
//!     "SELECT * FROM \"bar\" ORDER BY \"foo 1\" ASC, \"foo 2\" DESC, \"foo3\" ASC";
//! let select = parse(&from_url).unwrap();
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // Arguments to `limit` and `order` must be interger-valued:
//! let from_url = "bar?order=foo1.desc,foo 2.asc,foo%205.desc&limit=10&offset=30";
//! let expected_sql = "SELECT * FROM \"bar\" \
//!                     ORDER BY \"foo1\" DESC, \"foo 2\" ASC, \"foo 5\" DESC \
//!                     LIMIT 10 OFFSET 30";
//! let select = parse(from_url).unwrap();
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! ```
//!
//! ### A more complicated example:
//! ```rust
//! # use ontodev_sqlrest::parse;
//! # use urlencoding::decode;
//!
//! let from_url = "a%20bar?\
//!                 select=foo1,foo 2,foo%205\
//!                 &foo1=eq.0\
//!                 &foo 2=not_eq.\"10\"\
//!                 &foo3=lt.20\
//!                 &foo4=gt.5\
//!                 &foo%205=lte.30\
//!                 &foo6=gte.60\
//!                 &foo7=like.alpha\
//!                 &foo8=not_like.abby normal\
//!                 &foo9=ilike.beta\
//!                 &foo10=not_ilike.gamma\
//!                 &foo11=is.NULL\
//!                 &foo12=not_is.NULL\
//!                 &foo13=eq.terrible\
//!                 &foo14=in.(A fancy hat,\"5\",C page 21,delicious,NULL)\
//!                 &foo15=not_in.(1,2,3)\
//!                 &order=foo1.desc,foo 2.asc,foo%205.desc\
//!                 &limit=10\
//!                 &offset=30";
//!
//! let expected_sql = "SELECT \"foo1\", \"foo 2\", \"foo 5\" \
//!                     FROM \"a bar\" \
//!                     WHERE \"foo1\" = 0 \
//!                     AND \"foo 2\" <> '10' \
//!                     AND \"foo3\" < 20 \
//!                     AND \"foo4\" > 5 \
//!                     AND \"foo 5\" <= 30 \
//!                     AND \"foo6\" >= 60 \
//!                     AND \"foo7\" LIKE 'alpha' \
//!                     AND \"foo8\" NOT LIKE 'abby normal' \
//!                     AND \"foo9\" ILIKE 'beta' \
//!                     AND \"foo10\" NOT ILIKE 'gamma' \
//!                     AND \"foo11\" IS NOT DISTINCT FROM NULL \
//!                     AND \"foo12\" IS DISTINCT FROM NULL \
//!                     AND \"foo13\" = 'terrible' \
//!                     AND \"foo14\" IN ('A fancy hat', '5', 'C page 21', 'delicious', NULL) \
//!                     AND \"foo15\" NOT IN (1, 2, 3) \
//!                     ORDER BY \"foo1\" DESC, \"foo 2\" ASC, \"foo 5\" DESC \
//!                     LIMIT 10 \
//!                     OFFSET 30";
//!
//! let select = parse(&from_url).unwrap();
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(&from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//! ```

use enquote::unquote;
use futures::executor::block_on;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as SerdeMap, Value as SerdeValue};
use sqlx::{
    any::{Any, AnyArguments, AnyKind, AnyPool, AnyRow},
    query as sqlx_query,
    query::Query,
    Row,
};
use std::{collections::HashMap, fmt, str::FromStr};
use tree_sitter::{Node, Parser};
use urlencoding::{decode, encode};

pub const HTTP_SUCCESS: usize = 200;
pub const HTTP_SUCCESS_PARTIAL_CONTENT: usize = 206;
pub const DB_OBJECT_MATCH_STR: &str = r"^[\w_ ]+$";

lazy_static! {
    /// List of reserved characters that are accepted as part of a literal value string in URL input
    /// to the parse() function. Note that these must match the reserved characters accepted by the
    /// [tree-sitter-sqlrest grammar](https://github.com/ontodev/tree-sitter-sqlrest) grammar (see
    /// the file, Cargo.toml, in this repository for the specific version of tree-sitter-sqlrest
    /// used).
    static ref RESERVED: Vec<char> = vec![':', ',', '.', '(', ')'];

    /// Regular expression object used to match against database object names (table names, column
    /// names, etc.).
    #[derive(Debug)]
    static ref DB_OBJECT_REGEX: Regex = Regex::new(DB_OBJECT_MATCH_STR).unwrap();
}

/// Representation of a database type. Currently only Postgres and Sqlite are supported.
#[derive(Debug, PartialEq, Eq)]
pub enum DbType {
    Postgres,
    Sqlite,
}

/// Given a database connection pool, return the corresponding DbType.
pub fn get_db_type(pool: &AnyPool) -> Result<DbType, String> {
    if pool.any_kind() == AnyKind::Postgres {
        Ok(DbType::Postgres)
    } else if pool.any_kind() == AnyKind::Sqlite {
        Ok(DbType::Sqlite)
    } else {
        Err(format!("Unsupported database type: {:?}", pool.any_kind()))
    }
}

/// Representation of an operator of an SQL query.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Operator {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessThanEquals,
    GreaterThanEquals,
    Like,
    NotLike,
    ILike,
    NotILike,
    Is,
    IsNot,
    In,
    NotIn,
}

impl FromStr for Operator {
    type Err = String;

    /// Given a string representation of an operator, return the corresponding operator. The valid
    /// string representations of the various operators are the following:
    /// * "eq" => Operator::Equals
    /// * "not_eq" => Operator::NotEquals
    /// * "lt" => Operator::LessThan
    /// * "gt" => Operator::GreaterThan
    /// * "lte" => Operator::LessThanEquals
    /// * "gte" => Operator::GreaterThanEquals
    /// * "like" => Operator::Like
    /// * "not_like" => Operator::NotLike
    /// * "ilike" => Operator::ILike
    /// * "not_ilike" => Operator::NotILike
    /// * "is" => Operator::Is
    /// * "not_is" => Operator::IsNot
    /// * "in" => Operator::In
    /// * "not_in" => Operator::NotIn
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "eq" => Ok(Operator::Equals),
            "not_eq" => Ok(Operator::NotEquals),
            "lt" => Ok(Operator::LessThan),
            "gt" => Ok(Operator::GreaterThan),
            "lte" => Ok(Operator::LessThanEquals),
            "gte" => Ok(Operator::GreaterThanEquals),
            "like" => Ok(Operator::Like),
            "not_like" => Ok(Operator::NotLike),
            "ilike" => Ok(Operator::ILike),
            "not_ilike" => Ok(Operator::NotILike),
            "is" => Ok(Operator::Is),
            "not_is" => Ok(Operator::IsNot),
            "in" => Ok(Operator::In),
            "not_in" => Ok(Operator::NotIn),
            _ => Err(format!("Unable to parse '{}' as an operator.", s)),
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match &self {
            Operator::Equals => "eq",
            Operator::NotEquals => "not_eq",
            Operator::LessThan => "lt",
            Operator::GreaterThan => "gt",
            Operator::LessThanEquals => "lte",
            Operator::GreaterThanEquals => "gte",
            Operator::Like => "like",
            Operator::NotLike => "not_like",
            Operator::ILike => "ilike",
            Operator::NotILike => "not_ilike",
            Operator::Is => "is",
            Operator::IsNot => "not_is",
            Operator::In => "in",
            Operator::NotIn => "not_in",
        };
        write!(f, "{}", s)
    }
}

/// The direction of an ORDER BY clause in an SQL query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl FromStr for Direction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "asc" | "ascending" => Ok(Direction::Ascending),
            "desc" | "descending" => Ok(Direction::Descending),
            _ => Err(format!("Unable to parse '{}' as a direction.", s)),
        }
    }
}

impl Direction {
    /// Converts a Direction enum to a SQL string.
    pub fn to_sql(&self) -> &str {
        match self {
            Direction::Ascending => "ASC",
            Direction::Descending => "DESC",
        }
    }

    /// Converts a Direction enum to a format suitable for inclusion in a URL.
    pub fn to_url(&self) -> &str {
        match self {
            Direction::Ascending => "asc",
            Direction::Descending => "desc",
        }
    }
}

/// Representation of a filter in an SQL query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Filter {
    pub lhs: String,
    pub operator: Operator,
    pub rhs: SerdeValue,
}

/// Private helper function to surround the given string in double quotes if it is not already
/// quoted and contains whitespace.
fn quote_if_whitespace(token: &str) -> String {
    if (token.starts_with("\"") && token.ends_with("\"")) || !token.contains(char::is_whitespace) {
        token.to_string()
    } else {
        format!("\"{}\"", token)
    }
}

impl Filter {
    /// Given a left hand side, a right hand side, and an operator, create a new filter.
    pub fn new<S: Into<String>>(lhs: S, operator: S, rhs: SerdeValue) -> Result<Filter, String> {
        match Operator::from_str(&operator.into()) {
            Ok(operator) => Ok(Filter {
                lhs: lhs.into(),
                operator: operator,
                rhs: rhs,
            }),
            Err(error) => Err(error),
        }
    }

    /// Clone the given filter.
    pub fn clone(filter: &Filter) -> Filter {
        Filter { ..filter.clone() }
    }

    /// Given strings representing the left and right hand sides of a filter, render an SQL string
    /// of the form 'lhs LIKE rhs' when the `positive` flag is true, and 'lhs NOT LIKE rhs'
    /// otherwise.
    fn render_like_not_like<S: Into<String>>(
        lhs: S,
        rhs: S,
        positive: bool,
    ) -> Result<String, String> {
        let negation;
        if !positive {
            negation = " NOT";
        } else {
            negation = "";
        }

        let rhs = rhs.into();
        Ok(format!("{}{} LIKE {}", lhs.into(), negation, rhs))
    }

    /// Given strings representing the left and right hand sides of a filter, render an SQL string
    /// representing a case-insensitve LIKE relation between the lhs and rhs in the case when
    /// `positive` is true, or the negation of such a statement otherwise. The appropriate syntax
    /// will be determined on the basis of the given database type.
    fn render_ilike_not_ilike<S: Into<String>>(
        dbtype: &DbType,
        lhs: S,
        rhs: S,
        positive: bool,
    ) -> Result<String, String> {
        let negation;
        if !positive {
            negation = " NOT";
        } else {
            negation = "";
        }

        let rhs = rhs.into();
        if *dbtype == DbType::Postgres {
            Ok(format!("{}{} ILIKE {}", lhs.into(), negation, rhs))
        } else {
            Ok(format!(
                "LOWER({}){} LIKE LOWER({})",
                lhs.into(),
                negation,
                rhs
            ))
        }
    }

    /// Given a string representing the left hand side of a filter, and a vector of options
    /// representing the right hand side, render an SQL string expressing an IN relation between lhs
    /// and rhs when `positive` is true, or a NOT IN relation otherwise.
    fn render_in_not_in<S: Into<String>>(
        lhs: S,
        options: &Vec<SerdeValue>,
        positive: bool,
    ) -> Result<String, String> {
        let negation;
        if !positive {
            negation = " NOT";
        } else {
            negation = "";
        }

        let mut values = vec![];
        let mut is_string_list = false;
        for (i, option) in options.iter().enumerate() {
            match option {
                SerdeValue::String(s) => {
                    if i == 0 {
                        is_string_list = true;
                    } else if !is_string_list {
                        return Err(format!(
                            "{:?} contains both text and numeric types.",
                            options
                        ));
                    }
                    values.push(format!("{}", s))
                }
                SerdeValue::Number(n) => {
                    if i == 0 {
                        is_string_list = false;
                    } else if is_string_list {
                        return Err(format!(
                            "{:?} contains both text and numeric types.",
                            options
                        ));
                    }
                    values.push(format!("{}", n))
                }
                _ => {
                    return Err(format!(
                        "{:?} is not an array of strings or numbers.",
                        options
                    ))
                }
            };
        }
        let value_list = format!("({})", values.join(", "));
        let filter_sql = format!("{}{} IN {}", lhs.into(), negation, value_list);
        Ok(filter_sql)
    }

    /// Given a string representing the left hand side of a filter, a string or number (encoded as a
    /// SerdeValue) representing the right hand side, render an SQL string representing the
    /// equivalent of a case-insensitve IS relation between the lhs and rhs in the case when
    /// `positive` is true, or the negation of such a statement otherwise. The appropriate syntax
    /// will be determined on the basis of the given database type. For example, Postgres's IS NOT
    /// DISTINCT FROM is equivalent to Sqlite's IS operator, and IS DISTINCT FROM is the same as IS
    /// NOT.
    fn render_is_not_is<S: Into<String>>(
        dbtype: &DbType,
        lhs: S,
        rhs: &SerdeValue,
        positive: bool,
    ) -> Result<String, String> {
        let value = match rhs {
            SerdeValue::String(s) => s.to_string(),
            SerdeValue::Number(n) => {
                format!("{}", n)
            }
            _ => return Err(format!("{} is neither a string nor a number", rhs)),
        };

        if *dbtype == DbType::Sqlite {
            Ok(format!(
                "{} IS{} {}",
                lhs.into(),
                {
                    if positive {
                        ""
                    } else {
                        " NOT"
                    }
                },
                value
            ))
        } else {
            Ok(format!(
                "{} IS{} DISTINCT FROM {}",
                lhs.into(),
                {
                    if positive {
                        " NOT"
                    } else {
                        ""
                    }
                },
                value
            ))
        }
    }

    /// Convert the given filter into an SQL string suitable to be used in a WHERE clause, using the
    /// syntax appropriate to the given database type.
    pub fn to_sql(&self, dbtype: &DbType) -> Result<String, String> {
        // Note that we only check the rhs below since the lhs is guaranteed to be a String by
        // definition.
        let not_a_string_err = format!("RHS of filter: {:?} is not a string.", self);
        let not_a_string_or_number_err =
            format!("RHS of filter: {:?} is not a string or a number.", self);

        match self.operator {
            Operator::Equals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} = {}", quote_if_whitespace(&self.lhs), s)),
                SerdeValue::Number(n) => Ok(format!("{} = {}", quote_if_whitespace(&self.lhs), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::NotEquals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} <> {}", quote_if_whitespace(&self.lhs), s)),
                SerdeValue::Number(n) => Ok(format!("{} <> {}", quote_if_whitespace(&self.lhs), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::LessThan => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} < {}", quote_if_whitespace(&self.lhs), s)),
                SerdeValue::Number(n) => Ok(format!("{} < {}", quote_if_whitespace(&self.lhs), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::GreaterThan => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} > {}", quote_if_whitespace(&self.lhs), s)),
                SerdeValue::Number(n) => Ok(format!("{} > {}", quote_if_whitespace(&self.lhs), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::LessThanEquals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} <= {}", quote_if_whitespace(&self.lhs), s)),
                SerdeValue::Number(n) => Ok(format!("{} <= {}", quote_if_whitespace(&self.lhs), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::GreaterThanEquals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} >= {}", quote_if_whitespace(&self.lhs), s)),
                SerdeValue::Number(n) => Ok(format!("{} >= {}", quote_if_whitespace(&self.lhs), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::Like => match &self.rhs {
                SerdeValue::String(s) => {
                    Self::render_like_not_like(&quote_if_whitespace(&self.lhs), &s, true)
                }
                _ => Err(not_a_string_err),
            },
            Operator::NotLike => match &self.rhs {
                SerdeValue::String(s) => {
                    Self::render_like_not_like(&quote_if_whitespace(&self.lhs), &s, false)
                }
                _ => Err(not_a_string_err),
            },
            Operator::ILike => match &self.rhs {
                SerdeValue::String(s) => {
                    Self::render_ilike_not_ilike(dbtype, &quote_if_whitespace(&self.lhs), &s, true)
                }
                _ => Err(not_a_string_err),
            },
            Operator::NotILike => match &self.rhs {
                SerdeValue::String(s) => {
                    Self::render_ilike_not_ilike(dbtype, &quote_if_whitespace(&self.lhs), &s, false)
                }
                _ => Err(not_a_string_err),
            },
            Operator::Is => {
                Self::render_is_not_is(dbtype, &quote_if_whitespace(&self.lhs), &self.rhs, true)
            }
            Operator::IsNot => {
                Self::render_is_not_is(dbtype, &quote_if_whitespace(&self.lhs), &self.rhs, false)
            }
            Operator::In => match &self.rhs {
                SerdeValue::Array(options) => {
                    Self::render_in_not_in(&quote_if_whitespace(&self.lhs), options, true)
                }
                _ => Err(format!("RHS of filter: {:?} is not an array.", self)),
            },
            Operator::NotIn => match &self.rhs {
                SerdeValue::Array(options) => {
                    Self::render_in_not_in(&quote_if_whitespace(&self.lhs), options, false)
                }
                _ => Err(format!("RHS of filter: {:?} is not an array.", self)),
            },
        }
    }
}

/// Given a list of filters and a database type, convert each filter to an SQL string and
/// join them together using the keyword AND.
pub fn filters_to_sql(filters: &Vec<Filter>, dbtype: &DbType) -> Result<String, String> {
    let mut parts: Vec<String> = vec![];
    for filter in filters {
        match filter.to_sql(dbtype) {
            Ok(sql) => parts.push(sql),
            Err(err) => return Err(err),
        };
    }
    let joiner = " AND ";
    Ok(parts.join(joiner))
}

/// Representation of a window function in an SQL query. If the alias is None it defaults to
/// '<function_name>_<column_name>'.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Window {
    pub function: String,
    pub column: String,
    pub alias: Option<String>,
}

impl Window {
    /// Given a function name, a column name, and optionally an alias, create a new Window struct.
    pub fn new<S: Into<String>>(function: S, column: S, alias: Option<S>) -> Window {
        match alias {
            None => Window {
                function: function.into(),
                column: column.into(),
                alias: None,
            },
            Some(alias) => Window {
                function: function.into(),
                column: column.into(),
                alias: Some(alias.into()),
            },
        }
    }

    /// Clone the given Window.
    pub fn clone(window: &Window) -> Window {
        Window { ..window.clone() }
    }

    /// Convert the given window into an SQL string suitable to be used in a SELECT clause.
    pub fn to_sql(&self) -> String {
        format!(
            "{}({}) OVER() {}",
            self.function,
            self.column,
            match &self.alias {
                None => format!("{}_{}", self.function.to_lowercase(), self.column),
                Some(alias) => alias.to_string(),
            }
        )
    }
}

/// Representation of a column expression in a SELECT clause, with optional alias and cast.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SelectColumn {
    pub expression: String,
    pub alias: Option<String>,
    pub cast: Option<String>,
}

impl SelectColumn {
    /// Given an expression and optionally an alias and/or a cast, create a new SelectColumn struct.
    pub fn new<S: Into<String>>(expression: S, alias: Option<S>, cast: Option<S>) -> SelectColumn {
        let mut select_col = SelectColumn {
            expression: expression.into(),
            ..Default::default()
        };
        if let Some(alias) = alias {
            select_col.alias = Some(alias.into());
        }
        if let Some(cast) = cast {
            select_col.cast = Some(cast.into());
        }
        select_col
    }

    /// Clone the given SelectColumn.
    pub fn clone(select_column: &SelectColumn) -> SelectColumn {
        SelectColumn {
            ..select_column.clone()
        }
    }

    /// Convert the given window into an SQL string suitable to be used in a SELECT clause.
    pub fn to_sql(&self, dbtype: &DbType) -> String {
        let mut sql = quote_if_whitespace(&self.expression);
        if let Some(cast) = &self.cast {
            let cast = cast.to_uppercase();
            match *dbtype {
                DbType::Postgres => sql = format!("{}::{}", sql, cast),
                DbType::Sqlite => sql = format!("CAST({} AS {})", sql, cast),
            };
        }
        if let Some(alias) = &self.alias {
            sql.push_str(&format!(" AS {}", quote_if_whitespace(&alias)));
        }
        sql
    }
}

/// Representation of a column expression in an ORDER BY clause
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderByColumn {
    pub column: String,
    pub direction: Direction,
}

impl OrderByColumn {
    /// Given a column and a direction, create a new OrderByColumn struct.
    pub fn new<S: Into<String>>(column: S, direction: &Direction) -> OrderByColumn {
        OrderByColumn {
            column: column.into(),
            direction: direction.clone(),
        }
    }

    /// Clone the given OrderByColumn
    pub fn clone(order_by_column: &OrderByColumn) -> OrderByColumn {
        OrderByColumn {
            ..order_by_column.clone()
        }
    }

    /// Convert the given OrderByColumn into an SQL string suitable for use in an OEDER BY clause.
    pub fn to_sql(&self) -> String {
        format!(
            "{} {}",
            quote_if_whitespace(&self.column),
            self.direction.to_sql()
        )
    }
}

/// Used to represent the count strategy when determining row counts for queries.
#[derive(Debug, PartialEq, Eq)]
pub enum CountStrategy {
    /// Count the exact number of rows returned.
    Exact,
    /// Use internal database tables to estimate the number of rows returned.
    Planned,
    /// Use exact counts up to a predefined threshold, and planned counts for anything that
    /// exceeds the threshold.
    Estimated,
    /// Use a SQL window function to count the (exact) number of rows returned.
    Window,
}

/// Given an SQL string that has been bound to the given parameter vector, construct a database
/// query and return it.
pub fn construct_query<'a>(
    sql: &'a str,
    param_vec: &'a Vec<&'a SerdeValue>,
) -> Result<Query<'a, Any, AnyArguments<'a>>, String> {
    let mut query = sqlx_query::<Any>(&sql);
    for param in param_vec {
        match param {
            SerdeValue::String(s) => query = query.bind(s),
            SerdeValue::Number(n) => query = query.bind(n.as_i64()),
            _ => return Err(format!("{} is not a string or a number.", param)),
        };
    }
    Ok(query)
}

/// A structure to represent an SQL select statement.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Select {
    pub table: String,
    pub select: Vec<SelectColumn>,
    pub filter: Vec<Filter>,
    pub window: Option<Window>,
    pub group_by: Vec<String>,
    pub having: Vec<Filter>,
    pub order_by: Vec<OrderByColumn>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Select {
    /// Create a new Select struct with the given table name and with its other fields initialized
    /// to their default values.
    pub fn new<S: Into<String>>(table: S) -> Select {
        Select {
            table: table.into(),
            ..Default::default()
        }
    }

    /// Clone the given Select struct.
    pub fn clone(select: &Select) -> Select {
        Select { ..select.clone() }
    }

    /// Given a table name, set `self.table` to that name.
    pub fn table<S: Into<String>>(&mut self, table: S) -> &mut Select {
        self.table = table.into();
        self
    }

    /// Given a vector of column expressions, replace the current contents of `self.select` with the
    /// contents of the given vector, without specifying any aliases.
    pub fn select<S: Into<String>>(&mut self, select: Vec<S>) -> &mut Select {
        self.select.clear();
        for s in select {
            self.select.push(SelectColumn::new(s.into(), None, None));
        }
        self
    }

    /// Given a vector of tuples such that the first place of each tuple is a column expression
    /// and the second place is an alias for that expression, replace the current contents of
    /// `self.select` with the contents of the given vector.
    pub fn explicit_select(&mut self, select: Vec<&SelectColumn>) -> &mut Select {
        self.select.clear();
        for select_column in select {
            self.add_explicit_select(select_column);
        }
        self
    }

    /// Given a column expression, add it to the vector, `self.select` without an alias.
    pub fn add_select<S: Into<String>>(&mut self, select: S) -> &mut Select {
        self.select
            .push(SelectColumn::new(select.into(), None, None));
        self
    }

    /// Given a column expression and an alias for that expression, add the tuple (column, alias) to
    /// the vector, `self.select`.
    pub fn add_explicit_select(&mut self, select_column: &SelectColumn) -> &mut Select {
        self.select.push(select_column.clone());
        self
    }

    /// Given a vector of filters, replace the current contents of `self.filter` with the contents
    /// of the given vector.
    pub fn filter(&mut self, filters: Vec<Filter>) -> &mut Select {
        self.filter.clear();
        for f in filters {
            self.filter.push(f);
        }
        self
    }

    /// Given a filter, add it to the vector, `self.filter`.
    pub fn add_filter(&mut self, filter: Filter) -> &mut Select {
        self.filter.push(filter);
        self
    }

    /// Given a function name, column name, and optionally an alias, create a new Window struct
    /// and associate it with the Select struct.
    pub fn window<S: Into<String>>(
        &mut self,
        function: S,
        column: S,
        alias: Option<S>,
    ) -> &mut Select {
        self.window = Some(Window::new(function, column, alias));
        self
    }

    /// Given a vector of filters, replace the current contents of `self.having` with the contents
    /// of the given vector.
    pub fn having(&mut self, filters: Vec<Filter>) -> &mut Select {
        self.having.clear();
        for f in filters {
            self.having.push(f);
        }
        self
    }

    /// Given a filter, add it to the vector, `self.having`.
    pub fn add_having(&mut self, filter: Filter) -> &mut Select {
        self.having.push(filter);
        self
    }

    /// Given a vector of column names, replace the current contents of `self.group_by` with the
    /// contents of the given vector.
    pub fn group_by<S: Into<String>>(&mut self, group_by: Vec<S>) -> &mut Select {
        self.group_by.clear();
        for s in group_by {
            self.group_by.push(s.into());
        }
        self
    }

    /// Given a column name, add it to the vector, `self.group_by`.
    pub fn add_group_by<S: Into<String>>(&mut self, group_by: S) -> &mut Select {
        self.group_by.push(group_by.into());
        self
    }

    /// Given a vector of column names, replace the current contents of `self.order_by` with the
    /// contents of the given vector, such that the direction of each order by clause is
    /// [Direction::Ascending].
    pub fn order_by<S: Into<String>>(&mut self, order_by: Vec<S>) -> &mut Select {
        self.order_by.clear();
        for column in order_by {
            let clause = OrderByColumn::new(column.into(), &Direction::Ascending);
            self.order_by.push(clause);
        }
        self
    }

    /// Given a vector of [OrderByColumn] structs, replace the current contents of `self.order_by`
    /// with the contents of the given vector.
    pub fn explicit_order_by(&mut self, order_by: Vec<&OrderByColumn>) -> &mut Select {
        self.order_by.clear();
        for order_by_column in order_by {
            self.order_by.push(order_by_column.clone());
        }
        self
    }

    /// Given a column name, add it to the vector, `self.order_by` with the direction
    /// [Direction::Ascending].
    pub fn add_order_by<S: Into<String>>(&mut self, column: S) -> &mut Select {
        let clause = OrderByColumn::new(column.into(), &Direction::Ascending);
        self.order_by.push(clause);
        self
    }

    /// Given an [OrderByColumn], add it to the vector `self.order_by`.
    pub fn add_explicit_order_by(&mut self, order_by: &OrderByColumn) -> &mut Select {
        self.order_by.push(order_by.clone());
        self
    }

    /// Given an unsigned integer `limit`, set `self.limit` to the value of `limit`.
    pub fn limit(&mut self, limit: usize) -> &mut Select {
        self.limit = Some(limit);
        self
    }

    /// Given an unsigned integer `offset`, set `self.offset` to the value of `offset`.
    pub fn offset(&mut self, offset: usize) -> &mut Select {
        self.offset = Some(offset);
        self
    }

    /// Given a database pool, query the database for all of the columns corresponding to the
    /// `table` field of this Select struct (`self`), and add them to `self`'s `select` field.
    /// If `self.table` is not defined, return `self` back to the caller unchanged.
    pub fn select_all(&mut self, pool: &AnyPool) -> Result<&mut Select, String> {
        if self.table.is_empty() {
            // If no table has been defined, do nothing.
            return Ok(self);
        }

        // We are forcing the user to supply his/her own quotes around table names and column
        // names, etc. Because of this we need to unquote the table name for the metadata query
        // below.
        let unquoted_table = unquote(&self.table).unwrap_or(self.table.clone());
        let sql;
        if pool.any_kind() == AnyKind::Postgres {
            sql = format!(
                r#"SELECT "column_name" AS "name"
                   FROM "information_schema"."columns"
                   WHERE "table_name" = '{}'
                   ORDER BY "ordinal_position""#,
                unquoted_table,
            );
        } else {
            sql = format!(r#"PRAGMA TABLE_INFO('{}')"#, unquoted_table);
        }
        // Clear the current contents of the select field:
        self.select.clear();
        // Add all of the columns returned by the query above to the select field:
        let query = sqlx_query(&sql);
        let rows = block_on(query.fetch_all(pool));
        match rows {
            Ok(rows) => {
                for row in &rows {
                    let cname: &str = row.get("name");
                    self.add_select(format!(r#""{}""#, cname));
                }
            }
            Err(e) => return Err(e.to_string()),
        };

        Ok(self)
    }

    /// Given a database type, convert the given Select struct to an SQL statement, using the syntax
    /// appropriate for the kind of database specified. Returns an Error if `self.table` or
    /// `self.select` have not been defined.
    pub fn to_sql(&self, dbtype: &DbType) -> Result<String, String> {
        if self.table.is_empty() {
            return Err("Missing required field: `table` in to_sql()".to_string());
        }

        let mut select_clause;
        if self.select.is_empty() {
            select_clause = String::from("*");
        } else {
            let mut select_columns = vec![];
            for select_column in &self.select {
                select_columns.push(select_column.to_sql(dbtype));
            }
            select_clause = select_columns.join(", ");
        }
        if let Some(window) = &self.window {
            select_clause.push_str(&format!(", {}", window.to_sql()));
        };

        let table = quote_if_whitespace(&self.table);
        let mut sql = format!("SELECT {} FROM {}", select_clause, table);
        if !self.filter.is_empty() {
            let where_clause = match filters_to_sql(&self.filter, &dbtype) {
                Err(err) => return Err(err),
                Ok(s) => s,
            };
            sql.push_str(&format!(" WHERE {}", where_clause));
        }
        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }
        if !self.having.is_empty() {
            let having_clause = match filters_to_sql(&self.having, &dbtype) {
                Err(err) => return Err(err),
                Ok(s) => s,
            };
            sql.push_str(&format!(" HAVING {}", having_clause));
        }
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_strings = self
                .order_by
                .iter()
                .map(|o| o.to_sql())
                .collect::<Vec<String>>();
            sql.push_str(&format!("{}", order_strings.join(", ")));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }
        Ok(sql)
    }

    /// Convert the given Select struct to an SQL statement using Postgres syntax. This is a
    /// convenience method implemented as a wrapper around a call to to_sql(&DbType::Postgres).
    pub fn to_postgres(&self) -> Result<String, String> {
        self.to_sql(&DbType::Postgres)
    }

    /// Convert the given Select struct to an SQL statement using Sqlite syntax. This is a
    /// convenience method implemented as a wrapper around a call to to_sql(&DbType::Sqlite).
    pub fn to_sqlite(&self) -> Result<String, String> {
        self.to_sql(&DbType::Sqlite)
    }

    /// Given a database type, convert the given Select struct to an SQL statement that will
    /// return the number of rows in the database that satisfy the criteria (filters, etc.)
    /// associated with the Select struct, using the syntax appropriate for the kind of database
    /// specified.
    pub fn to_sql_count(&self, dbtype: &DbType) -> Result<String, String> {
        let mut count_select = self.clone();
        // We would like to see the number of rows that would be returned if not for the limit
        // and offset clauses, and the order by clause is irrelevant for counting:
        count_select.order_by.clear();
        count_select.limit = None;
        count_select.offset = None;
        let inner_sql = count_select.to_sql(dbtype);
        match inner_sql {
            Ok(inner_sql) => Ok(format!(
                "SELECT COUNT(1) AS count FROM ({}) AS t",
                inner_sql
            )),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Convert the given Select struct to an SQL statement, using Sqlite syntax, that will return
    /// the number of rows in the database that satisfy the criteria (filters, etc.) associated with
    /// the Select struct. This is a convenience method implemented as a wrapper aruond a call to
    /// to_sql_count(&DBType::Sqlite).
    pub fn to_sqlite_count(&self) -> Result<String, String> {
        self.to_sql_count(&DbType::Sqlite)
    }

    /// Convert the given Select struct to an SQL statement, using Postgres syntax, that will return
    /// the number of rows in the database that satisfy the criteria (filters, etc.) associated with
    /// the Select struct. This is a convenience method implemented as a wrapper aruond a call to
    /// to_sql_count(&DBType::Postgres).
    pub fn to_postgres_count(&self) -> Result<String, String> {
        self.to_sql_count(&DbType::Postgres)
    }

    /// Convert the given Select struct to a SerdeMap representing the query string. Returns an error
    /// in the following circumstances:
    /// - The `table` field has not been defined or it contains characters other than word
    ///   characters, underscores, and spaces.
    /// - One of the columns included in the `select`, `order_by`, or `filter` fields contains
    ///   characters other than word characters, underscores, and spaces.
    /// - The `group_by` or `having` clauses have been defined (these aren't supported by to_url()).
    pub fn to_params(&self) -> Result<SerdeMap<String, SerdeValue>, String> {
        if self.table.is_empty() {
            return Err("Missing required field: `table` in to_sql()".to_string());
        }
        if !self.group_by.is_empty() || !self.having.is_empty() {
            return Err("GROUP BY / HAVING clauses are not supported in to_url()".to_string());
        }
        if let Some(_) = self.window {
            return Err("Window functions are not supported in to_url()".to_string());
        }

        // Helper function to handle values that are strings. We: (i) replace any instances of '%'
        // with '*' in LIKE clauses, (ii) surround the value with double-quotes if it is composed
        // entirely of numeric symbols, or if it contains one of the special reserved characters
        // (see the static ref RESERVED defined above).
        fn handle_string_value(token: &str, operator: &Operator) -> String {
            let token = {
                let t = unquote(&token).unwrap_or(token.to_string());
                match operator {
                    Operator::Like | Operator::NotLike | Operator::ILike | Operator::NotILike => {
                        t.replace("%", "*")
                    }
                    _ => t,
                }
            };
            if token.chars().all(char::is_numeric) || RESERVED.iter().any(|&c| token.contains(c)) {
                format!("\"{}\"", token)
            } else {
                token.to_string()
            }
        }

        let mut params = SerdeMap::new();
        if self.select.len() > 0 {
            let mut parts = vec![];
            for select_column in &self.select {
                let column = &select_column.expression;
                let alias = &select_column.alias;
                let cast = &select_column.cast;
                let column = unquote(&column).unwrap_or(column.to_string());
                if let Err(e) = is_simple(&column) {
                    return Err(e.to_string());
                }
                let mut part = String::from("");
                if let Some(alias) = alias {
                    let alias = unquote(&alias).unwrap_or(alias.to_string());
                    if let Err(e) = is_simple(&alias) {
                        return Err(e.to_string());
                    }
                    part.push_str(&format!("{}:{}", alias, column));
                } else {
                    part.push_str(&column);
                }
                match cast {
                    Some(cast) if cast != "" => {
                        if let Err(e) = is_simple(&cast) {
                            return Err(e.to_string());
                        }
                        part.push_str(&format!("::{}", cast));
                    }
                    _ => (),
                };
                parts.push(part);
            }
            params.insert("select".into(), parts.join(",").into());
        }

        if self.filter.len() > 0 {
            for filter in &self.filter {
                let rhs = match &filter.rhs {
                    SerdeValue::String(s) => handle_string_value(&s, &filter.operator),
                    SerdeValue::Number(n) => format!("{}", n),
                    SerdeValue::Array(v) => {
                        let mut list = vec![];
                        for item in v {
                            match item {
                                SerdeValue::String(s) => {
                                    list.push(handle_string_value(&s, &filter.operator));
                                }
                                SerdeValue::Number(n) => list.push(n.to_string()),
                                _ => {
                                    return Err(format!(
                                        "Not all list items in {:?} are strings or numbers.",
                                        v
                                    ))
                                }
                            };
                        }
                        format!("({})", list.join(","))
                    }
                    _ => {
                        return Err(format!(
                            "RHS of Filter: {:?} is not a string, number, or list",
                            filter
                        ))
                    }
                };

                let lhs = unquote(&filter.lhs).unwrap_or(filter.lhs.to_string());
                if let Err(e) = is_simple(&lhs) {
                    return Err(format!("While reading filters, got error: {}", e));
                }
                params.insert(lhs, format!("{}.{}", filter.operator, rhs).into());
            }
        }
        if self.order_by.len() > 0 {
            let mut parts = vec![];
            for order_by_column in &self.order_by {
                let column = &order_by_column.column;
                let column = unquote(&column).unwrap_or(column.to_string());
                if let Err(e) = is_simple(&column) {
                    return Err(format!("While reading ORDER BY field, got error: {}", e));
                }
                let direction = order_by_column.direction.to_url();
                parts.push(format!("{}.{}", column, direction));
            }
            params.insert("order".into(), parts.join(",").into());
        }
        if let Some(limit) = self.limit {
            params.insert("limit".into(), limit.into());
        }
        if let Some(offset) = self.offset {
            params.insert("offset".into(), offset.into());
        }
        Ok(params)
    }

    /// Convert the given Select struct to a SQLRest URL. Returns an error in the following
    /// circumstances:
    /// - The `table` field has not been defined or it contains characters other than word
    ///   characters, underscores, and spaces.
    /// - One of the columns included in the `select`, `order_by`, or `filter` fields contains
    ///   characters other than word characters, underscores, and spaces.
    /// - The `group_by` or `having` clauses have been defined (these aren't supported by to_url()).
    pub fn to_url(&self) -> Result<String, String> {
        let params = &self.to_params()?.clone();
        let table = unquote(&self.table).unwrap_or(self.table.to_string());
        if let Err(e) = is_simple(&table) {
            return Err(format!("While reading table name, got error: {}", e));
        }
        if params.len() > 0 {
            let mut parts = vec![];
            for (key, value) in params.iter() {
                let s = match value {
                    serde_json::Value::String(s) => s.as_str().into(),
                    _ => format!("{}", value),
                };
                parts.push(format!("{}={}", key, s));
            }
            Ok(encode(&format!("{}?{}", table, parts.join("&"))).into())
        } else {
            Ok(encode(&table.clone()).to_string())
        }
    }

    /// Given a database connection pool and a parameter map, bind this Select to the parameter map,
    /// execute the resulting query against the database, and return the resulting rows.
    /// If `as_json` is set to true, the appropriate database function will be used to aggregate the
    /// result set as a single AnyRow with a single field called "row" containing a JSON-formatted
    /// string.
    pub fn execute_select(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
        as_json: bool,
    ) -> Result<Vec<AnyRow>, String> {
        let dbtype = match get_db_type(pool) {
            Err(e) => return Err(e),
            Ok(dbtype) => dbtype,
        };

        let sql = match self.to_sql(&dbtype) {
            Err(e) => return Err(e),
            Ok(sql) => sql,
        };
        let sql = if !as_json {
            sql
        } else if dbtype == DbType::Sqlite {
            let mut select = self.clone();
            if select.select.len() == 0 {
                // To use JSON_GROUP_ARRAY in the way that we want, we need to explicitly specify
                // every column name when all columns are selected.
                if let Err(s) = select.select_all(pool) {
                    return Err(s);
                }
            }
            let mut json_keys = vec![];
            for select_column in &select.select {
                // The `select_column.cast` field is not used here, since if there are any casts
                // they will have been taken account of in the to_sql() function above.
                let column = &select_column.expression;
                let alias = &select_column.alias;
                let unquoted_column = match alias {
                    Some(alias) => alias.to_string(),
                    None => unquote(&column).unwrap_or(column.to_string()),
                };
                json_keys.push(format!(r#"'{}', "{}""#, unquoted_column, unquoted_column));
            }
            // Add a key for the window function alias if one is associated with this query.
            if let Some(window) = &select.window {
                let alias = match &window.alias {
                    None => format!("{}_{}", window.function.to_lowercase(), window.column),
                    Some(alias) => alias.to_string(),
                };
                json_keys.push(format!(r#"'{}', "{}""#, alias, alias));
            }
            let json_select = json_keys.join(", ");
            format!(
                "SELECT JSON_GROUP_ARRAY(JSON_OBJECT({})) AS row FROM ({})",
                json_select, sql
            )
        } else {
            format!("SELECT JSON_AGG(t)::TEXT AS row FROM ({}) t", sql)
        };

        let (sql, param_vec) = match bind_sql(pool, &sql, param_map) {
            Err(e) => return Err(e),
            Ok((sql, param_vec)) => (sql, param_vec),
        };

        let result = match construct_query(&sql, &param_vec) {
            Err(e) => Err(e),
            Ok(query) => match block_on(query.fetch_all(pool)) {
                Err(e) => Err(format!("{}", e)),
                Ok(k) => Ok(k),
            },
        };
        result
    }

    /// Given a database connection pool and a parameter map, bind this Select to the parameter map,
    /// execute the resulting query against the database, and return the resulting rows.
    pub fn fetch_rows(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
    ) -> Result<Vec<AnyRow>, String> {
        self.execute_select(pool, param_map, false)
    }

    /// Given a database connection pool and a parameter map, bind this Select to the parameter map,
    /// execute the resulting query against the database, and return the resulting rows as JSON
    /// (i.e., as a SerdeValue).
    pub fn fetch_rows_as_json(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
    ) -> Result<Vec<SerdeMap<String, SerdeValue>>, String> {
        let mut rows = match self.execute_select(pool, param_map, true) {
            Err(e) => return Err(e),
            Ok(rows) => rows,
        };

        let row = {
            if rows.len() != 1 {
                return Err(format!(
                    "In fetch_rows_as_json(), expected 1 row, got {}",
                    rows.len()
                ));
            }
            rows.pop().unwrap()
        };

        let json_row = match row.try_get("row") {
            Err(e) => return Err(e.to_string()),
            Ok(json_row) => json_row,
        };
        extract_rows_from_json_str(json_row)
    }

    /// Maximum allowable value of [Select::limit] when querying from the web.
    pub const WEB_LIMIT_MAX: usize = 100;
    /// Default value to use for the limit parameter when querying from the web.
    pub const WEB_LIMIT_DEFAULT: usize = 20;
    /// Threshold for [estimated counts](CountStrategy::Estimated).
    pub const ESTIMATED_COUNT_THRESHOLD: usize = 1000;

    /// Given a database connection pool, a parameter map, and a [CountStrategy], bind this select
    /// to the parameter map and then fetch the number of rows that would be returned by the query
    /// using the given strategy. Note that [CountStrategy::Window] is not supported by this
    /// function.
    pub fn get_row_count(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
        strategy: &CountStrategy,
    ) -> Result<usize, String> {
        fn exact_count(
            select: &Select,
            pool: &AnyPool,
            param_map: &HashMap<&str, SerdeValue>,
        ) -> Result<usize, String> {
            let dbtype = match get_db_type(pool) {
                Err(e) => return Err(e),
                Ok(dbtype) => dbtype,
            };
            match select.to_sql_count(&dbtype) {
                Err(e) => return Err(e),
                Ok(sql) => match bind_sql(pool, sql, param_map) {
                    Err(e) => return Err(e),
                    Ok((bound_sql, params)) => match construct_query(&bound_sql, &params) {
                        Err(e) => return Err(e),
                        Ok(query) => match block_on(query.fetch_one(pool)) {
                            Err(e) => return Err(e.to_string()),
                            Ok(row) => match row.try_get::<i64, &str>("count") {
                                Err(e) => return Err(e.to_string()),
                                Ok(count) => Ok(count as usize),
                            },
                        },
                    },
                },
            }
        }

        fn planned_count(
            select: &Select,
            pool: &AnyPool,
            param_map: &HashMap<&str, SerdeValue>,
        ) -> Result<usize, String> {
            let mut select = select.clone();
            // We would like to see the number of rows that would be returned if not for the limit
            // and offset clauses, and the order by clause is irrelevant for counting:
            select.order_by.clear();
            select.limit = None;
            select.offset = None;

            if pool.any_kind() == AnyKind::Sqlite {
                // Planned count information is not available in SQLite, so we fallback to the exact
                // count:
                return exact_count(&select, pool, param_map);
            } else {
                // See https://wiki.postgresql.org/wiki/Count_estimate
                let explain_rows = match select.to_postgres() {
                    Err(e) => return Err(e),
                    Ok(sql) => match bind_sql(pool, sql, param_map) {
                        Err(e) => return Err(e),
                        Ok((bound_sql, params)) => {
                            match construct_query(&format!("EXPLAIN {}", bound_sql), &params) {
                                Err(e) => return Err(e),
                                Ok(query) => match block_on(query.fetch_all(pool)) {
                                    Err(e) => return Err(e.to_string()),
                                    Ok(rows) => rows,
                                },
                            }
                        }
                    },
                };

                let rowcount_regex = Regex::new(r" rows=([[:digit:]]+)").unwrap();
                for row in explain_rows {
                    let plan: &str = row.get("QUERY PLAN");
                    if let Some(captures) = rowcount_regex.captures(plan) {
                        if captures.len() > 1 {
                            match &captures[1].parse::<usize>() {
                                Ok(row_count) => return Ok(*row_count),
                                Err(e) => return Err(e.to_string()),
                            };
                        }
                    }
                }
                return Err(format!(
                    "Count not determine row count for query: {:?}",
                    select
                ));
            }
        }

        fn estimated_count(
            select: &Select,
            pool: &AnyPool,
            param_map: &HashMap<&str, SerdeValue>,
        ) -> Result<usize, String> {
            if pool.any_kind() == AnyKind::Sqlite {
                // Planned/estimated count information is not available in SQLite, so we fallback to
                // the exact count:
                exact_count(select, pool, param_map)
            } else {
                match planned_count(select, pool, param_map) {
                    Err(e) => Err(e),
                    Ok(count) if count > Select::ESTIMATED_COUNT_THRESHOLD => Ok(count),
                    _ => exact_count(select, pool, param_map),
                }
            }
        }

        match strategy {
            CountStrategy::Exact => exact_count(self, pool, param_map),
            CountStrategy::Window => {
                return Err("Window count strategy is not supported for get_row_count()".to_string())
            }
            CountStrategy::Planned => planned_count(self, pool, param_map),
            CountStrategy::Estimated => estimated_count(self, pool, param_map),
        }
    }

    /// Given a database connection pool and a parameter map, bind this Select to the parameter map,
    /// execute the resulting query against the database, and return the result as a JSON object
    /// with the following fields:
    ///   * `status`: HTTP status code
    ///   * `unit`:   The kind of thing referred to by `start`, `end`, and `count`
    ///   * `start`:  corresponds to [self.offset](Select::offset), or 0 if undefined
    ///   * `end`:    corresponds to `start` + [self.limit](Select::limit), where
    ///               [self.limit](Select::limit) defaults to
    ///               [WEB_LIMIT_DEFAULT](Self::WEB_LIMIT_DEFAULT) if unspecified, and cannot be
    ///               greater than [WEB_LIMIT_MAX](Self::WEB_LIMIT_MAX). (If
    ///               [self.limit](Select::limit) > [WEB_LIMIT_MAX](Self::WEB_LIMIT_MAX) then the
    ///               latter will be used instead.)
    ///   * `count`:  the total number of rows matching the query, irrespective of the values of
    ///               [self.limit](Select::limit) and [self.offset](Select::offset).
    ///               Note that the `fetch_as_json()` function always returns an exact count. To
    ///               use an alternative [CountStrategy], use the
    ///               [fetch_as_json_with_count_strategy()](Self::fetch_as_json_with_count_strategy)
    ///               function.
    ///   * `rows`:   the actual row records returned by the query given [self.limit](Select::limit)
    ///               and [self.offset](Select::offset), represented as a JSON array of JSON objects
    ///
    /// In case of an error a JSON object in the following format will be returned:
    ///   * `status`: HTTP status code
    ///   * `error`: The error message.
    pub fn fetch_as_json(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
    ) -> Result<SerdeMap<String, SerdeValue>, SerdeMap<String, SerdeValue>> {
        self.fetch_as_json_with_count_strategy(pool, param_map, &CountStrategy::Exact)
    }

    /// Given a database connection pool, a parameter map, and a count strategy, bind this Select to
    /// the parameter map, execute the resulting query against the database, and return the result
    /// as a JSON object (see [fetch_as_json()](Self::fetch_as_json) for the format of the JSON
    /// object that is returned), using the given [CountStrategy] to determine the row count.
    pub fn fetch_as_json_with_count_strategy(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
        strategy: &CountStrategy,
    ) -> Result<SerdeMap<String, SerdeValue>, SerdeMap<String, SerdeValue>> {
        fn error_status(err: &str) -> SerdeMap<String, SerdeValue> {
            let mut err_json = SerdeMap::new();
            err_json.insert("status".to_string(), json!(400));
            err_json.insert("error".to_string(), err.into());
            err_json
        }

        fn window_count(
            rows: &Vec<SerdeMap<String, SerdeValue>>,
        ) -> Result<usize, SerdeMap<String, SerdeValue>> {
            let first_row = &rows[0];
            match first_row.get("count") {
                None => {
                    return Err(error_status(&format!(
                        "No field called 'count' found in row: {:?}",
                        first_row
                    )))
                }
                Some(c) => match c.as_i64() {
                    Some(n) => Ok(n as usize),
                    None => return Err(error_status(&format!("Could not parse '{}' as usize", c))),
                },
            }
        }

        let mut limited_select = self.clone();
        if *strategy == CountStrategy::Window {
            limited_select.window("COUNT", "1", Some("count"));
        }
        match self.limit {
            None => limited_select.limit(Self::WEB_LIMIT_DEFAULT),
            Some(l) if l > Self::WEB_LIMIT_MAX => limited_select.limit(Self::WEB_LIMIT_MAX),
            Some(l) => limited_select.limit(l),
        };

        let rows = match limited_select.fetch_rows_as_json(pool, param_map) {
            Err(e) => return Err(error_status(&e)),
            Ok(rows) => rows,
        };
        let count = match *strategy {
            // Note that CountStrategy::Window will not work when no rows are returned.
            CountStrategy::Window if rows.len() > 0 => match window_count(&rows) {
                Err(e) => return Err(e),
                Ok(count) => count,
            },
            _ => match limited_select.get_row_count(pool, param_map, strategy) {
                Err(e) => return Err(error_status(&e)),
                Ok(c) => c,
            },
        };
        let http_status = {
            if count > rows.len() {
                HTTP_SUCCESS_PARTIAL_CONTENT
            } else {
                HTTP_SUCCESS
            }
        };

        let mut json_object = SerdeMap::new();
        json_object.insert("status".to_string(), json!(http_status));
        json_object.insert("unit".to_string(), json!("items"));
        let start = match limited_select.offset {
            None => 0,
            Some(i) => i,
        };
        json_object.insert("start".to_string(), json!(start));
        json_object.insert(
            "end".to_string(),
            match limited_select.limit {
                None => json!(start + Self::WEB_LIMIT_DEFAULT),
                Some(l) => json!(start + l),
            },
        );
        json_object.insert("count".to_string(), json!(count));
        if !(*strategy == CountStrategy::Window) {
            json_object.insert("rows".to_string(), rows.into());
        } else {
            json_object.insert("rows".to_string(), {
                let mut pruned_rows = vec![];
                for row in &rows {
                    let mut row = row.clone();
                    row.remove("count");
                    pruned_rows.push(row);
                }
                pruned_rows.into()
            });
        }
        Ok(json_object)
    }
}

/// Given a tree-sitter Node and a raw string, extract the token from the string that is indicated
/// by the node and return it.
pub fn get_from_raw(n: &Node, raw: &str) -> String {
    let start = n.start_position().column;
    let end = n.end_position().column;
    let extract = &raw[start..end];
    String::from(extract)
}

/// Given an input URL, parse it as a Select struct using the
/// [tree-sitter-sqlrest grammar](https://github.com/ontodev/tree-sitter-sqlrest) (see Cargo.toml
/// for the specific version used), and return it.
pub fn parse(url: &str) -> Result<Select, String> {
    let url = {
        match decode(url) {
            Ok(url) => url,
            Err(e) => return Err(e.to_string()),
        }
    };
    let url = url.to_string();

    let mut parser: Parser = Parser::new();
    parser
        .set_language(tree_sitter_sqlrest::language())
        .expect("Error loading sqlrest grammar");
    let tree = parser.parse(&url, None);
    match tree {
        Some(tree) => {
            let mut query_result: Result<Select, String> = Ok(Select::new(""));
            transduce(&tree.root_node(), &url, &mut query_result);
            query_result
        }
        None => Err(format!("Unable to parse tree from URL: {}", url)),
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce the select statement indicated by the node and raw string into the Select struct.
pub fn transduce(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match n.kind() {
        "query" => transduce_children(n, raw, query_result),
        "select" => transduce_select(n, raw, query_result),
        "table" => transduce_table(n, raw, query_result),
        "expression" => transduce_children(n, raw, query_result),
        "part" => transduce_children(n, raw, query_result),
        "filter" => transduce_children(n, raw, query_result),
        "simple_filter" => transduce_filter(n, raw, query_result),
        "special_filter" => transduce_children(n, raw, query_result),
        "in" => transduce_in(n, raw, query_result, false),
        "not_in" => transduce_in(n, raw, query_result, true),
        "order" => transduce_order(n, raw, query_result),
        "limit" => transduce_limit(n, raw, query_result),
        "offset" => transduce_offset(n, raw, query_result),
        "STRING" => *query_result = Err("Encountered STRING in top level translation".to_string()),
        _ => {
            *query_result = Err(format!(
                "Error parsing node of kind '{}': {:?} {} {:?}",
                n.kind(),
                n,
                raw,
                query_result
            ))
        }
    }
}

fn is_error(n: &Node) -> bool {
    n.kind().to_lowercase() == "error"
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// iterate over the node's child nodes and transduce them into the Select struct.
pub fn transduce_children(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match query_result {
        Err(_) => return,
        Ok(_) => {
            let child_count = n.named_child_count();
            for i in 0..child_count {
                match n.named_child(i) {
                    Some(named_child) if !is_error(&named_child) => {
                        transduce(&named_child, raw, query_result)
                    }
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract named child #{} from Node {:?}",
                            i, n
                        ));
                        return;
                    }
                };
            }
        }
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce the table name indicated by the node into the Select struct.
pub fn transduce_table(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match query_result {
        Err(_) => return,
        Ok(query) => match n.named_child(0) {
            Some(child) if !is_error(&child) => match decode(&get_from_raw(&child, raw)) {
                Ok(table) => {
                    query.table(format!("\"{}\"", table));
                }
                Err(e) => *query_result = Err(e.to_string()),
            },
            _ => {
                *query_result = Err(format!(
                    "Unable to extract 0th named child from Node: {:?}",
                    n
                ))
            }
        },
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce the filter indicated by the node into the Select struct.
pub fn transduce_filter(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match query_result {
        Err(_) => return,
        Ok(query) => {
            let column = {
                match n.named_child(0) {
                    Some(child) if !is_error(&child) => match decode(&get_from_raw(&child, raw)) {
                        Ok(column) => format!("\"{}\"", column),
                        Err(e) => {
                            *query_result = Err(e.to_string());
                            return;
                        }
                    },
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract 0th named child from Node: {:?}",
                            n
                        ));
                        return;
                    }
                }
            };
            let operator_string = {
                match n.named_child(1) {
                    Some(child) if !is_error(&child) => get_from_raw(&child, raw),
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract 1st named child from Node {:?}",
                            n
                        ));
                        return;
                    }
                }
            };

            let value_node = {
                match n.named_child(2) {
                    Some(value_node) if !is_error(&value_node) => value_node,
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract 2nd named child from Node {:?}",
                            n
                        ));
                        return;
                    }
                }
            };
            let value = {
                if value_node.kind() != "normal_value" && value_node.kind() != "like_value" {
                    *query_result = Err(format!("Unexpected Node kind: {}", value_node.kind()));
                    return;
                } else {
                    let value = get_from_raw(&value_node, raw);
                    match decode(&value) {
                        Err(e) => {
                            *query_result = Err(e.to_string());
                            return;
                        }
                        Ok(value) => match value.parse::<i64>() {
                            Ok(v) => json!(v),
                            Err(_) => match value.parse::<f64>() {
                                Ok(v) => json!(v),
                                Err(_) => {
                                    if value.to_lowercase() == "null" {
                                        json!(value)
                                    } else {
                                        let unquoted_value =
                                            unquote(&value).unwrap_or(value.to_string());
                                        let unquoted_value = {
                                            if value_node.kind() == "like_value" {
                                                // '*' in a URL is interpreted as '%' for LIKE:
                                                unquoted_value.replace("*", "%")
                                            } else {
                                                unquoted_value
                                            }
                                        };
                                        json!(format!("'{}'", unquoted_value))
                                    }
                                }
                            },
                        },
                    }
                }
            };
            match Filter::new(column, operator_string, value) {
                Ok(filter) => query.add_filter(filter),
                Err(e) => {
                    *query_result = Err(e.to_string());
                    return;
                }
            };
        }
    }
}

/// Given a tree-sitter node and a raw string, transduce the list indicated by the node into a
/// vector of strings.
pub fn transduce_list(n: &Node, raw: &str) -> Result<Vec<String>, String> {
    if n.kind() != "list" {
        return Err(format!(
            "Node: '{:?}' of kind '{}' is not a list",
            n,
            n.kind()
        ));
    }
    let mut vec = Vec::new();
    let child_count = n.named_child_count();
    for i in 0..child_count {
        match n.named_child(i) {
            None => {
                return Err(format!(
                    "Unable to extract named child #{} from Node {:?}",
                    i, n
                ))
            }
            Some(child) => match decode(&get_from_raw(&child, raw)) {
                Err(e) => return Err(e.to_string()),
                Ok(value) => vec.push(value.into_owned()),
            },
        }
    }
    Ok(vec)
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce the list of strings indicated by the node into an "IN" clause (or a "NOT IN" clause if
/// `negate` is set to true) for the Select struct.
pub fn transduce_in(n: &Node, raw: &str, query_result: &mut Result<Select, String>, negate: bool) {
    match query_result {
        Err(_) => return,
        Ok(query) => {
            let column = {
                match n.named_child(0) {
                    Some(child) if !is_error(&child) => match decode(&get_from_raw(&child, raw)) {
                        Ok(column) => format!("\"{}\"", column),
                        Err(e) => {
                            *query_result = Err(e.to_string());
                            return;
                        }
                    },
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract 0th named child from Node: {:?}",
                            n
                        ));
                        return;
                    }
                }
            };
            let values = {
                match n.named_child(1) {
                    Some(child) if !is_error(&child) => match transduce_list(&child, raw) {
                        Ok(values) => values,
                        Err(e) => {
                            *query_result = Err(e.to_string());
                            return;
                        }
                    },
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract 1st named child from Node: {:?}",
                            n
                        ));
                        return;
                    }
                }
            };
            let mut choices = vec![];
            for value in values {
                match value.parse::<i64>() {
                    Ok(v) => choices.push(json!(v)),
                    Err(_) => match value.parse::<f64>() {
                        Ok(v) => choices.push(json!(v)),
                        Err(_) => {
                            if value.to_lowercase() == "null" {
                                choices.push(json!(value));
                            } else {
                                let unquoted_value = unquote(&value).unwrap_or(value.to_string());
                                choices.push(json!(format!("'{}'", unquoted_value)));
                            }
                        }
                    },
                }
            }
            let operator_str = if negate {
                String::from("not_in")
            } else {
                String::from("in")
            };
            match Filter::new(column, operator_str, SerdeValue::Array(choices)) {
                Ok(filter) => query.add_filter(filter),
                Err(e) => {
                    *query_result = Err(e.to_string());
                    return;
                }
            };
        }
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce the list of strings indicated by the node into a "SELECT" clause for the Select
/// struct.
pub fn transduce_select(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    // Helper function to extract the column and alias from the given node and string:
    fn extract_column_qualifiers(
        n: &Node,
        raw: &str,
    ) -> Result<(String, Option<String>, Option<String>), String> {
        fn get_field(n: &Option<Node>, raw: &str) -> Result<String, String> {
            match n {
                Some(field) if !is_error(&field) => match decode(&get_from_raw(&field, raw)) {
                    Ok(field) => Ok(field.to_string()),
                    Err(e) => Err(e.to_string()),
                },
                _ => Err(format!("Unable to extract column from Node: {:?}", n)),
            }
        }

        let child_count = n.named_child_count();
        if child_count == 1 {
            let column = match get_field(&n.named_child(0), raw) {
                Ok(c) => c,
                Err(e) => return Err(e),
            };
            Ok((column, None, None))
        } else if child_count == 2 {
            let first_node = match n.named_child(0) {
                Some(n) if !is_error(&n) => n,
                _ => {
                    return Err(format!(
                        "Unable to extract 0th named child from node: {:?}",
                        n
                    ))
                }
            };
            if first_node.kind() == "column" {
                let column = match get_field(&n.named_child(0), raw) {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };
                let cast = match get_field(&n.named_child(1), raw) {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };
                Ok((column, None, Some(cast)))
            } else {
                let alias = match get_field(&n.named_child(0), raw) {
                    Ok(a) => a,
                    Err(e) => return Err(e),
                };
                let column = match get_field(&n.named_child(1), raw) {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };
                Ok((column, Some(alias), None))
            }
        } else if child_count == 3 {
            let alias = match get_field(&n.named_child(0), raw) {
                Ok(a) => a,
                Err(e) => return Err(e),
            };
            let column = match get_field(&n.named_child(1), raw) {
                Ok(c) => c,
                Err(e) => return Err(e),
            };
            let cast = match get_field(&n.named_child(2), raw) {
                Ok(c) => c,
                Err(e) => return Err(e),
            };
            Ok((column, Some(alias), Some(cast)))
        } else {
            return Err("Number of child nodes in node kind: {} should be 1 or 2.".to_string());
        }
    }

    match query_result {
        Err(_) => return,
        Ok(query) => {
            let child_count = n.named_child_count();
            for i in 0..child_count {
                let (column, alias, cast) = {
                    match n.named_child(i) {
                        Some(child) if !is_error(&child) => {
                            match decode(&get_from_raw(&child, raw)) {
                                Ok(_) => match extract_column_qualifiers(&child, raw) {
                                    Ok((column, alias, cast)) => (column, alias, cast),
                                    Err(e) => {
                                        *query_result = Err(e.to_string());
                                        return;
                                    }
                                },
                                Err(e) => {
                                    *query_result = Err(e.to_string());
                                    return;
                                }
                            }
                        }
                        _ => {
                            *query_result = Err(format!(
                                "Unable to extract named child #{} from Node: {:?}",
                                i, n
                            ));
                            return;
                        }
                    }
                };
                let mut select_column = SelectColumn::new(format!("\"{}\"", column), None, None);
                if let Some(alias) = alias {
                    select_column.alias = Some(format!("\"{}\"", alias));
                }
                if let Some(cast) = cast {
                    select_column.cast = Some(cast);
                }
                query.add_explicit_select(&select_column);
            }
        }
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce the list of strings indicated by the node into an "ORDER BY" clause for the Select
/// struct.
pub fn transduce_order(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match query_result {
        Err(_) => return,
        Ok(query) => {
            let child_count = n.named_child_count();
            let mut position = 0;
            while position < child_count {
                let named_child = match n.named_child(position) {
                    Some(named_child) if !is_error(&named_child) => named_child,
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract named child #{} from Node: {:?}",
                            position, n
                        ));
                        return;
                    }
                };

                let column = get_from_raw(&named_child, raw);
                let column = match decode(&column) {
                    Err(e) => {
                        *query_result = Err(e.to_string());
                        return;
                    }
                    Ok(column) => column,
                };

                let default_order =
                    OrderByColumn::new(format!("\"{}\"", column), &Direction::Ascending);
                position = position + 1;
                if position >= child_count {
                    query.add_explicit_order_by(&default_order);
                    return;
                }

                let named_child = match n.named_child(position) {
                    Some(named_child) if !is_error(&named_child) => named_child,
                    _ => {
                        *query_result = Err(format!(
                            "Unable to extract named child #{} from Node: {:?}",
                            position, n
                        ));
                        return;
                    }
                };

                if !named_child.kind().eq("ordering") {
                    query.add_explicit_order_by(&default_order);
                    return;
                }

                let ordering_string = get_from_raw(&named_child, raw);
                let ordering = Direction::from_str(&ordering_string);
                match ordering {
                    Ok(o) => {
                        position = position + 1;
                        let order = OrderByColumn::new(format!("\"{}\"", column), &o);
                        query.add_explicit_order_by(&order);
                    }
                    Err(e) => {
                        *query_result = Err(e.to_string());
                        return;
                    }
                };
            }
        }
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce integer indicated by the node into an "OFFSET" clause for the Select
/// struct.
pub fn transduce_offset(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match query_result {
        Err(_) => return,
        Ok(query) => {
            let offset_str = match n.named_child(0) {
                Some(named_child) if !is_error(&named_child) => get_from_raw(&named_child, raw),
                _ => {
                    *query_result = Err(format!(
                        "Unable to extract 0th named child from Node: {:?}",
                        n
                    ));
                    return;
                }
            };
            let offset: usize = match offset_str.parse() {
                Err(_) => {
                    *query_result = Err(format!("Unable to parse '{}' as an offset.", offset_str));
                    return;
                }
                Ok(offset) => offset,
            };
            query.offset(offset);
        }
    }
}

/// Given a tree-sitter node, a raw string, and a mutable Select struct wrapped in a Result enum,
/// transduce integer indicated by the node into an "LIMIT" clause for the Select
/// struct.
pub fn transduce_limit(n: &Node, raw: &str, query_result: &mut Result<Select, String>) {
    match query_result {
        Err(_) => return,
        Ok(query) => {
            let limit_str = match n.named_child(0) {
                Some(named_child) if !is_error(&named_child) => get_from_raw(&named_child, raw),
                _ => {
                    *query_result = Err(format!(
                        "Unable to extract 0th named child from Node: {:?}",
                        n
                    ));
                    return;
                }
            };
            let limit: usize = match limit_str.parse() {
                Err(_) => {
                    *query_result = Err(format!("Unable to parse '{}' as an limit.", limit_str));
                    return;
                }
                Ok(limit) => limit,
            };
            query.limit(limit);
        }
    }
}

/// Given a database type and two Select structs, generate an SQL statement such that the
/// first Select struct is interpreted as a simple CTE, and the second Select struct is
/// interpreted as the main query.
pub fn selects_to_sql(
    select1: &Select,
    select2: &Select,
    dbtype: &DbType,
) -> Result<String, String> {
    match select1.to_sql(dbtype) {
        Err(e) => Err(e),
        Ok(sql1) => match select2.to_sql(dbtype) {
            Err(e) => Err(e),
            Ok(sql2) => Ok(format!("WITH {} AS ({}) {}", select2.table, sql1, sql2)),
        },
    }
}

/// Given two Select structs, a database connection pool, and a parameter map: Generate a SQL
/// statement such that the first Select struct is interpreted as a simple CTE, and the second
/// Select struct is interpreted as the main query; then bind the SQL to the parameter map,
/// execute the resulting query against the database, and return the resulting rows.
pub fn fetch_rows_from_selects(
    select1: &Select,
    select2: &Select,
    pool: &AnyPool,
    param_map: &HashMap<&str, SerdeValue>,
) -> Result<Vec<AnyRow>, String> {
    let dbtype = get_db_type(&pool);
    let sql = match dbtype {
        Err(e) => return Err(e),
        Ok(dbtype) => match selects_to_sql(select1, select2, &dbtype) {
            Err(e) => return Err(e),
            Ok(sql) => sql,
        },
    };
    let (sql, params) = match bind_sql(pool, sql, param_map) {
        Err(e) => return Err(e),
        Ok((sql, params)) => (sql, params),
    };

    let result = match construct_query(&sql, &params) {
        Err(e) => Err(e),
        Ok(query) => match block_on(query.fetch_all(pool)) {
            Err(e) => Err(format!("{}", e)),
            Ok(k) => Ok(k),
        },
    };
    result
}

/// Given a JSON-formatted string representing an array of objects such that each object represents
/// a row, unwrap the objects and add them to a vector which is then returned to the caller.
fn extract_rows_from_json_str(json_row: &str) -> Result<Vec<SerdeMap<String, SerdeValue>>, String> {
    let mut rows = vec![];
    match serde_json::from_str::<SerdeValue>(json_row) {
        Err(e) => return Err(e.to_string()),
        Ok(json_row) => match json_row {
            SerdeValue::Array(json_row) => {
                for json_cell in json_row {
                    match json_cell {
                        SerdeValue::Object(json_cell) => rows.push(json_cell),
                        _ => return Err(format!("Expected object. Got: {}", json_cell)),
                    };
                }
            }
            _ => return Err(format!("Expected array. Got: {}", json_row)),
        },
    };
    Ok(rows)
}

/// Given two Select structs, a database connection pool, and a parameter map: Generate a SQL
/// statement such that the first Select struct is interpreted as a simple CTE, and the second
/// Select struct is interpreted as the main query; then bind the SQL to the parameter map,
/// execute the resulting query against the database, and return the resulting rows as a JSON
/// (i.e., as a SerdeValue).
pub fn fetch_rows_as_json_from_selects(
    select1: &Select,
    select2: &Select,
    pool: &AnyPool,
    param_map: &HashMap<&str, SerdeValue>,
) -> Result<Vec<SerdeMap<String, SerdeValue>>, String> {
    // Construct the SQL:
    let dbtype = get_db_type(&pool);
    let sql = match dbtype {
        Err(e) => return Err(e),
        Ok(dbtype) => match selects_to_sql(select1, select2, &dbtype) {
            Err(e) => return Err(e),
            Ok(sql) => {
                if dbtype == DbType::Sqlite {
                    let mut json_keys = vec![];
                    // Casting is irrelevant here. Any casting should be taken account of by the
                    // selects_to_sql() function.
                    for select_column in &select2.select {
                        let column = &select_column.expression;
                        let alias = &select_column.alias;
                        let unquoted_column = match alias {
                            Some(alias) => alias.to_string(),
                            None => unquote(&column).unwrap_or(column.to_string()),
                        };
                        json_keys.push(format!(r#"'{}', "{}""#, unquoted_column, unquoted_column));
                    }
                    let json_select = json_keys.join(", ");
                    format!(
                        "SELECT JSON_GROUP_ARRAY(JSON_OBJECT({})) AS row FROM ({})",
                        json_select, sql
                    )
                } else {
                    format!("SELECT JSON_AGG(t)::TEXT AS row FROM ({}) t", sql)
                }
            }
        },
    };

    // Prepare the query using the given parameter map::
    let (sql, params) = match bind_sql(pool, sql, param_map) {
        Err(e) => return Err(e),
        Ok((sql, params)) => (sql, params),
    };

    let result = match construct_query(&sql, &params) {
        Err(e) => Err(e),
        Ok(query) => match block_on(query.fetch_all(pool)) {
            Err(e) => Err(format!("{}", e)),
            Ok(rows) if rows.len() != 1 => Err(format!(
                "In fetch_rows_as_json(), expected 1 row, got {}",
                rows.len()
            )),
            Ok(mut rows) => {
                let row = rows.pop().unwrap();
                match row.try_get("row") {
                    Err(e) => Err(format!("{}", e)),
                    Ok(json_row) => extract_rows_from_json_str(json_row),
                }
            }
        },
    };
    result
}

/// Given a database pool and a SQL string containing a number of placeholders, `{key}`, where `key`
/// corresponds to one of the keys in the given parameter map, return a string and a vector of
/// SqlParams, where every placeholder `{key}` in the original SQL string is replaced in the
/// returned string by the placeholder corresponding to the type of the database pool.
/// For example, given a string: "SELECT column FROM table WHERE column IN ({beta}, {alpha})", and
/// given the parameter map: {"alpha": SqlParam::String("green"), "beta": SqlParam::String("red")},
/// then in the case of a Sqlite database, this function will return the String
/// "SELECT column FROM table WHERE column IN (?, ?)" along with the Vector
/// [SqlParam::String("red"), SqlParam::String("green")]. In the case of a Postgres database,
/// the returned vector will be the same but the string will be
/// "SELECT column FROM table WHERE column IN ($1, $2)".
pub fn bind_sql<'a, S: Into<String>>(
    pool: &AnyPool,
    sql: S,
    param_map: &'a HashMap<&str, SerdeValue>,
) -> Result<(String, Vec<&'a SerdeValue>), String> {
    let sql = sql.into();
    // This regex will find quoted strings as well as variables of the form `{key}` where `key` is
    // a token consisting of word characters and/or underscores:
    let rx = Regex::new(r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")|\B\{[\w_]+\}\B"#)
        .unwrap();

    // The variables that will be returned:
    let mut param_vec = vec![];
    let mut final_sql = String::from("");

    let mut pg_param_idx = 1;
    let mut saved_start = 0;
    for m in rx.find_iter(&sql) {
        let this_match = &sql[m.start()..m.end()];
        final_sql.push_str(&sql[saved_start..m.start()]);
        if (this_match.starts_with("\"") && this_match.ends_with("\""))
            || (this_match.starts_with("'") && this_match.ends_with("'"))
        {
            final_sql.push_str(this_match);
        } else {
            // Remove the opening and closing braces from the placeholder, `{key}`:
            let key = match this_match
                .strip_prefix("{")
                .and_then(|s| s.strip_suffix("}"))
            {
                None => return Err(format!("'{}' is not enclosed in curly braces.", this_match)),
                Some(k) => k,
            };

            match param_map.get(key) {
                None => return Err(format!("Key '{}' not found in parameter map", key)),
                Some(param) => {
                    // Add the corresponding parameter from the parameter map to the vector that
                    // will be returned:
                    param_vec.push(param);
                    if pool.any_kind() == AnyKind::Postgres {
                        final_sql.push_str(&format!("${}", pg_param_idx));
                        pg_param_idx += 1;
                    } else {
                        final_sql.push_str(&format!("?"));
                    }
                }
            }
        }
        saved_start = m.start() + this_match.len();
    }
    final_sql.push_str(&sql[saved_start..]);
    Ok((final_sql, param_vec))
}

/// Given a SQL string, possibly with unbound parameters represented by the placeholder string
/// sql_param, and given a database pool, if the pool is of type Sqlite, then change the syntax used
/// for unbound parameters to Sqlite syntax, which uses "?", otherwise use Postgres syntax, which
/// uses numbered parameters, i.e., $1, $2, ...
/// Note that SQL_PARAM must be a 'word' (in the regular expression sense)
pub fn local_sql_syntax<S: Into<String>>(pool: &AnyPool, sql_param: S, sql: S) -> String {
    let sql_param = sql_param.into();
    let sql = sql.into();
    // The reason that SQL_PARAM must be a word is that below we are matchng against it using '\b'
    // which represents a word boundary. If you want to use a non-word placeholder then you must
    // also change '\b' in the regex below to '\B'.
    //
    // Do not replace instances of sql_param if they are within quotation marks:
    let rx = Regex::new(&format!(
        r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")|\b{}\b"#,
        sql_param
    ))
    .unwrap();

    let mut final_sql = String::from("");
    let mut pg_param_idx = 1;
    let mut saved_start = 0;
    for m in rx.find_iter(&sql) {
        let this_match = &sql[m.start()..m.end()];
        final_sql.push_str(&sql[saved_start..m.start()]);
        if this_match == sql_param {
            if pool.any_kind() == AnyKind::Postgres {
                final_sql.push_str(&format!("${}", pg_param_idx));
                pg_param_idx += 1;
            } else {
                final_sql.push_str(&format!("?"));
            }
        } else {
            final_sql.push_str(&format!("{}", this_match));
        }
        saved_start = m.start() + this_match.len();
    }
    final_sql.push_str(&sql[saved_start..]);
    final_sql
}

/// Given a database pool, a SQL string with placeholders representing bound parameters, a vector
/// with the parameters corresponding to each placeholder, and (optionally) the string (which must
/// be a word in the regex-sense) to use to identify the placeholders in the SQL string, combine
/// the information into an interpolated string that is then returned. If `placeholder_str` is not
/// None, then use it to explicitly identify placeholders in `sql`. Otherwise, use `pool` to
/// determine the placeholder syntax to use for the specific kind of database the SQL is intended
/// for. VALVE currently supports:
/// (1) Postgres, which uses numbered variables $N, e.g., SELECT 1 FROM foo WHERE bar IN ($1, $2)
/// (2) Sqlite, which uses question marks, e.g., SELECT 1 FROM foo WHERE bar IN (?, ?)
pub fn interpolate_sql<S: Into<String>>(
    pool: &AnyPool,
    sql: S,
    params: &Vec<&SerdeValue>,
    placeholder_str: Option<S>,
) -> Result<String, String> {
    let sql = sql.into();
    let mut final_sql = String::from("");
    let mut saved_start = 0;

    let quotes = r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")"#;
    let rx = {
        if let Some(s) = placeholder_str {
            match Regex::new(&format!(r#"{}|\b{}\b"#, quotes, s.into())) {
                Err(e) => return Err(e.to_string()),
                Ok(r) => r,
            }
        } else if pool.any_kind() == AnyKind::Postgres {
            match Regex::new(&format!(r#"{}|\B[$]\d+\b"#, quotes)) {
                Err(e) => return Err(e.to_string()),
                Ok(r) => r,
            }
        } else {
            match Regex::new(&format!(r#"{}|\B[?]\B"#, quotes)) {
                Err(e) => return Err(e.to_string()),
                Ok(r) => r,
            }
        }
    };

    let mut param_index = 0;
    for m in rx.find_iter(&sql) {
        let this_match = &sql[m.start()..m.end()];
        final_sql.push_str(&sql[saved_start..m.start()]);
        if !((this_match.starts_with("\"") && this_match.ends_with("\""))
            || (this_match.starts_with("'") && this_match.ends_with("'")))
        {
            let param = params.get(param_index);
            match param {
                None => return Err(format!("No parameter at index {}", param_index)),
                Some(param) => {
                    match param {
                        SerdeValue::String(param) => final_sql.push_str(&format!("'{}'", param)),
                        SerdeValue::Number(param) => final_sql.push_str(&format!("{}", param)),
                        _ => return Err(format!("{} is not a string or a number.", param)),
                    };
                }
            };
            param_index += 1;
        } else {
            final_sql.push_str(&format!("{}", this_match));
        }
        saved_start = m.start() + this_match.len();
    }
    final_sql.push_str(&sql[saved_start..]);
    Ok(final_sql)
}

// Helper function to determine whether the given name is 'simple', i.e., such as to match
// the DB_OBJECT_REGEX defined above.
fn is_simple(db_object_name: &str) -> Result<(), String> {
    let db_object_root = db_object_name.splitn(2, ".").collect::<Vec<_>>()[0];
    if !DB_OBJECT_REGEX.is_match(&db_object_root) {
        Err(format!(
            "Illegal database object name: '{}' in '{}'. All names must match: '{}' for to_url().",
            db_object_root, db_object_name, DB_OBJECT_MATCH_STR,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;
    use sqlx::{
        any::{AnyConnectOptions, AnyPoolOptions},
        query as sqlx_query, Row, ValueRef,
    };
    use std::{collections::HashMap, str::FromStr};

    #[test]
    #[serial]
    fn test_real_datatype() {
        let sq_connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let sqlite_pool = block_on(
            AnyPoolOptions::new()
                .max_connections(5)
                .connect_with(sq_connection_options),
        )
        .unwrap();
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let postgresql_pool = block_on(
            AnyPoolOptions::new()
                .max_connections(5)
                .connect_with(pg_connection_options),
        )
        .unwrap();

        for pool in &vec![sqlite_pool, postgresql_pool] {
            let drop = r#"DROP TABLE IF EXISTS "my_table_with_reals""#;
            let create = r#"CREATE TABLE "my_table_with_reals" (
                          "row_number" BIGINT,
                          "column_1" TEXT,
                          "column_2" TEXT,
                          "column_3" REAL
                        )"#;
            let insert = r#"INSERT INTO "my_table_with_reals" VALUES
                        (1, 'one', 'eins', 1.1),
                        (2, 'two', 'zwei', 2.2),
                        (3, 'three', 'drei', 3.3)"#;

            for sql in &vec![drop, create, &insert] {
                let query = sqlx_query(sql);
                block_on(query.execute(pool)).unwrap();
            }

            let mut select = Select::new("my_table_with_reals");
            select
                .select_all(pool)
                .unwrap()
                .add_filter(Filter::new("column_3", "gt", json!(3.2)).unwrap());

            let mut rows = select.fetch_rows(pool, &HashMap::new()).unwrap();
            assert_eq!(rows.len(), 1);
            let row = rows.pop().unwrap();
            let column_3: f32 = row.try_get("column_3").unwrap();
            assert_eq!(column_3, 3.3);

            let mut rows = select.fetch_rows_as_json(pool, &HashMap::new()).unwrap();
            assert_eq!(rows.len(), 1);
            let row = rows.pop().unwrap();
            let column_3 = row.get("column_3").and_then(|c| c.as_f64()).unwrap();
            assert_eq!(column_3, 3.3);
        }
    }

    #[test]
    #[serial]
    fn test_json_datatype() {
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool = block_on(
            AnyPoolOptions::new()
                .max_connections(5)
                .connect_with(pg_connection_options),
        )
        .unwrap();

        let drop = r#"DROP TABLE IF EXISTS "my_table_with_json""#;
        let create = r#"CREATE TABLE "my_table_with_json" (
                          "row_number" BIGINT,
                          "column_1" TEXT,
                          "column_2" TEXT,
                          "column_3" JSON
                        )"#;
        let insert = r#"INSERT INTO "my_table_with_json" VALUES
                        (1, 'one', 'eins', '1'::JSON),
                        (2, 'two', 'zwei', '"dos"'::JSON),
                        (3, 'three', 'drei', '{"fookey": "foovalue"}'::JSON)"#;

        for sql in &vec![drop, create, &insert] {
            let query = sqlx_query(sql);
            block_on(query.execute(&pool)).unwrap();
        }

        let mut select = Select::new("my_table_with_json");
        select
            .select_all(&pool)
            .unwrap()
            .add_filter(Filter::new("row_number", "gt", json!(2)).unwrap());
        let mut rows = select.fetch_rows(&pool, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        let row = rows.pop().unwrap();
        let column_1: &str = row.try_get("column_1").unwrap();
        let column_2: &str = row.try_get("column_2").unwrap();
        let column_3 = match row.try_get_raw("column_3") {
            Err(e) => panic!("{}", e),
            Ok(column_3) => {
                if column_3.is_null() {
                    panic!("Expected a JSON but got NULL for 'row' field.");
                } else {
                    match serde_json::from_str::<SerdeValue>(row.get_unchecked("column_3")) {
                        Err(e) => panic!("{}", e),
                        Ok(column_3) => {
                            format!("{}", column_3)
                        }
                    }
                }
            }
        };
        assert_eq!(column_1, "three");
        assert_eq!(column_2, "drei");
        assert_eq!(column_3, r#"{"fookey":"foovalue"}"#);

        let mut rows = select.fetch_rows_as_json(&pool, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        let row = rows.pop().unwrap();
        let fooval = row
            .get("column_3")
            .and_then(|c| c.as_object())
            .and_then(|c| c.get("fookey"))
            .unwrap();
        match fooval {
            SerdeValue::String(s) => assert_eq!(s, "foovalue"),
            _ => panic!("'{}' does not match 'foovalue'", fooval),
        };
    }

    #[test]
    #[should_panic]
    fn test_simple_ddl() {
        let mut select = Select::new("my_table");
        select.add_select("COUNT(1)");
        if let Ok(_) = select.to_url() {
            return;
        }
        select.select.clear();

        select.add_filter(Filter::new("Uri$Mayberry", "lt", json!(0)).unwrap());
        if let Ok(_) = select.to_url() {
            return;
        }
        select.filter.clear();

        select.add_explicit_order_by(&OrderByColumn::new("Kluge$foo", &Direction::Ascending));
        if let Ok(_) = select.to_url() {
            return;
        }
        select.order_by.clear();

        select.table("My^Flurb");
        select.to_url().unwrap();
    }

    #[test]
    fn test_count() {
        let select = Select::new("my_table");
        let sql = select.to_sql_count(&DbType::Sqlite).unwrap();
        assert_eq!(
            sql,
            "SELECT COUNT(1) AS count FROM (SELECT * FROM my_table) AS t"
        );
    }

    #[test]
    fn test_json_fetch_error() {
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool = block_on(
            AnyPoolOptions::new()
                .max_connections(5)
                .connect_with(pg_connection_options),
        )
        .unwrap();

        let select = Select::new("nonexistent_table");
        let expected_json = "{\"status\":400,\"error\":\"error returned from database: relation \
                             \\\"nonexistent_table\\\" does not exist\"}";
        match select.fetch_as_json(&pool, &HashMap::new()) {
            Err(json) => assert_eq!(json!(json).to_string(), expected_json),
            Ok(json) => panic!(
                "Got successful response: {} but was expecting the error: {}",
                json!(json).to_string(),
                expected_json
            ),
        }
    }

    #[test]
    #[serial]
    fn test_count_strategies() {
        // Setup database connections and create the needed tables:
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool = block_on(
            AnyPoolOptions::new()
                .max_connections(5)
                .connect_with(pg_connection_options),
        )
        .unwrap();
        let drop_table1 = r#"DROP TABLE IF EXISTS "my_table""#;
        let create_table1 = r#"CREATE TABLE "my_table" (
                                 "row_number" BIGINT,
                                 "prefix" TEXT,
                                 "base" TEXT,
                                 "ontology IRI" TEXT,
                                 "version IRI" TEXT
                               )"#;
        let insert_table1 = r#"INSERT INTO "my_table" VALUES
                                (1, 'p1', 'b1', 'o1', 'v1'),
                                (2, 'p2', 'b2', 'o2', 'v2'),
                                (3, 'p3', 'b3', 'o3', 'v3'),
                                (4, 'p4', 'b4', 'o1', 'v4')"#;
        let drop_table2 = r#"DROP TABLE IF EXISTS "a table name with spaces""#;
        let create_table2 = r#"CREATE TABLE "a table name with spaces" (
                                 "foo" TEXT,
                                 "a column name with spaces" TEXT,
                                 "bar" TEXT
                               )"#;
        let insert_table2 = r#"INSERT INTO "a table name with spaces" VALUES
                                ('f1', 's1', 'b1'),
                                ('f2', 's2', 'b2'),
                                ('f3', 's3', 'b3'),
                                ('f4', 's4', 'b4'),
                                ('f5', 's5', 'b5'),
                                ('f6', 's6', 'b6')"#;
        for sql in &vec![
            drop_table1,
            create_table1,
            insert_table1,
            drop_table2,
            create_table2,
            insert_table2,
        ] {
            let query = sqlx_query(sql);
            block_on(query.execute(&pool)).unwrap();
        }

        // Run ANALYZE to bring the db statistics up to date:
        let query = sqlx_query("ANALYZE");
        block_on(query.execute(&pool)).unwrap();

        let select = Select::new("my_table");

        let result_map = select
            .fetch_as_json_with_count_strategy(&pool, &HashMap::new(), &CountStrategy::Exact)
            .unwrap();
        let row_count = result_map.get("count").unwrap();
        assert_eq!(row_count.as_u64().unwrap(), 4);

        let result_map = select
            .fetch_as_json_with_count_strategy(&pool, &HashMap::new(), &CountStrategy::Planned)
            .unwrap();
        let row_count = result_map.get("count").unwrap();
        assert_eq!(row_count.as_u64().unwrap(), 4);

        let result_map = select
            .fetch_as_json_with_count_strategy(&pool, &HashMap::new(), &CountStrategy::Estimated)
            .unwrap();
        let row_count = result_map.get("count").unwrap();
        assert_eq!(row_count.as_u64().unwrap(), 4);

        let result_map = select
            .fetch_as_json_with_count_strategy(&pool, &HashMap::new(), &CountStrategy::Window)
            .unwrap();
        let row_count = result_map.get("count").unwrap();
        assert_eq!(row_count.as_u64().unwrap(), 4);
    }

    #[test]
    #[should_panic]
    fn test_false_star() {
        // Wildcards ('*') are only allowed in LIKE clauses, so this should fail:
        let from_url = "bar?foo=eq.*yogi*";
        parse(from_url).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bad_select() {
        // Parentheses are not allowed in select clauses:
        let from_url = "penguin?select=Delta 15 N (o/oo)";
        parse(from_url).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bad_order_by() {
        // Parentheses are not allowed in order by clauses:
        let from_url = "penguin?order=Delta 15 N (o/ooo).desc";
        parse(from_url).unwrap();
    }
}
