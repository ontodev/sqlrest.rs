//! <!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
//!      in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
//!      `cargo readme > README.md` -->
//!
//! ## Working with Select structs
//! ```rust
//! use ontodev_sqlrest::{
//!     bind_sql, get_db_type, fetch_rows_from_selects, fetch_rows_as_json_from_selects,
//!     interpolate_sql, local_sql_syntax, DbType, Direction, Filter, Select, selects_to_sql,
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
//! let mut test_query = sqlx_query(&bound_sql);
//! for param in &test_params {
//!     match param {
//!         SerdeValue::String(s) => test_query = test_query.bind(s),
//!         SerdeValue::Number(n) => test_query = test_query.bind(n.as_i64()),
//!         _ => panic!("{} is not a string or a number.", param),
//!     };
//! }
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
//! select.aliased_select(vec![("foo", "foo"), (r#""a column name with spaces""#, "C")]);
//! select.add_select("bar");
//! select.add_aliased_select("COUNT(1)", "count");
//! select.filter(vec![Filter::new("foo", "is", json!("{foo}")).unwrap()]);
//! select.add_filter(Filter::new("bar", "in", json!(["{val1}", "{val2}"])).unwrap());
//! select.order_by(vec![("foo", Direction::Ascending), ("bar", Direction::Descending)]);
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
//!          ORDER BY foo ASC, bar DESC \
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
//!          ORDER BY foo ASC, bar DESC \
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
//!     let mut query = sqlx_query(&sql);
//!     for param in &params {
//!         match param {
//!             SerdeValue::String(s) => query = query.bind(s),
//!             SerdeValue::Number(n) => query = query.bind(n.as_i64()),
//!             _ => panic!("{} is not a string or a number.", param),
//!         };
//!     }
//!     block_on(query.execute(pool)).unwrap();
//! }
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
//!     .order_by(vec![("foo", Direction::Descending)]);
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
//!     .add_aliased_select("COUNT(1)", "count")
//!     .filter(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
//!     .order_by(vec![("foo", Direction::Ascending), ("bar", Direction::Descending)])
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
//!     .add_order_by(("prefix", Direction::Ascending));
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
//!     .add_order_by(("prefix", Direction::Ascending));
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
//! ```
//! ## Parsing Selects from URLs and vice versa.
//! ```rust
//! use ontodev_sqlrest::{parse, transduce};
//! use urlencoding::{decode, encode};
//!
//! // Some basic examples:
//! let from_url = "bar";
//! let expected_sql = "SELECT * FROM bar";
//! let select = parse(from_url);
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(from_url, select.to_url().unwrap());
//!
//! let from_url = "bar?select=foo,goo";
//! let expected_sql = "SELECT foo, goo FROM bar";
//! let select = parse(from_url);
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(encode(from_url), select.to_url().unwrap());
//!
//! // Table names with spaces can be specified in a URL using percent encoding, or explicitly
//! // using double quotes. In either case calling to_sql() will render the table name in double
//! // quotes, but double quotes will not be rendered upon calling to_url() unless they were
//! // included to begin with (either explicitly or using percent encoding) in the url from which
//! // the query was parsed.
//! let from_url = "a%20bar";
//! let expected_sql = "SELECT * FROM \"a bar\"";
//! let select = parse(from_url);
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(from_url, select.to_url().unwrap());
//!
//! let from_url = "\"a bar\"";
//! let expected_sql = "SELECT * FROM \"a bar\"";
//! let select = parse(from_url);
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(encode(from_url), select.to_url().unwrap());
//!
//! let from_url = "%22a%20bar%22";
//! let expected_sql = "SELECT * FROM \"a bar\"";
//! let select = parse(from_url);
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // Column names are handled similarly to table names:
//! let from_url = "bar?\
//!                 \"column 1\"=eq.5\
//!                 &\"column_2\"=eq.10\
//!                 &%22column%203%22=eq.30\
//!                 &column_4=eq.40";
//! let expected_sql = "SELECT * FROM bar \
//!                     WHERE \"column 1\" = 5 \
//!                     AND \"column_2\" = 10 \
//!                     AND \"column 3\" = 30 \
//!                     AND column_4 = 40";
//! let select = parse(from_url);
//! assert_eq!(expected_sql, select.to_sqlite().unwrap());
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//!
//! // Double quotes may be used when specifying literal string values. This is mandatory if
//! // a number is required to be interpreted as a string, e.g., 'foo=eq.\"10\"'. A second use
//! // case is when the literal value contains spaces, though alternatively percent encoding may
//! // be used instead. All literal string values will be rendered within single quotes in SQL.
//! // When converting the parsed Select struct back to a URL, these values will be enclosed in
//! // double-quotes in the URL.
//! let from_url = "bar?c1=eq.Henry%20Kissinger\
//!                 &c2=in.(Jim%20McMahon,\"William Perry\",\"72\",Nancy,NULL)\
//!                 &c3=eq.Fred";
//! let expected_sql = "SELECT * FROM bar WHERE c1 = 'Henry Kissinger' \
//!                     AND c2 IN ('Jim McMahon', 'William Perry', '72', 'Nancy', NULL) \
//!                     AND c3 = 'Fred'";
//! let expected_url = "bar?c1=eq.\"Henry Kissinger\"\
//!                     &c2=in.(\"Jim McMahon\",\"William Perry\",\"72\",\"Nancy\",NULL)\
//!                     &c3=eq.\"Fred\"";
//! let select = parse(&from_url);
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(expected_url, decode(&select.to_url().unwrap()).unwrap());
//!
//! // NULL values are treated differently from literal values (i.e., they are not rendered in
//! // quotes). Note also that they are not converted to uppercase in the generated SQL:
//! let from_url = "bar?select=c1,c2&c1=not_eq.null";
//! let expected_sql = "SELECT c1, c2 FROM bar WHERE c1 <> null";
//! let select = parse(&from_url);
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(from_url, decode(&select.to_url().unwrap()).unwrap());
//!
//! // More complicated example. Note that it is not necessary to use double quotes around literal
//! // strings that are not numbers and do not contain spaces, but we do so below so that the
//! // assert statement will pass, because the to_url() function will render all literal strings
//! // using double quotes.
//! let from_url = "a%20bar?\
//!      select=foo1,\"foo 2\",foo%205\
//!      &foo1=eq.0\
//!      &\"foo 2\"=not_eq.\"10\"\
//!      &foo3=lt.20\
//!      &foo4=gt.5\
//!      &foo%205=lte.30\
//!      &foo6=gte.60\
//!      &foo7=like.\"alpha\"\
//!      &foo8=not_like.\"abby normal\"\
//!      &foo9=ilike.\"beta\"\
//!      &foo10=not_ilike.\"gamma\"\
//!      &foo11=is.NULL\
//!      &foo12=not_is.NULL\
//!      &foo13=eq.\"terrible\"\
//!      &foo14=in.(\"A fancy hat\",\"5\",\"C page 21\",\"delicious\",NULL)\
//!      &foo15=not_in.(1,2,3)\
//!      &order=foo1.desc,\"foo 2\".asc,foo%205.desc\
//!      &limit=10\
//!      &offset=30";
//!
//! let expected_sql = "SELECT foo1, \"foo 2\", \"foo 5\" \
//!        FROM \"a bar\" \
//!        WHERE foo1 = 0 \
//!          AND \"foo 2\" <> '10' \
//!          AND foo3 < 20 \
//!          AND foo4 > 5 \
//!          AND \"foo 5\" <= 30 \
//!          AND foo6 >= 60 \
//!          AND foo7 LIKE 'alpha' \
//!          AND foo8 NOT LIKE 'abby normal' \
//!          AND foo9 ILIKE 'beta' \
//!          AND foo10 NOT ILIKE 'gamma' \
//!          AND foo11 IS NOT DISTINCT FROM NULL \
//!          AND foo12 IS DISTINCT FROM NULL \
//!          AND foo13 = 'terrible' \
//!          AND foo14 IN ('A fancy hat', '5', 'C page 21', 'delicious', NULL) \
//!          AND foo15 NOT IN (1, 2, 3) \
//!          ORDER BY foo1 DESC, \"foo 2\" ASC, \"foo 5\" DESC \
//!          LIMIT 10 \
//!          OFFSET 30";
//!
//! let select = parse(&from_url);
//! assert_eq!(expected_sql, select.to_postgres().unwrap());
//! assert_eq!(decode(&from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
//! ```

use enquote::unquote;
use futures::executor::block_on;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as SerdeMap, Value as SerdeValue};
use sqlx::{
    any::{AnyKind, AnyPool, AnyRow},
    query as sqlx_query, Row,
};
use std::{collections::HashMap, str::FromStr};
use tree_sitter::{Node, Parser};
use urlencoding::{decode, encode};

pub const LIMIT_DEFAULT: usize = 20;
pub const LIMIT_MAX: usize = 100;

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

/// Type of error returned in the case of a failure to parse a given string as an operator.
#[derive(Debug, PartialEq, Eq)]
pub struct ParseOperatorError;

impl FromStr for Operator {
    type Err = ParseOperatorError;

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
    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
            _ => Err(ParseOperatorError),
        }
    }
}

/// The direction of an ORDER BY clause in an SQL query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseDirectionError;

impl FromStr for Direction {
    type Err = ParseDirectionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "asc" | "ascending" => Ok(Direction::Ascending),
            "desc" | "descending" => Ok(Direction::Descending),
            _ => Err(ParseDirectionError),
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

impl Filter {
    /// Given a left hand side, a right hand side, and an operator, create a new filter.
    pub fn new<S: Into<String>>(
        lhs: S,
        operator: S,
        rhs: SerdeValue,
    ) -> Result<Filter, ParseOperatorError> {
        match Operator::from_str(&operator.into()) {
            Ok(operator) => Ok(Filter { lhs: lhs.into(), operator: operator, rhs: rhs }),
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
        let unquoted_rhs = unquote(&rhs).unwrap_or(rhs.to_string());
        let maybe_quoted_rhs;
        if unquoted_rhs.contains(char::is_whitespace) {
            maybe_quoted_rhs = format!("'{}'", unquoted_rhs);
        } else {
            maybe_quoted_rhs = rhs;
        }

        Ok(format!("{}{} LIKE {}", lhs.into(), negation, maybe_quoted_rhs))
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
        let unquoted_rhs = unquote(&rhs).unwrap_or(rhs.to_string());
        let maybe_quoted_rhs;
        if unquoted_rhs.contains(char::is_whitespace) {
            maybe_quoted_rhs = format!("'{}'", unquoted_rhs);
        } else {
            maybe_quoted_rhs = rhs;
        }

        if *dbtype == DbType::Postgres {
            Ok(format!("{}{} ILIKE {}", lhs.into(), negation, maybe_quoted_rhs))
        } else {
            Ok(format!("LOWER({}){} LIKE LOWER({})", lhs.into(), negation, maybe_quoted_rhs))
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
                        return Err(format!("{:?} contains both text and numeric types.", options));
                    }
                    values.push(format!("{}", s))
                }
                SerdeValue::Number(n) => {
                    if i == 0 {
                        is_string_list = false;
                    } else if is_string_list {
                        return Err(format!("{:?} contains both text and numeric types.", options));
                    }
                    values.push(format!("{}", n))
                }
                _ => return Err(format!("{:?} is not an array of strings or numbers.", options)),
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

        let unquoted_value = unquote(&value).unwrap_or(value.to_string());
        let maybe_quoted_value;
        if unquoted_value.contains(char::is_whitespace) {
            maybe_quoted_value = format!("'{}'", unquoted_value);
        } else {
            maybe_quoted_value = value.to_string();
        }

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
                maybe_quoted_value
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
                maybe_quoted_value
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

        fn maybe_quote(token: &str, double: bool) -> String {
            let quote_char;
            if double {
                quote_char = "\"";
            } else {
                quote_char = "'";
            }
            let unquoted_token = unquote(&token).unwrap_or(token.to_string());
            let maybe_quoted_string;
            if unquoted_token.contains(char::is_whitespace) {
                maybe_quoted_string = format!("{}{}{}", quote_char, unquoted_token, quote_char);
            } else {
                maybe_quoted_string = token.to_string();
            }
            maybe_quoted_string
        }

        match self.operator {
            Operator::Equals => match &self.rhs {
                SerdeValue::String(s) => {
                    Ok(format!("{} = {}", maybe_quote(&self.lhs, true), maybe_quote(&s, false)))
                }
                SerdeValue::Number(n) => Ok(format!("{} = {}", maybe_quote(&self.lhs, true), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::NotEquals => match &self.rhs {
                SerdeValue::String(s) => {
                    Ok(format!("{} <> {}", maybe_quote(&self.lhs, true), maybe_quote(&s, false)))
                }
                SerdeValue::Number(n) => Ok(format!("{} <> {}", maybe_quote(&self.lhs, true), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::LessThan => match &self.rhs {
                SerdeValue::String(s) => {
                    Ok(format!("{} < {}", maybe_quote(&self.lhs, true), maybe_quote(&s, false)))
                }
                SerdeValue::Number(n) => Ok(format!("{} < {}", maybe_quote(&self.lhs, true), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::GreaterThan => match &self.rhs {
                SerdeValue::String(s) => {
                    Ok(format!("{} > {}", maybe_quote(&self.lhs, true), maybe_quote(&s, false)))
                }
                SerdeValue::Number(n) => Ok(format!("{} > {}", maybe_quote(&self.lhs, true), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::LessThanEquals => match &self.rhs {
                SerdeValue::String(s) => {
                    Ok(format!("{} <= {}", maybe_quote(&self.lhs, true), maybe_quote(&s, false)))
                }
                SerdeValue::Number(n) => Ok(format!("{} <= {}", maybe_quote(&self.lhs, true), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::GreaterThanEquals => match &self.rhs {
                SerdeValue::String(s) => {
                    Ok(format!("{} >= {}", maybe_quote(&self.lhs, true), maybe_quote(&s, false)))
                }
                SerdeValue::Number(n) => Ok(format!("{} >= {}", maybe_quote(&self.lhs, true), n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::Like => match &self.rhs {
                SerdeValue::String(s) => Self::render_like_not_like(
                    &maybe_quote(&self.lhs, true),
                    &maybe_quote(s, false),
                    true,
                ),
                _ => Err(not_a_string_err),
            },
            Operator::NotLike => match &self.rhs {
                SerdeValue::String(s) => Self::render_like_not_like(
                    &maybe_quote(&self.lhs, true),
                    &maybe_quote(s, false),
                    false,
                ),
                _ => Err(not_a_string_err),
            },
            Operator::ILike => match &self.rhs {
                SerdeValue::String(s) => Self::render_ilike_not_ilike(
                    dbtype,
                    &maybe_quote(&self.lhs, true),
                    &maybe_quote(s, false),
                    true,
                ),
                _ => Err(not_a_string_err),
            },
            Operator::NotILike => match &self.rhs {
                SerdeValue::String(s) => Self::render_ilike_not_ilike(
                    dbtype,
                    &maybe_quote(&self.lhs, true),
                    &maybe_quote(s, false),
                    false,
                ),
                _ => Err(not_a_string_err),
            },
            Operator::Is => {
                Self::render_is_not_is(dbtype, &maybe_quote(&self.lhs, true), &self.rhs, true)
            }
            Operator::IsNot => {
                Self::render_is_not_is(dbtype, &maybe_quote(&self.lhs, true), &self.rhs, false)
            }
            Operator::In => match &self.rhs {
                SerdeValue::Array(options) => {
                    Self::render_in_not_in(&maybe_quote(&self.lhs, true), options, true)
                }
                _ => Err(format!("RHS of filter: {:?} is not an array.", self)),
            },
            Operator::NotIn => match &self.rhs {
                SerdeValue::Array(options) => {
                    Self::render_in_not_in(&maybe_quote(&self.lhs, true), options, false)
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

/// A structure to represent an SQL select statement.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Select {
    pub table: String,
    pub select: Vec<(String, Option<String>)>,
    pub filter: Vec<Filter>,
    pub group_by: Vec<String>,
    pub having: Vec<Filter>,
    pub order_by: Vec<(String, Direction)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Select {
    /// Create a new Select struct with the given table name and with its other fields initialized
    /// to their default values.
    pub fn new<S: Into<String>>(table: S) -> Select {
        Select { table: table.into(), ..Default::default() }
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
            self.select.push((s.into(), None));
        }
        self
    }

    /// Given a vector of tuples such that the first place of each tuple is a column expression
    /// and the second place is an alias for that expression, replace the current contents of
    /// `self.select` with the contents of the given vector.
    pub fn aliased_select<S: Into<String>>(&mut self, select: Vec<(S, S)>) -> &mut Select {
        self.select.clear();
        for (column, alias) in select {
            self.select.push((column.into(), Some(alias.into())));
        }
        self
    }

    /// Given a column expression, add it to the vector, `self.select` without an alias.
    pub fn add_select<S: Into<String>>(&mut self, select: S) -> &mut Select {
        self.select.push((select.into(), None));
        self
    }

    /// Given a column expression and an alias for that expression, add the tuple (column, alias) to
    /// the vector, `self.select`.
    pub fn add_aliased_select<S: Into<String>>(&mut self, column: S, alias: S) -> &mut Select {
        self.select.push((column.into(), Some(alias.into())));
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
    /// contents of the given vector.
    pub fn order_by<S: Into<String>>(&mut self, order_by: Vec<(S, Direction)>) -> &mut Select {
        self.order_by.clear();
        for (s, d) in order_by {
            self.order_by.push((s.into(), d));
        }
        self
    }

    /// Given a column name, add it to the vector, `self.order_by`.
    pub fn add_order_by<S: Into<String>>(&mut self, order_by: (S, Direction)) -> &mut Select {
        let (column, direction) = order_by;
        self.order_by.push((column.into(), direction));
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
        let rows = block_on(query.fetch_all(pool)).unwrap();
        for row in &rows {
            let cname: &str = row.get("name");
            self.select.push((format!(r#""{}""#, cname), None));
        }

        Ok(self)
    }

    /// Given a database type, convert the given Select struct to an SQL statement, using the syntax
    /// appropriate for the kind of database specified. Returns an Error if `self.table` or
    /// `self.select` have not been defined.
    pub fn to_sql(&self, dbtype: &DbType) -> Result<String, String> {
        if self.table.is_empty() {
            return Err("Missing required field: `table` in to_sql()".to_string());
        }

        let select_clause;
        if self.select.is_empty() {
            select_clause = String::from("*");
        } else {
            let mut select_columns = vec![];
            for (column, alias) in &self.select {
                let unquoted_column = unquote(&column).unwrap_or(column.clone());
                let naked_column;
                if unquoted_column.contains(char::is_whitespace) {
                    naked_column = format!("\"{}\"", unquoted_column);
                } else {
                    naked_column = column.to_string();
                }
                select_columns.push(format!("{}{}", naked_column, {
                    if let Some(alias) = alias {
                        format!(" AS {}", alias)
                    } else {
                        "".to_string()
                    }
                }));
            }
            select_clause = select_columns.join(", ");
        }

        let unquoted_table = unquote(&self.table).unwrap_or(self.table.clone());
        let table;
        if unquoted_table.contains(char::is_whitespace) {
            table = format!("\"{}\"", unquoted_table);
        } else {
            table = self.table.to_string();
        }
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
                .map(|(col, dir)| {
                    let unquoted_col = unquote(&col).unwrap_or(col.to_string());
                    let maybe_quoted_col;
                    if unquoted_col.contains(char::is_whitespace) {
                        maybe_quoted_col = format!("\"{}\"", unquoted_col);
                    } else {
                        maybe_quoted_col = col.to_string();
                    }
                    format!("{} {}", maybe_quoted_col, dir.to_sql())
                })
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

    /// Convert the given Select struct to a SQLRest URL. Returns an error if `self.table` or
    /// `self.select` have not been defined.
    pub fn to_url(&self) -> Result<String, String> {
        if self.table.is_empty() {
            return Err("Missing required field: `table` in to_sql()".to_string());
        }
        if !self.group_by.is_empty() || !self.having.is_empty() {
            return Err(
                "GROUP BY / HAVING clauses are not supported in Select::to_url()".to_string()
            );
        }

        let mut params: Vec<String> = vec![];
        let parts: Vec<String> = self
            .select
            .iter()
            // TODO: Handle aliases. See https://postgrest.org/en/stable/api.html#renaming-columns
            .map(|(column, _alias)| column.clone())
            .collect();

        if self.select.len() > 0 {
            params.push(format!("select={}", parts.join(",")));
        }
        if self.filter.len() > 0 {
            for filter in &self.filter {
                //println!("Filter: {:?}", filter);
                let rhs = match &filter.rhs {
                    SerdeValue::String(s) => {
                        println!("SSSSS: {}", s);
                        let s = unquote(&s).unwrap_or(s.clone());
                        if s.to_lowercase() == "null" {
                            s.to_string()
                        } else {
                            format!("\"{}\"", s)
                        }

                        //if s.contains(char::is_whitespace) || s.chars().all(char::is_numeric) {
                        //    format!("\"{}\"", s)
                        //} else {
                        //    s.to_string()
                        //}
                    }
                    SerdeValue::Number(n) => format!("{}", n),
                    SerdeValue::Array(v) => {
                        let mut list = vec![];
                        for item in v {
                            match item {
                                SerdeValue::String(s) => {
                                    println!("TTTTT: {}", s);
                                    let s = unquote(&s).unwrap_or(s.clone());
                                    if s.to_lowercase() == "null" {
                                        list.push(s.to_string());
                                    } else {
                                        list.push(format!("\"{}\"", s));
                                    }

                                    //if s.contains(char::is_whitespace)
                                    //    || s.chars().all(char::is_numeric)
                                    //{
                                    //    list.push(format!("\"{}\"", s));
                                    //} else {
                                    //    list.push(s.to_string());
                                    //}
                                }
                                SerdeValue::Number(n) => list.push(n.to_string()),
                                _ => panic!("Arrrggghhhhh!!!!"),
                            };
                        }
                        format!("({})", list.join(","))
                    }
                    _ => panic!("RHS of Filter: {:?} is not a string, number, or list", filter),
                };

                let x = match filter.operator {
                    Operator::Equals => format!(r#"{}=eq.{}"#, filter.lhs, rhs),
                    Operator::NotEquals => format!(r#"{}=not_eq.{}"#, filter.lhs, rhs),
                    Operator::LessThan => format!(r#"{}=lt.{}"#, filter.lhs, rhs),
                    Operator::GreaterThan => format!(r#"{}=gt.{}"#, filter.lhs, rhs),
                    Operator::LessThanEquals => format!(r#"{}=lte.{}"#, filter.lhs, rhs),
                    Operator::GreaterThanEquals => format!(r#"{}=gte.{}"#, filter.lhs, rhs),
                    Operator::Like => format!(r#"{}=like.{}"#, filter.lhs, rhs),
                    Operator::NotLike => format!(r#"{}=not_like.{}"#, filter.lhs, rhs),
                    Operator::ILike => format!(r#"{}=ilike.{}"#, filter.lhs, rhs),
                    Operator::NotILike => format!(r#"{}=not_ilike.{}"#, filter.lhs, rhs),
                    Operator::Is => format!(r#"{}=is.{}"#, filter.lhs, rhs),
                    Operator::IsNot => format!(r#"{}=not_is.{}"#, filter.lhs, rhs),
                    Operator::In => format!(r#"{}=in.{}"#, filter.lhs, rhs),
                    Operator::NotIn => format!(r#"{}=not_in.{}"#, filter.lhs, rhs),
                };
                params.push(x);
            }
        }
        if self.order_by.len() > 0 {
            let parts: Vec<String> =
                self.order_by.iter().map(|(c, d)| format!(r"{}.{}", c, d.to_url())).collect();
            params.push(format!("order={}", parts.join(",")));
        }
        if let Some(limit) = self.limit {
            if limit > 0 && limit != LIMIT_DEFAULT {
                params.push(format!("limit={}", limit));
            }
        }
        if let Some(offset) = self.offset {
            if offset > 0 {
                params.push(format!("offset={}", offset));
            }
        }

        //println!("TABLE: {}", self.table);

        if params.len() > 0 {
            Ok(encode(&format!("{}?{}", self.table, params.join("&"))).to_string())
        } else {
            Ok(encode(&self.table.clone()).to_string())
        }
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

    /// Given a database connection pool and a parameter map, bind this Select to the parameter map,
    /// execute the resulting query against the database, and return the resulting rows.
    /// If `as_json` is set to true, the appropriate database function will be used to aggregate the
    /// result set as a single AnyRow with a single field called "row" containing a JSON-formatted
    /// string.
    fn execute_select(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
        as_json: bool,
    ) -> Result<Vec<AnyRow>, String> {
        let dbtype = get_db_type(pool);
        if let Err(e) = dbtype {
            return Err(e);
        }
        let dbtype = dbtype.unwrap();

        let sql = self.to_sql(&dbtype);
        if let Err(e) = sql {
            return Err(e);
        }
        let sql = sql.unwrap();
        let sql = if !as_json {
            sql
        } else if dbtype == DbType::Sqlite {
            let mut json_keys = vec![];
            for (column, alias) in &self.select {
                let unquoted_column = match alias {
                    Some(alias) => alias.to_string(),
                    None => unquote(&column).unwrap_or(column.clone()),
                };
                json_keys.push(format!(r#"'{}', "{}""#, unquoted_column, unquoted_column));
            }
            let json_select = json_keys.join(", ");
            format!("SELECT JSON_GROUP_ARRAY(JSON_OBJECT({})) AS row FROM ({})", json_select, sql)
        } else {
            format!("SELECT JSON_AGG(t)::TEXT AS row FROM ({}) t", sql)
        };

        let bind_result = bind_sql(pool, &sql, param_map);
        if let Err(e) = bind_result {
            return Err(e);
        }
        let (sql, param_vec) = bind_result.unwrap();

        let mut query = sqlx_query(&sql);
        for param in &param_vec {
            match param {
                SerdeValue::String(s) => query = query.bind(s),
                SerdeValue::Number(n) => query = query.bind(n.as_i64()),
                _ => return Err(format!("{} is not a string or a number.", param)),
            };
        }

        match block_on(query.fetch_all(pool)) {
            Err(e) => Err(format!("{}", e)),
            Ok(k) => Ok(k),
        }
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
        let rows = self.execute_select(pool, param_map, true);
        if let Err(e) = rows {
            return Err(e);
        }
        let mut rows = rows.unwrap();

        if rows.len() != 1 {
            return Err(format!("In fetch_rows_as_json(), expected 1 row, got {}", rows.len()));
        }
        let row = rows.pop().unwrap();

        let json_row = row.try_get("row");
        if let Err(e) = json_row {
            return Err(e.to_string());
        }
        let json_row: &str = json_row.unwrap();
        extract_rows_from_json_str(json_row)
    }
}

/// TODO: Add a docstring here.
pub fn get_from_raw(n: &Node, raw: &str) -> String {
    println!("In get_from_raw() ...");
    //println!("RAW: {}", raw);
    //println!("NODE KIND: {}", n.kind());
    let start = n.start_position().column;
    //println!("START: {:?}", start);
    let end = n.end_position().column;
    //println!("END: {:?}", end);
    let extract = &raw[start..end];
    //println!("EXTRACT: {}", extract);
    String::from(extract)
}

/// TODO: Add a docstring here.
pub fn parse(input: &str) -> Select {
    println!("In parse() ...");
    let mut parser: Parser = Parser::new();
    let input = input.to_string();

    parser.set_language(tree_sitter_sqlrest::language()).expect("Error loading sqlrest grammar");

    let tree = parser.parse(&input, None).unwrap();

    let mut query = Select::new("");
    transduce(&tree.root_node(), &input, &mut query);
    query
}

/// TODO: Add a docstring here.
pub fn transduce(n: &Node, raw: &str, query: &mut Select) {
    println!("In transduce() for {} ...", n.kind());
    match n.kind() {
        "query" => transduce_children(n, raw, query),
        "select" => transduce_select(n, raw, query),
        "table" => transduce_table(n, raw, query),
        "expression" => transduce_children(n, raw, query),
        "part" => transduce_children(n, raw, query),
        "filter" => transduce_children(n, raw, query),
        "simple_filter" => transduce_filter(n, raw, query),
        "special_filter" => transduce_children(n, raw, query),
        "in" => transduce_in(n, raw, query, false),
        "not_in" => transduce_in(n, raw, query, true),
        "order" => transduce_order(n, raw, query),
        "limit" => transduce_limit(n, raw, query),
        "offset" => transduce_offset(n, raw, query),
        "STRING" => panic!("Encountered STRING in top level translation"),
        _ => {
            panic!("Error parsing node of kind '{}': {:?} {} {:?}", n.kind(), n, raw, query);
        }
    }
}

// TODO: REMOVE THE UNWRAPS FROM THE TRANSDUCE FUNCTIONS BELOW AND RETURN RESULT OBJECTS INSTEAD.

/// TODO: Add a docstring here.
pub fn transduce_children(n: &Node, raw: &str, q: &mut Select) {
    println!("In transduce_children() ...");
    let child_count = n.named_child_count();
    for i in 0..child_count {
        transduce(&n.named_child(i).unwrap(), raw, q);
    }
}

/// TODO: Add a docstring here.
pub fn transduce_table(n: &Node, raw: &str, query: &mut Select) {
    println!("In transduce_table() ...");
    let table = decode(&get_from_raw(&n.named_child(0).unwrap(), raw)).unwrap().into_owned();
    query.table = table;
}

/// TODO: Add a docstring here.
pub fn transduce_filter(n: &Node, raw: &str, query: &mut Select) {
    println!("In transduce_filter() ...");
    let column = decode(&get_from_raw(&n.named_child(0).unwrap(), raw)).unwrap().into_owned();
    let operator_string = get_from_raw(&n.named_child(1).unwrap(), raw);
    let value_node = n.named_child(2).unwrap();
    let value = decode(&get_from_raw(&value_node, raw)).unwrap().into_owned();
    let value: SerdeValue = {
        if value_node.kind() != "value" {
            panic!("Arghh!!! Unexpected Node kind: {}!!!", value_node.kind());
        } else {
            match value.parse::<i64>() {
                Ok(v) => json!(v),
                Err(_) => match value.parse::<f64>() {
                    Ok(v) => json!(v),
                    Err(_) => {
                        if value.to_lowercase() == "null" {
                            json!(value)
                        } else {
                            let unquoted_value = unquote(&value).unwrap_or(value.to_string());
                            json!(format!("'{}'", unquoted_value))
                        }
                    }
                },
            }
        }
    };

    let filter = Filter::new(column, operator_string, value).unwrap();
    query.filter.push(filter);
}

/// TODO: Add a docstring here.
pub fn transduce_list(n: &Node, raw: &str) -> Vec<String> {
    println!("In transduce_in() ...");
    let quoted_strings = match n.kind() {
        "list" => false,
        "list_of_strings" => true,
        _ => panic!("Not a valid list"),
    };

    let mut vec = Vec::new();

    let child_count = n.named_child_count();
    for i in 0..child_count {
        let value = decode(&get_from_raw(&n.named_child(i).unwrap(), raw)).unwrap().into_owned();
        if quoted_strings {
            let quoted_string = format!("{}", value);
            vec.push(quoted_string);
        } else {
            vec.push(value);
        }
    }
    vec
}

/// TODO: Add a docstring here.
pub fn transduce_in(n: &Node, raw: &str, query: &mut Select, negate: bool) {
    println!("In transduce_in() ...");
    let column = decode(&get_from_raw(&n.named_child(0).unwrap(), raw)).unwrap().into_owned();
    let values = transduce_list(&n.named_child(1).unwrap(), raw);
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

    let operator_str = if negate { String::from("not_in") } else { String::from("in") };
    let filter = Filter::new(column, operator_str, SerdeValue::Array(choices)).unwrap();
    query.filter.push(filter);
}

/// TODO: Add a docstring here.
pub fn transduce_select(n: &Node, raw: &str, query: &mut Select) {
    println!("In transduce_select() ...");
    let child_count = n.named_child_count();
    for position in 0..child_count {
        let column =
            decode(&get_from_raw(&n.named_child(position).unwrap(), raw)).unwrap().into_owned();
        query.select.push((column, None)); // TODO: Support aliases.
    }
}

pub fn transduce_order(n: &Node, raw: &str, query: &mut Select) {
    let child_count = n.named_child_count();
    let mut position = 0;

    while position < child_count {
        let column =
            decode(&get_from_raw(&n.named_child(position).unwrap(), raw)).unwrap().into_owned();
        position = position + 1;
        if position < child_count && n.named_child(position).unwrap().kind().eq("ordering") {
            let ordering_string = get_from_raw(&n.named_child(position).unwrap(), raw);
            let ordering = Direction::from_str(&ordering_string);
            match ordering {
                Ok(o) => {
                    position = position + 1;
                    let order = (column, o);
                    query.add_order_by(order);
                }
                Err(_) => {
                    //tracing::warn!("Unhandled order param '{}'", ordering_string);
                    panic!("Unhandled order param '{}'", ordering_string);
                }
            };
        } else {
            let ordering = Direction::Ascending; //default ordering is ASC
            let order = (column, ordering);
            query.add_order_by(order);
        }
    }
}

/// TODO: Add a docstring here:
pub fn transduce_offset(n: &Node, raw: &str, query: &mut Select) {
    let offset_string = get_from_raw(&n.named_child(0).unwrap(), raw);
    let offset: usize = offset_string.parse().unwrap();
    query.offset(offset);
}

/// TODO: Add a docstring here:
pub fn transduce_limit(n: &Node, raw: &str, query: &mut Select) {
    let limit_string = get_from_raw(&n.named_child(0).unwrap(), raw);
    let limit: usize = limit_string.parse().unwrap();
    query.limit(limit);
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
    let mut query = sqlx_query(&sql);
    for param in params {
        match param {
            SerdeValue::String(s) => query = query.bind(s),
            SerdeValue::Number(n) => query = query.bind(n.as_i64()),
            _ => return Err(format!("{} is not a string or a number.", param)),
        };
    }

    match block_on(query.fetch_all(pool)) {
        Err(e) => Err(format!("{}", e)),
        Ok(k) => Ok(k),
    }
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
                    for (column, alias) in &select2.select {
                        let unquoted_column = match alias {
                            Some(alias) => alias.to_string(),
                            None => unquote(&column).unwrap_or(column.clone()),
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
    let mut query = sqlx_query(&sql);
    for param in params {
        match param {
            SerdeValue::String(s) => query = query.bind(s),
            SerdeValue::Number(n) => query = query.bind(n.as_i64()),
            _ => return Err(format!("{} is not a string or a number.", param)),
        };
    }

    // Execute the query, extract the JSON row and return it:
    match block_on(query.fetch_all(pool)) {
        Err(e) => Err(format!("{}", e)),
        Ok(rows) if rows.len() != 1 => {
            Err(format!("In fetch_rows_as_json(), expected 1 row, got {}", rows.len()))
        }
        Ok(mut rows) => {
            let row = rows.pop().unwrap();
            match row.try_get("row") {
                Err(e) => Err(format!("{}", e)),
                Ok(json_row) => extract_rows_from_json_str(json_row),
            }
        }
    }
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
            let key = this_match.strip_prefix("{").and_then(|s| s.strip_suffix("}")).unwrap();
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
    let rx;
    if let Some(s) = placeholder_str {
        rx = Regex::new(&format!(r#"{}|\b{}\b"#, quotes, s.into())).unwrap();
    } else if pool.any_kind() == AnyKind::Postgres {
        rx = Regex::new(&format!(r#"{}|\B[$]\d+\b"#, quotes)).unwrap();
    } else {
        rx = Regex::new(&format!(r#"{}|\B[?]\B"#, quotes)).unwrap();
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serial_test::serial;
    use sqlx::{
        any::{AnyConnectOptions, AnyPool, AnyPoolOptions},
        query as sqlx_query, Row, ValueRef,
    };
    use std::{char, collections::HashMap, str::FromStr, time::Instant};

    fn perf_test_for_db(pool: &AnyPool) {
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
        let num_rows = 250000;
        let num_columns = 5;
        for i in 1..num_rows {
            insert.push_str(&format!("({}, ", i));
            let mut cell_ids = vec![];
            for j in 1..num_columns {
                let cell_id = format!("'{}{}'", col_to_a1(j), i);
                cell_ids.push(cell_id);
            }
            insert.push_str(&format!("{})", cell_ids.join(", ")));
            if i != (num_rows - 1) {
                insert.push_str(",");
            }
        }

        let num_iterations = 5;
        for i in 1..(num_iterations + 1) {
            println!(
                "Running performance test #{} of {} for {:?}",
                i,
                num_iterations,
                pool.any_kind()
            );
            for sql in &vec![drop, create, &insert] {
                let query = sqlx_query(sql);
                block_on(query.execute(pool)).unwrap();
            }

            let mut select = Select::new("my_table");
            select
                .select(vec!["row_number", "prefix", "base", "\"ontology IRI\"", "\"version IRI\""])
                .filter(vec![Filter::new("row_number", "lt", json!(num_rows / 2)).unwrap()]);

            // Run the VACUUM command to clear the cache:
            let query = sqlx_query("VACUUM");
            block_on(query.execute(pool)).unwrap();
            // Time the query:
            let start = Instant::now();
            let rows = select.fetch_rows(pool, &HashMap::new());
            println!("Elapsed time for Select::fetch_rows(): {:.2?}", start.elapsed());
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
            println!("Elapsed time for Select::fetch_rows_as_json(): {:.2?}", start.elapsed());
            for row in json_rows.unwrap() {
                let _ = row.get("prefix").unwrap();
            }
            println!("Elapsed time after iterating: {:.2?}", start.elapsed());
            println!("----------");
        }
    }

    #[test]
    #[ignore] // Performances test disabled by default.
    #[serial]
    /// Runs a performance test of the Select::fetch_rows() and Select::fetch_rows_as_json()
    /// functions on PostgreSQL. Note that to view the output of the test you must
    /// use the 'nocapture' option: `cargo test -- --nocapture`
    fn perf_test_postgresql() {
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let postgresql_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
                .unwrap();
        perf_test_for_db(&postgresql_pool);
    }

    #[test]
    #[ignore] // Performances test disabled by default.
    #[serial]
    /// Runs a performance test of the Select::fetch_rows() and Select::fetch_rows_as_json()
    /// functions on SQLite. Note that to view the output of the test you must
    /// use the 'nocapture' option: `cargo test -- --nocapture`
    fn perf_test_sqlite() {
        let sq_connection_options =
            AnyConnectOptions::from_str("sqlite://test.db?mode=rwc").unwrap();
        let sqlite_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
                .unwrap();
        perf_test_for_db(&sqlite_pool);
    }

    #[test]
    #[ignore]
    #[serial]
    fn test_real_datatype() {
        let sq_connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let sqlite_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
                .unwrap();
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let postgresql_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
                .unwrap();

        for pool in &vec![sqlite_pool, postgresql_pool] {
            let drop = r#"DROP TABLE IF EXISTS "my_table""#;
            let create = r#"CREATE TABLE "my_table" (
                          "row_number" BIGINT,
                          "column_1" TEXT,
                          "column_2" TEXT,
                          "column_3" REAL
                        )"#;
            let insert = r#"INSERT INTO "my_table" VALUES
                        (1, 'one', 'eins', 1.1),
                        (2, 'two', 'zwei', 2.2),
                        (3, 'three', 'drei', 3.3)"#;

            for sql in &vec![drop, create, &insert] {
                let query = sqlx_query(sql);
                block_on(query.execute(pool)).unwrap();
            }

            let mut select = Select::new("my_table");
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
    #[ignore]
    fn test_json_datatype() {
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
                .unwrap();

        let drop = r#"DROP TABLE IF EXISTS "my_table""#;
        let create = r#"CREATE TABLE "my_table" (
                          "row_number" BIGINT,
                          "column_1" TEXT,
                          "column_2" TEXT,
                          "column_3" JSON
                        )"#;
        let insert = r#"INSERT INTO "my_table" VALUES
                        (1, 'one', 'eins', '1'::JSON),
                        (2, 'two', 'zwei', '"dos"'::JSON),
                        (3, 'three', 'drei', '{"fookey": "foovalue"}'::JSON)"#;

        for sql in &vec![drop, create, &insert] {
            let query = sqlx_query(sql);
            block_on(query.execute(&pool)).unwrap();
        }

        let mut select = Select::new("my_table");
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
        let fooval =
            row.get("column_3").and_then(|c| c.as_object()).and_then(|c| c.get("fookey")).unwrap();
        match fooval {
            SerdeValue::String(s) => assert_eq!(s, "foovalue"),
            _ => panic!("'{}' does not match 'foovalue'", fooval),
        };
    }
}
