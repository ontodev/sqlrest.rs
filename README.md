# ontodev_sqlrest

<!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
     in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
     `cargo readme > README.md` -->

SQLRest.rs

## Examples
```rust
use ontodev_sqlrest::{
    bind_sql, get_db_type, fetch_rows_from_selects, fetch_rows_as_json_from_selects,
    interpolate_sql, local_sql_syntax, DbType, Direction, Filter, Select, selects_to_sql,
};
use futures::executor::block_on;
use indoc::indoc;
use serde_json::{json, Value as SerdeValue};
use sqlx::{
    any::AnyKind,
    query as sqlx_query, Row,
};
use std::{
    collections::HashMap,
};

/*
 * Use the local_sql_syntax() function to convert a SQL string with the given placeholder
 * to the syntax appropriate to the given database connection pool.
 */
let generic_sql = r#"SELECT "foo" FROM "bar" WHERE "xyzzy" IN (VAL, VAL, VAL)"#;
let local_sql = local_sql_syntax(&sqlite_pool, "VAL", generic_sql);
assert_eq!("SELECT \"foo\" FROM \"bar\" WHERE \"xyzzy\" IN (?, ?, ?)", local_sql);
let local_sql = local_sql_syntax(&postgresql_pool, "VAL", generic_sql);
assert_eq!("SELECT \"foo\" FROM \"bar\" WHERE \"xyzzy\" IN ($1, $2, $3)", local_sql);

/*
 * Initialise a Select struct using struct syntax.
 */
let select = Select {
    table: String::from("my_table"),
    select: vec![
        "row_number".to_string(),
        "prefix".to_string(),
        "base".to_string(),
        r#""ontology IRI""#.to_string(),
        r#""version IRI""#.to_string(),
    ],
    limit: Some(10),
    ..Default::default()
};
let expected_sql = format!(
    r#"{} {}"#,
    r#"SELECT row_number, prefix, base, "ontology IRI", "version IRI""#,
    "FROM my_table LIMIT 10"
);
assert_eq!(select.to_sql(&DbType::Postgres).unwrap(), expected_sql);
assert_eq!(select.to_sql(&DbType::Sqlite).unwrap(), expected_sql);

/*
 * Use the bind_sql() function to bind a given parameter map to a given SQL string, then call
 * interpolate_sql() on the bound SQL and parameter vector that are returned. Finally, create a
 * sqlx_query with the bound SQL and parameter vector and execute it.
 */
let test_sql = indoc! {r#"
       SELECT "table", "row_num", "col1", "col2"
       FROM "test"
       WHERE "table" = {table}
         AND "row_num" = {row_num}
         AND "col1" = {column}
         AND "col2" = {column}
    "#};
let mut test_params = HashMap::new();
test_params.insert("table", json!("foo"));
test_params.insert("row_num", json!(1));
test_params.insert("column", json!("bar"));
let (bound_sql, test_params) = bind_sql(pool, test_sql, &test_params).unwrap();

let interpolated_sql = interpolate_sql(pool, &bound_sql, &test_params, None).unwrap();
assert_eq!(
    indoc! {r#"
       SELECT "table", "row_num", "col1", "col2"
       FROM "test"
       WHERE "table" = 'foo'
         AND "row_num" = 1
         AND "col1" = 'bar'
         AND "col2" = 'bar'
    "#},
    interpolated_sql
);

let mut test_query = sqlx_query(&bound_sql);
for param in &test_params {
    match param {
        SerdeValue::String(s) => test_query = test_query.bind(s),
        SerdeValue::Number(n) => test_query = test_query.bind(n.as_i64()),
        _ => panic!("{} is not a string or a number.", param),
    };
}
let row = block_on(test_query.fetch_one(pool)).unwrap();

/*
 * Create a new Select struct by calling new() and progressively adding fields.
 */
let mut select = Select::new(r#""a table name with spaces""#);
select.select(vec!["foo", r#""a column name with spaces""#]);
select.add_select("bar");
select.add_select("COUNT(1)");
select.filters(vec![Filter::new("foo", "is", json!("{foo}")).unwrap()]);
select.add_filter(Filter::new("bar", "in", json!(["{val1}", "{val2}"])).unwrap());
select.order_by(vec![("foo", Direction::Ascending), ("bar", Direction::Descending)]);
select.group_by(vec!["foo"]);
select.add_group_by(r#""a column name with spaces""#);
select.add_group_by("bar");
select.having(vec![Filter::new("COUNT(1)", "gt", json!(1)).unwrap()]);
select.limit(11);
select.offset(50);

/*
 * Convert the Select defined above to SQL for both Postgres and Sqlite, bind the SQL using
 * bind_sql(), and then create and execure a sqlx_query using the bound SQL and parameter
 * vector that are returned.
 */
for pool in vec![&postgresql_pool, &sqlite_pool] {
    let (expected_is_clause, placeholder1, placeholder2, placeholder3);
    if pool.any_kind() == AnyKind::Postgres {
        expected_is_clause = "IS NOT DISTINCT FROM";
        placeholder1 = "$1";
        placeholder2 = "$2";
        placeholder3 = "$3";
    } else {
        expected_is_clause = "IS";
        placeholder1 = "?";
        placeholder2 = "?";
        placeholder3 = "?";
    }

    let expected_sql_with_mapvars = format!(
        "SELECT foo, \"a column name with spaces\", bar, COUNT(1) \
         FROM \"a table name with spaces\" \
         WHERE foo {} {{foo}} AND bar IN ({{val1}}, {{val2}}) \
         GROUP BY foo, \"a column name with spaces\", bar \
         HAVING COUNT(1) > 1 \
         ORDER BY foo ASC, bar DESC \
         LIMIT 11 OFFSET 50",
        expected_is_clause,
    );
    let dbtype = get_db_type(&pool).unwrap();
    let sql = select.to_sql(&dbtype).unwrap();
    assert_eq!(expected_sql_with_mapvars, sql);

    let mut param_map = HashMap::new();
    param_map.insert("foo", json!("foo_val"));
    param_map.insert("val1", json!("bar_val1"));
    param_map.insert("val2", json!("bar_val2"));

    let expected_sql_with_listvars = format!(
        "SELECT foo, \"a column name with spaces\", bar, COUNT(1) \
         FROM \"a table name with spaces\" \
         WHERE foo {} {} AND bar IN ({}, {}) \
         GROUP BY foo, \"a column name with spaces\", bar \
         HAVING COUNT(1) > 1 \
         ORDER BY foo ASC, bar DESC \
         LIMIT 11 OFFSET 50",
        expected_is_clause, placeholder1, placeholder2, placeholder3,
    );
    let (sql, params) = bind_sql(pool, &sql, &param_map).unwrap();
    assert_eq!(expected_sql_with_listvars, sql);
    let mut query = sqlx_query(&sql);
    for param in &params {
        match param {
            SerdeValue::String(s) => query = query.bind(s),
            SerdeValue::Number(n) => query = query.bind(n.as_i64()),
            _ => panic!("{} is not a string or a number.", param),
        };
    }
    block_on(query.execute(pool)).unwrap();
}

/*
 * Generate the SQL for a simple combined query.
 */
let mut cte = Select::new("my_table");
cte.select(vec!["prefix"]);
// Note: When building a Select struct, chaining is possible but you must first create
// a mutable struct in a separate statement using new() before before chaining:
let mut main_select = Select::new("cte");
main_select.select(vec!["prefix"]).limit(10).offset(20);

let sql = selects_to_sql(&cte, &main_select, &DbType::Postgres).unwrap();
assert_eq!(
    sql,
    "WITH cte AS (SELECT prefix FROM my_table) SELECT prefix FROM cte LIMIT 10 OFFSET 20",
);
block_on(sqlx_query(&sql).execute(&sqlite_pool)).unwrap();
block_on(sqlx_query(&sql).execute(&postgresql_pool)).unwrap();
/*
 * Fetch database rows using Select::fetch_rows().
 */
let mut select = Select::new(r#""a table name with spaces""#);
select
    .select_all(&sqlite_pool)
    .expect("")
    .filters(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
    .order_by(vec![("foo", Direction::Descending)]);

let mut param_map = HashMap::new();
param_map.insert("foo1", json!("f5"));
param_map.insert("foo2", json!("f6"));
let sqlite_rows = select.fetch_rows(&sqlite_pool, &param_map).unwrap();
let postgresql_rows = select.fetch_rows(&postgresql_pool, &param_map).unwrap();

/*
 * Fetch database rows in JSON format using Select::fetch_rows_as_json().
 */
let mut select = Select::new(r#""a table name with spaces""#);
select
    .select(vec!["foo", r#""a column name with spaces""#, "bar", "COUNT(1)"])
    .filters(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
    .order_by(vec![("foo", Direction::Ascending), ("bar", Direction::Descending)])
    .group_by(vec!["foo", r#""a column name with spaces""#, "bar"])
    .having(vec![Filter::new("COUNT(1)", "gte", json!(1)).unwrap()])
    .limit(10)
    .offset(1);

let mut param_map = HashMap::new();
param_map.insert("foo1", json!("f5"));
param_map.insert("foo2", json!("f6"));
for pool in vec![postgresql_pool, sqlite_pool] {
    let json_rows = select.fetch_rows_as_json(&pool, &param_map).unwrap();
}
/*
 * Fetch rows from a combined query.
 */
let mut cte = Select::new("my_table");
cte.select(vec!["prefix"]);
let mut main_select = Select::new("cte");
main_select
    .select(vec!["prefix"])
    .limit(10)
    .offset(0)
    .add_order_by(("prefix", Direction::Ascending));
for pool in vec![sqlite_pool, postgresql_pool] {
    let rows = fetch_rows_from_selects(&cte, &main_select, &pool, &HashMap::new()).unwrap();
}
/*
 * Fetch rows as json from a combined query.
 */
let mut cte = Select::new("my_table");
cte.select(vec!["prefix"]);
let mut main_select = Select::new("cte");
main_select
    .select(vec!["prefix"])
    .limit(10)
    .offset(0)
    .add_order_by(("prefix", Direction::Ascending));
for pool in vec![sqlite_pool, postgresql_pool] {
    let json_rows =
        fetch_rows_as_json_from_selects(&cte, &main_select, &pool, &HashMap::new())
            .unwrap();
}
```
