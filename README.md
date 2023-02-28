# ontodev_sqlrest

<!-- Please do not edit README.md directly. To generate a new readme from the crate documentation
     in src/lib.rs, install cargo-readme using `cargo install cargo-readme` and then run:
     `cargo readme > README.md` -->

### Working with Select structs
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
select.aliased_select(vec![("foo", "foo"), (r#""a column name with spaces""#, "C")]);
select.add_select("bar");
select.add_aliased_select("COUNT(1)", "count");
select.filter(vec![Filter::new("foo", "is", json!("{foo}")).unwrap()]);
select.add_filter(Filter::new("bar", "in", json!(["{val1}", "{val2}"])).unwrap());
select.order_by(vec![("foo", Direction::Ascending), ("bar", Direction::Descending)]);
select.group_by(vec!["foo"]);
select.add_group_by("C");
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
        "SELECT foo AS foo, \"a column name with spaces\" AS C, bar, COUNT(1) AS count \
         FROM \"a table name with spaces\" \
         WHERE foo {} {{foo}} AND bar IN ({{val1}}, {{val2}}) \
         GROUP BY foo, C, bar \
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
        "SELECT foo AS foo, \"a column name with spaces\" AS C, bar, COUNT(1) AS count \
         FROM \"a table name with spaces\" \
         WHERE foo {} {} AND bar IN ({}, {}) \
         GROUP BY foo, C, bar \
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
    .filter(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
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
    .select(vec!["foo", r#""a column name with spaces""#, "bar"])
    .add_aliased_select("COUNT(1)", "count")
    .filter(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
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
### Parsing Selects from URLs and vice versa.
#### Select all columns from the table "bar", with no filtering.
```rust
use ontodev_sqlrest::parse;
use urlencoding::{decode, encode};

let from_url = "bar";
// Table names and column names are always rendered in double quotes in SQL:
let expected_sql = "SELECT * FROM \"bar\"";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(from_url, select.to_url().unwrap());

// Table names with spaces can be specified in a URL:
let from_url = "a bar";
let expected_sql = "SELECT * FROM \"a bar\"";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(encode(from_url), select.to_url().unwrap());

// Alternately, spaces can in table names can be specified in a URL using percent encoding:
let from_url = "a%20bar";
let expected_sql = "SELECT * FROM \"a bar\"";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(decode(&from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());

// Quoted strings in table names are not allowed:
let from_url = "\"a bar\"";
let result = parse(from_url);
assert!(result.is_err());

// Quoted strings in table names are not allowed, even if the quotes are encoded:
let from_url = "%22a%20bar%22";
let result = parse(from_url);
assert!(result.is_err());
```

#### Select the columns "foo" and "goo" from the table "bar" with no filtering.
```rust
let from_url = "bar?select=foo,goo";
let expected_sql = "SELECT \"foo\", \"goo\" FROM \"bar\"";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(encode(from_url), select.to_url().unwrap());

// Column names with spaces are allowed in a URL:
let from_url = "bar?select=foo moo,goo";
let expected_sql = "SELECT \"foo moo\", \"goo\" FROM \"bar\"";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(encode(from_url), select.to_url().unwrap());

// Column names with percent-encoded spaces are allowed in a URL:
let from_url = "bar?select=foo%20moo,goo%20hoo";
let expected_sql = "SELECT \"foo moo\", \"goo hoo\" FROM \"bar\"";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());

// Quoted strings in column names are not allowed in a URL.
let from_url = "bar?select=\"foo moo\",goo";
let result = parse(from_url);
assert!(result.is_err());

// Quoted strings in column names are not allowed in a URL, not even when the quotes are
// percent-encoded.
let from_url = "bar?select=%22foo%20moo%22,goo";
let result = parse(from_url);
assert!(result.is_err());
```

#### Select all columns from the table, bar, with filtering.
```rust
// Column names in filters are handled similarly to column names in select clauses.
let from_url = "bar?\
                column 1=eq.5\
                &column_2=eq.10\
                &column%203=eq.30";
let expected_sql = "SELECT * FROM \"bar\" \
                    WHERE \"column 1\" = 5 \
                    AND \"column_2\" = 10 \
                    AND \"column 3\" = 30";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());

// Quotes are not allowed in column names in URLs.
let from_url = "bar?\"column 1\"=eq.5";
let result = parse(from_url);
assert!(result.is_err());

// Quotes are not allowed in column names in URLs, even when they are encoded.
let from_url = "bar?%22column 1%22=eq.5";
let result = parse(from_url);
assert!(result.is_err());
```

#### Literals and NULLs
<i>Double quotes may be used when specifying literal string values. This is mandatory if
a number is required to be interpreted as a string, e.g., 'foo=eq.\"10\"' (otherwise, in
'foo=eq.10', 10 is interpreted as a number). Note that all literal string values will be
rendered within single quotes in SQL. When converting the parsed Select struct back to a
URL, these values will never be enclosed in double-quotes in the URL except for the case
of a numeric string or a string containing one of the reserved chars
(see [RESERVED](RESERVED)).</i>
```rust
let from_url = "bar?c1=eq.Henry%20Kissinger\
                &c2=in.(\"McMahon, Jim\",William Perry,\"72\",Nancy,NULL)\
                &c3=eq.Fred";
let expected_sql = "SELECT * FROM \"bar\" WHERE \"c1\" = 'Henry Kissinger' \
                    AND \"c2\" IN ('McMahon, Jim', 'William Perry', '72', 'Nancy', NULL) \
                    AND \"c3\" = 'Fred'";
let select = parse(&from_url).unwrap();
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());

// NULL values are treated differently from literal values (i.e., they are not rendered in
// quotes). Note also that they are not converted to uppercase in the generated SQL:
let from_url = "bar?select=c1,c2&c1=not_eq.null";
let expected_sql = "SELECT \"c1\", \"c2\" FROM \"bar\" WHERE \"c1\" <> null";
let select = parse(&from_url).unwrap();
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(from_url, decode(&select.to_url().unwrap()).unwrap());
```

#### ORDER BY, LIMIT, and OFFSET
```rust
// Columns in order_by clauses are handled similarly to table and column names:
let from_url = "bar?order=foo1.asc,foo2.desc,foo3.asc";
let expected_sql =
    "SELECT * FROM \"bar\" ORDER BY \"foo1\" ASC, \"foo2\" DESC, \"foo3\" ASC";
let select = parse(&from_url).unwrap();
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(from_url, decode(&select.to_url().unwrap()).unwrap());

let from_url = "bar?order=foo%201.asc,foo 2.desc,foo3.asc";
let expected_sql =
    "SELECT * FROM \"bar\" ORDER BY \"foo 1\" ASC, \"foo 2\" DESC, \"foo3\" ASC";
let select = parse(&from_url).unwrap();
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(decode(from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());

// Arguments to `limit` and `order` must be interger-valued:
let from_url = "bar?order=foo1.desc,foo 2.asc,foo%205.desc&limit=10&offset=30";
let expected_sql = "SELECT * FROM \"bar\" \
                    ORDER BY \"foo1\" DESC, \"foo 2\" ASC, \"foo 5\" DESC \
                    LIMIT 10 OFFSET 30";
let select = parse(from_url).unwrap();
assert_eq!(expected_sql, select.to_sqlite().unwrap());
assert_eq!(expected_sql, select.to_postgres().unwrap());
```

#### A more complicated example:
```rust

let from_url = "a%20bar?\
                select=foo1,foo 2,foo%205\
                &foo1=eq.0\
                &foo 2=not_eq.\"10\"\
                &foo3=lt.20\
                &foo4=gt.5\
                &foo%205=lte.30\
                &foo6=gte.60\
                &foo7=like.alpha\
                &foo8=not_like.abby normal\
                &foo9=ilike.beta\
                &foo10=not_ilike.gamma\
                &foo11=is.NULL\
                &foo12=not_is.NULL\
                &foo13=eq.terrible\
                &foo14=in.(A fancy hat,\"5\",C page 21,delicious,NULL)\
                &foo15=not_in.(1,2,3)\
                &order=foo1.desc,foo 2.asc,foo%205.desc\
                &limit=10\
                &offset=30";

let expected_sql = "SELECT \"foo1\", \"foo 2\", \"foo 5\" \
                    FROM \"a bar\" \
                    WHERE \"foo1\" = 0 \
                    AND \"foo 2\" <> '10' \
                    AND \"foo3\" < 20 \
                    AND \"foo4\" > 5 \
                    AND \"foo 5\" <= 30 \
                    AND \"foo6\" >= 60 \
                    AND \"foo7\" LIKE 'alpha' \
                    AND \"foo8\" NOT LIKE 'abby normal' \
                    AND \"foo9\" ILIKE 'beta' \
                    AND \"foo10\" NOT ILIKE 'gamma' \
                    AND \"foo11\" IS NOT DISTINCT FROM NULL \
                    AND \"foo12\" IS DISTINCT FROM NULL \
                    AND \"foo13\" = 'terrible' \
                    AND \"foo14\" IN ('A fancy hat', '5', 'C page 21', 'delicious', NULL) \
                    AND \"foo15\" NOT IN (1, 2, 3) \
                    ORDER BY \"foo1\" DESC, \"foo 2\" ASC, \"foo 5\" DESC \
                    LIMIT 10 \
                    OFFSET 30";

let select = parse(&from_url).unwrap();
assert_eq!(expected_sql, select.to_postgres().unwrap());
assert_eq!(decode(&from_url).unwrap(), decode(&select.to_url().unwrap()).unwrap());
```
