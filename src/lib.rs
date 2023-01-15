use regex::Regex;
use sqlx::any::{AnyKind, AnyPool};
use std::collections::HashMap;

/// Enum used to hold a parameter value, which can be a string or an integer, for binding
/// to placeholders in SQL statements.
#[derive(Debug, Clone)]
pub enum SqlParam {
    SignedInt(i64),
    String(String),
}

/// Given a database pool and a SQL string containing a number of placeholders, `{key}`, where `key`
/// corresponds to one of the keys in the given parameter map, return a string and a vector of
/// SqlParams, where every placeholder `{key}` in the original SQL string is replaced in the
/// returned string by the placeholder corresponding to the type of the database pool.
/// For example, given a string: "SELECT column FROM table WHERE column IN ({beta}, {alpha})", and
/// given the parameter map: {"alpha": SqlParam::String("green"), "beta": SqlParam::String("red")},
/// then in the case of a SQLite database, this function will return the String
/// "SELECT column FROM table WHERE column IN (?, ?)" along with the Vector
/// [SqlParam::String("red"), SqlParam::String("green")]. In the case of a PostgreSQL database,
/// the returned vector will be the same but the string will be
/// "SELECT column FROM table WHERE column IN ($1, $2)".
pub fn bind_sql<'a>(
    pool: &AnyPool,
    sql: &str,
    param_map: &'a HashMap<&str, SqlParam>,
) -> Result<(String, Vec<&'a SqlParam>), String> {
    // This regex will find quoted strings as well as variables of the form `{key}` where `key` is
    // a token consisting of word characters and/or underscores:
    let rx = Regex::new(r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")|\B\{[\w_]+\}\B"#)
        .unwrap();

    // The variables that will be returned:
    let mut param_vec = vec![];
    let mut final_sql = String::from("");

    let mut pg_param_idx = 1;
    let mut saved_start = 0;
    for m in rx.find_iter(sql) {
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
pub fn local_sql_syntax(pool: &AnyPool, sql_param: &str, sql: &str) -> String {
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
    for m in rx.find_iter(sql) {
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
/// (1) PostgreSQL, which uses numbered variables $N, e.g., SELECT 1 FROM foo WHERE bar IN ($1, $2)
/// (2) SQLite, which uses question marks, e.g., SELECT 1 FROM foo WHERE bar IN (?, ?)
pub fn interpolate_sql(
    pool: &AnyPool,
    sql: &str,
    params: &Vec<&SqlParam>,
    placeholder_str: Option<&str>,
) -> Result<String, String> {
    let mut final_sql = String::from("");
    let mut saved_start = 0;

    let quotes = r#"('[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*")"#;
    let rx;
    if let Some(s) = placeholder_str {
        rx = Regex::new(&format!(r#"{}|\b{}\b"#, quotes, s)).unwrap();
    } else if pool.any_kind() == AnyKind::Postgres {
        rx = Regex::new(&format!(r#"{}|\B[$]\d+\b"#, quotes)).unwrap();
    } else {
        rx = Regex::new(&format!(r#"{}|\B[?]\B"#, quotes)).unwrap();
    }

    let mut param_index = 0;
    for m in rx.find_iter(sql) {
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
                        SqlParam::String(param) => final_sql.push_str(&format!("'{}'", param)),
                        SqlParam::SignedInt(param) => final_sql.push_str(&format!("{}", param)),
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
    use futures::executor::block_on;
    use indoc::indoc;
    use sqlx::{
        any::{AnyConnectOptions, AnyPool, AnyPoolOptions},
        query as sqlx_query, Row,
    };
    use std::{collections::HashMap, str::FromStr};

    fn bind_execute_interpolate_sql(pool: &AnyPool) {
        let drop_query = sqlx_query(r#"DROP TABLE IF EXISTS "test""#);
        block_on(drop_query.execute(pool)).unwrap();

        let create_query = sqlx_query(
            r#"CREATE TABLE "test" ("table" TEXT, "row_num" BIGINT, "col1" TEXT, "col2" TEXT)"#,
        );
        block_on(create_query.execute(pool)).unwrap();

        let insert_query = sqlx_query(r#"INSERT INTO "test" VALUES ('foo', 1, 'bar', 'bar')"#);
        block_on(insert_query.execute(pool)).unwrap();

        let test_sql = indoc! {r#"
               SELECT "table", "row_num", "col1", "col2"
               FROM "test"
               WHERE "table" = {table}
                 AND "row_num" = {row_num}
                 AND "col1" = {column}
                 AND "col2" = {column}
            "#};
        let mut test_params = HashMap::new();
        test_params.insert("table", SqlParam::String("foo".to_string()));
        test_params.insert("row_num", SqlParam::SignedInt(1));
        test_params.insert("column", SqlParam::String("bar".to_string()));
        let (test_sql, test_params) = bind_sql(pool, &test_sql, &test_params).unwrap();
        let mut test_query = sqlx_query(&test_sql);
        for param in &test_params {
            match param {
                SqlParam::String(s) => test_query = test_query.bind(s),
                SqlParam::SignedInt(n) => test_query = test_query.bind(n),
            };
        }

        let row = block_on(test_query.fetch_one(pool)).unwrap();
        let table: &str = row.get("table");
        assert_eq!(table, "foo");
        let row_num: i64 = row.get("row_num");
        assert_eq!(row_num, 1);
        let col1: &str = row.get("col1");
        assert_eq!(col1, "bar");
        let col2: &str = row.get("col2");
        assert_eq!(col2, "bar");

        let interpolated_sql = interpolate_sql(pool, &test_sql, &test_params, None).unwrap();
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
    }

    #[test]
    fn sqlite_bind_execute_interpolate_sql() {
        let connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
                .unwrap();
        bind_execute_interpolate_sql(&pool);
    }

    #[test]
    fn sqlite_localise_interpolate_sql() {
        let connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
                .unwrap();
        let generic_sql = r#"SELECT "foo" FROM "bar" WHERE "xyzzy" IN (VAL, VAL, VAL)"#;
        let local_sql = local_sql_syntax(&pool, "VAL", generic_sql);
        assert_eq!("SELECT \"foo\" FROM \"bar\" WHERE \"xyzzy\" IN (?, ?, ?)", local_sql);
    }

    #[test]
    fn postgres_bind_execute_interpolate_sql() {
        let connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
                .unwrap();
        bind_execute_interpolate_sql(&pool);
    }

    #[test]
    fn postgres_localise_interpolate_sql() {
        let connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
                .unwrap();
        let generic_sql = r#"SELECT "foo" FROM "bar" WHERE "xyzzy" IN (VAL, VAL, VAL)"#;
        let local_sql = local_sql_syntax(&pool, "VAL", generic_sql);
        assert_eq!("SELECT \"foo\" FROM \"bar\" WHERE \"xyzzy\" IN ($1, $2, $3)", local_sql);
    }
}
