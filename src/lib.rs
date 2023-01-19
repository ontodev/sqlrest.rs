use enquote::unquote;
use futures::executor::block_on;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as SerdeValue;
use sqlx::{
    any::{AnyKind, AnyPool},
    query as sqlx_query, Row,
};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
pub enum Operator {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessThanEquals,
    GreaterThanEquals,
    Like,
    ILike,
    Is,
    IsNot,
    In,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Direction {
    fn to_sql(&self) -> &str {
        match self {
            Direction::Ascending => "ASC",
            Direction::Descending => "DESC",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Filter {
    lhs: String,
    operator: Operator,
    rhs: SerdeValue,
}

impl Filter {
    pub fn clone(filter: &Filter) -> Filter {
        Filter { ..filter.clone() }
    }

    fn to_sql(&self, pool: &AnyPool) -> Result<String, String> {
        let not_a_string_err = format!("RHS of filter: {:?} is not a string.", self);
        let not_a_string_or_number_err =
            format!("RHS of filter: {:?} is not a string or a number.", self);
        match self.operator {
            Operator::Equals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} = {}", self.lhs, s)),
                SerdeValue::Number(n) => Ok(format!("{} = {}", self.lhs, n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::NotEquals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} <> {}", self.lhs, s)),
                SerdeValue::Number(n) => Ok(format!("{} <> {}", self.lhs, n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::LessThan => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} < {}", self.lhs, s)),
                SerdeValue::Number(n) => Ok(format!("{} < {}", self.lhs, n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::GreaterThan => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} > {}", self.lhs, s)),
                SerdeValue::Number(n) => Ok(format!("{} > {}", self.lhs, n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::LessThanEquals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} <= {}", self.lhs, s)),
                SerdeValue::Number(n) => Ok(format!("{} <= {}", self.lhs, n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::GreaterThanEquals => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} >= {}", self.lhs, s)),
                SerdeValue::Number(n) => Ok(format!("{} >= {}", self.lhs, n)),
                _ => Err(not_a_string_or_number_err),
            },
            Operator::Like => match &self.rhs {
                SerdeValue::String(s) => Ok(format!("{} LIKE {}", self.lhs, s)),
                _ => Err(not_a_string_err),
            },
            Operator::ILike => match &self.rhs {
                SerdeValue::String(s) => {
                    if pool.any_kind() == AnyKind::Postgres {
                        Ok(format!("{} ILIKE {}", self.lhs, s))
                    } else {
                        Ok(format!("LOWER({}) LIKE LOWER({})", self.lhs, s))
                    }
                }
                _ => Err(not_a_string_err),
            },
            Operator::Is => {
                let value = match &self.rhs {
                    SerdeValue::String(s) => s.to_string(),
                    SerdeValue::Number(n) => {
                        format!("{}", n)
                    }
                    _ => return Err(not_a_string_or_number_err),
                };
                if pool.any_kind() == AnyKind::Sqlite {
                    Ok(format!("{} IS {}", self.lhs, value))
                } else {
                    Ok(format!("{} IS NOT DISTINCT FROM {}", self.lhs, value))
                }
            }
            Operator::IsNot => {
                let value = match &self.rhs {
                    SerdeValue::String(s) => s.to_string(),
                    SerdeValue::Number(n) => {
                        format!("{}", n)
                    }
                    _ => return Err(not_a_string_or_number_err),
                };
                if pool.any_kind() == AnyKind::Sqlite {
                    Ok(format!("{} IS NOT {}", self.lhs, value))
                } else {
                    Ok(format!("{} IS DISTINCT FROM {}", self.lhs, value))
                }
            }
            Operator::In => match &self.rhs {
                SerdeValue::Array(options) => {
                    let mut values = vec![];
                    for option in options {
                        match option {
                            SerdeValue::String(s) => values.push(format!("{}", s)),
                            SerdeValue::Number(n) => values.push(format!("{}", n)),
                            _ => {
                                return Err(format!(
                                    "{:?} is not an array of strings or numbers.",
                                    options
                                ))
                            }
                        };
                    }
                    let value_list = format!("({})", values.join(", "));
                    let filter_sql = format!("{} IN {}", self.lhs, value_list);
                    Ok(filter_sql)
                }
                _ => Err(format!("RHS of filter: {:?} is not an array.", self)),
            },
        }
    }
}

pub fn filters_to_sql(filters: &Vec<Filter>, pool: &AnyPool) -> Result<String, String> {
    let mut parts: Vec<String> = vec![];
    for filter in filters {
        match filter.to_sql(pool) {
            Ok(sql) => parts.push(sql),
            Err(err) => return Err(err),
        };
    }
    let joiner = " AND ";
    Ok(parts.join(joiner))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Select {
    table: String,
    select: Vec<String>,
    filters: Vec<Filter>,
    group_by: Vec<String>,
    having: Vec<Filter>,
    order_by: Vec<(String, Direction)>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl Select {
    pub fn new() -> Select {
        Default::default()
    }

    pub fn clone(select: &Select) -> Select {
        Select { ..select.clone() }
    }

    pub fn to_sql(&self, pool: &AnyPool) -> Result<String, String> {
        let mut select_columns = vec![];
        if self.select.is_empty() {
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
            let query = sqlx_query(&sql);
            let rows = block_on(query.fetch_all(pool)).unwrap();
            for row in &rows {
                let cname: &str = row.get("name");
                select_columns.push(String::from(cname));
            }
        } else {
            select_columns = self.select.clone();
        }
        let select_clause = select_columns.join(", ");

        let mut sql = String::from("");
        if select_clause != "" && self.table != "" {
            sql.push_str(&format!("SELECT {} FROM {}", select_clause, self.table));
        }

        if !self.filters.is_empty() {
            let where_clause = match filters_to_sql(&self.filters, &pool) {
                Err(err) => return Err(err),
                Ok(r) => r,
            };
            sql.push_str(&format!(" WHERE {}", where_clause));
        }

        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }

        if !self.having.is_empty() {
            let having_clause = match filters_to_sql(&self.having, &pool) {
                Err(err) => return Err(err),
                Ok(r) => r,
            };
            sql.push_str(&format!(" HAVING {}", having_clause));
        }

        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_strings = self
                .order_by
                .iter()
                .map(|(col, dir)| format!("{} {}", col, dir.to_sql()))
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

    pub fn table<S: Into<String>>(&mut self, table: S) -> &mut Select {
        self.table = table.into();
        self
    }

    pub fn select<S: Into<String>>(&mut self, select: Vec<S>) -> &mut Select {
        for s in select {
            self.select.push(s.into());
        }
        self
    }

    pub fn add_select<S: Into<String>>(&mut self, select: S) -> &mut Select {
        self.select.push(select.into());
        self
    }

    pub fn filters<F: Into<Filter>>(&mut self, filters: Vec<F>) -> &mut Select {
        for f in filters {
            self.filters.push(f.into());
        }
        self
    }

    pub fn add_filter<F: Into<Filter>>(&mut self, filter: F) -> &mut Select {
        self.filters.push(filter.into());
        self
    }

    pub fn having<F: Into<Filter>>(&mut self, filters: Vec<F>) -> &mut Select {
        for f in filters {
            self.having.push(f.into());
        }
        self
    }

    pub fn add_having<F: Into<Filter>>(&mut self, filter: F) -> &mut Select {
        self.having.push(filter.into());
        self
    }

    pub fn group_by<S: Into<String>>(&mut self, group_by: Vec<S>) -> &mut Select {
        for s in group_by {
            self.group_by.push(s.into());
        }
        self
    }

    pub fn add_group_by<S: Into<String>>(&mut self, group_by: S) -> &mut Select {
        self.group_by.push(group_by.into());
        self
    }

    pub fn order_by<S: Into<String>>(&mut self, order_by: Vec<(S, Direction)>) -> &mut Select {
        for (s, d) in order_by {
            self.order_by.push((s.into(), d));
        }
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Select {
        self.limit = Some(limit);
        self
    }

    pub fn offset(&mut self, offset: usize) -> &mut Select {
        self.offset = Some(offset);
        self
    }
}

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
    use indoc::indoc;
    use serde_json::Number as SerdeNumber;
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

    #[test]
    fn select() {
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pg_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
                .unwrap();

        let sq_connection_options = AnyConnectOptions::from_str(
            "sqlite:///home/mike/Knocean/ontodev_demo/valve.rs/build/valve.db?mode=ro",
        )
        .unwrap();
        let sq_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
                .unwrap();

        // TODO: Clean up below.

        let select = Select {
            table: String::from("\"table1\""),
            select: vec![],
            filters: vec![],
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: Some(10),
            offset: None,
        };
        eprintln!("{}", select.to_sql(&pg_pool).unwrap());
        eprintln!("{}", select.to_sql(&sq_pool).unwrap());

        eprintln!("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

        let mut select = Select::new();
        eprintln!("NEW SELECT: '{}'", select.to_sql(&pg_pool).unwrap());
        select.limit(11);
        select.offset(50);
        select.table("\"table1\"".to_string());
        select.select(vec!["\"prefix\"".to_string(), "\"base\"".to_string()]);
        select.add_select("\"ontology IRI\"".to_string());
        select.add_select("COUNT(1)".to_string());
        select.add_select("SUM(1)".to_string());
        select.filters(vec![Filter {
            lhs: String::from(r#""prefix""#),
            operator: Operator::Equals,
            rhs: SerdeValue::String("{prefix}".to_string()),
        }]);

        select.add_filter(Filter {
            lhs: String::from(r#""base""#),
            operator: Operator::Is,
            rhs: SerdeValue::String("{base}".to_string()),
        });
        select.add_filter(Filter {
            lhs: String::from(r#""base""#),
            operator: Operator::GreaterThan,
            rhs: SerdeValue::Number(SerdeNumber::from(22)),
        });
        select.add_filter(Filter {
            lhs: r#""ontology IRI""#.to_string(),
            operator: Operator::In,
            rhs: SerdeValue::Array(vec![
                SerdeValue::String("{ontiri1}".to_string()),
                SerdeValue::String("{ontiri2}".to_string()),
                SerdeValue::Number(SerdeNumber::from(23)),
            ]),
        });
        select.order_by(vec![
            ("base".to_string(), Direction::Ascending),
            ("prefix".to_string(), Direction::Descending),
        ]);
        select.group_by(vec![r#""prefix""#.to_string()]);
        select.add_group_by(r#""base""#.to_string());
        select.add_group_by(r#""ontology IRI""#.to_string());
        select.having(vec![Filter {
            lhs: String::from("COUNT(1)"),
            operator: Operator::GreaterThan,
            rhs: SerdeValue::Number(SerdeNumber::from(1)),
        }]);
        select.add_having(Filter {
            lhs: String::from("SUM(1)"),
            operator: Operator::GreaterThan,
            rhs: SerdeValue::Number(SerdeNumber::from(1)),
        });
        eprintln!("{}", select.to_sql(&pg_pool).unwrap());
        eprintln!("{}", select.to_sql(&sq_pool).unwrap());
    }
}
