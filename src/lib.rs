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

/// Representation of an operator of an SQL query.
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

/// The direction of an ORDER BY clause in an SQL query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Direction {
    /// Converts a Direction enum to a SQL string.
    pub fn to_sql(&self) -> &str {
        match self {
            Direction::Ascending => "ASC",
            Direction::Descending => "DESC",
        }
    }
}

/// Representation of a filter in an SQL query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Filter {
    lhs: String,
    operator: Operator,
    rhs: SerdeValue,
}

impl Filter {
    /// Clone the given filter.
    pub fn clone(filter: &Filter) -> Filter {
        Filter { ..filter.clone() }
    }

    /// Use the given database connection pool to convert the given filter into an SQL string
    /// suitable to be used in a WHERE clause, using the syntax appropriate to the kind of database
    /// represented by the pool. Currently supported databases are PostgreSQL and SQLite.
    pub fn to_sql(&self, pool: &AnyPool) -> Result<String, String> {
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
                    let filter_sql = format!("{} IN {}", self.lhs, value_list);
                    Ok(filter_sql)
                }
                _ => Err(format!("RHS of filter: {:?} is not an array.", self)),
            },
        }
    }
}

/// Given a list of filters and a database connection pool, convert each filter to an SQL string and
/// join them together using the keyword AND.
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

/// A structure to represent an SQL select statement.
/// Examples:
/// ```rust,ignore
/// // Initialise a new Select object using struct syntax:
/// let select = Select {
///     // When names require quotation marks (e.g. if they contain a space), then these must be
///     // supplied by the user:
///     table: String::from(r#""my table""#),
///     // If the select field is empty, then the database will be queried when converting the
///     // struct to SQL, so as to include all of the table's defined columns in the SELECT clause.
///     select: vec![],
///     filters: vec![],
///     group_by: vec![],
///     having: vec![],
///     order_by: vec![],
///     limit: Some(10),
///     offset: None,
/// };
///
/// let expected_sql = format!(
///     r#"{} {}"#,
///     r#"SELECT "my column 1", "my column 2", "my column 3", "my column 4""#,
///     r#"FROM "my table" LIMIT 10"#
/// );
/// assert_eq!(select.to_sql(&postgresql_pool).unwrap(), expected_sql);
/// assert_eq!(select.to_sql(&sqlite_pool).unwrap(), expected_sql);
///
/// // Initialise a new Select object by calling new() and then progressively add further
/// // information.
/// let mut select = Select::new();
/// select.table(r#""a table name with spaces""#.to_string());
/// select.select(vec!["foo".to_string(), r#""a column name with spaces""#.to_string()]);
/// select.add_select("bar".to_string());
/// select.add_select("COUNT(1)".to_string());
/// select.filters(vec![Filter {
///     lhs: String::from("foo"),
///     operator: Operator::Is,
///     rhs: SerdeValue::String("{foo}".to_string()),
/// }]);
/// select.add_filter(Filter {
///     lhs: "bar".to_string(),
///     operator: Operator::In,
///     rhs: SerdeValue::Array(vec![
///         SerdeValue::String("{val1}".to_string()),
///         SerdeValue::String("{val2}".to_string()),
///     ]),
/// });
/// select.order_by(vec![
///     ("foo".to_string(), Direction::Ascending),
///     ("bar".to_string(), Direction::Descending),
/// ]);
/// select.group_by(vec!["foo".to_string()]);
/// select.add_group_by(r#""a column name with spaces""#.to_string());
/// select.add_group_by("bar".to_string());
/// select.having(vec![Filter {
///     lhs: String::from("COUNT(1)"),
///     operator: Operator::GreaterThan,
///     rhs: SerdeValue::Number(SerdeNumber::from(1)),
/// }]);
/// select.limit(11);
/// select.offset(50);
/// assert_eq!(
///     select.to_sql(&postgresql_pool).unwrap(),
///     format!(
///         "{} {} {} {} {} {} {}",
///         r#"SELECT foo, "a column name with spaces", bar, COUNT(1)"#,
///         r#"FROM "a table name with spaces""#,
///         r#"WHERE foo IS NOT DISTINCT FROM {foo} AND bar IN ({val1}, {val2})"#,
///         r#"GROUP BY foo, "a column name with spaces", bar"#,
///         r#"HAVING COUNT(1) > 1"#,
///         r#"ORDER BY foo ASC, bar DESC"#,
///         r#"LIMIT 11 OFFSET 50"#
///     )
/// );
///
/// assert_eq!(
///     select.to_sql(&sqlite_pool).unwrap(),
///     format!(
///         "{} {} {} {} {} {} {}",
///         r#"SELECT foo, "a column name with spaces", bar, COUNT(1)"#,
///         r#"FROM "a table name with spaces""#,
///         r#"WHERE foo IS {foo} AND bar IN ({val1}, {val2})"#,
///         r#"GROUP BY foo, "a column name with spaces", bar"#,
///         r#"HAVING COUNT(1) > 1"#,
///         r#"ORDER BY foo ASC, bar DESC"#,
///         r#"LIMIT 11 OFFSET 50"#
///     )
/// );
/// ```
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
    /// Create a new Select struct with private fields initialized to their default values.
    pub fn new() -> Select {
        Default::default()
    }

    /// Clone the given Select struct.
    pub fn clone(select: &Select) -> Select {
        Select { ..select.clone() }
    }

    /// Given a database connection pool, convert the given Select struct to an SQL statement,
    /// using the syntax appropriate for the kind of database connected to by the pool. Currently
    /// supported databases are PostgreSQL and SQLite. If the given Select struct has an empty
    /// `select` field, then the columns of the given table are looked up in the database and all
    /// of them are explicitly added to the SELECT statement generated.
    pub fn to_sql(&self, pool: &AnyPool) -> Result<String, String> {
        if self.table == "" {
            return Err("Cannot convert Select to SQL: Missing required field: table".to_string());
        }

        let mut select_columns = vec![];
        // If `self.select` is empty, look up the columns corresponding to the table in the db:
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
                select_columns.push(format!(r#""{}""#, cname));
            }
        } else {
            select_columns = self.select.clone();
        }

        if select_columns.is_empty() {
            return Err(format!("Could not find any columns for table '{}'", self.table));
        }

        let select_clause = select_columns.join(", ");
        let mut sql = format!("SELECT {} FROM {}", select_clause, self.table);
        if !self.filters.is_empty() {
            let where_clause = match filters_to_sql(&self.filters, &pool) {
                Err(err) => return Err(err),
                Ok(s) => s,
            };
            sql.push_str(&format!(" WHERE {}", where_clause));
        }
        if !self.group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by.join(", ")));
        }
        if !self.having.is_empty() {
            let having_clause = match filters_to_sql(&self.having, &pool) {
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

    /// Given a table name, set `self.table` to that name.
    pub fn table<S: Into<String>>(&mut self, table: S) -> &mut Select {
        self.table = table.into();
        self
    }

    /// Given a vector of column names, replace the current contents of `self.select` with the
    /// contents of the given vector.
    pub fn select<S: Into<String>>(&mut self, select: Vec<S>) -> &mut Select {
        self.select.clear();
        for s in select {
            self.select.push(s.into());
        }
        self
    }

    /// Given a column name, add it to the vector, `self.select`.
    pub fn add_select<S: Into<String>>(&mut self, select: S) -> &mut Select {
        self.select.push(select.into());
        self
    }

    /// Given a vector of filters, replace the current contents of `self.filters` with the contents
    /// of the given vector.
    pub fn filters<F: Into<Filter>>(&mut self, filters: Vec<F>) -> &mut Select {
        self.filters.clear();
        for f in filters {
            self.filters.push(f.into());
        }
        self
    }

    /// Given a filter, add it to the vector, `self.filters`.
    pub fn add_filter<F: Into<Filter>>(&mut self, filter: F) -> &mut Select {
        self.filters.push(filter.into());
        self
    }

    /// Given a vector of filters, replace the current contents of `self.having` with the contents
    /// of the given vector.
    pub fn having<F: Into<Filter>>(&mut self, filters: Vec<F>) -> &mut Select {
        self.having.clear();
        for f in filters {
            self.having.push(f.into());
        }
        self
    }

    /// Given a filter, add it to the vector, `self.having`.
    pub fn add_having<F: Into<Filter>>(&mut self, filter: F) -> &mut Select {
        self.having.push(filter.into());
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
    param_map: &'a HashMap<&str, SerdeValue>,
) -> Result<(String, Vec<&'a SerdeValue>), String> {
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
    params: &Vec<&SerdeValue>,
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
        test_params.insert("table", SerdeValue::String("foo".to_string()));
        test_params.insert("row_num", SerdeValue::Number(SerdeNumber::from(1)));
        test_params.insert("column", SerdeValue::String("bar".to_string()));
        let (test_sql, test_params) = bind_sql(pool, &test_sql, &test_params).unwrap();
        let mut test_query = sqlx_query(&test_sql);
        for param in &test_params {
            match param {
                SerdeValue::String(s) => test_query = test_query.bind(s),
                SerdeValue::Number(n) => test_query = test_query.bind(n.as_i64()),
                _ => panic!("{} is not a string or a number.", param),
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
        let postgresql_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
                .unwrap();

        let sq_connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let sqlite_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
                .unwrap();

        let drop_table1 = "DROP TABLE IF EXISTS my_table";
        let create_table1 = r#"CREATE TABLE "my_table" (
                                 "row_number" BIGINT,
                                 "prefix" TEXT,
                                 "base" TEXT,
                                 "ontology IRI" TEXT,
                                 "version IRI" TEXT
                               )"#;
        let drop_table2 = r#"DROP TABLE IF EXISTS "a table name with spaces""#;
        let create_table2 = r#"CREATE TABLE "a table name with spaces" (
                                 "foo" TEXT,
                                 "a column name with spaces" TEXT,
                                 "bar" TEXT
                               )"#;

        for pool in vec![&sqlite_pool, &postgresql_pool] {
            for sql in vec![drop_table1, create_table1, drop_table2, create_table2] {
                let query = sqlx_query(sql);
                block_on(query.execute(pool)).unwrap();
            }
        }

        let select = Select {
            table: String::from("my_table"),
            select: vec![
                "row_number".to_string(),
                "prefix".to_string(),
                "base".to_string(),
                r#""ontology IRI""#.to_string(),
                r#""version IRI""#.to_string(),
            ],
            filters: vec![],
            group_by: vec![],
            having: vec![],
            order_by: vec![],
            limit: Some(10),
            offset: None,
        };
        let expected_sql = format!(
            r#"{} {}"#,
            r#"SELECT row_number, prefix, base, "ontology IRI", "version IRI""#,
            "FROM my_table LIMIT 10"
        );
        assert_eq!(select.to_sql(&postgresql_pool).unwrap(), expected_sql);
        assert_eq!(select.to_sql(&sqlite_pool).unwrap(), expected_sql);

        let mut select = Select::new();
        select.table(r#""a table name with spaces""#.to_string());
        select.select(vec!["foo".to_string(), r#""a column name with spaces""#.to_string()]);
        select.add_select("bar".to_string());
        select.add_select("COUNT(1)".to_string());
        select.filters(vec![Filter {
            lhs: String::from("foo"),
            operator: Operator::Is,
            rhs: SerdeValue::String("{foo}".to_string()),
        }]);
        select.add_filter(Filter {
            lhs: "bar".to_string(),
            operator: Operator::In,
            rhs: SerdeValue::Array(vec![
                SerdeValue::String("{val1}".to_string()),
                SerdeValue::String("{val2}".to_string()),
            ]),
        });
        select.order_by(vec![
            ("foo".to_string(), Direction::Ascending),
            ("bar".to_string(), Direction::Descending),
        ]);
        select.group_by(vec!["foo".to_string()]);
        select.add_group_by(r#""a column name with spaces""#.to_string());
        select.add_group_by("bar".to_string());
        select.having(vec![Filter {
            lhs: String::from("COUNT(1)"),
            operator: Operator::GreaterThan,
            rhs: SerdeValue::Number(SerdeNumber::from(1)),
        }]);
        select.limit(11);
        select.offset(50);
        let postgresql_sql = select.to_sql(&postgresql_pool).unwrap();
        assert_eq!(
            postgresql_sql,
            format!(
                "{} {} {} {} {} {} {}",
                r#"SELECT foo, "a column name with spaces", bar, COUNT(1)"#,
                r#"FROM "a table name with spaces""#,
                r#"WHERE foo IS NOT DISTINCT FROM {foo} AND bar IN ({val1}, {val2})"#,
                r#"GROUP BY foo, "a column name with spaces", bar"#,
                r#"HAVING COUNT(1) > 1"#,
                r#"ORDER BY foo ASC, bar DESC"#,
                r#"LIMIT 11 OFFSET 50"#
            )
        );

        let sqlite_sql = select.to_sql(&sqlite_pool).unwrap();
        assert_eq!(
            sqlite_sql,
            format!(
                "{} {} {} {} {} {} {}",
                r#"SELECT foo, "a column name with spaces", bar, COUNT(1)"#,
                r#"FROM "a table name with spaces""#,
                r#"WHERE foo IS {foo} AND bar IN ({val1}, {val2})"#,
                r#"GROUP BY foo, "a column name with spaces", bar"#,
                r#"HAVING COUNT(1) > 1"#,
                r#"ORDER BY foo ASC, bar DESC"#,
                r#"LIMIT 11 OFFSET 50"#
            )
        );

        let mut param_map = HashMap::new();
        param_map.insert("foo", SerdeValue::String("foo_val".to_string()));
        param_map.insert("val1", SerdeValue::String("bar_val1".to_string()));
        param_map.insert("val2", SerdeValue::String("bar_val2".to_string()));

        let (sql, params) = bind_sql(&postgresql_pool, &postgresql_sql, &param_map).unwrap();
        assert_eq!(
            sql,
            format!(
                "{} {} {} {} {} {} {}",
                r#"SELECT foo, "a column name with spaces", bar, COUNT(1)"#,
                r#"FROM "a table name with spaces""#,
                r#"WHERE foo IS NOT DISTINCT FROM $1 AND bar IN ($2, $3)"#,
                r#"GROUP BY foo, "a column name with spaces", bar"#,
                r#"HAVING COUNT(1) > 1"#,
                r#"ORDER BY foo ASC, bar DESC"#,
                r#"LIMIT 11 OFFSET 50"#
            )
        );
        match params[0] {
            SerdeValue::String(s) if s == "foo_val" => assert!(true),
            _ => assert!(false, "{} != 'foo_val'", params[0]),
        };
        match params[1] {
            SerdeValue::String(s) if s == "bar_val1" => assert!(true),
            _ => assert!(false, "{} != 'bar_val1'", params[1]),
        };
        match params[2] {
            SerdeValue::String(s) if s == "bar_val2" => assert!(true),
            _ => assert!(false, "{} != 'bar_val2'", params[2]),
        };
        let mut query = sqlx_query(&sql);
        for param in &params {
            match param {
                SerdeValue::String(s) => query = query.bind(s),
                SerdeValue::Number(n) => query = query.bind(n.as_i64()),
                _ => panic!("{} is not a string or a number.", param),
            };
        }
        block_on(query.execute(&postgresql_pool)).unwrap();

        let (sql, params) = bind_sql(&sqlite_pool, &sqlite_sql, &param_map).unwrap();
        assert_eq!(
            sql,
            format!(
                "{} {} {} {} {} {} {}",
                r#"SELECT foo, "a column name with spaces", bar, COUNT(1)"#,
                r#"FROM "a table name with spaces""#,
                r#"WHERE foo IS ? AND bar IN (?, ?)"#,
                r#"GROUP BY foo, "a column name with spaces", bar"#,
                r#"HAVING COUNT(1) > 1"#,
                r#"ORDER BY foo ASC, bar DESC"#,
                r#"LIMIT 11 OFFSET 50"#
            )
        );
        match params[0] {
            SerdeValue::String(s) if s == "foo_val" => assert!(true),
            _ => assert!(false, "{} != 'foo_val'", params[0]),
        };
        match params[1] {
            SerdeValue::String(s) if s == "bar_val1" => assert!(true),
            _ => assert!(false, "{} != 'bar_val1'", params[1]),
        };
        match params[2] {
            SerdeValue::String(s) if s == "bar_val2" => assert!(true),
            _ => assert!(false, "{} != 'bar_val2'", params[2]),
        };
        let mut query = sqlx_query(&sql);
        for param in &params {
            match param {
                SerdeValue::String(s) => query = query.bind(s),
                SerdeValue::Number(n) => query = query.bind(n.as_i64()),
                _ => panic!("{} is not a string or a number.", param),
            };
        }
        block_on(query.execute(&sqlite_pool)).unwrap();
    }
}
