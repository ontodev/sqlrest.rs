use enquote::unquote;
use futures::executor::block_on;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as SerdeValue;
use sqlx::{
    any::{AnyKind, AnyPool, AnyRow},
    query as sqlx_query, Row,
};
use std::{collections::HashMap, str::FromStr};

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
#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
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
        Ok(format!("{}{} LIKE {}", lhs.into(), negation, rhs.into()))
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
        if *dbtype == DbType::Postgres {
            Ok(format!("{}{} ILIKE {}", lhs.into(), negation, rhs.into()))
        } else {
            Ok(format!("LOWER({}){} LIKE LOWER({})", lhs.into(), negation, rhs.into()))
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
                SerdeValue::String(s) => Self::render_like_not_like(&self.lhs, s, true),
                _ => Err(not_a_string_err),
            },
            Operator::NotLike => match &self.rhs {
                SerdeValue::String(s) => Self::render_like_not_like(&self.lhs, s, false),
                _ => Err(not_a_string_err),
            },
            Operator::ILike => match &self.rhs {
                SerdeValue::String(s) => Self::render_ilike_not_ilike(dbtype, &self.lhs, s, true),
                _ => Err(not_a_string_err),
            },
            Operator::NotILike => match &self.rhs {
                SerdeValue::String(s) => Self::render_ilike_not_ilike(dbtype, &self.lhs, s, false),
                _ => Err(not_a_string_err),
            },
            Operator::Is => Self::render_is_not_is(dbtype, &self.lhs, &self.rhs, true),
            Operator::IsNot => Self::render_is_not_is(dbtype, &self.lhs, &self.rhs, false),
            Operator::In => match &self.rhs {
                SerdeValue::Array(options) => Self::render_in_not_in(&self.lhs, options, true),
                _ => Err(format!("RHS of filter: {:?} is not an array.", self)),
            },
            Operator::NotIn => match &self.rhs {
                SerdeValue::Array(options) => Self::render_in_not_in(&self.lhs, options, false),
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
/// Examples:
/// ```rust,ignore
/// // Initialise a new Select object using struct syntax.
/// let mut select = Select {
///     // When names require quotation marks (e.g. if they contain a space), then these must be
///     // supplied by the user:
///     table: String::from(r#""my table""#),
///     limit: Some(10),
///     ..Default::default()
/// };
/// // Query the database for the columns of the corresponding table and add them all to the
/// // `select` field:
/// select.select_all(&postgresql_pool);
/// let expected_sql = format!(
///     r#"{} {}"#,
///     r#"SELECT "my column 1", "my column 2", "my column 3", "my column 4""#,
///     r#"FROM "my table" LIMIT 10"#
/// );
/// assert_eq!(select.to_sql(&DbType::Postgres).unwrap(), expected_sql);
///
/// // Initialise a new Select object by calling new() and then progressively add further
/// // information. Note that method chaining is possible, as illustrated below, but you must
/// // first create the struct in a separate statement.
/// let mut select = Select::new(r#""a table name with spaces""#);
/// select
///     .select(vec!["foo", r#""a column name with spaces""#, "bar", "COUNT(1)"])
///     .filters(vec![
///         Filter::new("foo", "is", json!("{foo}")).unwrap(),
///         Filter::new("bar", "in", json!(["{val1}", "{val2}"])).unwrap(),
///     ])
///     .order_by(vec![("foo", Direction::Ascending), ("bar", Direction::Descending)])
///     .group_by(vec!["foo", r#""a column name with spaces""#, "bar"])
///     .having(vec![Filter::new("COUNT(1)", "gt", json!(1)).unwrap()])
///     .limit(11)
///     .offset(50);
///
/// // Note that the convenience methods, `to_sqlite()` and `to_postgres()` may be used instead
/// // of `to_sql(&DbType::Sqlite)` and `to_sql(&DbType::Postgres)`, respectively.
/// assert_eq!(
///     select.to_postgres().unwrap(),
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
/// assert_eq!(
///     select.to_sqlite().unwrap(),
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
    pub fn filters(&mut self, filters: Vec<Filter>) -> &mut Select {
        self.filters.clear();
        for f in filters {
            self.filters.push(f);
        }
        self
    }

    /// Given a filter, add it to the vector, `self.filters`.
    pub fn add_filter(&mut self, filter: Filter) -> &mut Select {
        self.filters.push(filter);
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
        if self.table == "" {
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
            self.select.push(format!(r#""{}""#, cname));
        }

        Ok(self)
    }

    /// Given a database type, convert the given Select struct to an SQL statement, using the syntax
    /// appropriate for the kind of database specified. Returns an Error if `self.table` or
    /// `self.select` have not been defined.
    pub fn to_sql(&self, dbtype: &DbType) -> Result<String, String> {
        if self.table == "" {
            return Err("Missing required field: `table` in to_sql()".to_string());
        }

        if self.select.is_empty() {
            return Err("Missing required field: `select` in to_sql()".to_string());
        }

        let select_clause = self.select.join(", ");
        let mut sql = format!("SELECT {} FROM {}", select_clause, self.table);
        if !self.filters.is_empty() {
            let where_clause = match filters_to_sql(&self.filters, &dbtype) {
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
    pub fn fetch_rows(
        &self,
        pool: &AnyPool,
        param_map: &HashMap<&str, SerdeValue>,
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

        let bind_result = bind_sql(pool, sql, param_map);
        if let Err(e) = bind_result {
            return Err(e);
        }
        let (sql, param_vec) = bind_result.unwrap();

        let mut query = sqlx_query(&sql);
        for param in &param_vec {
            match param {
                SerdeValue::String(s) => query = query.bind(s),
                SerdeValue::Number(n) => query = query.bind(n.as_i64()),
                _ => panic!("{} is not a string or a number.", param),
            };
        }

        match block_on(query.fetch_all(pool)) {
            Err(e) => Err(format!("{}", e)),
            Ok(k) => Ok(k),
        }
    }

    /// TODO: Add docstring here.
    pub fn fetch_rows_as_json(&self, _pool: &AnyPool, _param_map: &HashMap<&str, SerdeValue>) {
        // TODO: To be implemented.
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
        Err(e) => return Err(e),
        Ok(sql1) => match select2.to_sql(dbtype) {
            Err(e) => return Err(e),
            Ok(sql2) => return Ok(format!("WITH {} AS ({}) {}", select2.table, sql1, sql2)),
        },
    };
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
    use indoc::indoc;
    use serde_json::json;
    use serial_test::serial;
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
        test_params.insert("table", json!("foo"));
        test_params.insert("row_num", json!(1));
        test_params.insert("column", json!("bar"));
        let (test_sql, test_params) = bind_sql(pool, test_sql, &test_params).unwrap();
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
    #[serial]
    fn sqlite_bind_execute_interpolate_sql() {
        let connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
                .unwrap();
        bind_execute_interpolate_sql(&pool);
    }

    #[test]
    #[serial]
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
    #[serial]
    fn postgres_bind_execute_interpolate_sql() {
        let connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(connection_options))
                .unwrap();
        bind_execute_interpolate_sql(&pool);
    }

    #[test]
    #[serial]
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

    fn setup_for_select_test() -> (AnyPool, AnyPool) {
        // Setup database connections and create the needed tables:
        let pg_connection_options =
            AnyConnectOptions::from_str("postgresql:///valve_postgres").unwrap();
        let postgresql_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(pg_connection_options))
                .unwrap();

        let sq_connection_options = AnyConnectOptions::from_str("sqlite://:memory:").unwrap();
        let sqlite_pool =
            block_on(AnyPoolOptions::new().max_connections(5).connect_with(sq_connection_options))
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
        for pool in vec![&sqlite_pool, &postgresql_pool] {
            for sql in &vec![
                drop_table1,
                create_table1,
                insert_table1,
                drop_table2,
                create_table2,
                insert_table2,
            ] {
                let query = sqlx_query(sql);
                block_on(query.execute(pool)).unwrap();
            }
        }

        (sqlite_pool, postgresql_pool)
    }

    #[test]
    #[serial]
    fn select_to_sql() {
        /////////////////////////////
        // Create a Select object using struct syntax and convert it to an SQL string, testing both
        // Postgres and Sqlite:
        /////////////////////////////
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
    }

    #[test]
    #[serial]
    fn select_to_sql_and_execute() {
        let (sqlite_pool, postgresql_pool) = setup_for_select_test();

        /////////////////////////////
        // Create a Select object by initializing an empty object, progressively add subclauses,
        // and then convert it to an SQL string, in Postgres and SQLIte syntax. Finally, bind the
        // SQL and verify the bindings.
        /////////////////////////////
        // Progressively create a Select object:
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

        // Convert the Select to SQL in both Postgres and Sqlite:
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

            // Bind the SQL and verify the binding:
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

            // Create and execute the query:
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
    }

    #[test]
    #[serial]
    fn selects_to_sql_and_execute() {
        let (sqlite_pool, postgresql_pool) = setup_for_select_test();

        /////////////////////////////
        // Combine two selects
        /////////////////////////////
        let mut cte = Select::new("my_table");
        cte.select(vec!["prefix"]);
        // Note: When building a Select struct, chaining is possible but you must first create
        // a mutable struct in a separate statement before before chaining:
        let mut main_select = Select::new("cte");
        main_select.select(vec!["prefix"]).limit(10).offset(20);

        let sql = selects_to_sql(&cte, &main_select, &DbType::Postgres).unwrap();
        assert_eq!(
            sql,
            "WITH cte AS (SELECT prefix FROM my_table) SELECT prefix FROM cte LIMIT 10 OFFSET 20",
        );
        block_on(sqlx_query(&sql).execute(&sqlite_pool)).unwrap();
        block_on(sqlx_query(&sql).execute(&postgresql_pool)).unwrap();
    }

    #[test]
    #[serial]
    fn select_to_rows() {
        fn validate_rows(rows: &Vec<AnyRow>) {
            let num_rows = rows.len();
            for (i, row) in rows.iter().enumerate() {
                let foo: &str = row.get("foo");
                let spaces: &str = row.get("a column name with spaces");
                let bar: &str = row.get("bar");
                assert_eq!(foo.to_string(), format!("f{}", num_rows - i));
                assert_eq!(spaces.to_string(), format!("s{}", num_rows - i));
                assert_eq!(bar.to_string(), format!("b{}", num_rows - i));
            }
        }

        let (sqlite_pool, postgresql_pool) = setup_for_select_test();
        let mut select = Select::new(r#""a table name with spaces""#);
        select
            .select_all(&sqlite_pool)
            .expect("")
            .filters(vec![Filter::new("foo", "not_in", json!(["{foo1}", "{foo2}"])).unwrap()])
            .order_by(vec![("foo", Direction::Descending)]);

        let mut param_map = HashMap::new();
        param_map.insert("foo1", json!("f5"));
        param_map.insert("foo2", json!("f6"));
        let rows = select.fetch_rows(&sqlite_pool, &param_map).unwrap();
        validate_rows(&rows);
        let rows = select.fetch_rows(&postgresql_pool, &param_map).unwrap();
        validate_rows(&rows);
    }

    #[test]
    #[serial]
    fn select_to_json_rows() {
        // TODO: To be implemented.
    }
}
