# `%%sparksql`: Spark SQL Magic

`%%sparksql` runs Spark SQL against the active session, classic or Spark
Connect, and displays the result as a table.

```python
%%sparksql
SELECT * FROM your_table
```

It is one of the [interactive extras](../README.md#interactive-extras), so it is
loaded when you import `google.cloud.managed_spark_connect` in a notebook. You
can also load it explicitly:

```python
%load_ext google.cloud.managed_spark_magics
```

## Options

By default, results are rendered as an interactive table with `colabsqlviz`,
which paginates through all rows and lets you switch to charts. To render a
plain HTML table instead (showing the first 20 rows by default), pass `-d html`
per cell or configure it for the whole notebook:

```python
%config SparkSql.display = "html"
```

To cap the number of rows displayed, pass `-l N` per cell or set:

```python
%config SparkSql.limit = 100
```

Options go on the first line, optionally naming a variable to store the
resulting DataFrame in (by default, `%%sparksql` stores it in `_df`). The
variable receives the full, un-truncated `DataFrame` (the `-l` display limit
only affects what is shown on screen):

```python
%%sparksql --cache --view result_view df
SELECT * FROM your_table WHERE condition = true
```

- `-d MODE` / `--display MODE`: How to display the result (`colabsqlviz` /
  `viz`, the default, or `html`; configurable via `SparkSql.display`). If
  `google-colabsqlviz` is not installed, `%%sparksql` falls back to `html`.
- `-l N` / `--limit N`: Maximum rows to display (default: `SparkSql.limit`;
  when unset, `colabsqlviz` paginates all rows and `html` shows 20).
- `-v VIEW` / `--view VIEW`: Register or replace a temporary view with this name.
- `-c` / `--cache`: Cache the resulting DataFrame (lazily). Redundant with `-e`,
  whose checkpoint is already stored.
- `-e` / `--eager`: Compute the result immediately via `localCheckpoint(eager=True)`.
- `-q` / `--quiet`: Suppress table display.
- `--explain [MODE]`: Print the query plan instead of displaying results (`simple`,
  `extended`, `codegen`, `cost`, or `formatted`, the default).
- `-s NAME` / `--session NAME`: Run against the SparkSession in variable `NAME`.
  Otherwise the active session is used, or else the only SparkSession in the notebook.

### Line form (`%sparksql`)

As a line magic, `%sparksql` takes options followed by the SQL query on the
same line. It is always `--quiet` (it does not display the table, so `-d` and
`-l` are rejected) and returns the resulting DataFrame directly instead of
taking a variable name argument:

```python
df = %sparksql SELECT * FROM your_table
```

## Passing Python values

Notebook variables are passed into queries using either `:name` or `{name}`:

- **`:name` (SQL literals)**: Binds the notebook variable `name` as a typed SQL
  parameter marker (via `spark.sql(args=...)`), passed directly to Spark
  without string interpolation or SQL injection risk.
- **`IDENTIFIER(:name)` (Catalog & column identifiers)**: In SQL grammar,
  parameter markers only substitute for literal values, not table or column
  identifiers. Use Spark's built-in `IDENTIFIER()` clause to parameterize table,
  view, or column names from a string variable:

  ```python
  table, min_id = "my_catalog.db.events", 100
  ```

  ```python
  %%sparksql
  SELECT * FROM IDENTIFIER(:table) WHERE id > :min_id
  ```

- **`{name}` (DataFrames)**: References a Spark `DataFrame` notebook variable
  to query as a relation:

  ```python
  %%sparksql
  SELECT country, count(*) FROM {events_df} GROUP BY country
  ```

`:name` and `{name}` are only recognized in SQL expression positions; text
inside string literals (e.g. `'{not_a_variable}'` or `':id'`) and SQL comments
is preserved verbatim.

## Multiple statements and errors

A cell can hold multiple statements separated by `;`. They run in sequence,
and only the final statement's result is displayed or stored in the target
variable. If an intermediate statement fails, earlier statements have already
executed. A cell starting with `BEGIN` is passed through whole as a Spark SQL
compound script.

When a statement fails, `%%sparksql` prints a clean summary with Spark's error
condition, message, SQLSTATE, and a caret pointing at the offending line in the
cell, without JVM or Python wrapper tracebacks. To re-raise the original
exception with the full traceback, set:

```python
%config SparkSql.full_traceback = True
```
