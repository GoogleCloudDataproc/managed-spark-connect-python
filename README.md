# Managed Spark Connect Client

A wrapper of the Apache [Spark Connect](https://spark.apache.org/spark-connect/)
client with additional functionalities that allow applications to communicate
with a remote Managed Spark Session using the Spark Connect protocol without
requiring additional steps.

## Install

```sh
pip install google-cloud-spark-connect
```

## Uninstall

```sh
pip uninstall google-cloud-spark-connect
```

## Setup

This client requires permissions to
manage [Managed Spark Sessions and Session Templates](https://cloud.google.com/dataproc-serverless/docs/concepts/iam).

If you are running the client outside of Google Cloud, you need to provide
authentication credentials. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable to point to
your [Application Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc)
file.

You can specify the project and region either via environment variables or directly
in your code using the builder API:

* Environment variables: `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_REGION`
* Builder API: `.projectId()` and `.location()` methods (recommended)

## Usage

1. Install the latest version of Managed Spark Connect:

   ```sh
   pip install -U google-cloud-spark-connect
   ```

2. Add the required imports into your PySpark application or notebook and start
   a Spark session using the fluent API:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   spark = ManagedSparkSession.builder.getOrCreate()
   ```

3. You can configure Spark properties using the `.config()` method:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   spark = ManagedSparkSession.builder.config('spark.executor.memory', '4g').config('spark.executor.cores', '2').getOrCreate()
   ```

4. For advanced configuration, you can use the `Session` class to customize
   settings like subnetwork or other environment configurations:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   from google.cloud.dataproc_v1 import Session
   session_config = Session()
   session_config.environment_config.execution_config.subnetwork_uri = '<subnet>'
   session_config.runtime_config.version = '3.0'
   spark = ManagedSparkSession.builder.projectId('my-project').location('us-central1').sessionConfig(session_config).getOrCreate()
   ```

### Builder Configuration

The `ManagedSparkSession.builder` provides a fluent API to configure the session. Below is a list of available methods:

| Method | Description |
|--------|-------------|
| `config(key, value)` | Sets a Spark configuration property. |
| `sessionConfig(session_config)` | Sets the [Dataproc Session](https://docs.cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Session) configuration object. |
| `sessionId(session_id)` | Sets a custom session ID for creating or reusing sessions. |
| `idleTtl(duration)` | Sets the idle time-to-live (idle TTL) for the session using a `datetime.timedelta` object. |
| `label(key, value)` | Adds a single label to the session. |
| `labels(labels)` | Adds multiple labels to the session. |
| `location(location)` | Sets the Google Cloud region. |
| `projectId(project_id)` | Sets the Google Cloud project ID. |
| `runtimeVersion(version)` | Sets the Managed Spark runtime version (e.g., "3.0"). |
| `serviceAccount(account)` | Sets the service account for the session. |
| `sessionTemplate(profile)` | Sets the Session Template to use. |
| `subnetwork(subnet)` | Sets the subnetwork URI for the session. |
| `ttl(duration)` | Sets the time-to-live (TTL) for the session using a `datetime.timedelta` object. |

### Reusing Named Sessions Across Notebooks

Named sessions allow you to share a single Spark session across multiple notebooks, improving efficiency by avoiding repeated session startup times and reducing costs.

To create or connect to a named session:

1. Create a session with a custom ID in your first notebook:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   session_id = 'my-ml-pipeline-session'
   spark = ManagedSparkSession.builder.sessionId(session_id).getOrCreate()
   df = spark.createDataFrame([(1, 'data')], ['id', 'value'])
   df.show()
   ```

2. Reuse the same session in another notebook by specifying the same session ID:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   session_id = 'my-ml-pipeline-session'
   spark = ManagedSparkSession.builder.sessionId(session_id).getOrCreate()
   df = spark.createDataFrame([(2, 'more-data')], ['id', 'value'])
   df.show()
   ```

3. Session IDs must be 4-63 characters long, start with a lowercase letter, contain only lowercase letters, numbers, and hyphens, and not end with a hyphen.

4. Named sessions persist until explicitly terminated or reach their configured TTL.

5. A session with a given ID that is in a TERMINATED state cannot be reused. It must be deleted before a new session with the same ID can be created.

### Interactive Extras

For notebooks and other interactive use, install the `interactive` extra:

```sh
pip install 'google-cloud-spark-connect[interactive]'
```

When you import the package inside an IPython kernel, it then automatically sets
up a few interactive conveniences:

- `explore_dataframe()` from
  [google-colabsqlviz](https://pypi.org/project/google-colabsqlviz/) is injected
  into your notebook globals.
- The `%dpip` line magic is loaded.
- The [`%%sparksql`](#spark-sql-magic-sparksql) magic is loaded.

```python
import google.cloud.managed_spark_connect  # extras load here
```

Each extra is only set up if its dependency is importable, so without the
`interactive` extra you get whichever of them your environment already happens
to provide. On import, a single line lists the extras that were loaded.

The extras won't override anything you've already set up, for example if `explore_dataframe` is already present, or `%%sparksql` was loaded from somewhere else, it is left alone.

#### Opting out

Set the environment variable before starting the kernel:

```sh
export MANAGED_SPARK_CONNECT_ENABLE_EXTRAS=false
```

Alternatively, configure it through IPython, either persistently in
`~/.ipython/profile_default/ipython_config.py`:

```python
c.ManagedSparkConnect.enable_extras = False
```

Or at runtime:

```python
%config ManagedSparkConnect.enable_extras = False
```

An explicit IPython setting takes precedence over the environment variable.

### Spark SQL Magic (`%%sparksql`)

`%%sparksql` runs Spark SQL against the active session, classic or Spark
Connect, and displays the result as a table. It is one of the interactive
extras, so with the `interactive` extra installed it is loaded on import; you
can also load it explicitly with `%load_ext google.cloud.managed_spark_magics`.

```python
%%sparksql
SELECT * FROM your_table
```

The cell displays the first 20 rows by default. Change that limit per cell with
`-l N`, or globally for the notebook with:

```python
%config SparkSql.limit = 100
```

Options go on the first line, optionally naming a variable to store the
resulting DataFrame in. When a variable name is provided, it receives the full,
un-truncated `DataFrame` (the `-l` display limit only affects what is shown on
screen):

```python
%%sparksql --cache --view result_view df
SELECT * FROM your_table WHERE condition = true
```

- `-l N` / `--limit N`: Maximum rows to display (default: `SparkSql.limit`, 20).
- `-v VIEW` / `--view VIEW`: Register or replace a temporary view with this name.
- `-c` / `--cache`: Cache the resulting DataFrame (lazily).
- `-e` / `--eager`: Compute the result immediately via `localCheckpoint(eager=True)`.
- `-q` / `--quiet`: Suppress table display.
- `--explain [MODE]`: Print the query plan instead of displaying results (`simple`,
  `extended`, `codegen`, `cost`, or `formatted`, the default).
- `-s NAME` / `--session NAME`: Run against the SparkSession in variable `NAME`.
  Otherwise the active session is used, or else the only SparkSession in the notebook.

To return the DataFrame directly for assignment or method chaining instead of
displaying a table, use the line magic form `%sparksql` (which is always quiet):

```python
df = %sparksql SELECT * FROM your_table
```

#### Passing Python values

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

#### Multiple statements and errors

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

## Migrating from dataproc-spark-connect

The `dataproc-spark-connect` package has been renamed to `google-cloud-spark-connect`. This is a breaking change with no compatibility shims — you need to update your code in the following places when you switch to the new package.

### 1. Update the package you install

```sh
# Before
pip install dataproc-spark-connect

# After
pip install google-cloud-spark-connect
```

### 2. Update your imports and session class

`google.cloud.dataproc_spark_connect` is now `google.cloud.managed_spark_connect`, and `DataprocSparkSession` is now `ManagedSparkSession`:

```python
# Before
from google.cloud.dataproc_spark_connect import DataprocSparkSession
spark = DataprocSparkSession.builder.getOrCreate()

# After
from google.cloud.managed_spark_connect import ManagedSparkSession
spark = ManagedSparkSession.builder.getOrCreate()
```

If you use the Jupyter magic commands, `google.cloud.dataproc_magics` is now `google.cloud.managed_spark_magics` and `DataprocMagics` is now `ManagedSparkMagics` (the `%dpip` magic itself is unchanged).

### 3. Rename `dataprocSessionConfig(...)` and `dataprocSessionId(...)` calls

These builder methods drop the `dataproc` prefix — they take the same arguments and behave identically, only the names change:

```python
# Before
spark = (
    DataprocSparkSession.builder
        .dataprocSessionId('my-session')
        .dataprocSessionConfig(session_config)
        .getOrCreate()
)

# After
spark = (
    ManagedSparkSession.builder
        .sessionId('my-session')
        .sessionConfig(session_config)
        .getOrCreate()
)
```

### 4. Rename any `DATAPROC_SPARK_CONNECT_*` environment variables

If you set any of the library's own environment variables (as opposed to standard GCP ones like `GOOGLE_CLOUD_PROJECT`), rename the `DATAPROC_SPARK_CONNECT_` prefix to `MANAGED_SPARK_CONNECT_`:

| Before | After |
|--------|-------|
| `DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT` | `MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT` |
| `DATAPROC_SPARK_CONNECT_SUBNET` | `MANAGED_SPARK_CONNECT_SUBNET` |
| `DATAPROC_SPARK_CONNECT_AUTH_TYPE` | `MANAGED_SPARK_CONNECT_AUTH_TYPE` |
| `DATAPROC_SPARK_CONNECT_TTL_SECONDS` | `MANAGED_SPARK_CONNECT_TTL_SECONDS` |
| `DATAPROC_SPARK_CONNECT_IDLE_TTL_SECONDS` | `MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS` |
| `DATAPROC_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT` | `MANAGED_SPARK_CONNECT_SESSION_TERMINATE_AT_EXIT` |
| `DATAPROC_SPARK_CONNECT_DEFAULT_DATASOURCE` | `MANAGED_SPARK_CONNECT_DEFAULT_DATASOURCE` |
| `DATAPROC_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH` | `MANAGED_SPARK_CONNECT_ACTIVE_SESSION_FILE_PATH` |

Note that `GOOGLE_CLOUD_DATAPROC_API_ENDPOINT` and other variables naming the actual Dataproc API (not this library's own config) are unchanged.

## Developing

For development instructions see [guide](DEVELOPING.md).

## Contributing

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

### Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

### Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.
