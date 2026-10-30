# Managed Spark Connect Client

A wrapper of the Apache [Spark Connect](https://spark.apache.org/spark-connect/)
client with additional functionalities that allow applications to communicate
with a remote Managed Spark Session using the Spark Connect protocol without
requiring additional steps.

## Install

For notebooks and interactive development, install with the `client` and
`interactive` extras to get the lightweight `pyspark-client` package along with
the interactive extras (`explore_dataframe()`, `%dpip`, `%%sparksql`):

```sh
pip install 'google-cloud-spark-connect[client,interactive]'
```

Other installation options:

* `pip install 'google-cloud-spark-connect[client]'`: Installs `pyspark-client`
  without the interactive extras (for standalone scripts or applications).
* `pip install 'google-cloud-spark-connect[spark]'` (or
  `'[spark,interactive]'`): Installs the full `pyspark[connect]` distribution
  instead of `pyspark-client`, for environments that also need a local Spark JVM
  or already have full `pyspark` installed.
* `pip install google-cloud-spark-connect` (or `'[interactive]'`): Does not
  install a Spark package, relying on whichever `pyspark` or `pyspark-client` is
  already present in your environment (such as pre-installed Dataproc or Colab
  runtime images). Use this in Dataproc batch and Serverless runtimes, which
  must keep their bundled `pyspark`.

> **Note:** Avoid installing both `pyspark` and `pyspark-client` in the same
> Python environment. Both packages write to the `pyspark` module namespace, so
> mixing them can cause version skew or break the remaining package if one is
> later uninstalled. In particular, never combine the `client` and `spark`
> extras (`'[client,spark]'`).

### Python versions

This package requires Python 3.10 or later. To use Python UDFs (including
`applyInPandas`, `mapInPandas`, and similar APIs), the client's Python minor
version must match the Python of the Managed Spark runtime the session runs on,
for example Python 3.12 for runtime 3.0. The client warns when they differ.

## Uninstall

```sh
pip uninstall google-cloud-spark-connect pyspark-client
```

`pip uninstall` never removes dependencies, so this also names the
`pyspark-client` that the `client` extra installed. If you installed the `spark`
extra instead, uninstall `pyspark` too if nothing else needs it.

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
   pip install -U 'google-cloud-spark-connect[client,interactive]'
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
| `instrumentation(inst)` | Records what the library does on your behalf; see [Instrumentation (opt-in)](#instrumentation-opt-in). |

### Configuration (environment variables)

These environment variables change how the client behaves.

| Variable | Default | Effect |
|----------|---------|--------|
| `MANAGED_SPARK_CONNECT_REPLAY_ON_POSITION_LOST` | `1` | When a read query (`collect`, `toPandas`, `count`, ...) fails with `INVALID_CURSOR.POSITION_NOT_AVAILABLE` because the server no longer buffers the result the client has to resume from, re-run it once under a new operation id and log a `WARNING`. Commands (writes, DDL) are never re-run. Set to `0` to disable, e.g. for queries that are expensive or not deterministic. Read when the session is created. |
| `MANAGED_SPARK_CONNECT_WS_SILENCE_LIMIT_S` | `300` | Cut the proxy WebSocket connection if an RPC is in flight and the server stays silent (no frames received) for this many seconds, so a wedged stream fails instead of hanging. The check runs on each 60 s WebSocket receive poll, so the cut happens between this limit and the limit plus 60 s. Set to `0` to disable. |
| `MANAGED_SPARK_CONNECT_LIVENESS_TTL_SECONDS` | unset | Before an RPC, the client checks that the Dataproc session is still alive. Once a check finds it `ACTIVE`, that answer is reused for this many seconds: unset or negative reuses it until an RPC fails, `0` checks before every RPC. A failed RPC always triggers a fresh check. The session only counts as gone when Dataproc reports it not found or in a terminal state; when the check itself fails (for example during a control-plane outage), the RPC proceeds. |

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

5. If a session with the specified ID is already in a `TERMINATED` or `FAILED` state (or is `TERMINATING`), the client emits a warning, deletes the old session, and creates a new session with the same ID. Deleting the old session removes its previous metadata and Spark UI history from Dataproc, though logs remain available in Cloud Logging.

### Instrumentation (opt-in)

The library can record what it does on your behalf — Dataproc session
lifecycle steps, every Spark Connect gRPC call (with and without the local
proxy's overhead), the proxy's own connections, and the client-side plan and
result conversions — as timed *spans*. It is off by default; opt in via
`ManagedSparkSession.builder.instrumentation(...)` or the
`MANAGED_SPARK_CONNECT_INSTRUMENTATION_FILE` and
`MANAGED_SPARK_CONNECT_INSTRUMENTATION_LOG` environment variables.

See [docs/instrumentation.md](https://github.com/GoogleCloudDataproc/managed-spark-connect-python/blob/main/docs/instrumentation.md)
for usage, the API, the record schema, and the span and event catalogues.

### Interactive Extras

For notebooks and other interactive use, include the `interactive` extra (along
with `client` or `spark`, unless PySpark is already in your environment):

```sh
pip install 'google-cloud-spark-connect[client,interactive]'
```

When you import the package inside an IPython kernel, it then automatically sets
up a few interactive conveniences:

- `explore_dataframe()` from
  [google-colabsqlviz](https://pypi.org/project/google-colabsqlviz/) is injected
  into your notebook globals.
- The [`%dpip` and `%%sparksql`](#bundled-magics) magics are loaded.

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

### Bundled Magics

These are loaded with the interactive extras, or explicitly with
`%load_ext google.cloud.managed_spark_magics`.

#### `%dpip`

Installs PyPI packages on the session's driver and executors, so your UDFs can
import them:

```python
%dpip install spacy==3.8.4 torch
```

See [docs/dpip.md](https://github.com/GoogleCloudDataproc/managed-spark-connect-python/blob/main/docs/dpip.md)
for details.

#### `%%sparksql`

Runs Spark SQL against the active session and displays the result as a table,
with `:name` binding notebook variables as SQL values:

```python
%%sparksql
SELECT * FROM IDENTIFIER(:table) WHERE id > :min_id
```

See [docs/sparksql.md](https://github.com/GoogleCloudDataproc/managed-spark-connect-python/blob/main/docs/sparksql.md)
for options, passing Python values (including `IDENTIFIER()` for table names)
and more.

## Migrating from dataproc-spark-connect

The `dataproc-spark-connect` package has been renamed to `google-cloud-spark-connect`. This is a breaking change with no compatibility shims — you need to update your code in the following places when you switch to the new package.

### 1. Update the package you install

`dataproc-spark-connect` previously pulled in full `pyspark` by default. When
upgrading an existing environment in-place, use the `spark` extra so `pip`
reuses your existing `pyspark` installation without installing `pyspark-client`
alongside it (along with `interactive` for the interactive extras):

```sh
# Before
pip install dataproc-spark-connect

# After (upgrading an existing environment with pyspark already installed)
pip install 'google-cloud-spark-connect[spark,interactive]'
```

In a fresh environment (or after running `pip uninstall dataproc-spark-connect pyspark`),
prefer the lightweight `client` extra instead:

```sh
pip install 'google-cloud-spark-connect[client,interactive]'
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
