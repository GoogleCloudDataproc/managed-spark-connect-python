# Managed Spark Connect Client

A wrapper of the Apache [Spark Connect](https://spark.apache.org/spark-connect/)
client with additional functionalities that allow applications to communicate
with a remote Managed Spark Session using the Spark Connect protocol without
requiring additional steps.

## Install

```sh
pip install managed_spark_connect
```

## Uninstall

```sh
pip uninstall managed_spark_connect
```

## Setup

This client requires permissions to
manage [Managed Spark Sessions and Runtime Profiles](https://cloud.google.com/dataproc-serverless/docs/concepts/iam).

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
   pip install -U managed-spark-connect
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
   spark = ManagedSparkSession.builder.projectId('my-project').location('us-central1').dataprocSessionConfig(session_config).getOrCreate()
   ```

### Builder Configuration

The `ManagedSparkSession.builder` provides a fluent API to configure the session. Below is a list of available methods:

| Method | Description |
|--------|-------------|
| `config(key, value)` | Sets a Spark configuration property. |
| `dataprocSessionConfig(dataproc_config)` | Sets the [Dataproc Session](https://docs.cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.Session) configuration object. |
| `dataprocSessionId(session_id)` | Sets a custom session ID for creating or reusing sessions. |
| `idleTtl(duration)` | Sets the idle time-to-live (idle TTL) for the session using a `datetime.timedelta` object. |
| `label(key, value)` | Adds a single label to the session. |
| `labels(labels)` | Adds multiple labels to the session. |
| `location(location)` | Sets the Google Cloud region. |
| `projectId(project_id)` | Sets the Google Cloud project ID. |
| `runtimeProfile(profile)` | Sets the Runtime Profile to use. |
| `runtimeVersion(version)` | Sets the Managed Spark runtime version (e.g., "3.0"). |
| `serviceAccount(account)` | Sets the service account for the session. |
| `subnetwork(subnet)` | Sets the subnetwork URI for the session. |
| `ttl(duration)` | Sets the time-to-live (TTL) for the session using a `datetime.timedelta` object. |

### Reusing Named Sessions Across Notebooks

Named sessions allow you to share a single Spark session across multiple notebooks, improving efficiency by avoiding repeated session startup times and reducing costs.

To create or connect to a named session:

1. Create a session with a custom ID in your first notebook:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   session_id = 'my-ml-pipeline-session'
   spark = ManagedSparkSession.builder.dataprocSessionId(session_id).getOrCreate()
   df = spark.createDataFrame([(1, 'data')], ['id', 'value'])
   df.show()
   ```

2. Reuse the same session in another notebook by specifying the same session ID:

   ```python
   from google.cloud.managed_spark_connect import ManagedSparkSession
   session_id = 'my-ml-pipeline-session'
   spark = ManagedSparkSession.builder.dataprocSessionId(session_id).getOrCreate()
   df = spark.createDataFrame([(2, 'more-data')], ['id', 'value'])
   df.show()
   ```

3. Session IDs must be 4-63 characters long, start with a lowercase letter, contain only lowercase letters, numbers, and hyphens, and not end with a hyphen.

4. Named sessions persist until explicitly terminated or reach their configured TTL.

5. A session with a given ID that is in a TERMINATED state cannot be reused. It must be deleted before a new session with the same ID can be created.

### Using Spark SQL Magic Commands (Jupyter Notebooks)

The package supports the [sparksql-magic](https://github.com/cryeo/sparksql-magic) library for executing Spark SQL queries directly in Jupyter notebooks.

**Installation**: To use magic commands, install the required dependencies manually:
```bash
pip install managed-spark-connect
pip install IPython sparksql-magic
```

1. Load the magic extension:
   ```python
   %load_ext sparksql_magic
   ```

2. Configure default settings (optional):
   ```python
   %config SparkSql.limit=20
   ```

3. Execute SQL queries:
   ```python
   %%sparksql
   SELECT * FROM your_table
   ```

4. Advanced usage with options:
   ```python
   # Cache results and create a view
   %%sparksql --cache --view result_view df
   SELECT * FROM your_table WHERE condition = true
   ```

Available options:
- `--cache` / `-c`: Cache the DataFrame
- `--eager` / `-e`: Cache with eager loading
- `--view VIEW` / `-v VIEW`: Create a temporary view
- `--limit N` / `-l N`: Override default row display limit
- `variable_name`: Store result in a variable

See [sparksql-magic](https://github.com/cryeo/sparksql-magic) for more examples.

**Note**: Magic commands are optional. If you only need basic ManagedSparkSession functionality without Jupyter magic support, install only the base package:
```bash
pip install managed-spark-connect
```

## Migrating from dataproc-spark-connect

The `dataproc-spark-connect` package and the `google.cloud.dataproc_spark_connect` module have been renamed to `managed-spark-connect` / `google.cloud.managed_spark_connect`, and `DataprocSparkSession` has been renamed to `ManagedSparkSession`. The old import path and class name still work but emit a `DeprecationWarning` — update your imports when convenient:

```python
# Before
from google.cloud.dataproc_spark_connect import DataprocSparkSession

# After
from google.cloud.managed_spark_connect import ManagedSparkSession
```

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
