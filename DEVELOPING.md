# Developing

To get started, create a fresh virtual environment (`uv venv` or `python -m venv`)
and install the development and test dependencies:

```sh
# Using uv (recommended):
uv pip install -r requirements-dev.txt

# Or using pip:
pip install -U pip
pip install -r requirements-dev.txt
```

`requirements-dev.txt` installs the package in editable mode with the `[client]`
and `[interactive]` extras (`-e .[client,interactive]`), along with
`requirements-test.txt` (`pytest`, `pytest-xdist`), `ipywidgets`, and `pyink`.

# Linting/formatting

We use `pyink` to lint/format the code. To apply changes to your local
environment, run:

```sh
pyink .
```

This will ensure that your changes pass CI code linting.

# Testing

We use `pytest` for testing. To run unit tests in parallel:

```sh
pytest -n auto tests/unit
```

To run integration tests against Google Cloud Managed Spark, provide your
project and region (plus optional subnet or service account configuration):

```sh
env \
  GOOGLE_CLOUD_PROJECT='project-id' \
  GOOGLE_CLOUD_REGION='us-central1' \
  pytest tests/integration --tb=short -v
```

`MANAGED_SPARK_CONNECT_SUBNET` is optional (defaults to `"default"`) and accepts
either a short subnetwork name in `GOOGLE_CLOUD_REGION` (e.g., `default`) or a
full `projects/.../regions/.../subnetworks/...` URI. Integration tests share a
session-scoped default cluster and automatically set safety TTLs
(`MANAGED_SPARK_CONNECT_TTL_SECONDS=1800` and
`MANAGED_SPARK_CONNECT_IDLE_TTL_SECONDS=600`), so run `tests/integration`
without `-n auto` (~8 minutes total).

## Testing the Interactive Extras

The dependencies of the interactive extras (`explore_dataframe`, `%dpip`,
`%%sparksql`) live in the `interactive` extra
(`pip install '.[client,interactive]'`), and `requirements-dev.txt` already
includes them along with `pyspark-client`. The tests in
`tests/unit/test_init.py` assume `google-colabsqlviz`, `ipython`, `sqlparse`
and `traitlets` are importable and will fail, not skip, if they are not.
Missing dependencies are simulated within the tests themselves, by setting
`sys.modules[name] = None`.

`tests/local` runs `%%sparksql` and local session creation end to end against
local classic and Spark Connect sessions. It needs no Google Cloud project, only
Java 17 or 21 (set `JAVA_HOME` if your default `java` is another version, or the
tests skip), and starts a JVM in-process, so run it on its own rather than under
`xdist`:

```sh
pytest tests/local
```

## Testing Against Full PySpark

`requirements-dev.txt` installs the lightweight `pyspark-client`, under which
`tests/local` skips. To run local JVM Spark tests, switch the environment (or
create a separate virtual environment) with the `[spark]` extra. Both packages
write to the same `pyspark` directory, so always uninstall both before
installing either:

```sh
uv pip uninstall pyspark-client pyspark
uv pip install -e '.[spark,interactive]' -r requirements-test.txt
```

To switch back to `pyspark-client`:

```sh
uv pip uninstall pyspark-client pyspark
uv pip install -e '.[client,interactive]' -r requirements-test.txt
```
