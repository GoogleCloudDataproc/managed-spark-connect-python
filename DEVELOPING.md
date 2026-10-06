# Developing

To get started, create a fresh `venv` or conda environment. Then, update `pip`
and install the required dev dependencies within the new environment.

```sh
pip install -U pip
pip install -r requirements-dev.txt

# Install the test dependencies to run unit and integration tests.
pip install -r requirements-test.txt
```

# Linting/formatting

We use `pyink` to lint/format the code. To apply changes to your local
environment, run:

```sh
pyink .
```

This will ensure that your changes pass CI code linting.

# Testing

We use `pytest` for testing. To run _all_ tests, simply run `pytest`. For this
to work, you have to have your ambient environment configured to correctly
resolve all configuration details such as GCP project, region, etc.

To make testing more deterministic, it is instead recommended to specify
configuration details on the command line. For example:

```sh
env \
  GOOGLE_CLOUD_PROJECT='project-id' \
  GOOGLE_CLOUD_REGION='us-central1' \
  MANAGED_SPARK_CONNECT_SUBNET='subnet-id' \
  pytest --tb=auto -v
```

## Testing the Interactive Extras

The dependencies of the interactive extras (`explore_dataframe`, `%dpip`,
`%%sparksql`) live in the `interactive` extra (`pip install '.[interactive]'`),
and `requirements-dev.txt` already includes them. The tests in
`tests/unit/test_init.py` assume `google-colabsqlviz`, `ipython`, `sqlparse`
and `traitlets` are importable and will fail, not skip, if they are not.
Missing dependencies are simulated within the tests themselves, by setting
`sys.modules[name] = None`.

`tests/local` runs `%%sparksql` end to end against local classic and Spark
Connect sessions. It needs no Google Cloud project, only Java 17 or 21 (set
`JAVA_HOME` if your default `java` is another version, or the tests skip), and
starts a JVM in-process, so run it on its own rather than under `xdist`:

```sh
pytest tests/local
```

The integration tests in particular can take a while to run. To speed up the
testing cycle, you can run them in parallel. You can do so using the `xdist`
plugin by setting the `-n` flag to the number of parallel runners you want to
use. This will be set automatically if you set it to `auto`. For example:

```sh
env \
  GOOGLE_CLOUD_PROJECT='project-id' \
  GOOGLE_CLOUD_REGION='us-central1' \
  MANAGED_SPARK_CONNECT_SUBNET='subnet-id' \
  MANAGED_SPARK_CONNECT_SERVICE_ACCOUNT='service@account.test' \
  pytest -n auto --tb=auto -v
```
