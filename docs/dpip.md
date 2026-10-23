# `%dpip`: Install PyPI Packages into a Managed Spark Session

`%dpip` installs PyPI packages, with their dependencies, on the driver and
executors of the Managed Spark session in your notebook, so that UDFs and other
code running in Spark can import them.

```python
%dpip install spacy==3.8.4 torch
```

It is one of the [interactive extras](../README.md#interactive-extras), so it is
loaded when you import `google.cloud.managed_spark_connect` in a notebook. You
can also load it explicitly:

```python
%load_ext google.cloud.managed_spark_magics
```

## Usage

```
%dpip install <package> [<package> ...]
```

- Each package is a pip requirement: a name, optionally with a version
  specifier such as `==3.8.4` (quote any specifier containing `<` or `>`).
- pip options such as `--upgrade` are not supported.
- The notebook must have exactly one `ManagedSparkSession` in a variable;
  `%dpip` installs into that session.

`%dpip` only affects the remote session. To use a package in the notebook
itself, install it locally with `%pip` as well.

`%dpip` is equivalent to calling
`spark.addArtifacts("spacy==3.8.4", "torch", pypi=True)` on the session.
