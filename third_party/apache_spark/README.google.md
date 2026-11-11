# Apache Spark Connect `GetStatus` & Dataproc Cell Monitor Protos

This directory contains minimal protocol buffer definitions and generated Python
stubs derived from Apache Spark 4.2 (`dataproc-branch-4.2.0`), following
Google's non-google3 third-party policy (`go/thirdparty/non-google3`).

At wheel build time (`setup.py`), this directory is remapped into the private
package namespace
`google.cloud.managed_spark_connect._third_party.apache_spark` so no top-level
`third_party` package is ever installed into `site-packages/`.

## Source Provenance

- **Upstream Apache Spark Base Release:** [`v4.2.0`](https://github.com/apache/spark/tree/v4.2.0) (`32f7299601108917fb01920a54e084595b7b3bf8`)
- **Internal Dataproc Branch (`dataproc-branch-4.2.0` HEAD):** `298a4f5511997642dbe4cc3ee50303cf4e28b281` (`bigdataoss-internal/third_party/apache/spark`)
- **Internal Dataproc Source Commit:** `bb002ebc1834d6a587f9c1ee6bc1b11a7d74b748` (`refs/changes/87/196087/7`)

## Included Files & Local Modifications

- `LICENSE` & `NOTICE`: Apache License 2.0 and upstream Apache Spark `NOTICE`
  attribution file (also packaged into the wheel under
  `google/cloud/managed_spark_connect/_third_party/apache_spark/`).
- `METADATA`: Third-party compliance metadata (`devtools.metadata.Metadata`).
- `spark_connect_status.proto`: Extracted subset of
  `sql/connect/common/src/main/protobuf/spark/connect/base.proto` containing
  only `UserContext`, `GetStatusRequest`, `GetStatusResponse`, and
  `SparkConnectService.GetStatus` (`package spark.connect;`). Extracting only
  these messages avoids bundling ~15,000 lines of Spark Connect plan/expression
  protos already provided by `pyspark-client` / `pyspark`.
- `cell_monitor.proto`: Verbatim copy of
  `sql/connect/common/src/main/protobuf/dataproc/cellmonitor/v1/cell_monitor.proto`
  (`package dataproc.cellmonitor.v1;`) defining `CellMonitorTelemetryRequest`
  and `CellMonitorTelemetry`.
- `spark_connect_status_pb2.py` & `cell_monitor_pb2.py`: Generated Python
  protobuf stubs produced by `scripts/regen_protos.py`.
  `spark_connect_status_pb2.py` loads its `spark.connect.*` descriptors into an
  isolated `DescriptorPool` and `SymbolDatabase` so importing it alongside
  `pyspark.sql.connect.proto.base_pb2` never causes duplicate-symbol collisions
  across PySpark 4.0, 4.1, or 4.2+.

## Regenerating Stubs

From a virtual environment with `grpcio-tools` (and `pyink`) installed:

```sh
# Regenerate *_pb2.py from local .proto files:
python scripts/regen_protos.py

# Verify checked-in *_pb2.py files are up to date:
python scripts/regen_protos.py --check

# Refresh .proto files, NOTICE, METADATA, and stubs from bigdataoss-internal
# (for example, once Gerrit CL 196087 merges into dataproc-branch-4.2.0):
python scripts/regen_protos.py --ref refs/heads/dataproc-branch-4.2.0
```
