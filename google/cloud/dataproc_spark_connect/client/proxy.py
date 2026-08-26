"""Deprecated: use ``google.cloud.managed_spark_connect.client.proxy`` instead."""

from google.cloud.managed_spark_connect.client.proxy import (
    ManagedSparkSessionProxy,
    connect_sockets,
    connect_tcp_bridge,
    forward_bytes,
    forward_connection,
    managed_spark_session_proxy,
)

DataprocSessionProxy = ManagedSparkSessionProxy
dataproc_session_proxy = managed_spark_session_proxy
