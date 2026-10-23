# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import uuid

from google.cloud.dataproc_v1 import (
    CreateSessionTemplateRequest,
    DeleteSessionRequest,
    DeleteSessionTemplateRequest,
    GetSessionRequest,
    GetSessionTemplateRequest,
    Session,
    SessionTemplate,
    TerminateSessionRequest,
)
from google.cloud.managed_spark_connect import ManagedSparkSession
from pyspark.errors.exceptions import connect as connect_exceptions
from pyspark.sql.types import StringType
import pytest


def test_create_spark_session_with_default_notebook_behavior(
    auth_type, connect_session, session_name, session_controller_client
):
    """Test creating a Spark session with default notebook behavior using auto-detected authentication."""
    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)
    assert session.state == Session.State.ACTIVE

    df = connect_session.createDataFrame([(1, "Sarah"), (2, "Maria")]).toDF(
        "id", "name"
    )
    assert str(df) == "DataFrame[id: bigint, name: string]"
    connect_session.sql("DROP TABLE IF EXISTS FOO")
    connect_session.sql("CREATE TABLE FOO (bar long, baz long) USING PARQUET")
    try:
        with pytest.raises(connect_exceptions.AnalysisException) as ex:
            connect_session.sql(
                "CREATE TABLE FOO (bar long, baz long) USING PARQUET"
            )
        assert "[TABLE_OR_VIEW_ALREADY_EXISTS]" in str(ex.value)
    finally:
        connect_session.sql("DROP TABLE IF EXISTS FOO")

    assert ManagedSparkSession._active_s8s_session_uuid is not None


def test_reuse_s8s_spark_session(connect_session):
    """Test that Spark sessions can be reused within the same process."""
    assert ManagedSparkSession._active_s8s_session_uuid is not None

    first_session_id = ManagedSparkSession._active_s8s_session_id
    first_session_uuid = ManagedSparkSession._active_s8s_session_uuid

    reused_session = ManagedSparkSession.builder.getOrCreate()
    second_session_id = ManagedSparkSession._active_s8s_session_id
    second_session_uuid = ManagedSparkSession._active_s8s_session_uuid

    assert reused_session is connect_session
    assert first_session_id == second_session_id
    assert first_session_uuid == second_session_uuid
    assert ManagedSparkSession._active_s8s_session_uuid is not None
    assert ManagedSparkSession._active_s8s_session_id is not None


def test_sql_functions(connect_session):
    """Test basic SQL functions like col(), sum(), count(), etc."""
    from pyspark.sql.connect.functions import col, count, sum

    df = connect_session.createDataFrame(
        [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 150)],
        ["id", "name", "amount"],
    )

    result_col = df.select(col("name")).collect()
    assert len(result_col) == 3
    assert result_col[0]["name"] == "Alice"

    sum_result = df.select(sum("amount")).collect()[0][0]
    assert sum_result == 450

    count_result = df.select(count("id")).collect()[0][0]
    assert count_result == 3

    filtered_df = df.where(col("amount") > 150)
    filtered_count = filtered_df.count()
    assert filtered_count == 1

    df_with_calc = df.select(
        col("id"),
        col("name"),
        col("amount"),
        (col("amount") * 0.1).alias("tax"),
    )
    tax_results = df_with_calc.collect()
    assert tax_results[0]["tax"] == 10.0
    assert tax_results[1]["tax"] == 20.0
    assert tax_results[2]["tax"] == 15.0


def test_sql_udf(connect_session):
    """Test SQL UDF registration and usage on remote executors."""
    from pyspark.sql.connect.functions import col, udf

    df = connect_session.createDataFrame(
        [(1, "hello"), (2, "world"), (3, "spark")], ["id", "text"]
    )

    def uppercase_func(text):
        return text.upper() if text else None

    uppercase_udf = udf(uppercase_func, StringType())
    df_with_udf = df.select(
        "id", "text", uppercase_udf(col("text")).alias("upper_text")
    )
    df_result = df_with_udf.collect()

    assert df_result[0]["upper_text"] == "HELLO"
    assert df_result[1]["upper_text"] == "WORLD"


def test_sparksql_magic_with_managed_spark_session(connect_session):
    """Test that %%sparksql works with the active ManagedSparkSession."""
    pytest.importorskip(
        "IPython", reason="IPython not available (install [interactive])"
    )
    pytest.importorskip(
        "sqlparse", reason="sqlparse not available (install [interactive])"
    )

    from IPython.terminal.interactiveshell import TerminalInteractiveShell

    shell = TerminalInteractiveShell.instance()
    try:
        shell.run_line_magic("load_ext", "google.cloud.managed_spark_magics")
        shell.user_ns["factor"] = 5

        shell.run_cell_magic(
            "sparksql",
            "-q result_df",
            """
            SELECT
                10 * :factor as multiplication,
                SQRT(16) as square_root,
                CONCAT('Dataproc', '-', 'Spark') as joined_string
            """,
        )

        assert "result_df" in shell.user_ns
        df = shell.user_ns["result_df"]
        assert df is not None

        data = df.collect()
        assert len(data) == 1
        row = data[0]

        assert row["multiplication"] == 50
        assert row["square_root"] == 4.0
        assert row["joined_string"] == "Dataproc-Spark"
    finally:
        TerminalInteractiveShell.clear_instance()


@pytest.fixture
def session_template_name(
    test_project,
    test_region,
    test_subnetwork_uri,
    session_template_controller_client,
):
    create_session_template_request = CreateSessionTemplateRequest()
    create_session_template_request.parent = (
        f"projects/{test_project}/locations/{test_region}"
    )
    session_template = SessionTemplate()
    session_template.environment_config.execution_config.subnetwork_uri = (
        test_subnetwork_uri
    )
    session_template_name = (
        f"projects/{test_project}/locations/{test_region}/sessionTemplates/"
        f"spark-connect-test-template-{uuid.uuid4().hex[0:12]}"
    )
    session_template.name = session_template_name
    create_session_template_request.session_template = session_template
    session_template_controller_client.create_session_template(
        create_session_template_request
    )
    try:
        get_session_template_request = GetSessionTemplateRequest()
        get_session_template_request.name = session_template_name
        created_template = (
            session_template_controller_client.get_session_template(
                get_session_template_request
            )
        )
        yield created_template.name
    finally:
        delete_session_template_request = DeleteSessionTemplateRequest()
        delete_session_template_request.name = session_template_name
        session_template_controller_client.delete_session_template(
            delete_session_template_request
        )


def test_create_spark_session_with_session_template_and_user_provided_session_config(
    test_project,
    test_region,
    session_template_name,
    session_controller_client,
    isolated_session_state,
):
    """Test creating a Spark session with a session template and user-provided Dataproc configuration."""
    session_config = Session()
    session_config.environment_config.execution_config.ttl = {"seconds": 64800}
    session_config.session_template = session_template_name
    # Use spark.master=local (single-node mode) and 4-core override (instead of
    # 16) so this control-plane configuration test only provisions a single
    # 4-vCPU driver VM without an executor node pool.
    connect_session = (
        ManagedSparkSession.builder.projectId(test_project)
        .location(test_region)
        .config("spark.executor.cores", "8")
        .sessionConfig(session_config)
        .config("spark.executor.cores", "4")
        .config("spark.master", "local")
        .getOrCreate()
    )
    session_name = (
        f"projects/{test_project}/locations/{test_region}/sessions/"
        f"{ManagedSparkSession._active_s8s_session_id}"
    )

    get_session_request = GetSessionRequest()
    get_session_request.name = session_name
    session = session_controller_client.get_session(get_session_request)

    assert session.state == Session.State.ACTIVE
    assert session.session_template == session_template_name
    assert (
        session.runtime_config.version
        == ManagedSparkSession._DEFAULT_RUNTIME_VERSION
    )
    assert (
        session.environment_config.execution_config.ttl
        == datetime.timedelta(seconds=64800)
    )
    assert (
        session.runtime_config.properties["spark:spark.executor.cores"] == "4"
    )
    assert session.runtime_config.properties["spark:spark.master"] == "local"
    assert ManagedSparkSession._active_s8s_session_uuid is not None

    connect_session.stop()
    session = session_controller_client.get_session(get_session_request)

    assert session.state in [
        Session.State.TERMINATING,
        Session.State.TERMINATED,
    ]
    assert ManagedSparkSession._active_s8s_session_uuid is None


@pytest.mark.skip(
    reason="Skipping PyPI package installation test since it's not supported yet"
)
def test_add_artifacts_pypi_package():
    """Test adding PyPI packages as artifacts to a Spark session.

    Note: Skipped in CI due to infrastructure issues with PyPI package installation.
    """
    connect_session = ManagedSparkSession.builder.getOrCreate()
    from pyspark.sql.connect.functions import sum, udf
    from pyspark.sql.types import IntegerType

    def generate_random2(row) -> int:
        import random2 as random

        return row + random.Random().randint(1, 5)

    connect_session.addArtifacts("random2", pypi=True)

    sum_random = (
        connect_session.range(1, 10)
        .withColumn(
            "anotherCol", udf(generate_random2)("id").cast(IntegerType())
        )
        .select(sum("anotherCol"))
        .collect()[0][0]
    )

    assert isinstance(sum_random, int), "Result is not of type int"
    connect_session.stop()


def test_session_reuse_with_custom_id(
    auth_type,
    test_project,
    test_region,
    session_controller_client,
    isolated_session_state,
):
    """Test named session lifecycle: create -> reuse -> stop (stays active) -> reattach -> stop(terminate=True)."""
    custom_session_id = f"ml-pipeline-session-{uuid.uuid4().hex[:8]}"
    session_name = (
        f"projects/{test_project}/locations/{test_region}/sessions/"
        f"{custom_session_id}"
    )

    try:
        # PHASE 1: Create initial session with custom ID in single-node mode
        spark1 = (
            ManagedSparkSession.builder.sessionId(custom_session_id)
            .projectId(test_project)
            .location(test_region)
            .config("spark.master", "local")
            .getOrCreate()
        )

        assert ManagedSparkSession._active_s8s_session_id == custom_session_id
        first_session_uuid = spark1._active_s8s_session_uuid

        df1 = spark1.createDataFrame([(1, "initial")], ["id", "stage"])
        assert df1.count() == 1

        # PHASE 2: Test session reuse while active when _default_session is cleared
        ManagedSparkSession._default_session = None

        spark2 = (
            ManagedSparkSession.builder.sessionId(custom_session_id)
            .projectId(test_project)
            .location(test_region)
            .getOrCreate()
        )

        assert ManagedSparkSession._active_s8s_session_id == custom_session_id
        assert spark2._active_s8s_session_uuid == first_session_uuid

        df2 = spark2.createDataFrame([(2, "reused")], ["id", "stage"])
        assert df2.count() == 1

        # PHASE 3: Default stop() on a named session does NOT terminate on the server
        spark2.stop()
        assert ManagedSparkSession._active_s8s_session_id is None

        get_session_request = GetSessionRequest(name=session_name)
        session = session_controller_client.get_session(get_session_request)
        assert session.state == Session.State.ACTIVE

        # PHASE 4: Reattach to the active server-side session with the same custom ID
        ManagedSparkSession._default_session = None
        ManagedSparkSession._active_s8s_session_id = None
        ManagedSparkSession._active_s8s_session_uuid = None

        spark3 = (
            ManagedSparkSession.builder.sessionId(custom_session_id)
            .projectId(test_project)
            .location(test_region)
            .getOrCreate()
        )

        assert ManagedSparkSession._active_s8s_session_id == custom_session_id
        assert spark3._active_s8s_session_uuid == first_session_uuid

        df3 = spark3.createDataFrame([(3, "recreated")], ["id", "stage"])
        assert df3.count() == 1

        # PHASE 5: stop(terminate=True) terminates the named session on the server
        spark3.stop(terminate=True)
        assert ManagedSparkSession._active_s8s_session_id is None

        session = session_controller_client.get_session(get_session_request)
        assert session.state in [
            Session.State.TERMINATING,
            Session.State.TERMINATED,
        ]
    finally:
        # Ensure the named session is never leaked on the server if an assertion fails mid-test
        try:
            session = session_controller_client.get_session(
                GetSessionRequest(name=session_name)
            )
            if session.state in [
                Session.State.CREATING,
                Session.State.ACTIVE,
            ]:
                session_controller_client.terminate_session(
                    TerminateSessionRequest(name=session_name)
                )
        except Exception:
            pass


def test_serverless_session_termination_and_deletion_lifecycle(
    test_project,
    test_region,
    session_controller_client,
    isolated_session_state,
):
    """Test managed session stop(terminate=False), server-side termination recovery, and server-side deletion."""
    first_session_name = None
    second_session_name = None
    try:
        # PHASE 1: Create a managed session (auto-generated ID) in single-node mode
        # and verify stop(terminate=False) leaves the server-side session ACTIVE.
        first_spark = (
            ManagedSparkSession.builder.projectId(test_project)
            .location(test_region)
            .config("spark.master", "local")
            .getOrCreate()
        )
        assert ManagedSparkSession._active_s8s_session_id is not None
        assert ManagedSparkSession._active_session_uses_custom_id is False
        first_session_id = ManagedSparkSession._active_s8s_session_id
        first_session_uuid = ManagedSparkSession._active_s8s_session_uuid
        first_session_name = (
            f"projects/{test_project}/locations/{test_region}/sessions/"
            f"{first_session_id}"
        )

        df = first_spark.createDataFrame([(1, "test")], ["id", "value"])
        assert df.count() == 1

        # Stop client without terminating server
        first_spark.stop(terminate=False)
        assert ManagedSparkSession._active_s8s_session_id is None

        get_first_request = GetSessionRequest(name=first_session_name)
        session = session_controller_client.get_session(get_first_request)
        assert session.state == Session.State.ACTIVE

        # PHASE 2: Re-associate client with first_session, terminate it on the
        # server, and verify getOrCreate() stops the terminated session and creates
        # a replacement session.
        ManagedSparkSession._active_s8s_session_id = first_session_id
        ManagedSparkSession._active_s8s_session_uuid = first_session_uuid

        terminate_request = TerminateSessionRequest(name=first_session_name)
        session_controller_client.terminate_session(terminate_request).result()

        second_spark = (
            ManagedSparkSession.builder.projectId(test_project)
            .location(test_region)
            .config("spark.master", "local")
            .getOrCreate()
        )
        second_session_id = ManagedSparkSession._active_s8s_session_id
        second_session_uuid = ManagedSparkSession._active_s8s_session_uuid
        second_session_name = (
            f"projects/{test_project}/locations/{test_region}/sessions/"
            f"{second_session_id}"
        )

        assert first_session_uuid != second_session_uuid
        assert second_session_uuid is not None
        assert second_session_id is not None

        first_state = session_controller_client.get_session(
            get_first_request
        ).state
        assert first_state in [
            Session.State.TERMINATING,
            Session.State.TERMINATED,
        ]

        get_second_request = GetSessionRequest(name=second_session_name)
        second_state = session_controller_client.get_session(
            get_second_request
        ).state
        assert second_state == Session.State.ACTIVE

        # PHASE 3: Delete the active session on the server and verify stop() cleans
        # up client state without error.
        delete_request = DeleteSessionRequest(name=second_session_name)
        session_controller_client.delete_session(delete_request).result()

        second_spark.stop()
        assert ManagedSparkSession._active_s8s_session_uuid is None
        assert ManagedSparkSession._active_s8s_session_id is None
    finally:
        for name in (first_session_name, second_session_name):
            if name:
                try:
                    session = session_controller_client.get_session(
                        GetSessionRequest(name=name)
                    )
                    if session.state in [
                        Session.State.CREATING,
                        Session.State.ACTIVE,
                    ]:
                        session_controller_client.terminate_session(
                            TerminateSessionRequest(name=name)
                        )
                except Exception:
                    pass
