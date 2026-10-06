# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""%%sparksql against real local Spark, both classic and Spark Connect.

These start an in-process JVM, so they need Java 17 or 21 and are kept out of
tests/unit. Run them on their own, without xdist:

    python -m pytest tests/local
"""

import io
import os
import re
import shutil
import subprocess
from contextlib import redirect_stdout
from unittest import mock

import pytest
from IPython.core.interactiveshell import InteractiveShell
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.util import is_remote_only

from google.cloud.managed_spark_magics import sparksql


def _java_version():
    """The major version of the Java pyspark would launch, or None."""
    java_home = os.environ.get("JAVA_HOME")
    java = (
        os.path.join(java_home, "bin", "java")
        if java_home
        else shutil.which("java")
    )
    if not java:
        return None
    try:
        out = subprocess.run(
            [java, "-version"], capture_output=True, text=True, check=True
        ).stderr
    except (OSError, subprocess.SubprocessError):
        return None
    m = re.search(r'version "(\d+)', out)
    return int(m.group(1)) if m else None


_JAVA_VERSION = _java_version()

pytestmark = [
    pytest.mark.skipif(
        # Spark 4.0 supports exactly these; newer JDKs fail at JVM startup.
        _JAVA_VERSION not in (17, 21),
        reason=(
            f"needs Java 17 or 21 (found {_JAVA_VERSION}); point JAVA_HOME at"
            " one"
        ),
    ),
    pytest.mark.skipif(
        is_remote_only(),
        reason="needs full pyspark, with its jars, not pyspark-client",
    ),
]


@pytest.fixture(scope="module", params=["classic", "connect"])
def spark(request):
    with pytest.MonkeyPatch.context() as mp:
        builder = SparkSession.builder
        if request.param == "classic":
            builder = builder.master("local[2]").config(
                "spark.ui.enabled", "false"
            )
        else:
            # Makes the local Connect server bind an ephemeral port, and the
            # client find it, rather than both assuming 15002, which may be
            # taken.
            mp.setenv("SPARK_TESTING", "1")
            builder = builder.remote("local[2]")
        session = (
            builder.appName(f"sparksql-{request.param}")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.scripting.enabled", "true")
            .getOrCreate()
        )
        yield session
        session.stop()


@pytest.fixture
def ip(spark):
    shell = InteractiveShell.instance()
    shell.run_line_magic(
        "load_ext", "google.cloud.managed_spark_magics.sparksql"
    )
    sparksql.get_config(shell).limit = 20
    sparksql.get_config(shell).full_traceback = False
    shell.user_ns["spark"] = spark
    yield shell
    shell.run_line_magic(
        "unload_ext", "google.cloud.managed_spark_magics.sparksql"
    )
    InteractiveShell.clear_instance()


@pytest.fixture
def displayed():
    """The bundles %%sparksql displayed, in order."""
    calls = []
    with mock.patch(
        "IPython.display.display",
        side_effect=lambda obj, raw=False: calls.append(obj),
    ):
        yield calls


def test_select_renders_table(ip, displayed):
    ip.run_cell_magic(
        "sparksql",
        "",
        "SELECT id, CAST(id AS STRING) AS s FROM range(3) ORDER BY id",
    )
    (bundle,) = displayed
    html = bundle["text/html"]
    assert '<span class="t">bigint</span>' in html
    assert '<span class="t">string</span>' in html
    assert '<td class="n">2</td>' in html
    assert "3 rows" in html
    assert bundle["text/plain"].splitlines()[1] == "|id|s|"


def test_limit_and_truncation(ip, displayed):
    ip.run_cell_magic("sparksql", "-l 2", "SELECT * FROM range(5)")
    assert "Showing the first 2 rows" in displayed[0]["text/html"]
    sparksql.get_config(ip).limit = 5
    ip.run_cell_magic("sparksql", "", "SELECT * FROM range(5)")
    assert "5 rows" in displayed[1]["text/html"]


def test_values_are_formatted_by_spark(ip, displayed):
    ip.run_cell_magic(
        "sparksql",
        "",
        "SELECT CAST('bin' AS BINARY) AS b, CAST(NULL AS INT) AS n,"
        " array(1, NULL) AS a, map('k', 1.5D) AS m, DATE'2026-01-02' AS d",
    )
    html = displayed[0]["text/html"]
    assert "<td>[62 69 6E]</td>" in html
    assert '<td class="n null">NULL</td>' in html
    assert "<td>[1, null]</td>" in html
    assert "<td>{k -&gt; 1.5}</td>" in html
    assert "<td>2026-01-02</td>" in html


def test_ddl_displays_ok(ip, displayed):
    ip.run_cell_magic(
        "sparksql", "", "CREATE OR REPLACE TEMP VIEW ddl_v AS SELECT 1 AS a"
    )
    assert displayed == [{"text/plain": "OK"}]


def test_parameter_markers(ip, spark, displayed):
    spark.range(10).createOrReplaceTempView("param_t")
    ip.user_ns.update(t="param_t", lo=3, hi=5)
    ip.run_cell_magic(
        "sparksql",
        "out",
        "SELECT id FROM IDENTIFIER(:t) WHERE id BETWEEN :lo AND :hi"
        " AND ':nope' <> '' ORDER BY id",
    )
    assert [r.id for r in ip.user_ns["out"].collect()] == [3, 4, 5]


def test_dataframe_references(ip, spark):
    from IPython.core.error import UsageError

    ip.user_ns["df"] = spark.range(4)
    ip.user_ns["label"] = "it's"
    df = ip.run_line_magic(
        "sparksql",
        "SELECT :label AS l, '{not a field}' AS s, count(*) AS c FROM {df}",
    )
    assert df.collect()[0].asDict() == {
        "l": "it's",
        "s": "{not a field}",
        "c": 4,
    }
    with pytest.raises(
        UsageError, match=r"\{label\}: expected a Spark DataFrame"
    ):
        ip.run_line_magic("sparksql", "SELECT {label}")


def test_multiple_statements(ip, displayed):
    ip.run_cell_magic(
        "sparksql",
        "out",
        "CREATE OR REPLACE TEMP VIEW multi_v AS SELECT 1 AS a;\n"
        "-- a comment\n"
        "SELECT a + 1 AS b FROM multi_v;\n",
    )
    assert ip.user_ns["out"].collect()[0].b == 2
    assert len(displayed) == 1


def test_script(ip, displayed):
    ip.run_cell_magic(
        "sparksql",
        "out",
        "BEGIN\n  DECLARE x INT = 41;\n  SELECT x + 1 AS y;\nEND",
    )
    assert ip.user_ns["out"].collect()[0].y == 42


def test_view_cache_eager(ip, spark, displayed):
    ip.run_cell_magic("sparksql", "-q -c -e -v vce out", "SELECT 7 AS a")
    assert displayed == []
    assert spark.table("vce").collect()[0].a == 7
    assert ip.user_ns["out"].collect()[0].a == 7


def test_explain(ip, displayed):
    out = io.StringIO()
    with redirect_stdout(out):
        ip.run_cell_magic("sparksql", "--explain", "SELECT 1 AS a")
    assert "Physical Plan" in out.getvalue()
    assert displayed == []


def test_named_session(ip, spark, displayed):
    ip.user_ns["other"] = spark
    ip.run_cell_magic("sparksql", "-s other", "SELECT 1 AS a")
    assert "1 row" in displayed[0]["text/html"]


def test_error_with_caret(ip):
    cell = "SELECT 1;\nSELECT\n  nope FROM range(1)"
    with pytest.raises(sparksql.SparkSqlError) as info:
        ip.run_cell_magic("sparksql", "", cell)
    lines = info.value.message.splitlines()
    assert lines[0].startswith("[UNRESOLVED_COLUMN")
    assert "SQLSTATE: 42703" in lines[0]
    assert lines[1] == (
        "(line 3; statement 2 of 2; earlier statements already ran)"
    )
    assert lines[2:] == ["    nope FROM range(1)", "    ^^^^"]
    assert isinstance(info.value.__cause__, PySparkException)


def test_parse_error(ip):
    with pytest.raises(sparksql.SparkSqlError) as info:
        ip.run_cell_magic("sparksql", "", "SELECT 1;\n\n  SELEC 2")
    assert info.value.message.splitlines() == [
        "[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'. SQLSTATE: 42601",
        "(line 3; statement 2 of 2; earlier statements already ran)",
        "    SELEC 2",
        "    ^^^^^",
    ]


def test_full_traceback(ip):
    sparksql.get_config(ip).full_traceback = True
    with pytest.raises(PySparkException):
        ip.run_cell_magic("sparksql", "", "SELECT nope")
