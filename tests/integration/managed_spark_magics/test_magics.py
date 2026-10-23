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
import pytest


@pytest.fixture
def ipython_shell(connect_session):
    """Provides an IPython shell with the shared ManagedSparkSession in user_ns."""
    try:
        from google.cloud import managed_spark_magics
        from IPython.terminal.interactiveshell import TerminalInteractiveShell

        shell = TerminalInteractiveShell.instance()
        shell.user_ns = {"spark": connect_session}

        managed_spark_magics.load_ipython_extension(shell)

        yield shell
    finally:
        from IPython.terminal.interactiveshell import TerminalInteractiveShell

        TerminalInteractiveShell.clear_instance()


def test_dpip_install_success(connect_session, ipython_shell, capsys):
    """Test installing PyPI packages with %dpip and running a UDF on remote executors."""
    ipython_shell.run_line_magic("dpip", "install roman numpy")
    captured = capsys.readouterr()
    assert "Active session found:" in captured.out
    assert "Installing packages:" in captured.out
    assert "Finished installing packages." in captured.out

    from pyspark.sql.connect.functions import udf
    from pyspark.sql.types import StringType

    df = connect_session.createDataFrame([(1666,)], ["number"])

    def to_roman(number):
        import roman

        return roman.toRoman(number)

    df_result = df.withColumn(
        "roman", udf(to_roman, StringType())("number")
    ).collect()

    assert df_result[0]["roman"] == "MDCLXVI"


def test_dpip_install_failure(ipython_shell):
    """Test error message on remote PyPI installation failure."""
    with pytest.raises(
        RuntimeError,
        match="No matching distribution found",
    ):
        ipython_shell.run_line_magic("dpip", "install dp-non-existent-package")
