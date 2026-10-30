# Copyright 2026 Google LLC
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

import json
import logging
import os
import subprocess
import sys
from unittest import mock
import warnings

from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.cloud.dataproc_v1 import Session
from google.cloud.managed_spark_connect import ManagedSparkSession
from google.cloud.managed_spark_connect import _logging
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.session import (
    clear_dataproc_caches,
    get_active_s8s_session_response,
    invalidate_s8s_session_active,
)
import pytest


@pytest.fixture(autouse=True)
def _clean_session_state(monkeypatch):
    """Reset global session state and package logger around each test."""
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("GOOGLE_CLOUD_REGION", "test-region")
    monkeypatch.delenv("MANAGED_SPARK_CONNECT_LOG_LEVEL", raising=False)
    monkeypatch.delenv(
        "MANAGED_SPARK_CONNECT_SHOW_OPERATION_LINKS", raising=False
    )
    monkeypatch.delenv("MANAGED_SPARK_CONNECT_PROGRESS_BAR", raising=False)
    monkeypatch.delenv(
        "MANAGED_SPARK_CONNECT_PROGRESS_BAR_LEAVE", raising=False
    )
    monkeypatch.delenv(
        "MANAGED_SPARK_CONNECT_FORCE_PROGRESS_BAR", raising=False
    )
    pkg_logger = logging.getLogger("google.cloud.managed_spark_connect")
    orig_level = pkg_logger.level
    orig_handlers = list(pkg_logger.handlers)
    orig_propagate = pkg_logger.propagate

    def reset():
        clear_dataproc_caches()
        invalidate_s8s_session_active()
        ManagedSparkSession._active_s8s_session_id = None
        ManagedSparkSession._active_s8s_session_uuid = None
        ManagedSparkSession._active_session_uses_custom_id = False
        ManagedSparkSession._project_id = None
        ManagedSparkSession._region = None
        ManagedSparkSession._default_session = None
        ManagedSparkSession._set_tqdm_factory(None)
        ManagedSparkSession._execution_progress_bar.clear()

    reset()
    yield
    reset()
    pkg_logger.setLevel(orig_level)
    pkg_logger.handlers[:] = orig_handlers
    pkg_logger.propagate = orig_propagate


def _bare_session(
    show_operation_links=None, show_progress_bar=None, progress_bar_leave=None
):
    """A ManagedSparkSession with only the display settings initialized."""
    session = object.__new__(ManagedSparkSession)
    session.instrumentation = None
    session._show_operation_links = show_operation_links
    session._show_progress_bar = show_progress_bar
    session._progress_bar_leave = progress_bar_leave
    return session


def test_import_does_not_mutate_root_logger_and_attaches_null_handler():
    """Importing the package in a fresh subprocess must not touch root logger."""
    code = """
import json
import logging
import warnings

# Other packages' import-time warnings (google-api-core on old Pythons,
# pyspark on pandas 3) are not ours to silence; this package's still show.
warnings.filterwarnings("ignore", module="(?!google[.]cloud[.]managed_spark_connect)")

root = logging.getLogger()
before_handlers = len(root.handlers)
before_level = root.level

import google.cloud.managed_spark_connect  # noqa: F401

pkg = logging.getLogger("google.cloud.managed_spark_connect")
print(json.dumps({
    "before_handlers": before_handlers,
    "after_handlers": len(root.handlers),
    "before_level": before_level,
    "after_level": root.level,
    "pkg_level": pkg.level,
    "pkg_handler_types": [type(h).__name__ for h in pkg.handlers],
}))
"""
    env = dict(os.environ)
    env.pop("MANAGED_SPARK_CONNECT_LOG_LEVEL", None)
    proc = subprocess.run(
        # Code on stdin, not -c: see _run_python in test_init.py.
        [sys.executable, "-"],
        input=code,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc.stderr == ""
    data = json.loads(proc.stdout.strip())
    assert data["before_handlers"] == 0
    assert data["after_handlers"] == 0
    assert data["before_level"] == logging.WARNING
    assert data["after_level"] == logging.WARNING
    assert data["pkg_level"] == logging.NOTSET
    assert "NullHandler" in data["pkg_handler_types"]
    assert all(h == "NullHandler" for h in data["pkg_handler_types"])


def test_log_level_env_configures_only_package_logger_in_subprocess():
    """MANAGED_SPARK_CONNECT_LOG_LEVEL configures only package logger, not root."""
    code = """
import json
import logging

import google.cloud.managed_spark_connect  # noqa: F401

root = logging.getLogger()
pkg = logging.getLogger("google.cloud.managed_spark_connect")
print(json.dumps({
    "root_handlers": len(root.handlers),
    "root_level": root.level,
    "pkg_level": pkg.level,
    "pkg_handler_types": [type(h).__name__ for h in pkg.handlers],
}))
"""
    env = dict(os.environ)
    env["MANAGED_SPARK_CONNECT_LOG_LEVEL"] = "DEBUG"
    proc = subprocess.run(
        # Code on stdin, not -c: see _run_python in test_init.py.
        [sys.executable, "-"],
        input=code,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout.strip())
    assert data["root_handlers"] == 0
    assert data["root_level"] == logging.WARNING
    assert data["pkg_level"] == logging.DEBUG
    assert "NullHandler" in data["pkg_handler_types"]
    assert "StreamHandler" in data["pkg_handler_types"]


def test_configure_package_logger_in_process(monkeypatch):
    """configure_package_logger respects MANAGED_SPARK_CONNECT_LOG_LEVEL without touching root."""
    root_logger = logging.getLogger()
    root_handlers_before = list(root_logger.handlers)
    root_level_before = root_logger.level

    pkg_logger = logging.getLogger("google.cloud.managed_spark_connect")
    pkg_logger.handlers[:] = []
    pkg_logger.setLevel(logging.NOTSET)

    monkeypatch.setenv("MANAGED_SPARK_CONNECT_LOG_LEVEL", "DEBUG")
    _logging.configure_package_logger()

    assert pkg_logger.level == logging.DEBUG
    assert any(isinstance(h, logging.NullHandler) for h in pkg_logger.handlers)
    assert root_logger.level == root_level_before
    assert list(root_logger.handlers) == root_handlers_before


def test_log_level_stream_handler_does_not_propagate(monkeypatch):
    """With its own StreamHandler, the package logger doesn't also propagate
    to a root handler configured later (no duplicate lines)."""
    pkg_logger = logging.getLogger("google.cloud.managed_spark_connect")
    pkg_logger.handlers[:] = []
    monkeypatch.setattr(logging.getLogger(), "handlers", [])
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_LOG_LEVEL", "INFO")

    _logging.configure_package_logger()

    assert any(type(h) is logging.StreamHandler for h in pkg_logger.handlers)
    assert pkg_logger.propagate is False


@pytest.mark.parametrize("raw", ["0", "-10", "NOTSET", "loud", ""])
def test_log_level_ignores_non_positive_and_unknown_levels(monkeypatch, raw):
    pkg_logger = logging.getLogger("google.cloud.managed_spark_connect")
    pkg_logger.handlers[:] = []
    pkg_logger.setLevel(logging.NOTSET)
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_LOG_LEVEL", raw)

    _logging.configure_package_logger()

    assert pkg_logger.level == logging.NOTSET
    assert all(isinstance(h, logging.NullHandler) for h in pkg_logger.handlers)


def test_non_interactive_lifecycle_zero_stdout_stderr_and_warnings(
    capsys, monkeypatch
):
    """Non-interactive getOrCreate(), 404/503 liveness checks, actions, and stop() emit 0 bytes and 0 warnings."""
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: False,
    )

    session_response = Session()
    session_response.name = (
        "projects/test-project/locations/test-region/sessions/sc-test-123456"
    )
    session_response.uuid = "11111111-2222-3333-4444-555555555555"
    session_response.state = Session.State.ACTIVE
    session_response.runtime_config.version = "3.0.2"
    session_response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect.example.com:443"
    }

    mock_operation = mock.Mock()
    mock_operation.result.return_value = session_response

    mock_controller = mock.Mock()
    mock_controller.create_session.return_value = mock_operation
    mock_controller.get_session.return_value = session_response
    mock_controller.terminate_session.return_value = mock.Mock()

    cred = mock.MagicMock()
    cred.token = "fake-token"

    with (
        mock.patch("google.auth.default", return_value=(cred, "test-project")),
        mock.patch(
            "google.cloud.dataproc_v1.SessionControllerClient",
            return_value=mock_controller,
        ),
        mock.patch.object(
            ManagedSparkSession.Builder,
            "generate_session_id",
            return_value="sc-test-123456",
        ),
        # These warn by design when the client's Python or PySpark does not
        # match the runtime's, as on most CI lanes (see test_init.py).
        mock.patch.object(
            ManagedSparkSession.Builder, "_check_python_version_compatibility"
        ),
        mock.patch.object(
            ManagedSparkSession.Builder, "_check_pyspark_version_compatibility"
        ),
        mock.patch(
            # The new session's first RPC, which would go to the fake server.
            "google.cloud.managed_spark_connect.session._server_spark_version",
            return_value="4.0.2",
        ),
        mock.patch("tqdm.trange") as mock_trange,
        warnings.catch_warnings(record=True) as caught_warnings,
    ):
        warnings.simplefilter("always")

        # 1. Initial getOrCreate() in non-interactive mode
        session = ManagedSparkSession.builder.getOrCreate()
        mock_trange.assert_not_called()

        # 2. Repeated getOrCreate() returning the active singleton
        session2 = ManagedSparkSession.builder.getOrCreate()
        assert session2 is session

        # 3. _get_session_by_id on ACTIVE and TERMINATED sessions
        assert (
            ManagedSparkSession.builder._get_session_by_id("sc-test-123456")
            is session_response
        )
        terminated_session = Session()
        terminated_session.state = Session.State.TERMINATED
        mock_controller.get_session.return_value = terminated_session
        assert (
            ManagedSparkSession.builder._get_session_by_id("sc-test-123456")
            is None
        )

        # 4. get_active_s8s_session_response on 404 NotFound and 503 ServiceUnavailable
        mock_controller.get_session.side_effect = NotFound("Session not found")
        assert (
            get_active_s8s_session_response(
                "projects/test-project/locations/test-region/sessions/missing-404",
                None,
            )
            is None
        )

        # A 503 is not a verdict on the session: it propagates (silently).
        mock_controller.get_session.side_effect = ServiceUnavailable(
            "Transient 503"
        )
        with pytest.raises(ServiceUnavailable):
            get_active_s8s_session_response(
                "projects/test-project/locations/test-region/sessions/missing-503",
                None,
            )

        # 5. Simulated DataFrame action (_display_operation_link + progress handler)
        session._display_operation_link("op-123")
        for handler in list(session.client._progress_handlers):
            stage = mock.Mock(num_tasks=4, num_completed_tasks=2)
            handler([stage], 2, "op-123", False)
            stage_done = mock.Mock(num_tasks=4, num_completed_tasks=4)
            handler([stage_done], 0, "op-123", True)

        # 6. Stop session
        mock_controller.get_session.side_effect = None
        terminating_session = Session()
        terminating_session.state = Session.State.TERMINATED
        mock_controller.get_session.return_value = terminating_session
        session.stop()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    unexpected_warnings = [
        w
        for w in caught_warnings
        if issubclass(w.category, (UserWarning, ResourceWarning))
    ]
    assert unexpected_warnings == []


def test_interactive_attach_prints_once_and_singleton_reuse_is_silent(
    capsys, monkeypatch
):
    """First-time interactive attach emits 'Using existing Managed Spark Session...', while singleton reuse is silent."""
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: True,
    )
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive_terminal",
        lambda: True,
    )

    session_response = Session()
    session_response.name = (
        "projects/test-project/locations/test-region/sessions/sc-existing"
    )
    session_response.uuid = "aaaa1111-bbbb-2222-cccc-333333333333"
    session_response.state = Session.State.ACTIVE
    session_response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect.example.com:443"
    }

    fake_session = _bare_session()
    ManagedSparkSession._active_s8s_session_id = "sc-existing"

    with (
        mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=session_response,
        ),
        mock.patch.object(
            ManagedSparkSession.Builder,
            "_create_spark_connect_session_from_s8s",
            return_value=fake_session,
        ),
    ):
        # First-time attach (no in-process active session yet)
        ManagedSparkSession._default_session = None
        with mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=None
        ):
            attached = ManagedSparkSession.builder._get_exiting_active_session()
        assert attached is fake_session
        first_out = capsys.readouterr()
        assert "Using existing Managed Spark Session" in first_out.out
        assert "configuration changes may not be applied" in first_out.out

        # Subsequent call when in-process singleton already exists -> must be
        # silent, and must not change the live session's display settings.
        with mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=fake_session
        ):
            reused = ManagedSparkSession.builder.showProgressBar(
                False
            )._get_exiting_active_session()
        assert reused is fake_session
        assert fake_session._show_progress_bar is None
        second_out = capsys.readouterr()
        assert second_out.out == ""
        assert second_out.err == ""


def test_non_interactive_first_time_attach_is_silent(capsys, monkeypatch):
    """Attaching to an existing remote session prints nothing in a script."""
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: False,
    )
    session_response = Session()
    session_response.name = (
        "projects/test-project/locations/test-region/sessions/sc-existing"
    )
    session_response.state = Session.State.ACTIVE
    session_response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect.example.com:443"
    }
    fake_session = _bare_session()
    ManagedSparkSession._active_s8s_session_id = "sc-existing"

    with (
        mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=session_response,
        ),
        mock.patch.object(
            ManagedSparkSession.Builder,
            "_create_spark_connect_session_from_s8s",
            return_value=fake_session,
        ),
        mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=None
        ),
    ):
        attached = ManagedSparkSession.builder._get_exiting_active_session()

    assert attached is fake_session
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err == ""


def test_inactive_session_message_escapes_the_session_id(monkeypatch):
    """The session ID can come from .sessionId(...), so the HTML escapes it."""
    shown = []
    monkeypatch.setattr(
        ManagedSparkSession.Builder,
        "_output_element_or_message",
        lambda self, plain, html: shown.append(html),
    )
    fake_session = mock.Mock(spec=ManagedSparkSession)
    ManagedSparkSession._active_s8s_session_id = "<b>x</b>"

    with (
        mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=None,
        ),
        mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=fake_session
        ),
    ):
        ManagedSparkSession.builder._get_exiting_active_session()

    (html,) = shown
    assert "<b>" not in html
    assert "&lt;b&gt;x&lt;/b&gt;" in html


def test_inactive_session_message_gated_on_interactive(capsys, monkeypatch):
    """When active session becomes inactive on server, non-interactive is silent and interactive prints via _output_element_or_message."""
    fake_session = mock.Mock(spec=ManagedSparkSession)
    ManagedSparkSession._active_s8s_session_id = "sc-expired"

    with (
        mock.patch(
            "google.cloud.managed_spark_connect.session.get_active_s8s_session_response",
            return_value=None,
        ),
        mock.patch.object(
            ManagedSparkSession, "getActiveSession", return_value=fake_session
        ),
    ):
        monkeypatch.setattr(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            lambda: False,
        )
        res = ManagedSparkSession.builder._get_exiting_active_session()
        assert res is None
        fake_session.stop.assert_called_once()
        out_non_interactive = capsys.readouterr()
        assert out_non_interactive.out == ""
        assert out_non_interactive.err == ""

        fake_session.stop.reset_mock()
        monkeypatch.setattr(
            "google.cloud.managed_spark_connect.environment.is_interactive",
            lambda: True,
        )
        monkeypatch.setattr(
            "google.cloud.managed_spark_connect.environment.is_interactive_terminal",
            lambda: True,
        )
        res = ManagedSparkSession.builder._get_exiting_active_session()
        assert res is None
        fake_session.stop.assert_called_once()
        out_interactive = capsys.readouterr()
        assert (
            "sc-expired Managed Spark Session is not active, stopping and creating a new one"
            in out_interactive.out
        )


def test_show_operation_links_env_and_builder(capsys, monkeypatch):
    """MANAGED_SPARK_CONNECT_SHOW_OPERATION_LINKS and Builder.showOperationLinks control _display_operation_link."""
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: True,
    )
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive_terminal",
        lambda: True,
    )

    session = _bare_session()
    ManagedSparkSession._project_id = "test-project"
    ManagedSparkSession._region = "test-region"
    ManagedSparkSession._active_s8s_session_id = "sc-op-test"

    # Default in interactive mode: prints Spark Query link
    session._display_operation_link("op-default")
    assert "Spark Query:" in capsys.readouterr().out

    # Disabled via environment variable
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_SHOW_OPERATION_LINKS", "0")
    session._display_operation_link("op-env-off")
    assert capsys.readouterr().out == ""

    # An explicit True (Builder.showOperationLinks(True)) overrides env=0
    session._show_operation_links = True
    session._display_operation_link("op-builder-on")
    assert "Spark Query:" in capsys.readouterr().out

    # An explicit False disables even when env=true
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_SHOW_OPERATION_LINKS", "true")
    session._show_operation_links = False
    session._display_operation_link("op-builder-off")
    assert capsys.readouterr().out == ""

    # Settings only suppress: nothing is shown outside interactive use.
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: False,
    )
    session._show_operation_links = True
    session._display_operation_link("op-non-interactive")
    assert capsys.readouterr().out == ""


def test_builder_passes_display_settings_to_the_new_session():
    builder = (
        ManagedSparkSession.Builder()
        .showOperationLinks(False)
        .showProgressBar(False)
        .progressBarLeave(True)
    )
    session_response = Session()
    session_response.uuid = "aaaa1111-bbbb-2222-cccc-333333333333"
    session_response.runtime_info.endpoints = {
        "Spark Connect Server": "sc://spark-connect.example.com:443"
    }
    with (
        mock.patch.object(session_mod, "ManagedSparkSession") as session_cls,
        mock.patch.object(session_mod, "ManagedSparkChannelBuilder"),
        mock.patch.object(session_mod, "mark_s8s_session_active"),
    ):
        builder._Builder__connect(session_response, "projects/p/sessions/s")

    kwargs = session_cls.call_args.kwargs
    assert kwargs["show_operation_links"] is False
    assert kwargs["show_progress_bar"] is False
    assert kwargs["progress_bar_leave"] is True


def test_progress_bar_and_leave_env_and_builder(monkeypatch):
    """MANAGED_SPARK_CONNECT_PROGRESS_BAR, Builder.showProgressBar, and MANAGED_SPARK_CONNECT_PROGRESS_BAR_LEAVE work as expected."""
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: True,
    )

    created_bars = []

    class RecordingTqdm:

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.total = kwargs.get("total", 0)
            self.n = 0
            self.closed = False
            created_bars.append(self)

        def reset(self, total=None):
            if total is not None:
                self.total = total

        def refresh(self):
            pass

        def close(self):
            self.closed = True

    ManagedSparkSession._set_tqdm_factory(RecordingTqdm)

    registered_handlers = []
    session = _bare_session()
    session.registerProgressHandler = lambda h: registered_handlers.append(h)
    session._register_progress_execution_handler()
    assert len(registered_handlers) == 1
    handler = registered_handlers[0]
    stage = mock.Mock(num_tasks=5, num_completed_tasks=2)

    # 1. Default in interactive mode: bar created with leave=False
    handler([stage], 3, "op-1", False)
    assert len(created_bars) == 1
    assert created_bars[-1].kwargs["leave"] is False

    # 2. MANAGED_SPARK_CONNECT_PROGRESS_BAR_LEAVE=1 -> leave=True
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_PROGRESS_BAR_LEAVE", "1")
    handler([stage], 3, "op-2", False)
    assert len(created_bars) == 2
    assert created_bars[-1].kwargs["leave"] is True

    # 3. MANAGED_SPARK_CONNECT_PROGRESS_BAR=0 -> no bar created
    monkeypatch.setenv("MANAGED_SPARK_CONNECT_PROGRESS_BAR", "0")
    handler([stage], 3, "op-3", False)
    assert len(created_bars) == 2

    # 4. Explicit settings (showProgressBar(True), progressBarLeave(False))
    # override the env vars
    session._show_progress_bar = True
    session._progress_bar_leave = False
    handler([stage], 3, "op-4", False)
    assert len(created_bars) == 3
    assert created_bars[-1].kwargs["leave"] is False

    # 5. showProgressBar(False) disables the progress bar
    session._show_progress_bar = False
    handler([stage], 3, "op-5", False)
    assert len(created_bars) == 3
    assert session_mod._progress_bar_enabled(False) is False
    assert session_mod._progress_bar_enabled(True) is True


def test_check_python_version_compatibility_normalizes_patch_versions():
    """_check_python_version_compatibility('3.0.2') normalizes '3.0.2' to '3.0'."""
    builder = ManagedSparkSession.Builder()

    # Matching Python 3.12 -> no warning for "3.0" or "3.0.2"
    with mock.patch.object(sys, "version_info", (3, 12, 4, "final", 0)):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            builder._check_python_version_compatibility("3.0.2")
            assert len(w) == 0

    # Mismatched Python 3.11 -> emits warning for "3.0.2" by normalizing to "3.0"
    with mock.patch.object(sys, "version_info", (3, 11, 9, "final", 0)):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            builder._check_python_version_compatibility("3.0.2")
            assert len(w) == 1
            assert "Python version mismatch detected" in str(w[0].message)
            assert "Client is using Python 3.11" in str(w[0].message)
            assert "runtime 3.0.2 uses Python 3.12" in str(w[0].message)


def test_pypi_artifacts_unpinned_package_logs_at_debug_not_info(caplog):
    """PyPiArtifacts logs unpinned package recommendation at DEBUG, not INFO."""
    from google.cloud.managed_spark_connect.pypi_artifacts import PyPiArtifacts

    with caplog.at_level(
        logging.DEBUG, logger="google.cloud.managed_spark_connect"
    ):
        PyPiArtifacts({"spacy"})

    matching = [
        r
        for r in caplog.records
        if "It is recommended to pin the version of the package" in r.message
    ]
    assert len(matching) == 1
    assert matching[0].levelno == logging.DEBUG


def test_proxied_channel_check_active_logs_at_debug_not_warning(caplog):
    """ProxiedChannel._check_active logs at DEBUG (not WARNING) before raising RuntimeError."""
    from google.cloud.managed_spark_connect.client.core import ProxiedChannel

    from google.cloud.managed_spark_connect.client import core

    ch = object.__new__(ProxiedChannel)
    ch._is_active_callback = lambda: False
    ch._guard_errors = core._OutageLog(core.logger)

    with caplog.at_level(
        logging.DEBUG, logger="google.cloud.managed_spark_connect"
    ):
        with pytest.raises(RuntimeError, match="Session not active"):
            ch._check_active()

    matching = [
        r for r in caplog.records if "Session is no longer active" in r.message
    ]
    assert len(matching) == 1
    assert matching[0].levelno == logging.DEBUG


def test_display_operation_link_returns_cleanly_when_session_metadata_missing(
    capsys, monkeypatch
):
    """_display_operation_link returns cleanly instead of raising AssertionError when metadata is None."""
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive",
        lambda: True,
    )
    monkeypatch.setattr(
        "google.cloud.managed_spark_connect.environment.is_interactive_terminal",
        lambda: True,
    )

    session = _bare_session()
    ManagedSparkSession._project_id = None
    ManagedSparkSession._region = None
    ManagedSparkSession._active_s8s_session_id = None

    # Must not raise AssertionError
    session._display_operation_link("op-missing-meta")
    assert capsys.readouterr().out == ""


def test_pyspark_query_context_loggers_do_not_dump_json_to_stderr(
    capsys, monkeypatch
):
    """SQLQueryContextLogger and DataFrameQueryContextLogger do not dump
    duplicate JSON/JVM stacktraces to stderr when AnalysisException is constructed.
    """
    monkeypatch.delenv("SPARK_CONNECT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MANAGED_SPARK_CONNECT_LOG_LEVEL", raising=False)
    _logging.quiet_pyspark_query_context_loggers()

    from pyspark.logger.logger import PySparkLogger

    for logger_name in ("SQLQueryContextLogger", "DataFrameQueryContextLogger"):
        q_logger = PySparkLogger.getLogger(logger_name)
        q_logger.exception(
            "[TABLE_OR_VIEW_NOT_FOUND] The table `non_existent` cannot be found.",
            errorClass="TABLE_OR_VIEW_NOT_FOUND",
        )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_session_construction_quiets_pyspark_query_context_loggers():
    with mock.patch.object(
        _logging, "quiet_pyspark_query_context_loggers"
    ) as quiet:
        session = ManagedSparkSession(connection="sc://localhost:15002")
    try:
        quiet.assert_called_once_with()
    finally:
        session.stop()
