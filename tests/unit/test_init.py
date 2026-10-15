# Copyright 2025 Google LLC
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
import unittest
from unittest import mock

from google.cloud.managed_spark_connect.session import ManagedSparkSession
from google.cloud.managed_spark_connect.exceptions import ManagedSparkConnectException


class TestPythonVersionCheck(unittest.TestCase):

    def test_python_version_mismatch_warning_for_runtime_30(self):
        """Test that warning is shown when client Python doesn't match runtime 3.0 (Python 3.12)"""
        runtime_version = "3.0"
        server_py_major, server_py_minor = 3, 12
        client_py_major, client_py_minor = 3, 11

        with mock.patch(
            "sys.version_info", (client_py_major, client_py_minor, 0)
        ):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = ManagedSparkSession.Builder()
                session_builder._check_python_version_compatibility(
                    runtime_version
                )

                expected_warning = (
                    f"Python version mismatch detected: Client is using Python {client_py_major}.{client_py_minor}, "
                    f"but Managed Spark runtime {runtime_version} uses Python {server_py_major}.{server_py_minor}. "
                    "This mismatch may cause issues with Python UDF (User Defined Function) compatibility. "
                    f"Consider using Python {server_py_major}.{server_py_minor} for optimal UDF execution."
                )
                mock_warn.assert_called_once_with(
                    expected_warning, stacklevel=3
                )

    def test_no_warning_when_python_versions_match_runtime_30(self):
        """Test that no warning is shown when client Python matches runtime 3.0 (Python 3.12)"""
        runtime_version = "3.0"
        client_py_major, client_py_minor = 3, 12
        with mock.patch(
            "sys.version_info", (client_py_major, client_py_minor, 0)
        ):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = ManagedSparkSession.Builder()
                session_builder._check_python_version_compatibility(
                    runtime_version
                )

                mock_warn.assert_not_called()

    def test_no_warning_for_unknown_runtime_version(self):
        """Test that no warning is shown for unknown runtime versions"""
        with mock.patch("sys.version_info", (3, 10, 0)):
            with mock.patch("warnings.warn") as mock_warn:
                session_builder = ManagedSparkSession.Builder()
                session_builder._check_python_version_compatibility("unknown")

                mock_warn.assert_not_called()


class TestRuntimeVersionCompatibility(unittest.TestCase):

    def test_older_runtimes_raise_exception(self):
        """Test that runtime versions < MIN_SUPPORTED_RUNTIME_VERSION raise ManagedSparkConnectException"""
        session_builder = ManagedSparkSession.Builder()
        old_versions = ["2.4", "2.2", "1.0"]

        for version in old_versions:
            with self.subTest(version=version):
                mock_session_config = mock.Mock()
                mock_session_config.runtime_config.version = version

                with self.assertRaises(ManagedSparkConnectException) as context:
                    session_builder._check_runtime_compatibility(
                        mock_session_config
                    )

                min_version = ManagedSparkSession._MIN_RUNTIME_VERSION
                expected_message = (
                    f"Specified {version} Managed Spark Runtime version is not supported, "
                    f"use {min_version} version or higher."
                )
                self.assertEqual(str(context.exception), expected_message)

    def test_newer_runtimes_succeed(self):
        """Test that runtime versions >= MIN_RUNTIME_VERSION succeed"""
        session_builder = ManagedSparkSession.Builder()
        new_versions = ["3.0", "3.1", "4.0"]

        for version in new_versions:
            with self.subTest(version=version):
                mock_session_config = mock.Mock()
                mock_session_config.runtime_config.version = version

                try:
                    session_builder._check_runtime_compatibility(
                        mock_session_config
                    )
                except ManagedSparkConnectException:
                    self.fail(
                        f"_check_runtime_compatibility raised ManagedSparkConnectException unexpectedly for version {version}"
                    )

    @mock.patch("google.cloud.managed_spark_connect.session.logger")
    def test_invalid_runtime_version_logs_warning(self, mock_logger):
        """Test that invalid runtime versions are logged as warnings but don't fail"""
        session_builder = ManagedSparkSession.Builder()

        # Mock dataproc config with invalid runtime version
        mock_session_config = mock.Mock()
        mock_session_config.runtime_config.version = "invalid.version"

        # Should not raise any exception, but should log warning
        try:
            session_builder._check_runtime_compatibility(mock_session_config)
        except Exception:
            self.fail(
                "_check_runtime_compatibility raised exception unexpectedly"
            )

        mock_logger.warning.assert_called_once_with(
            "Could not parse runtime version: invalid.version"
        )


class TestDependencies(unittest.TestCase):

    def test_colabsqlviz_available(self):
        import importlib.metadata
        from packaging.version import Version
        import google.colabsqlviz
        from google.colabsqlviz.explore_dataframe import (
            display,
            explore_dataframe,
        )

        self.assertIsNotNone(google.colabsqlviz)
        self.assertTrue(callable(display))
        self.assertTrue(callable(explore_dataframe))
        version = importlib.metadata.version("google-colabsqlviz")
        self.assertGreaterEqual(Version(version), Version("0.4.0"))

    def test_managed_spark_magics_available(self):
        import google.cloud.managed_spark_magics as magics_ext

        self.assertTrue(callable(magics_ext.load_ipython_extension))
        self.assertTrue(callable(magics_ext.unload_ipython_extension))


def _make_mock_shell(user_ns=None, config=None):
    """Builds a mock shell whose push() mutates user_ns, like the real one.

    Without the side effect, mock shells silently swallow ip.push() and only
    pass if the code under test also writes to user_ns directly.
    """
    mock_ip = mock.Mock()
    mock_ip.user_ns = {} if user_ns is None else user_ns
    mock_ip.config = config
    mock_ip.configurables = []
    mock_ip.push.side_effect = mock_ip.user_ns.update
    mock_ip.extension_manager.load_extension.return_value = None
    return mock_ip


class TestExtrasUnit(unittest.TestCase):

    def setUp(self):
        import os
        from google.cloud.managed_spark_connect import _ipython

        env_patcher = mock.patch.dict(os.environ)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        os.environ.pop(_ipython.ENV_ENABLE_EXTRAS, None)
        _ipython._SHELL_STATES.clear()
        self.addCleanup(_ipython._SHELL_STATES.clear)

    @mock.patch("sys.stdout", new_callable=unittest.mock.MagicMock)
    @mock.patch("IPython.get_ipython", return_value=None)
    def test_no_extras_when_not_in_ipython(self, mock_get_ipython, mock_stdout):
        from google.cloud.managed_spark_connect._ipython import _init_extras

        _init_extras()
        mock_get_ipython.assert_called_once()
        mock_stdout.write.assert_not_called()

    @mock.patch("IPython.get_ipython")
    def test_successful_extras_loading(self, mock_get_ipython):
        import io
        from google.colabsqlviz.explore_dataframe import explore_dataframe
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell()
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        mock_ip.push.assert_called_once_with(
            {"explore_dataframe": explore_dataframe}
        )
        self.assertIs(mock_ip.user_ns["explore_dataframe"], explore_dataframe)
        mock_ip.extension_manager.load_extension.assert_called_once_with(
            "google.cloud.managed_spark_magics"
        )
        output = stdout_capture.getvalue()
        self.assertIn("[google.cloud.managed_spark_connect]", output)
        self.assertIn("Loaded interactive extras", output)
        self.assertIn("explore_dataframe()", output)
        self.assertIn("%dpip", output)
        # A line+cell magic is listed once, under its cell name.
        self.assertIn("%%sparksql", output)
        self.assertEqual(output.count("sparksql"), 1)
        self.assertIn("ManagedSparkConnect.enable_extras = False", output)
        # One summary line, not one per extra.
        self.assertEqual(len(output.splitlines()), 1)

    @mock.patch("IPython.get_ipython")
    def test_fallback_when_already_defined_short_repr(self, mock_get_ipython):
        import io
        from google.cloud.managed_spark_connect._ipython import (
            _get_or_create_config,
            _init_extras,
        )

        mock_ip = _make_mock_shell(user_ns={"explore_dataframe": 42})
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        mock_ip.push.assert_not_called()
        output = stdout_capture.getvalue()
        self.assertIn("[google.cloud.managed_spark_connect]", output)
        self.assertIn("Did not inject", output)
        self.assertIn("explore_dataframe()", output)
        self.assertIn("because it is already defined as: 42", output)
        self.assertNotIn("42...", output)

        # Disabling extras via traitlet must not remove user-defined explore_dataframe
        _get_or_create_config(mock_ip).enable_extras = False
        self.assertEqual(mock_ip.user_ns.get("explore_dataframe"), 42)

    @mock.patch("IPython.get_ipython")
    def test_fallback_when_already_defined_long_repr(self, mock_get_ipython):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        long_val = "x" * 100
        mock_ip = _make_mock_shell(user_ns={"explore_dataframe": long_val})
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        mock_ip.push.assert_not_called()
        output = stdout_capture.getvalue()
        expected_repr = f"{repr(long_val)[:60]}..."
        self.assertIn("[google.cloud.managed_spark_connect]", output)
        self.assertIn("Did not inject", output)
        self.assertIn("explore_dataframe()", output)
        self.assertIn(
            f"because it is already defined as: {expected_repr}",
            output,
        )

    @mock.patch("IPython.get_ipython")
    def test_fallback_when_already_defined_unreprable(self, mock_get_ipython):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        class Unreprable:

            def __repr__(self):
                raise ValueError("no repr for you")

        mock_ip = _make_mock_shell(user_ns={"explore_dataframe": Unreprable()})
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        mock_ip.push.assert_not_called()
        output = stdout_capture.getvalue()
        self.assertIn("Did not inject", output)
        self.assertIn("Unreprable", output)
        # A broken __repr__ must not abort the rest of the initialization.
        mock_ip.extension_manager.load_extension.assert_called_once_with(
            "google.cloud.managed_spark_magics"
        )

    @mock.patch("IPython.get_ipython")
    def test_no_warning_when_already_defined_same_function(
        self, mock_get_ipython
    ):
        import io
        from google.colabsqlviz.explore_dataframe import explore_dataframe
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell(
            user_ns={"explore_dataframe": explore_dataframe}
        )
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        # The name already refers to the exact function we would have injected,
        # so there is nothing to warn the user about, and since we didn't load
        # it, it isn't listed either.
        mock_ip.push.assert_not_called()
        output = stdout_capture.getvalue()
        self.assertNotIn("Did not inject", output)
        self.assertNotIn("explore_dataframe", output)
        self.assertIn("Loaded interactive extras", output)
        self.assertIs(mock_ip.user_ns["explore_dataframe"], explore_dataframe)

    @mock.patch("IPython.get_ipython")
    def test_opt_out_via_env_var(self, mock_get_ipython):
        import os
        from google.cloud.managed_spark_connect._ipython import (
            ENV_ENABLE_EXTRAS,
            _init_extras,
        )

        for falsy_val in ("false", "FALSE", "0", "no", "off", " false "):
            with self.subTest(val=falsy_val):
                mock_ip = _make_mock_shell()
                mock_get_ipython.return_value = mock_ip

                with mock.patch.dict(
                    os.environ, {ENV_ENABLE_EXTRAS: falsy_val}
                ):
                    _init_extras()

                mock_ip.push.assert_not_called()
                mock_ip.extension_manager.load_extension.assert_not_called()
                self.assertNotIn("explore_dataframe", mock_ip.user_ns)

    @mock.patch("IPython.get_ipython")
    def test_opt_out_via_traitlet_config(self, mock_get_ipython):
        from traitlets.config import Config
        from google.cloud.managed_spark_connect._ipython import _init_extras

        cfg = Config()
        cfg.ManagedSparkConnect.enable_extras = False
        mock_ip = _make_mock_shell(config=cfg)
        mock_get_ipython.return_value = mock_ip

        _init_extras()

        mock_ip.push.assert_not_called()
        mock_ip.extension_manager.load_extension.assert_not_called()
        self.assertNotIn("explore_dataframe", mock_ip.user_ns)

    @mock.patch("IPython.get_ipython")
    def test_explicit_config_beats_env_var(self, mock_get_ipython):
        import io
        import os
        from traitlets.config import Config
        from google.cloud.managed_spark_connect._ipython import (
            ENV_ENABLE_EXTRAS,
            _init_extras,
        )

        # An explicit `c.ManagedSparkConnect.enable_extras = True` is a more
        # specific signal than the environment variable, so it must win.
        cfg = Config()
        cfg.ManagedSparkConnect.enable_extras = True
        mock_ip = _make_mock_shell(config=cfg)
        mock_get_ipython.return_value = mock_ip

        with mock.patch.dict(os.environ, {ENV_ENABLE_EXTRAS: "false"}):
            with mock.patch("sys.stdout", io.StringIO()):
                _init_extras()

        self.assertIn("explore_dataframe", mock_ip.user_ns)
        mock_ip.extension_manager.load_extension.assert_called_once_with(
            "google.cloud.managed_spark_magics"
        )

    @mock.patch("IPython.get_ipython")
    def test_blank_env_var_is_treated_as_unset(self, mock_get_ipython):
        import io
        import os
        from google.cloud.managed_spark_connect._ipython import (
            ENV_ENABLE_EXTRAS,
            _init_extras,
        )

        for blank_val in ("", "   "):
            with self.subTest(val=blank_val):
                mock_ip = _make_mock_shell()
                mock_get_ipython.return_value = mock_ip

                with mock.patch.dict(
                    os.environ, {ENV_ENABLE_EXTRAS: blank_val}
                ):
                    with mock.patch("sys.stdout", io.StringIO()):
                        _init_extras()

                self.assertIn("explore_dataframe", mock_ip.user_ns)

    @mock.patch("IPython.get_ipython")
    def test_traitlet_disable_preserves_overwritten_explore_dataframe(
        self, mock_get_ipython
    ):
        import io
        from google.cloud.managed_spark_connect._ipython import (
            _get_or_create_config,
            _init_extras,
        )

        mock_ip = _make_mock_shell()
        mock_get_ipython.return_value = mock_ip

        with mock.patch("sys.stdout", io.StringIO()):
            _init_extras()

        custom_fn = lambda df: "custom"
        mock_ip.user_ns["explore_dataframe"] = custom_fn

        _get_or_create_config(mock_ip).enable_extras = False
        self.assertIs(mock_ip.user_ns.get("explore_dataframe"), custom_fn)
        mock_ip.extension_manager.unload_extension.assert_called_once_with(
            "google.cloud.managed_spark_magics"
        )

    @mock.patch("IPython.get_ipython", side_effect=RuntimeError("Kernel error"))
    def test_exception_silently_caught(self, mock_get_ipython):
        from google.cloud.managed_spark_connect._ipython import _init_extras

        try:
            _init_extras()
        except Exception as e:
            self.fail(f"_init_extras raised an exception: {e}")

    @mock.patch("IPython.get_ipython", side_effect=RuntimeError("Kernel error"))
    def test_no_failure_message_outside_a_shell(self, mock_get_ipython):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        # There is no shell, so there are no extras to miss.
        self.assertEqual(stdout_capture.getvalue(), "")

    @mock.patch("IPython.get_ipython")
    def test_failure_message_printed_once(self, mock_get_ipython):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell()
        mock_ip.extension_manager.load_extension.side_effect = RuntimeError(
            "boom"
        )
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch(
            "google.cloud.managed_spark_connect._ipython"
            "._import_explore_dataframe",
            side_effect=RuntimeError("boom"),
        ):
            with mock.patch("sys.stdout", stdout_capture):
                _init_extras()

        output = stdout_capture.getvalue()
        self.assertIn("Failed to load interactive extras", output)
        # Two separate failures (the injection and the extension) must not
        # produce two separate messages.
        self.assertEqual(output.count("Failed to load interactive extras"), 1)

    @mock.patch("IPython.get_ipython")
    def test_failure_message_when_colabsqlviz_fails_to_import(
        self, mock_get_ipython
    ):
        import builtins
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell()
        mock_get_ipython.return_value = mock_ip

        real_import = builtins.__import__

        def fail_colabsqlviz(name, *args, **kwargs):
            if name.startswith("google.colabsqlviz"):
                raise ImportError("no colabsqlviz here")
            return real_import(name, *args, **kwargs)

        stdout_capture = io.StringIO()
        with mock.patch("builtins.__import__", side_effect=fail_colabsqlviz):
            with mock.patch("sys.stdout", stdout_capture):
                _init_extras()

        # colabsqlviz is installed but broken, which is worth reporting.
        self.assertNotIn("explore_dataframe", mock_ip.user_ns)
        self.assertIn(
            "Failed to load interactive extras", stdout_capture.getvalue()
        )
        # The magics are independent of colabsqlviz and should still load.
        mock_ip.extension_manager.load_extension.assert_called_once_with(
            "google.cloud.managed_spark_magics"
        )

    @mock.patch("IPython.get_ipython")
    def test_colabsqlviz_not_installed_is_skipped(self, mock_get_ipython):
        import io
        import sys
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell()
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        # A None entry in sys.modules makes both find_spec() and import behave
        # as if the module were not installed.
        with mock.patch.dict(sys.modules, {"google.colabsqlviz": None}):
            with mock.patch("sys.stdout", stdout_capture):
                _init_extras()

        mock_ip.push.assert_not_called()
        self.assertNotIn("explore_dataframe", mock_ip.user_ns)
        mock_ip.extension_manager.load_extension.assert_called_once_with(
            "google.cloud.managed_spark_magics"
        )
        # Only what was actually loaded is listed, and a missing optional
        # dependency is not a failure.
        output = stdout_capture.getvalue()
        self.assertEqual(len(output.splitlines()), 1)
        self.assertIn("Loaded interactive extras", output)
        self.assertIn("%dpip", output)
        self.assertNotIn("explore_dataframe", output)
        self.assertNotIn("Failed to load interactive extras", output)

    @mock.patch("IPython.get_ipython")
    def test_colabsqlviz_not_installed_does_not_warn_about_existing_name(
        self, mock_get_ipython
    ):
        import io
        import sys
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell(user_ns={"explore_dataframe": 42})
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch.dict(sys.modules, {"google.colabsqlviz": None}):
            with mock.patch("sys.stdout", stdout_capture):
                _init_extras()

        # We would not have injected anything, so there is no conflict to
        # warn about.
        self.assertNotIn("Did not inject", stdout_capture.getvalue())
        self.assertEqual(mock_ip.user_ns["explore_dataframe"], 42)

    @mock.patch("IPython.get_ipython")
    def test_nothing_printed_when_nothing_is_loaded(self, mock_get_ipython):
        import io
        import sys
        from google.cloud.managed_spark_connect._ipython import _init_extras

        mock_ip = _make_mock_shell()
        mock_get_ipython.return_value = mock_ip

        stdout_capture = io.StringIO()
        with mock.patch.dict(
            sys.modules,
            {
                "google.colabsqlviz": None,
                "google.cloud.managed_spark_magics": None,
            },
        ):
            with mock.patch("sys.stdout", stdout_capture):
                _init_extras()

        mock_ip.extension_manager.load_extension.assert_not_called()
        self.assertEqual(stdout_capture.getvalue(), "")

    @mock.patch("IPython.get_ipython")
    def test_summary_not_repeated_on_second_load(self, mock_get_ipython):
        import io
        from google.cloud.managed_spark_connect._ipython import (
            _init_extras,
            _load_extras_for_shell,
        )

        mock_ip = _make_mock_shell()
        mock_get_ipython.return_value = mock_ip

        with mock.patch("sys.stdout", io.StringIO()):
            _init_extras()

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _load_extras_for_shell(mock_ip)

        # Everything is already loaded, so there is nothing new to report.
        self.assertEqual(stdout_capture.getvalue(), "")

    def test_is_module_available(self):
        from google.cloud.managed_spark_connect._ipython import (
            _is_module_available,
        )

        self.assertTrue(_is_module_available("json"))
        self.assertTrue(_is_module_available("google.colabsqlviz"))
        self.assertFalse(_is_module_available("no_such_module_for_test"))
        # Missing leaf in an existing (namespace) package.
        self.assertFalse(_is_module_available("google.no_such_module_for_test"))
        # Missing parent package.
        self.assertFalse(
            _is_module_available("no_such_module_for_test.submodule")
        )

    def test_module_import_calls_init_extras(self):
        import importlib
        import google.cloud.managed_spark_connect as msc

        # Configured by class name; not part of the package's API.
        self.assertFalse(hasattr(msc, "ManagedSparkConnect"))
        self.assertFalse(hasattr(msc, "enable_extras"))
        self.assertFalse(hasattr(msc, "disable_extras"))

        # reload() re-executes `from ._ipython import _init_extras`, which
        # copies whatever is patched at that moment into the package namespace.
        # Undoing the patch does not undo that copy, so reload again outside it
        # or every later test in this process sees the mock.
        self.addCleanup(importlib.reload, msc)
        with mock.patch(
            "google.cloud.managed_spark_connect._ipython._init_extras"
        ) as mock_init:
            importlib.reload(msc)
            mock_init.assert_called_once()


_OUR_MAGICS = (("line", "dpip"), ("line", "sparksql"), ("cell", "sparksql"))


def _pop_our_magics(ip):
    for kind, name in _OUR_MAGICS:
        ip.magics_manager.magics.get(kind, {}).pop(name, None)


class TestExtrasInteractiveShell(unittest.TestCase):

    def _assert_magics(self, present):
        magics = self.ip.magics_manager.magics
        for kind, name in _OUR_MAGICS:
            with self.subTest(kind=kind, name=name):
                self.assertEqual(name in magics[kind], present)

    def setUp(self):
        import os
        from IPython.core.interactiveshell import InteractiveShell
        from google.cloud.managed_spark_connect import _ipython

        env_patcher = mock.patch.dict(os.environ)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        os.environ.pop(_ipython.ENV_ENABLE_EXTRAS, None)
        _ipython._SHELL_STATES.clear()

        self.ip = InteractiveShell.instance()
        self.ip.user_ns.pop("explore_dataframe", None)
        _pop_our_magics(self.ip)
        self.ip.extension_manager.loaded.discard(
            "google.cloud.managed_spark_magics"
        )
        self.ip.extension_manager.loaded.discard(
            "google.cloud.managed_spark_magics.sparksql"
        )
        if "ManagedSparkConnect" in self.ip.config:
            del self.ip.config["ManagedSparkConnect"]
        self.ip.configurables[:] = [
            c
            for c in self.ip.configurables
            if c.__class__.__name__ != "ManagedSparkConnect"
        ]

        self._get_ipython_patcher = mock.patch(
            "IPython.get_ipython", return_value=self.ip
        )
        self._get_ipython_patcher.start()

    def tearDown(self):
        from IPython.core.interactiveshell import InteractiveShell
        from google.cloud.managed_spark_connect import _ipython

        self._get_ipython_patcher.stop()
        self.ip.user_ns.pop("explore_dataframe", None)
        _pop_our_magics(self.ip)
        self.ip.extension_manager.loaded.discard(
            "google.cloud.managed_spark_magics"
        )
        self.ip.extension_manager.loaded.discard(
            "google.cloud.managed_spark_magics.sparksql"
        )
        if "ManagedSparkConnect" in self.ip.config:
            del self.ip.config["ManagedSparkConnect"]
        self.ip.configurables[:] = [
            c
            for c in self.ip.configurables
            if c.__class__.__name__ != "ManagedSparkConnect"
        ]
        InteractiveShell.clear_instance()
        _ipython._SHELL_STATES.clear()

    def test_real_shell_loads_all_extras_and_toggles_via_config(self):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        with mock.patch("sys.stdout", io.StringIO()):
            _init_extras()

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)

        self.ip.run_line_magic(
            "config", "ManagedSparkConnect.enable_extras = False"
        )

        self.assertNotIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=False)

        with mock.patch("sys.stdout", io.StringIO()):
            self.ip.run_line_magic(
                "config", "ManagedSparkConnect.enable_extras = True"
            )

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)

    def test_real_shell_config_magic_pre_and_post_import(self):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        # 1. Opt out via %config BEFORE _init_extras()
        self.ip.run_line_magic(
            "config", "ManagedSparkConnect.enable_extras = False"
        )
        with mock.patch("sys.stdout", io.StringIO()):
            _init_extras()

        self.assertNotIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=False)

        # 2. Enable via %config AFTER _init_extras()
        with mock.patch("sys.stdout", io.StringIO()):
            self.ip.run_line_magic(
                "config", "ManagedSparkConnect.enable_extras = True"
            )

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)

        # 3. Disable via %config AFTER _init_extras()
        self.ip.run_line_magic(
            "config", "ManagedSparkConnect.enable_extras = False"
        )

        self.assertNotIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=False)

    def test_real_shell_partial_undo_preserves_preloaded_extension(self):
        import io
        from google.cloud.managed_spark_connect._ipython import _init_extras

        # User manually loads %dpip before managed_spark_connect initializes extras
        self.ip.extension_manager.load_extension(
            "google.cloud.managed_spark_magics"
        )
        self._assert_magics(present=True)

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)
        # %dpip was the user's doing, so the summary must not claim it.
        output = stdout_capture.getvalue()
        self.assertIn("explore_dataframe()", output)
        self.assertNotIn("%dpip", output)

        # Disabling extras via %config must keep %dpip (pre-loaded by user) while removing explore_dataframe
        self.ip.run_line_magic(
            "config", "ManagedSparkConnect.enable_extras = False"
        )

        self.assertNotIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)

    def test_real_shell_preserves_preloaded_sparksql_extension(self):
        import io
        from google.cloud.managed_spark_connect._ipython import (
            _get_or_create_config,
            _init_extras,
        )

        self.ip.extension_manager.load_extension(
            "google.cloud.managed_spark_magics.sparksql"
        )
        magics = self.ip.magics_manager.magics
        line_fn = magics["line"]["sparksql"]
        cell_fn = magics["cell"]["sparksql"]

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self.assertIn("dpip", magics["line"])
        self.assertIs(magics["line"]["sparksql"], line_fn)
        self.assertIs(magics["cell"]["sparksql"], cell_fn)
        output = stdout_capture.getvalue()
        self.assertIn("%dpip", output)
        self.assertNotIn("sparksql", output)

        _get_or_create_config(self.ip).enable_extras = False

        self.assertNotIn("explore_dataframe", self.ip.user_ns)
        self.assertNotIn("dpip", magics["line"])
        self.assertIs(magics["line"]["sparksql"], line_fn)
        self.assertIs(magics["cell"]["sparksql"], cell_fn)

    def test_real_shell_preserves_other_sparksql_magic(self):
        import io
        from google.cloud.managed_spark_connect._ipython import (
            _get_or_create_config,
            _init_extras,
        )

        def other_sparksql(line, cell=None):
            pass

        magics = self.ip.magics_manager.magics
        self.ip.register_magic_function(
            other_sparksql, magic_kind="line_cell", magic_name="sparksql"
        )
        line_fn = magics["line"]["sparksql"]
        cell_fn = magics["cell"]["sparksql"]

        stdout_capture = io.StringIO()
        with mock.patch("sys.stdout", stdout_capture):
            _init_extras()

        # %dpip is still ours to add, but the existing %%sparksql stays put
        # and is not claimed in the summary.
        self.assertIn("dpip", magics["line"])
        self.assertIs(magics["line"]["sparksql"], line_fn)
        self.assertIs(magics["cell"]["sparksql"], cell_fn)
        output = stdout_capture.getvalue()
        self.assertIn("%dpip", output)
        self.assertNotIn("sparksql", output)

        _get_or_create_config(self.ip).enable_extras = False

        self.assertNotIn("dpip", magics["line"])
        self.assertIs(magics["line"]["sparksql"], line_fn)
        self.assertIs(magics["cell"]["sparksql"], cell_fn)

    def test_real_shell_unload_keeps_sparksql_registered_later(self):
        import io
        from google.cloud.managed_spark_connect._ipython import (
            _get_or_create_config,
            _init_extras,
        )

        with mock.patch("sys.stdout", io.StringIO()):
            _init_extras()

        # Something else takes over %%sparksql after the extras loaded.
        def other_sparksql(line, cell=None):
            pass

        magics = self.ip.magics_manager.magics
        self.ip.register_magic_function(
            other_sparksql, magic_kind="line_cell", magic_name="sparksql"
        )
        line_fn = magics["line"]["sparksql"]
        cell_fn = magics["cell"]["sparksql"]

        _get_or_create_config(self.ip).enable_extras = False

        self.assertNotIn("dpip", magics["line"])
        self.assertIs(magics["line"]["sparksql"], line_fn)
        self.assertIs(magics["cell"]["sparksql"], cell_fn)

    def test_real_shell_config_magic_overrides_env_opt_out(self):
        import io
        import os
        from google.cloud.managed_spark_connect._ipython import (
            ENV_ENABLE_EXTRAS,
            _init_extras,
        )

        # %config is evaluated before the import, so the merged shell config
        # must beat the environment variable rather than being vetoed by it.
        self.ip.run_line_magic(
            "config", "ManagedSparkConnect.enable_extras = True"
        )
        with mock.patch.dict(os.environ, {ENV_ENABLE_EXTRAS: "false"}):
            with mock.patch("sys.stdout", io.StringIO()):
                _init_extras()

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)

    def test_real_shell_env_opt_out_then_enable_via_config_magic(self):
        import io
        import os
        from google.cloud.managed_spark_connect._ipython import (
            ENV_ENABLE_EXTRAS,
            _init_extras,
        )

        with mock.patch.dict(os.environ, {ENV_ENABLE_EXTRAS: "false"}):
            with mock.patch("sys.stdout", io.StringIO()):
                _init_extras()
            self.assertNotIn("explore_dataframe", self.ip.user_ns)

            # Turning the trait on must actually load, and the trait must
            # report the same thing the shell is really in.
            with mock.patch("sys.stdout", io.StringIO()):
                self.ip.run_line_magic(
                    "config", "ManagedSparkConnect.enable_extras = True"
                )

        self.assertIn("explore_dataframe", self.ip.user_ns)
        self._assert_magics(present=True)


_IMPORT_ORDER_PROBE = """
import io, sys
from contextlib import redirect_stdout
from IPython.core.interactiveshell import InteractiveShell

ip = InteractiveShell.instance()
with redirect_stdout(io.StringIO()):
    if sys.argv[1] == "magics_first":
        import google.cloud.managed_spark_magics
    elif sys.argv[1] == "load_ext_first":
        ip.run_line_magic("load_ext", "google.cloud.managed_spark_magics")
    import google.cloud.managed_spark_connect

print("explore_dataframe" in ip.user_ns)
print("dpip" in ip.magics_manager.magics["line"])
print("sparksql" in ip.magics_manager.magics["line"])
print("sparksql" in ip.magics_manager.magics["cell"])
"""


class TestExtrasImportOrder(unittest.TestCase):
    """Each import order must end up with the same set of extras loaded.

    Has to run out of process: once a module is in sys.modules, the import
    order that produced it cannot be replayed.
    """

    def _run(self, order):
        import os
        import subprocess
        import sys

        env = dict(os.environ)
        env.pop("MANAGED_SPARK_CONNECT_ENABLE_EXTRAS", None)
        result = subprocess.run(
            [sys.executable, "-c", _IMPORT_ORDER_PROBE, order],
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.split()

    def test_all_import_orders_load_all_extras(self):
        for order in ("connect_first", "magics_first", "load_ext_first"):
            with self.subTest(order=order):
                # Importing managed_spark_magics first used to re-enter
                # _init_extras() while that module was still initializing,
                # which silently dropped %dpip.
                self.assertEqual(
                    self._run(order),
                    ["True", "True", "True", "True"],
                    "expected explore_dataframe, %dpip and %%sparksql",
                )


_NO_INTERACTIVE_DEPS_PROBE = """
import sys

# Simulate an install without the [interactive] extra. A None entry makes both
# importlib.util.find_spec() and import treat the module as absent.
for name in ("IPython", "sqlparse", "traitlets", "google.colabsqlviz"):
    sys.modules[name] = None

import google.cloud.managed_spark_connect as msc

print(msc.ManagedSparkSession is not None)
print(hasattr(msc, "ManagedSparkConnect"))
print("google.cloud.managed_spark_connect._ipython" in sys.modules)
"""


class TestWithoutInteractiveExtra(unittest.TestCase):
    """The base package must not need anything from the [interactive] extra.

    Runs out of process so that nothing already imported by the test runner
    (IPython in particular) leaks in.
    """

    def test_import_without_interactive_deps(self):
        import os
        import subprocess
        import sys

        env = dict(os.environ)
        env.pop("MANAGED_SPARK_CONNECT_ENABLE_EXTRAS", None)
        result = subprocess.run(
            [sys.executable, "-c", _NO_INTERACTIVE_DEPS_PROBE],
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(), ["True", "False", "False"])


if __name__ == "__main__":
    unittest.main()
