# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import dataclasses
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

from google.cloud.managed_spark_connect import ide

# product.json values (subset) recorded from shipped editors on macOS:
# Antigravity IDE 2.5.5, Visual Studio Code 1.142.0, Cursor 3.24.18.
AGY_IDE_PRODUCT = {
    "nameShort": "Antigravity IDE",
    "nameLong": "Antigravity IDE",
    "applicationName": "antigravity-ide",
    "dataFolderName": ".antigravity-ide",
    "urlProtocol": "antigravity-ide",
    "darwinBundleIdentifier": "com.google.antigravity-ide",
}
VSCODE_PRODUCT = {
    "nameShort": "Code",
    "nameLong": "Visual Studio Code",
    "applicationName": "code",
    "dataFolderName": ".vscode",
    "urlProtocol": "vscode",
    "darwinBundleIdentifier": "com.microsoft.VSCode",
}
CURSOR_PRODUCT = {
    "nameShort": "Cursor",
    "nameLong": "Cursor",
    "applicationName": "cursor",
    "dataFolderName": ".cursor",
    "urlProtocol": "cursor",
    "darwinBundleIdentifier": "com.todesktop.230313mzl4w4u92",
}
# `code serve-web` / remote server product.json (urlProtocol vscode, server
# data folder .vscode-server), next to the server's `node` binary.
VSCODE_SERVER_PRODUCT = dict(
    VSCODE_PRODUCT, serverDataFolderName=".vscode-server"
)

# Extensions directory entries as written by the editors (trimmed).
DAK_ENTRY = {
    "identifier": {"id": "googlecloudtools.datacloud"},
    "version": "99.99.99",
    "location": {
        "$mid": 1,
        "path": "/Users/u/.antigravity-ide/extensions/googlecloudtools.datacloud-99.99.99",
        "scheme": "file",
    },
    "relativeLocation": "googlecloudtools.datacloud-99.99.99",
}
JUPYTER_ENTRY = {
    "identifier": {"id": "ms-toolsai.jupyter"},
    "version": "2025.9.1",
    "relativeLocation": "ms-toolsai.jupyter-2025.9.1-universal",
}

MAC_HELPER = (
    "Contents/Frameworks/{name} Helper (Plugin).app/Contents/MacOS/"
    "{name} Helper (Plugin)"
)


def _proc(pid, ppid, exe, cmdline=()):
    return ide._ProcInfo(pid, ppid, exe, tuple(cmdline))


def _lookup(*procs):
    table = {p.pid: p for p in procs}
    return table.get


class IdeTestBase(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.home = os.path.join(self.tmp, "home")
        os.makedirs(self.home)
        ide._reset_cache_for_testing()
        self.addCleanup(ide._reset_cache_for_testing)

    def write_json(self, rel, data):
        path = os.path.join(self.tmp, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(data if isinstance(data, str) else json.dumps(data))
        return path

    def mac_app(self, app, product):
        self.write_json(f"{app}/Contents/Resources/app/product.json", product)
        name = app[: -len(".app")]
        return os.path.join(self.tmp, app, MAC_HELPER.format(name=name))

    def detect(self, lookup, env=None, start_pid=100):
        return ide._detect(env or {}, lookup, start_pid, self.home)


class DetectIdeTest(IdeTestBase):

    def test_antigravity_ide_macos(self):
        helper = self.mac_app("Antigravity IDE.app", AGY_IDE_PRODUCT)
        info = self.detect(
            _lookup(
                _proc(100, 50, helper),
                _proc(50, 1, "/Applications/Antigravity IDE.app/x"),
            )
        )
        self.assertEqual(info.family, "vscode")
        self.assertEqual(info.flavor, "antigravity-ide")
        self.assertEqual(info.name, "Antigravity IDE")
        self.assertEqual(info.url_protocol, "antigravity-ide")
        self.assertEqual(info.data_folder_name, ".antigravity-ide")
        self.assertEqual(
            info.extensions_dir,
            os.path.join(self.home, ".antigravity-ide", "extensions"),
        )
        self.assertFalse(info.launched_from_terminal)
        self.assertTrue(info.source.startswith("process:100:"))

    def test_cursor_macos(self):
        helper = self.mac_app("Cursor.app", CURSOR_PRODUCT)
        info = self.detect(_lookup(_proc(100, 1, helper)))
        self.assertEqual(info.url_protocol, "cursor")
        self.assertEqual(info.flavor, "cursor")
        self.assertEqual(
            info.extensions_dir,
            os.path.join(self.home, ".cursor", "extensions"),
        )

    def test_vscode_linux_through_wrapper_processes(self):
        self.write_json(
            "usr/share/code/resources/app/product.json", VSCODE_PRODUCT
        )
        code = os.path.join(self.tmp, "usr/share/code/code")
        info = self.detect(
            _lookup(
                _proc(100, 90, "/usr/bin/bash"),
                _proc(90, 80, "/opt/conda/bin/python3.12"),
                _proc(80, 1, code, [code, "--type=utility"]),
            )
        )
        self.assertEqual(info.url_protocol, "vscode")
        self.assertEqual(info.flavor, "code")
        self.assertIn("process:80:", info.source)

    def test_serve_web_node_with_sibling_product_json(self):
        root = os.path.join(self.tmp, "cli/serve-web/6d62b1d5")
        self.write_json(
            "cli/serve-web/6d62b1d5/product.json", VSCODE_SERVER_PRODUCT
        )
        node = os.path.join(root, "node")
        server_data = os.path.join(self.tmp, "server-data")
        os.makedirs(os.path.join(server_data, "extensions"))
        info = self.detect(
            _lookup(
                _proc(100, 90, node, [node, "out/bootstrap-fork"]),
                _proc(
                    90,
                    1,
                    node,
                    [
                        node,
                        "out/server-main.js",
                        "--server-data-dir",
                        server_data,
                    ],
                ),
            )
        )
        self.assertEqual(info.url_protocol, "vscode")
        self.assertEqual(info.data_folder_name, ".vscode-server")
        # --server-data-dir is taken from the ancestor's command line.
        self.assertEqual(
            info.extensions_dir, os.path.join(server_data, "extensions")
        )

    def test_server_data_dir_flag_unusable(self):
        # A flag that is present but cannot be used means unknown, never the
        # default ~/.vscode-server.
        self.write_json("srv/product.json", VSCODE_SERVER_PRODUCT)
        node = os.path.join(self.tmp, "srv/node")
        for flags in (
            ["--server-data-dir", "/does/not/exist"],
            ["--server-data-dir", "--without-connection-token"],
            ["--server-data-dir"],
        ):
            info = self.detect(_lookup(_proc(100, 1, node, [node] + flags)))
            self.assertEqual(info.url_protocol, "vscode")
            self.assertIsNone(info.extensions_dir, flags)

    def test_server_default_extensions_dir(self):
        self.write_json("srv/product.json", VSCODE_SERVER_PRODUCT)
        info = self.detect(
            _lookup(_proc(100, 1, os.path.join(self.tmp, "srv/node")))
        )
        self.assertEqual(
            info.extensions_dir,
            os.path.join(self.home, ".vscode-server", "extensions"),
        )

    def test_extensions_dir_flag_from_main_process(self):
        helper = self.mac_app("Antigravity IDE.app", AGY_IDE_PRODUCT)
        ext = os.path.join(self.tmp, "ext dir")
        os.makedirs(ext)
        # Only a directory with extensions.json counts.
        info = self.detect(
            _lookup(
                _proc(100, 50, helper, [helper]),
                _proc(
                    50,
                    1,
                    "/x/Electron",
                    ["/x/Electron", f"--extensions-dir={ext}"],
                ),
            )
        )
        self.assertIsNone(info.extensions_dir)
        self.write_json(os.path.join(ext, "extensions.json"), [])
        info = self.detect(
            _lookup(
                _proc(100, 50, helper, [helper]),
                _proc(
                    50,
                    1,
                    "/x/Electron",
                    ["/x/Electron", f"--extensions-dir={ext}"],
                ),
            )
        )
        self.assertEqual(info.extensions_dir, ext)

    def test_linux_electron_reordered_main_process_flags(self):
        # Real /proc cmdlines from VS Code 1.142 on Linux: one space-joined
        # string each, and the main process's flag values moved to the end.
        self.write_json(
            "usr/share/code/resources/app/product.json", VSCODE_PRODUCT
        )
        code = os.path.join(self.tmp, "usr/share/code/code")
        ud = os.path.join(self.tmp, "user-data")
        ext = os.path.join(self.tmp, "server-data", "extensions")
        os.makedirs(os.path.join(ud, "User"))
        os.makedirs(ext)
        host = (
            f"{code} --type=utility --utility-sub-type=node.mojom.NodeService "
            "--lang=en-US --service-sandbox-type=none --no-sandbox "
            f"--dns-result-order=ipv4first --user-data-dir={ud} "
            "--standard-schemes=vscode-webview,vscode-file"
        )
        main = (
            f"{code} --remote-debugging-port=59673 --user-data-dir "
            "--extensions-dir --disable-workspace-trust --skip-welcome "
            "--skip-release-notes --disable-telemetry --no-sandbox "
            "--disable-gpu --ozone-platform=x11 --new-window "
            f"{ud} {ext} /srv/nb /srv/nb/probe.ipynb"
        )
        info = self.detect(
            _lookup(
                _proc(100, 50, code, [host]),
                _proc(50, 1, code, [main]),
            )
        )
        self.assertEqual(info.url_protocol, "vscode")
        # --user-data-dir=<path> on the extension host is reliable.
        self.assertEqual(info.user_data_dir, ud)
        # --extensions-dir is present but its value is detached: unknown,
        # not the default ~/.vscode/extensions.
        self.assertIsNone(info.extensions_dir)
        self.assertEqual(
            ide._flag_lookup([[main]], "--extensions-dir"), (True, None)
        )
        self.assertEqual(
            ide._flag_lookup([[main]], "--user-data-dir"), (True, None)
        )
        self.assertEqual(
            ide._flag_lookup([[host]], "--user-data-dir"), (True, ud)
        )

    def test_flag_lookup(self):
        f = ide._flag_lookup
        # In a joined Linux Electron title, positionals follow all switches,
        # so a detached switch that is last is followed by a positional that
        # is not its value (verifier repro).
        ud = os.path.join(self.tmp, "ud")
        exts = os.path.join(self.tmp, "exts")
        os.makedirs(os.path.join(ud, "User"))
        self.write_json(os.path.join(exts, "extensions.json"), [DAK_ENTRY])
        joined = (
            f"/usr/share/code/code --user-data-dir --no-sandbox "
            f"--extensions-dir {ud} {exts} /srv/nb"
        )
        self.assertEqual(f([[joined]], "--extensions-dir"), (True, None))
        self.assertEqual(f([[joined]], "--user-data-dir"), (True, None))
        self.assertIsNone(
            ide._resolve_extensions_dir(
                VSCODE_PRODUCT, False, [[joined]], {}, self.home
            )[0]
        )
        self.assertIsNone(
            ide._resolve_user_data_dir(
                VSCODE_PRODUCT, [[joined]], {}, self.home, "linux"
            )
        )
        self.assertIsNone(
            ide._dak_disabled_by_flag(
                [
                    [
                        f"/c/code --disable-extension {DAK_ENTRY['identifier']['id']}"
                    ]
                ]
            )
        )
        self.assertEqual(
            f([["x", "--profile", "Data"]], "--profile"), (True, "Data")
        )
        self.assertEqual(
            f([["x", "--profile=My Data"]], "--profile"), (True, "My Data")
        )
        self.assertEqual(
            f([["x", "--profile", "--new-window"]], "--profile"), (True, None)
        )
        self.assertEqual(f([["x", "--profile="]], "--profile"), (True, None))
        self.assertEqual(f([["x", "--profile"]], "--profile"), (True, None))
        self.assertEqual(f([["x"]], "--profile"), (False, None))
        self.assertEqual(f([], "--profile"), (False, None))
        # A multi-element cmdline keeps values with spaces intact.
        self.assertEqual(
            f(
                [
                    [
                        "/a/Code Helper",
                        "--user-data-dir=/Users/u/Application Support/Code",
                    ]
                ],
                "--user-data-dir",
            ),
            (True, "/Users/u/Application Support/Code"),
        )

    def test_extensions_dir_env(self):
        helper = self.mac_app("Cursor.app", CURSOR_PRODUCT)
        lookup = _lookup(_proc(100, 1, helper))
        self.assertEqual(
            self.detect(lookup, {"VSCODE_EXTENSIONS": "/ve"}).extensions_dir,
            "/ve",
        )
        self.assertEqual(
            self.detect(lookup, {"VSCODE_PORTABLE": "/p"}).extensions_dir,
            os.path.join("/p", "extensions"),
        )

    def test_not_vscode_plain_processes(self):
        info = self.detect(
            _lookup(
                _proc(100, 90, "/usr/bin/python3"),
                _proc(90, 80, "/usr/local/bin/jupyter-lab"),
                _proc(80, 1, "/bin/zsh"),
                _proc(1, 0, "/sbin/launchd"),
            )
        )
        self.assertEqual(info, ide.IdeInfo())
        self.assertIsNone(info.family)

    def test_colab_like_env_is_not_vscode(self):
        info = self.detect(
            _lookup(_proc(100, 1, "/usr/bin/python3")),
            {"COLAB_RELEASE_TAG": "x", "JPY_PARENT_PID": "7"},
        )
        self.assertIsNone(info.family)

    def test_vscode_pid_alone_is_not_enough(self):
        # VSCODE_PID points at a process without a product.json.
        info = self.detect(
            _lookup(_proc(100, 1, "/usr/bin/python3"), _proc(7, 1, "/bin/x")),
            {"VSCODE_PID": "7"},
        )
        self.assertIsNone(info.family)

    def test_vscode_pid_fallback_when_reparented(self):
        helper = self.mac_app("Antigravity IDE.app", AGY_IDE_PRODUCT)
        info = self.detect(
            _lookup(_proc(100, 1, "/usr/bin/python3"), _proc(7, 1, helper)),
            {"VSCODE_PID": "7"},
        )
        self.assertEqual(info.url_protocol, "antigravity-ide")
        self.assertTrue(info.source.startswith("VSCODE_PID:"))

    def test_cycle_in_parent_chain(self):
        lookup = mock.Mock(
            side_effect=_lookup(
                _proc(100, 200, "/bin/a"), _proc(200, 100, "/bin/b")
            )
        )
        info = self.detect(lookup)
        self.assertIsNone(info.family)
        self.assertEqual(lookup.call_count, 2)

    def test_depth_limit(self):
        procs = [_proc(i, i + 1, "/bin/x") for i in range(100, 130)]
        lookup = mock.Mock(side_effect=_lookup(*procs))
        self.detect(lookup)
        self.assertEqual(lookup.call_count, ide._MAX_PARENT_DEPTH)

    def test_lookup_errors_do_not_raise(self):
        def lookup(pid):
            raise PermissionError("denied")

        self.assertIsNone(self.detect(lookup).family)

    def test_no_process_lookup_available(self):
        self.assertIsNone(self.detect(None).family)

    def test_missing_exe(self):
        info = self.detect(_lookup(_proc(100, 1, None)))
        self.assertIsNone(info.family)

    def test_invalid_product_json_ignored(self):
        for i, bad in enumerate(
            ["not json", "[1, 2]", json.dumps({"urlProtocol": "x"})]
        ):
            self.write_json(f"bad{i}/resources/app/product.json", bad)
            exe = os.path.join(self.tmp, f"bad{i}/code")
            self.assertIsNone(self.detect(_lookup(_proc(100, 1, exe))).family)

    def test_relative_exe_ignored(self):
        # macOS `ps` reports some commands by bare name (e.g. "tmux"). They
        # must not be resolved against our cwd, which may be an editor
        # checkout with its own product.json.
        self.write_json("product.json", VSCODE_SERVER_PRODUCT)
        self.write_json("resources/app/product.json", VSCODE_PRODUCT)
        cwd = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, cwd)
        for exe in ("node", "tmux", os.path.join("bin", "code")):
            self.assertIsNone(ide._find_product_json(exe))
            self.assertIsNone(self.detect(_lookup(_proc(100, 1, exe))).family)

    def test_product_json_not_searched_beside_non_node_exe(self):
        # A stray product.json next to an arbitrary binary is not an editor.
        self.write_json("bin/product.json", VSCODE_PRODUCT)
        exe = os.path.join(self.tmp, "bin/python3")
        self.assertIsNone(self.detect(_lookup(_proc(100, 1, exe))).family)

    def test_launched_from_integrated_terminal(self):
        # `jupyter lab` started in an Antigravity IDE terminal: the editor is
        # an ancestor, but the notebook is likely rendered in a browser.
        helper = self.mac_app("Antigravity IDE.app", AGY_IDE_PRODUCT)
        info = self.detect(
            _lookup(
                _proc(100, 90, "/usr/local/bin/jupyter-lab"),
                _proc(90, 80, "/bin/zsh"),
                _proc(80, 1, helper),
            ),
            {"TERM_PROGRAM": "vscode", "DATA_CLOUD_CURR_IDE_NAME": "x"},
        )
        self.assertEqual(info.url_protocol, "antigravity-ide")
        self.assertTrue(info.launched_from_terminal)
        self.assertFalse(ide.is_rendered_by_editor(info))

    def test_nls_config_app_root_fallback(self):
        root = os.path.join(self.tmp, "usr/share/code/resources/app")
        self.write_json(
            "usr/share/code/resources/app/product.json", VSCODE_PRODUCT
        )
        nls = json.dumps(
            {"defaultMessagesFile": f"{root}/out/nls.messages.json"}
        )
        info = self.detect(None, {"VSCODE_NLS_CONFIG": nls})
        self.assertEqual(info.url_protocol, "vscode")
        self.assertTrue(info.source.startswith("env:VSCODE_NLS_CONFIG:"))

    def test_nls_config_malformed(self):
        for nls in ("{", "[]", json.dumps({"defaultMessagesFile": 3})):
            info = self.detect(None, {"VSCODE_NLS_CONFIG": nls})
            self.assertIsNone(info.family)

    def test_antigravity_app_root_env_fallback(self):
        self.write_json("agy/app/product.json", AGY_IDE_PRODUCT)
        info = self.detect(
            None,
            {"ANTIGRAVITY_EDITOR_APP_ROOT": os.path.join(self.tmp, "agy/app")},
        )
        self.assertEqual(info.url_protocol, "antigravity-ide")

    def test_code_cache_path_identifies_without_scheme(self):
        info = self.detect(
            None,
            {
                "VSCODE_CODE_CACHE_PATH": "/Users/u/Library/Application Support/"
                "Antigravity IDE/CachedData/ecfbad74"
            },
        )
        self.assertEqual(info.family, "vscode")
        self.assertEqual(info.flavor, "antigravity-ide")
        self.assertIsNone(info.url_protocol)

    def test_code_cache_path_windows(self):
        info = self.detect(
            None,
            {
                "VSCODE_CODE_CACHE_PATH": "C:\\Users\\u\\AppData\\Roaming\\"
                "Cursor\\CachedData\\be4c9e53"
            },
        )
        self.assertEqual(info.flavor, "cursor")
        self.assertIsNone(info.url_protocol)

    def test_bundle_id_identifies_without_scheme(self):
        info = self.detect(
            None, {"__CFBundleIdentifier": "com.microsoft.VSCode"}
        )
        self.assertEqual(info.flavor, "code")
        self.assertIsNone(info.url_protocol)
        # The standalone Antigravity app is not a VS Code fork.
        info = self.detect(
            None, {"__CFBundleIdentifier": "com.google.antigravity"}
        )
        self.assertIsNone(info.family)

    def test_dak_env_identifies_family_only(self):
        info = self.detect(None, {"DATA_CLOUD_CURR_IDE_NAME": "x"})
        self.assertEqual(info.family, "vscode")
        self.assertIsNone(info.flavor)
        self.assertIsNone(info.url_protocol)

    def test_process_tree_beats_env(self):
        helper = self.mac_app("Cursor.app", CURSOR_PRODUCT)
        info = self.detect(
            _lookup(_proc(100, 1, helper)),
            {"__CFBundleIdentifier": "com.microsoft.VSCode"},
        )
        self.assertEqual(info.flavor, "cursor")

    def test_detect_ide_caches_and_never_raises(self):
        with mock.patch.object(
            ide, "_default_proc_lookup", side_effect=RuntimeError("boom")
        ), mock.patch.object(ide, "_detect", wraps=ide._detect) as detect:
            first = ide.detect_ide()
            second = ide.detect_ide()
        self.assertIs(first, second)
        self.assertEqual(detect.call_count, 1)

        ide._reset_cache_for_testing()
        with mock.patch.object(ide, "_detect", side_effect=RuntimeError("x")):
            self.assertEqual(ide.detect_ide(), ide.IdeInfo(source="error"))


class ProcLookupTest(IdeTestBase):

    def write_proc(self, pid, stat, exe=None, cmdline=b""):
        base = os.path.join(self.tmp, "proc", str(pid))
        os.makedirs(base)
        with open(os.path.join(base, "stat"), "w") as f:
            f.write(stat)
        with open(os.path.join(base, "cmdline"), "wb") as f:
            f.write(cmdline)
        if exe:
            os.symlink(exe, os.path.join(base, "exe"))

    def test_linux_proc(self):
        self.write_proc(
            100,
            "100 (Code Helper (x) y) S 42 100 100 0 -1",
            exe="/usr/share/code/code",
            cmdline=b"/usr/share/code/code\0--type=utility\0",
        )
        lookup = ide._linux_proc_lookup(os.path.join(self.tmp, "proc"))
        self.assertEqual(
            lookup(100),
            _proc(
                100,
                42,
                "/usr/share/code/code",
                ["/usr/share/code/code", "--type=utility"],
            ),
        )
        self.assertIsNone(lookup(999))

    def test_linux_proc_unreadable_exe_falls_back_to_cmdline(self):
        self.write_proc(5, "5 (node) S 1 5", cmdline=b"/srv/node\0x\0")
        self.write_proc(7, "7 (node) S 1 7", cmdline=b"   ")
        lookup = ide._linux_proc_lookup(os.path.join(self.tmp, "proc"))
        self.assertEqual(lookup(5).exe, "/srv/node")
        self.assertIsNone(lookup(7).exe)

    def test_linux_proc_deleted_exe(self):
        self.write_proc(6, "6 (code) S 1 6", exe="/opt/code/code (deleted)")
        lookup = ide._linux_proc_lookup(os.path.join(self.tmp, "proc"))
        self.assertEqual(lookup(6).exe, "/opt/code/code")

    def test_psutil_lookup(self):
        class AccessDenied(Exception):
            pass

        class Process:

            def __init__(self, pid):
                if pid == 9:
                    raise AccessDenied()
                self.pid = pid

            def ppid(self):
                return 1

            def exe(self):
                raise AccessDenied()

            def cmdline(self):
                return ["/a/node", "x"]

        fake = types.SimpleNamespace(Process=Process)
        lookup = ide._psutil_proc_lookup(fake)
        self.assertEqual(lookup(5), _proc(5, 1, None, ["/a/node", "x"]))
        self.assertIsNone(lookup(9))

    def test_ps_lookup(self):
        out = (
            "  100    50 /opt/homebrew/bin/python3.15\n"
            "   50     1 /Applications/Antigravity IDE.app/Contents/Frameworks/"
            "Antigravity IDE Helper (Plugin).app/Contents/MacOS/"
            "Antigravity IDE Helper (Plugin)\n"
            "garbage\n"
        )
        run = mock.Mock(return_value=types.SimpleNamespace(stdout=out))
        lookup = ide._ps_proc_lookup(run)
        self.assertEqual(lookup(100).ppid, 50)
        self.assertTrue(
            lookup(50).exe.endswith("Antigravity IDE Helper (Plugin)")
        )
        self.assertIsNone(lookup(7))
        run.assert_called_once()
        self.assertEqual(
            run.call_args[0][0], ["ps", "-axo", "pid=,ppid=,comm="]
        )

    def test_ps_lookup_end_to_end_with_nested_helpers(self):
        # Recorded `ps -axo pid=,ppid=,comm=` lines from macOS with VS Code
        # 1.142.0 open; nested Helper (Plugin) processes, app root moved into
        # the temp dir.
        self.write_json(
            "Visual Studio Code.app/Contents/Resources/app/product.json",
            VSCODE_PRODUCT,
        )
        helper = os.path.join(
            self.tmp, "Visual Studio Code.app", MAC_HELPER.format(name="Code")
        )
        out = (
            "25700 25677 /opt/homebrew/bin/python3.15\n"
            f"25677 25672 {helper}\n"
            f"25672 89287 {helper}\n"
        )
        run = mock.Mock(return_value=types.SimpleNamespace(stdout=out))
        info = self.detect(ide._ps_proc_lookup(run), start_pid=25700)
        self.assertEqual(info.url_protocol, "vscode")
        self.assertEqual(info.flavor, "code")
        self.assertTrue(info.source.startswith("process:25677:"))
        run.assert_called_once()

    def test_ps_lookup_failure(self):
        run = mock.Mock(side_effect=subprocess.TimeoutExpired("ps", 2))
        self.assertIsNone(ide._ps_proc_lookup(run)(1))

    def test_default_lookup_selection(self):
        with mock.patch.object(sys, "platform", "win32"), mock.patch.dict(
            sys.modules, {"psutil": None}
        ):
            self.assertIsNone(ide._default_proc_lookup())
        with mock.patch.object(sys, "platform", "darwin"), mock.patch.dict(
            sys.modules, {"psutil": None}
        ), mock.patch.object(ide, "_ps_proc_lookup") as ps:
            self.assertIs(ide._default_proc_lookup(), ps.return_value)


def _write_state_db(path, items):
    """Writes a state.vscdb like VS Code's (ItemTable, JSON values)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ItemTable "
            "(key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB)"
        )
        for key, value in items.items():
            conn.execute(
                "INSERT INTO ItemTable VALUES (?, ?)",
                (key, value if isinstance(value, str) else json.dumps(value)),
            )
        conn.commit()
    finally:
        conn.close()


def _ids(*ids):
    return [
        {"id": i, "uuid": "00000000-0000-0000-0000-000000000000"} for i in ids
    ]


class DakListedTest(IdeTestBase):
    """extensions.json / .obsolete parsing."""

    def setUp(self):
        super().setUp()
        self.ext = os.path.join(self.tmp, "ext")

    def listed(self, extensions_json=None):
        return ide._dak_in_extensions_dir(self.ext, extensions_json)

    def test_listed(self):
        self.write_json("ext/extensions.json", [JUPYTER_ENTRY, DAK_ENTRY])
        self.write_json(
            "ext/.obsolete", {"redhat.java-1.53.0-darwin-arm64": True}
        )
        self.assertIs(self.listed(), True)

    def test_not_listed(self):
        self.write_json("ext/extensions.json", [JUPYTER_ENTRY])
        self.assertIs(self.listed(), False)

    def test_obsolete_by_directory_name(self):
        self.write_json("ext/extensions.json", [DAK_ENTRY])
        self.write_json(
            "ext/.obsolete", {"googlecloudtools.datacloud-99.99.99": True}
        )
        self.assertIs(self.listed(), False)

    def test_obsolete_matched_via_location_path(self):
        entry = {k: v for k, v in DAK_ENTRY.items() if k != "relativeLocation"}
        self.write_json("ext/extensions.json", [entry])
        self.write_json(
            "ext/.obsolete", {"googlecloudtools.datacloud-99.99.99": True}
        )
        self.assertIs(self.listed(), False)

    def test_id_case_insensitive(self):
        entry = dict(DAK_ENTRY, identifier={"id": "GoogleCloudTools.datacloud"})
        self.write_json("ext/extensions.json", [entry])
        self.assertIs(self.listed(), True)

    def test_unknown(self):
        self.assertIsNone(self.listed())
        self.write_json("ext/extensions.json", "{")
        self.assertIsNone(self.listed())
        self.write_json("ext/extensions.json", {"a": 1})
        self.assertIsNone(self.listed())

    def test_malformed_obsolete_ignored(self):
        self.write_json("ext/extensions.json", [DAK_ENTRY, "x", {}])
        self.write_json("ext/.obsolete", "[")
        self.assertIs(self.listed(), True)

    def test_profile_extensions_json_with_shared_obsolete(self):
        profile_json = self.write_json("p/extensions.json", [DAK_ENTRY])
        self.write_json("ext/extensions.json", [])
        self.assertIs(self.listed(profile_json), True)
        self.write_json(
            "ext/.obsolete", {"googlecloudtools.datacloud-99.99.99": True}
        )
        self.assertIs(self.listed(profile_json), False)


class DakStateTest(IdeTestBase):
    """dak_state(): env var, desktop enablement, server."""

    def setUp(self):
        super().setUp()
        patcher = mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("DATA_CLOUD_CURR_IDE_NAME", None)
        self.ext = os.path.join(self.tmp, "ext")
        self.ud = os.path.join(self.tmp, "ud")
        self.info = ide.IdeInfo(
            family="vscode",
            extensions_dir=self.ext,
            user_data_dir=self.ud,
            source="process:1:x",
        )
        self.notebook = os.path.join(self.tmp, "proj", "sub", "nb.ipynb")
        self.write_json("ext/extensions.json", [JUPYTER_ENTRY, DAK_ENTRY])

    def state(self, info=None, notebook=None):
        with mock.patch.object(ide, "_notebook_path", return_value=notebook):
            return ide.dak_state(info or self.info)

    def global_db(self, profile=None):
        base = os.path.join(self.ud, "User")
        if profile:
            base = os.path.join(base, "profiles", profile)
        return os.path.join(base, "globalStorage", "state.vscdb")

    def workspace_db(self, folder, name="397e073366938603c8f35452a27f3c7e"):
        d = os.path.join(self.ud, "User", "workspaceStorage", name)
        self.write_json(
            os.path.join(d, "workspace.json"),
            {"folder": pathlib.Path(folder).as_uri()},
        )
        return os.path.join(d, "state.vscdb")

    def write_storage_json(self, profiles=(), workspaces=None):
        self.write_json(
            os.path.join(self.ud, "User", "globalStorage", "storage.json"),
            {
                "userDataProfiles": list(profiles),
                "profileAssociations": {
                    "workspaces": workspaces or {},
                    "emptyWindows": {},
                },
            },
        )

    def test_enabled_when_listed_and_not_disabled(self):
        self.assertEqual(self.state(), ide.DAK_ENABLED)
        self.assertIs(ide.is_dak_installed(self.info), True)

    def test_not_installed(self):
        self.write_json("ext/extensions.json", [JUPYTER_ENTRY])
        self.assertEqual(self.state(), ide.DAK_NOT_INSTALLED)

    def test_globally_disabled(self):
        _write_state_db(
            self.global_db(),
            {
                "extensionsIdentifiers/disabled": _ids(
                    "saoudrizwan.claude-dev", "GoogleCloudTools.datacloud"
                )
            },
        )
        self.assertEqual(self.state(), ide.DAK_DISABLED)
        self.assertIs(ide.is_dak_installed(self.info), False)

    def test_other_extensions_disabled(self):
        _write_state_db(
            self.global_db(),
            {"extensionsIdentifiers/disabled": _ids("meta.pyrefly")},
        )
        self.assertEqual(self.state(), ide.DAK_ENABLED)

    def test_workspace_disabled(self):
        db = self.workspace_db(os.path.join(self.tmp, "proj"))
        _write_state_db(
            db, {"extensionsIdentifiers/disabled": _ids(ide.DAK_EXTENSION_ID)}
        )
        self.assertEqual(self.state(notebook=self.notebook), ide.DAK_DISABLED)
        # Without the notebook path the workspace cannot be found.
        self.assertEqual(self.state(), ide.DAK_ENABLED)

    def test_workspace_enabled_overrides_global_disabled(self):
        _write_state_db(
            self.global_db(),
            {"extensionsIdentifiers/disabled": _ids(ide.DAK_EXTENSION_ID)},
        )
        db = self.workspace_db(os.path.join(self.tmp, "proj"))
        _write_state_db(
            db, {"extensionsIdentifiers/enabled": _ids(ide.DAK_EXTENSION_ID)}
        )
        self.assertEqual(self.state(notebook=self.notebook), ide.DAK_ENABLED)

    def test_workspace_longest_ancestor_wins(self):
        outer = self.workspace_db(os.path.join(self.tmp, "proj"), name="a")
        inner = self.workspace_db(
            os.path.join(self.tmp, "proj", "sub"), name="b"
        )
        self.workspace_db(os.path.join(self.tmp, "projx"), name="c")
        _write_state_db(
            outer,
            {"extensionsIdentifiers/disabled": _ids(ide.DAK_EXTENSION_ID)},
        )
        _write_state_db(inner, {})
        self.assertEqual(self.state(notebook=self.notebook), ide.DAK_ENABLED)

    def test_disabled_by_command_line(self):
        info = dataclasses.replace(self.info, dak_disabled_by_flag=True)
        self.assertEqual(self.state(info), ide.DAK_DISABLED)

    def test_non_default_profile(self):
        self.write_storage_json(
            profiles=[{"name": "Data", "location": "-6a1b2c3d"}],
            workspaces={
                pathlib.Path(os.path.join(self.tmp, "proj")).as_uri(): (
                    "-6a1b2c3d"
                )
            },
        )
        profile_dir = os.path.join(self.ud, "User", "profiles", "-6a1b2c3d")
        self.write_json(
            os.path.join(profile_dir, "extensions.json"), [JUPYTER_ENTRY]
        )
        # DAK is listed for the default profile but not for "Data".
        self.assertEqual(
            self.state(notebook=self.notebook), ide.DAK_NOT_INSTALLED
        )
        self.write_json(
            os.path.join(profile_dir, "extensions.json"), [DAK_ENTRY]
        )
        _write_state_db(
            self.global_db("-6a1b2c3d"),
            {"extensionsIdentifiers/disabled": _ids(ide.DAK_EXTENSION_ID)},
        )
        self.assertEqual(self.state(notebook=self.notebook), ide.DAK_DISABLED)
        # The default profile's state does not apply.
        self.assertEqual(self.state(), ide.DAK_ENABLED)

    def test_profile_use_default_flags(self):
        self.write_storage_json(
            profiles=[
                {
                    "name": "Shared",
                    "location": "-1",
                    "useDefaultFlags": {
                        "extensions": True,
                        "globalState": True,
                    },
                }
            ],
            workspaces={pathlib.Path(self.tmp).as_uri(): "-1"},
        )
        _write_state_db(
            self.global_db(),
            {"extensionsIdentifiers/disabled": _ids(ide.DAK_EXTENSION_ID)},
        )
        self.assertEqual(self.state(notebook=self.notebook), ide.DAK_DISABLED)

    def test_profile_flag(self):
        self.write_storage_json(profiles=[{"name": "Data", "location": "-7"}])
        profile_dir = os.path.join(self.ud, "User", "profiles", "-7")
        self.write_json(os.path.join(profile_dir, "extensions.json"), [])
        info = dataclasses.replace(self.info, profile_name="Data")
        self.assertEqual(self.state(info), ide.DAK_NOT_INSTALLED)
        info = dataclasses.replace(self.info, profile_name="Missing")
        self.assertEqual(self.state(info), ide.DAK_UNKNOWN)

    def test_default_profile_association(self):
        self.write_storage_json(
            workspaces={
                pathlib.Path(os.path.join(self.tmp, "proj")).as_uri(): (
                    "__default__profile__"
                )
            }
        )
        self.assertEqual(self.state(notebook=self.notebook), ide.DAK_ENABLED)

    def test_unreadable_state_is_unknown(self):
        for bad in ("not json", "{"):
            self.write_json(
                os.path.join(self.ud, "User", "globalStorage", "storage.json"),
                bad,
            )
            self.assertEqual(self.state(), ide.DAK_UNKNOWN)
        os.remove(
            os.path.join(self.ud, "User", "globalStorage", "storage.json")
        )
        # A corrupt database.
        self.write_json(self.global_db(), "this is not sqlite")
        self.assertEqual(self.state(), ide.DAK_UNKNOWN)
        # A malformed value.
        os.remove(self.global_db())
        _write_state_db(
            self.global_db(), {"extensionsIdentifiers/disabled": "{"}
        )
        self.assertEqual(self.state(), ide.DAK_UNKNOWN)

    def test_locked_database_is_unknown(self):
        _write_state_db(self.global_db(), {})
        writer = sqlite3.connect(self.global_db())
        self.addCleanup(writer.close)
        writer.execute("BEGIN EXCLUSIVE")
        with mock.patch.object(ide, "_SQLITE_TIMEOUT_SECONDS", 0.01):
            self.assertEqual(self.state(), ide.DAK_UNKNOWN)
        writer.rollback()
        self.assertEqual(self.state(), ide.DAK_ENABLED)

    def test_database_opened_read_only(self):
        _write_state_db(self.global_db(), {})
        before = os.stat(self.global_db()).st_mtime_ns
        self.state()
        self.assertEqual(os.stat(self.global_db()).st_mtime_ns, before)
        self.assertFalse(os.path.exists(self.global_db() + "-journal"))

    def test_unknown_without_user_data_dir(self):
        info = dataclasses.replace(self.info, user_data_dir=None)
        self.assertEqual(self.state(info), ide.DAK_UNKNOWN)

    def test_unknown_outside_vscode(self):
        self.assertEqual(self.state(ide.IdeInfo()), ide.DAK_UNKNOWN)

    def test_server_listed_is_unknown(self):
        info = dataclasses.replace(self.info, is_server=True)
        self.assertEqual(self.state(info), ide.DAK_UNKNOWN)
        self.assertIsNone(ide.is_dak_installed(info))
        self.write_json("ext/extensions.json", [JUPYTER_ENTRY])
        self.assertEqual(self.state(info), ide.DAK_NOT_INSTALLED)

    def test_dak_env_with_editor_ancestor_is_enabled(self):
        os.environ["DATA_CLOUD_CURR_IDE_NAME"] = "antigravityide"
        _write_state_db(
            self.global_db(),
            {"extensionsIdentifiers/disabled": _ids(ide.DAK_EXTENSION_ID)},
        )
        # Disabling a running DAK only takes effect after a restart, and
        # the variable shows it is running.
        self.assertEqual(self.state(), ide.DAK_ENABLED)
        self.assertEqual(
            self.state(dataclasses.replace(self.info, is_server=True)),
            ide.DAK_ENABLED,
        )

    def test_dak_env_without_editor_ancestor(self):
        os.environ["DATA_CLOUD_CURR_IDE_NAME"] = "visualstudiocode"
        env_only = ide.IdeInfo(family="vscode", source="env:X")
        self.assertEqual(self.state(env_only), ide.DAK_UNKNOWN)
        self.assertEqual(
            self.state(dataclasses.replace(self.info, source="env:X")),
            ide.DAK_ENABLED,  # Listed in extensions.json.
        )

    def test_dak_env_leaked_into_editor_without_dak_is_unknown(self):
        # E.g. `antigravity-ide .` run from a VS Code terminal where DAK set
        # the variable, while this editor has no DAK.
        os.environ["DATA_CLOUD_CURR_IDE_NAME"] = "visualstudiocode"
        self.write_json("ext/extensions.json", [JUPYTER_ENTRY])
        self.assertEqual(self.state(), ide.DAK_UNKNOWN)

    def test_never_raises(self):
        with mock.patch.object(
            ide, "_dak_in_extensions_dir", side_effect=RuntimeError()
        ):
            self.assertEqual(self.state(), ide.DAK_UNKNOWN)
            self.assertIsNone(ide.is_dak_installed(self.info))

    def test_notebook_path_from_ipython(self):
        shell = types.SimpleNamespace(
            user_ns={"__vsc_ipynb_file__": "/a/b.ipynb"}
        )
        with mock.patch("IPython.get_ipython", return_value=shell):
            self.assertEqual(ide._notebook_path(), "/a/b.ipynb")
        shell.user_ns["__vsc_ipynb_file__"] = "relative.ipynb"
        with mock.patch("IPython.get_ipython", return_value=shell):
            self.assertIsNone(ide._notebook_path())


class UserDataDirTest(unittest.TestCase):

    PRODUCT = {"nameShort": "Antigravity IDE"}

    def resolve(self, env=None, cmdlines=(), platform="linux"):
        return ide._resolve_user_data_dir(
            self.PRODUCT, cmdlines, env or {}, "/home/u", platform
        )

    def test_from_code_cache_path(self):
        self.assertEqual(
            self.resolve(
                {
                    "VSCODE_CODE_CACHE_PATH": "/Users/u/Library/Application "
                    "Support/Antigravity IDE/CachedData/ecfbad74"
                }
            ),
            "/Users/u/Library/Application Support/Antigravity IDE",
        )

    def test_flag_then_defaults(self):
        with tempfile.TemporaryDirectory() as ud:
            # Only a directory that looks like a user data dir is accepted.
            self.assertIsNone(
                self.resolve(cmdlines=[["x", f"--user-data-dir={ud}"]])
            )
            os.makedirs(os.path.join(ud, "User"))
            self.assertEqual(
                self.resolve(cmdlines=[["x", f"--user-data-dir={ud}"]]), ud
            )
        # Present but unusable: unknown, not the default.
        self.assertIsNone(self.resolve(cmdlines=[["x", "--user-data-dir"]]))
        self.assertIsNone(
            self.resolve(cmdlines=[["x", "--user-data-dir", "--new-window"]])
        )
        self.assertEqual(
            self.resolve({"VSCODE_PORTABLE": "/p"}),
            os.path.join("/p", "user-data"),
        )
        self.assertEqual(
            self.resolve({"VSCODE_APPDATA": "/a"}),
            os.path.join("/a", "Antigravity IDE"),
        )
        self.assertEqual(
            self.resolve(platform="darwin"),
            "/home/u/Library/Application Support/Antigravity IDE",
        )
        self.assertEqual(self.resolve(), "/home/u/.config/Antigravity IDE")
        self.assertEqual(
            self.resolve({"XDG_CONFIG_HOME": "/x"}), "/x/Antigravity IDE"
        )
        self.assertEqual(
            self.resolve({"APPDATA": "/w"}, platform="win32"),
            os.path.join("/w", "Antigravity IDE"),
        )
        self.assertIsNone(self.resolve(platform="win32"))

    def test_disable_flags(self):
        f = ide._dak_disabled_by_flag
        self.assertTrue(f([["code", "--disable-extensions"]]))
        self.assertTrue(
            f([["code", "--disable-extension", "GoogleCloudTools.datacloud"]])
        )
        self.assertTrue(
            f([["code", "--disable-extension=googlecloudtools.datacloud"]])
        )
        self.assertFalse(f([["code", "--disable-extension", "x.y"]]))
        self.assertFalse(f([["code"]]))
        # Detached value (Chromium reordering) or joined Linux cmdline.
        self.assertIsNone(
            f([["code", "--disable-extension", "--new-window", "x.y"]])
        )
        self.assertTrue(
            f([["/c/code --disable-extension=googlecloudtools.datacloud --x"]])
        )

    def test_unparsed_flags_make_state_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            ext = os.path.join(tmp, "ext")
            os.makedirs(ext)
            with open(os.path.join(ext, "extensions.json"), "w") as f:
                json.dump([DAK_ENTRY], f)
            base = ide.IdeInfo(
                family="vscode",
                extensions_dir=ext,
                user_data_dir=tmp,
                source="process:1:x",
            )
            with mock.patch.dict(os.environ, clear=True), mock.patch.object(
                ide, "_notebook_path", return_value=None
            ):
                self.assertEqual(ide.dak_state(base), ide.DAK_ENABLED)
                for info in (
                    dataclasses.replace(base, dak_disabled_by_flag=None),
                    dataclasses.replace(base, profile_flag_unparsed=True),
                ):
                    self.assertEqual(ide.dak_state(info), ide.DAK_UNKNOWN)

    def test_info_from_product_unparsed_profile(self):
        info = ide._info_from_product(
            AGY_IDE_PRODUCT,
            False,
            [["x", "--profile", "--new-window"]],
            {},
            "/home/u",
            "s",
        )
        self.assertIsNone(info.profile_name)
        self.assertTrue(info.profile_flag_unparsed)

    def test_info_from_product_fields(self):
        info = ide._info_from_product(
            AGY_IDE_PRODUCT,
            False,
            [["x", "--profile", "Data", "--disable-extensions"]],
            {},
            "/home/u",
            "s",
        )
        self.assertEqual(info.profile_name, "Data")
        self.assertTrue(info.dak_disabled_by_flag)
        self.assertFalse(info.is_server)
        self.assertIsNotNone(info.user_data_dir)
        server = ide._info_from_product(
            VSCODE_SERVER_PRODUCT, True, [], {}, "/home/u", "s"
        )
        self.assertTrue(server.is_server)
        self.assertIsNone(server.user_data_dir)


class P4ReplayTestBase(IdeTestBase):
    """Replays a real kernel capture (testdata/ide/p4_*.json)."""

    FIXTURE = None
    FLAVOR = None
    NAME = None
    URL_PROTOCOL = None

    def setUp(self):
        super().setUp()
        path = os.path.join(
            os.path.dirname(__file__), "testdata", "ide", self.FIXTURE
        )
        with open(path) as f:
            self.p4 = self.rebase(json.load(f))
        self.helper_pid = self.p4["process_chain"][1]["pid"]
        self.write_json(self.p4["product_json_path"], self.p4["product_json"])
        ext = self.p4["extensions_dir"]
        self.write_json(
            os.path.join(ext, "extensions.json"), self.p4["extensions_json"]
        )
        self.write_json(os.path.join(ext, ".obsolete"), self.p4["obsolete"])
        if self.p4.get("server"):
            # The server's node binary sits next to product.json.
            with open(self.p4["process_chain"][1]["exe"], "w"):
                pass
        self.lookup = _lookup(
            *(
                _proc(p["pid"], p["ppid"], p["exe"], p["cmdline"])
                for p in self.p4["process_chain"]
            )
        )
        patcher = mock.patch.dict(os.environ, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def rebase(self, value):
        """Moves app, probe and home dirs from the capture into the temp dir."""
        if isinstance(value, dict):
            return {k: self.rebase(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.rebase(v) for v in value]
        if isinstance(value, str):
            value = value.replace("/Users/user", self.home)
            value = value.replace("/home/user", self.home)
            value = value.replace("/srv/", self.tmp + "/srv/")
            return value.replace("/Applications/", self.tmp + "/Applications/")
        return value

    def detect_p4(self, lookup="default"):
        lookup = self.lookup if lookup == "default" else lookup
        ppid = self.p4["process_chain"][0]["ppid"]
        return self.detect(lookup, self.p4["env"], start_pid=ppid)

    def editor_shell(self):
        shell = types.SimpleNamespace(user_ns=self.p4["ipython_user_ns"])
        return mock.patch("IPython.get_ipython", return_value=shell)

    def test_detects_editor_from_extension_host(self):
        info = self.detect_p4()
        self.assertEqual(info.family, "vscode")
        self.assertEqual(info.flavor, self.FLAVOR)
        self.assertEqual(info.name, self.NAME)
        self.assertEqual(info.url_protocol, self.URL_PROTOCOL)
        self.assertEqual(
            info.extensions_dir,
            self.p4.get("detected_extensions_dir", self.p4["extensions_dir"]),
        )
        self.assertFalse(info.launched_from_terminal)
        # Found at the extension host (depth 1).
        self.assertEqual(
            info.source,
            f"process:{self.helper_pid}:{self.p4['product_json_path']}",
        )
        self.assertEqual(info.is_server, bool(self.p4.get("server")))

    def test_nls_config_fallback_without_process_info(self):
        info = self.detect_p4(lookup=None)
        self.assertEqual(info.url_protocol, self.URL_PROTOCOL)
        self.assertEqual(
            info.source, f"env:VSCODE_NLS_CONFIG:{self.p4['product_json_path']}"
        )
        self.assertEqual(info.is_server, bool(self.p4.get("server")))


class P4DesktopReplayTestBase(P4ReplayTestBase):
    """Desktop captures: enablement is readable from the editor's files."""

    def test_dak_enabled(self):
        os.environ.update(self.p4["env_dak_enabled"])
        info = self.detect_p4()
        with self.editor_shell():
            self.assertIs(ide.is_dak_installed(info), True)
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(
            links,
            [
                (
                    ide.DAK_SESSION_LABEL,
                    "dataproc-resource:/p/l/sc-1.spark.session",
                )
            ],
        )

    def test_listed_without_env_and_not_disabled_is_enabled(self):
        # E.g. the kernel started before DAK finished activating (#52).
        info = self.detect_p4()
        self.assertEqual(
            info.user_data_dir,
            os.path.dirname(
                os.path.dirname(self.p4["env"]["VSCODE_CODE_CACHE_PATH"])
            ),
        )
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_ENABLED)
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(
            links,
            [
                (
                    ide.DAK_SESSION_LABEL,
                    "dataproc-resource:/p/l/sc-1.spark.session",
                )
            ],
        )

    def test_disabled_globally(self):
        # Real state.vscdb values from before and after a UI disable.
        info = self.detect_p4()
        db = os.path.join(
            info.user_data_dir, "User", "globalStorage", "state.vscdb"
        )
        _write_state_db(
            db,
            {
                "extensionsIdentifiers/disabled": self.p4[
                    "global_disabled_before"
                ]
            },
        )
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_ENABLED)
        os.remove(db)
        _write_state_db(
            db,
            {
                "extensionsIdentifiers/disabled": self.p4[
                    "global_disabled_after"
                ]
            },
        )
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_DISABLED)
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(links, [(ide.DAK_ENABLE_LABEL, ide.DAK_INSTALL_URL)])
        # A still-running DAK (variable set until extensions restart) wins.
        os.environ.update(self.p4["env_dak_enabled"])
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_ENABLED)

    def test_dak_not_listed(self):
        self.write_json(
            os.path.join(self.p4["extensions_dir"], "extensions.json"), []
        )
        info = self.detect_p4()
        with self.editor_shell():
            self.assertIs(ide.is_dak_installed(info), False)
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(links, [(ide.DAK_INSTALL_LABEL, ide.DAK_INSTALL_URL)])

    def test_listed_and_not_editor_rendered_is_installed(self):
        # E.g. a plain Python process whose ancestor is the editor but which
        # was started from an integrated terminal.
        env = dict(self.p4["env"], TERM_PROGRAM="vscode")
        info = self.detect(self.lookup, env, start_pid=self.helper_pid)
        self.assertIs(ide.is_dak_installed(info), True)

    def test_vscode_pid_fallback_when_reparented(self):
        # Kernel reparented away from the extension host: VSCODE_PID points
        # at the main process (Contents/MacOS/Code or .../Electron).
        info = self.detect(self.lookup, self.p4["env"], start_pid=1)
        self.assertEqual(info.url_protocol, self.URL_PROTOCOL)
        self.assertTrue(info.source.startswith("VSCODE_PID:"))


class P4VscodeMacosTest(P4DesktopReplayTestBase):
    """VS Code 1.142.0 on macOS."""

    FIXTURE = "p4_vscode_macos.json"
    FLAVOR = "code"
    NAME = "Visual Studio Code"
    URL_PROTOCOL = "vscode"


class P4AntigravityIdeMacosTest(P4DesktopReplayTestBase):
    """Antigravity IDE 2.5.5 on macOS, first start after installing DAK."""

    FIXTURE = "p4_antigravity_ide_macos.json"
    FLAVOR = "antigravity-ide"
    NAME = "Antigravity IDE"
    URL_PROTOCOL = "antigravity-ide"

    def test_url_handler_link(self):
        info = self.detect_p4()
        links = ide.dak_session_links(
            "p",
            "us-central1",
            "sc-1",
            html=False,
            style=ide.DAK_LINK_STYLE_URL_HANDLER,
            info=info,
        )
        self.assertEqual(
            links,
            [
                (
                    ide.DAK_SESSION_LABEL,
                    "antigravity-ide://googlecloudtools.datacloud/dataproc/"
                    "projects/p/locations/us-central1/sessions/sc-1",
                )
            ],
        )

    def test_obsolete_dev_build_alone_is_not_installed(self):
        self.write_json(
            os.path.join(self.p4["extensions_dir"], "extensions.json"),
            [
                {
                    "identifier": {"id": "googlecloudtools.datacloud"},
                    "version": "99.99.99",
                    "relativeLocation": "googlecloudtools.datacloud-99.99.99",
                }
            ],
        )
        self.assertIs(ide.is_dak_installed(self.detect_p4()), False)


class P4ServerReplayTestBase(P4ReplayTestBase):
    """`code serve-web` captures: enablement lives in the browser."""

    def test_server_extensions_dir_from_server_data_dir(self):
        info = self.detect_p4()
        # --server-data-dir is on the server-main process, one level above
        # the extension host; ~/.vscode-server is not used.
        self.assertTrue(info.extensions_dir.endswith("/server-data/extensions"))
        self.assertIsNone(info.user_data_dir)
        self.assertIsNone(self.p4["env"].get("VSCODE_PID"))

    def test_dak_state(self):
        os.environ.update(self.p4["env_dak_enabled"])
        info = self.detect_p4()
        listed = bool(self.p4["extensions_json"])
        expected = (
            ide.DAK_ENABLED
            if self.p4["env_dak_enabled"]
            else (ide.DAK_UNKNOWN if listed else ide.DAK_NOT_INSTALLED)
        )
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), expected)

    def test_listed_without_env_is_unknown(self):
        self.write_json(
            os.path.join(self.p4["extensions_dir"], "extensions.json"),
            [DAK_ENTRY],
        )
        info = self.detect_p4()
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_UNKNOWN)
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        # Unknown shows the Console link (not DAK's) plus the install link.
        self.assertEqual(links, [(ide.DAK_INSTALL_LABEL, ide.DAK_INSTALL_URL)])


class P4ServeWeb1142DakLinuxTest(P4ServerReplayTestBase):
    """VS Code 1.142.0 serve-web on Linux, DAK installed and running."""

    FIXTURE = "p4_serveweb_1142_dak_linux.json"
    FLAVOR = "code"
    NAME = "Visual Studio Code"
    URL_PROTOCOL = "vscode"

    def test_resource_link_only(self):
        os.environ.update(self.p4["env_dak_enabled"])
        info = self.detect_p4()
        with self.editor_shell():
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(
            links,
            [
                (
                    ide.DAK_SESSION_LABEL,
                    "dataproc-resource:/p/l/sc-1.spark.session",
                )
            ],
        )


class P4ServeWeb1142NoDakLinuxTest(P4ServerReplayTestBase):
    """VS Code 1.142.0 serve-web on Linux, no DAK in the server data dir."""

    FIXTURE = "p4_serveweb_1142_nodak_linux.json"
    FLAVOR = "code"
    NAME = "Visual Studio Code"
    URL_PROTOCOL = "vscode"

    def test_install_link_only(self):
        info = self.detect_p4()
        with self.editor_shell():
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(links, [(ide.DAK_INSTALL_LABEL, ide.DAK_INSTALL_URL)])


class P4ServeWeb1107DakLinuxTest(P4ServerReplayTestBase):
    """VS Code 1.107.0 serve-web on Linux, DAK installed and running."""

    FIXTURE = "p4_serveweb_1107_dak_linux.json"
    FLAVOR = "code"
    NAME = "Visual Studio Code"
    URL_PROTOCOL = "vscode"


class P4Desktop1142DakLinuxTest(P4ReplayTestBase):
    """VS Code 1.142.0 desktop on Linux with an isolated --extensions-dir."""

    FIXTURE = "p4_desktop_1142_dak_linux.json"
    FLAVOR = "code"
    NAME = "Visual Studio Code"
    URL_PROTOCOL = "vscode"

    def test_user_data_dir_from_code_cache_path(self):
        os.makedirs(os.path.join(self.p4["user_data_dir"], "User"))
        self.assertEqual(
            self.detect_p4().user_data_dir, self.p4["user_data_dir"]
        )

    def test_dak_running(self):
        os.environ.update(self.p4["env_dak_enabled"])
        info = self.detect_p4()
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_ENABLED)
            links = ide.dak_session_links(
                "p", "l", "sc-1", html=True, info=info
            )
        self.assertEqual(
            links,
            [
                (
                    ide.DAK_SESSION_LABEL,
                    "dataproc-resource:/p/l/sc-1.spark.session",
                )
            ],
        )

    def test_without_env_extensions_dir_is_unknown(self):
        # The real --extensions-dir cannot be read, and the default
        # ~/.vscode/extensions must not be used instead.
        self.write_json(
            os.path.join(self.home, ".vscode", "extensions", "extensions.json"),
            [DAK_ENTRY],
        )
        info = self.detect_p4()
        with self.editor_shell():
            self.assertEqual(ide.dak_state(info), ide.DAK_UNKNOWN)

    def test_vscode_pid_fallback_when_reparented(self):
        info = self.detect(self.lookup, self.p4["env"], start_pid=1)
        self.assertEqual(info.url_protocol, "vscode")
        self.assertTrue(info.source.startswith("VSCODE_PID:"))
        self.assertIsNone(info.extensions_dir)


# Only the concrete P4 subclasses should be collected.
del P4ReplayTestBase
del P4DesktopReplayTestBase
del P4ServerReplayTestBase


class DakLinksTest(IdeTestBase):

    AGY = ide.IdeInfo(
        family="vscode",
        flavor="antigravity-ide",
        url_protocol="antigravity-ide",
        source="process:1:/x/product.json",
    )
    RESOURCE = "dataproc-resource:/p/us-central1/sc-1.spark.session"

    _STATES = {
        True: ide.DAK_ENABLED,
        False: ide.DAK_NOT_INSTALLED,
        None: ide.DAK_UNKNOWN,
    }

    def links(self, info, installed=True, html=True, style=None, **kw):
        args = dict(project_id="p", location="us-central1", session_id="sc-1")
        args.update(kw)
        state = self._STATES.get(installed, installed)
        with mock.patch.object(ide, "dak_state", return_value=state):
            return ide.dak_session_links(
                html=html, style=style, info=info, **args
            )

    def test_dak_disabled(self):
        enable = (ide.DAK_ENABLE_LABEL, ide.DAK_INSTALL_URL)
        self.assertEqual(
            self.links(self.AGY, installed=ide.DAK_DISABLED), [enable]
        )
        self.assertEqual(
            self.links(
                self.AGY,
                installed=ide.DAK_DISABLED,
                style=ide.DAK_LINK_STYLE_URL_HANDLER,
            ),
            [enable],
        )

    def test_resource_link(self):
        self.assertEqual(
            self.links(self.AGY), [(ide.DAK_SESSION_LABEL, self.RESOURCE)]
        )

    def test_resource_link_works_in_vscode_too(self):
        info = ide.IdeInfo(
            family="vscode", url_protocol="vscode", source="process:1:x"
        )
        self.assertEqual(
            self.links(info), [(ide.DAK_SESSION_LABEL, self.RESOURCE)]
        )

    def test_resource_link_dak_missing_or_unknown(self):
        install = (ide.DAK_INSTALL_LABEL, ide.DAK_INSTALL_URL)
        self.assertEqual(self.links(self.AGY, installed=False), [install])
        # Unknown defaults to the Console link (no DAK session link).
        self.assertEqual(self.links(self.AGY, installed=None), [install])
        with mock.patch.object(
            ide, "DAK_UNKNOWN_STATE_LINK", ide.DAK_UNKNOWN_SHOWS_DAK
        ):
            self.assertEqual(
                self.links(self.AGY, installed=None),
                [(ide.DAK_SESSION_LABEL, self.RESOURCE), install],
            )

    def test_dak_links_structure(self):
        with mock.patch.object(ide, "dak_state", return_value=ide.DAK_ENABLED):
            links = ide.dak_links("p", "l", "sc-1", html=True, info=self.AGY)
        self.assertEqual(
            links,
            ide.DakLinks(
                session=(
                    ide.DAK_SESSION_LABEL,
                    "dataproc-resource:/p/l/sc-1.spark.session",
                )
            ),
        )
        with mock.patch.object(
            ide, "dak_state", return_value=ide.DAK_NOT_INSTALLED
        ):
            links = ide.dak_links("p", "l", "sc-1", html=True, info=self.AGY)
            self.assertEqual(
                links,
                ide.DakLinks(
                    call_to_action=(ide.DAK_INSTALL_LABEL, ide.DAK_INSTALL_URL)
                ),
            )
            # Plain text never gets a call to action.
            self.assertEqual(
                ide.dak_links("p", "l", "sc-1", html=False, info=self.AGY),
                ide.DakLinks(),
            )

    def test_install_url(self):
        self.assertEqual(
            ide.DAK_INSTALL_URL,
            "command:workbench.extensions.search?"
            "%5B%22googlecloudtools.datacloud%22%5D",
        )

    def test_resource_link_plain_text(self):
        self.assertEqual(self.links(self.AGY, html=False), [])

    def test_resource_link_needs_editor_frontend(self):
        terminal = ide.IdeInfo(
            family="vscode",
            url_protocol="antigravity-ide",
            launched_from_terminal=True,
            source="process:1:x",
        )
        self.assertEqual(self.links(terminal), [])
        env_only = ide.IdeInfo(
            family="vscode", source="env:__CFBundleIdentifier"
        )
        self.assertEqual(self.links(env_only), [])

    def test_vsc_ipynb_file_beats_terminal_heuristic(self):
        # TERM_PROGRAM=vscode leaks into an editor launched from another
        # editor's terminal; the Jupyter extension's marker takes precedence.
        info = ide.IdeInfo(
            family="vscode",
            url_protocol="antigravity-ide",
            launched_from_terminal=True,
            source="process:1:x",
        )
        shell = types.SimpleNamespace(
            user_ns={"__vsc_ipynb_file__": "/a.ipynb"}
        )
        self.assertFalse(ide.is_rendered_by_editor(info))
        with mock.patch("IPython.get_ipython", return_value=shell):
            self.assertTrue(ide.is_rendered_by_editor(info))

    def test_vsc_ipynb_file_marks_editor_frontend(self):
        env_only = ide.IdeInfo(family="vscode", source="env:X")
        shell = types.SimpleNamespace(
            user_ns={"__vsc_ipynb_file__": "/a.ipynb"}
        )
        with mock.patch("IPython.get_ipython", return_value=shell):
            self.assertEqual(
                self.links(env_only), [(ide.DAK_SESSION_LABEL, self.RESOURCE)]
            )

    def test_url_handler_link(self):
        expected = (
            "antigravity-ide://googlecloudtools.datacloud/dataproc/projects/"
            "google.com:p/locations/us-central1/sessions/sc-1"
        )
        for html in (True, False):
            self.assertEqual(
                self.links(
                    self.AGY,
                    html=html,
                    style=ide.DAK_LINK_STYLE_URL_HANDLER,
                    project_id="google.com:p",
                ),
                [(ide.DAK_SESSION_LABEL, expected)],
            )

    def test_url_handler_never_uses_vscode_or_unknown_scheme(self):
        for scheme in ("vscode", None):
            info = ide.IdeInfo(
                family="vscode", url_protocol=scheme, source="process:1:x"
            )
            self.assertEqual(
                self.links(info, style=ide.DAK_LINK_STYLE_URL_HANDLER), []
            )

    def test_segments_are_quoted(self):
        links = self.links(self.AGY, session_id="a/b c")
        self.assertEqual(
            links[0][1],
            "dataproc-resource:/p/us-central1/a%2Fb%20c.spark.session",
        )

    def test_no_links_outside_vscode(self):
        self.assertEqual(self.links(ide.IdeInfo(), installed=None), [])

    def test_no_links_without_project_or_location(self):
        self.assertEqual(self.links(self.AGY, project_id=None), [])
        self.assertEqual(self.links(self.AGY, location=None), [])

    def test_never_raises(self):
        with mock.patch.object(
            ide, "is_rendered_by_editor", side_effect=RuntimeError()
        ):
            self.assertEqual(self.links(self.AGY), [])


if __name__ == "__main__":
    unittest.main()
