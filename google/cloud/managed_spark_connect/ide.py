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
"""Detects the VS Code-family editor, if any, that hosts this process.

VS Code forks (Antigravity IDE, Cursor, ...) differ in their URL scheme and
data folder, and that changes between versions of the same fork. The only
authoritative source for those values is the editor's own ``product.json``,
which this module finds by walking up the parent processes of the kernel to
the editor's extension host. Environment variables are used only as
fallbacks.

Nothing in this module raises, and it adds no hard dependencies: it uses
``/proc`` on Linux, ``psutil`` if it happens to be importable, and ``ps`` on
macOS.
"""

import dataclasses
import functools
import json
import logging
import os
import subprocess
import sys
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)
from urllib.parse import quote, unquote, urlparse

logger = logging.getLogger(__name__)

DAK_EXTENSION_ID = "googlecloudtools.datacloud"
"""Extension ID of the Data Agent Kit (DAK) VS Code extension."""

VSCODE_FAMILY = "vscode"

# Parent processes to inspect. The extension host is normally the kernel's
# direct parent; this leaves room for environment-manager wrappers.
_MAX_PARENT_DEPTH = 10
# Ancestors of the editor process that are searched for launch flags such as
# --extensions-dir, which may be on the main process rather than the host.
_MAX_FLAG_DEPTH = 3
# Directory levels above an executable searched for product.json. The macOS
# extension host is nested 5 levels deep inside the outer .app bundle:
# <App>.app/Contents/Frameworks/<Name> Helper (Plugin).app/Contents/MacOS/x
_MAX_EXE_ANCESTORS = 6
_MAX_PRODUCT_JSON_BYTES = 4 * 1024 * 1024
_PS_TIMEOUT_SECONDS = 2

# product.json locations relative to an ancestor directory of the executable.
_PRODUCT_JSON_RELPATHS = (
    # macOS: <App>.app/Contents/Resources/app/product.json
    os.path.join("Contents", "Resources", "app", "product.json"),
    # Linux and Windows desktop: <install>/resources/app/product.json
    os.path.join("resources", "app", "product.json"),
)
_SERVER_NODE_NAMES = ("node", "node.exe")

# Identification-only fallbacks. These name the editor but cannot give its URL
# scheme, which must come from product.json. Values come from the shipped
# product.json/Info.plist of each editor.
#
# VSCODE_CODE_CACHE_PATH is <userDataPath>/CachedData/<commit>, and the last
# segment of userDataPath is product.json's nameShort.
_NAME_SHORT_TO_FLAVOR = {
    "antigravity ide": "antigravity-ide",
    "code": "code",
    "cursor": "cursor",
}
_BUNDLE_ID_TO_FLAVOR = {
    "com.google.antigravity-ide": "antigravity-ide",
    "com.microsoft.vscode": "code",
    "com.todesktop.230313mzl4w4u92": "cursor",
}

# Set by VS Code (and forks) in integrated terminals:
# src/vs/workbench/contrib/terminal/common/terminalEnvironment.ts
_TERM_PROGRAM_VSCODE = "vscode"
# Set by DAK in its extension host and in integrated terminals.
_DAK_IDE_ENV = "DATA_CLOUD_CURR_IDE_NAME"
# Injected into the kernel's user namespace by the ms-toolsai.jupyter
# extension, i.e. the notebook is being rendered by the editor.
_VSC_IPYNB_FILE = "__vsc_ipynb_file__"


@dataclasses.dataclass(frozen=True)
class IdeInfo:
    """What is known about the editor hosting this process.

    Attributes:
        family: ``"vscode"`` for any VS Code-family editor, else None.
        flavor: The editor's ``applicationName`` (``"code"``, ``"cursor"``,
            ``"antigravity-ide"``, ...), if known.
        name: The editor's ``nameLong``, if known.
        url_protocol: The URL scheme the editor registers with the OS
            (``urlProtocol`` in product.json). Only set when read from the
            editor's product.json, never guessed.
        data_folder_name: ``dataFolderName`` (or ``serverDataFolderName`` for
            server installs) from product.json.
        extensions_dir: The editor's extensions directory, if it could be
            determined.
        launched_from_terminal: True if this process descends from an
            integrated terminal (e.g. ``jupyter lab`` started in a VS Code
            terminal), in which case the notebook may be rendered elsewhere.
        is_server: True for server installs (``code serve-web``, Remote-SSH,
            tunnels), whose extension enablement lives on the client.
        user_data_dir: The editor's user data directory (desktop only).
        profile_name: The ``--profile`` the editor was started with, if any.
        profile_flag_unparsed: True if ``--profile`` is present but its value
            could not be read reliably.
        dak_disabled_by_flag: True if the editor was started with
            ``--disable-extensions`` or ``--disable-extension`` for DAK, None
            if a ``--disable-extension`` value could not be read.
        source: Which signal produced this result, for debugging.
    """

    family: Optional[str] = None
    flavor: Optional[str] = None
    name: Optional[str] = None
    url_protocol: Optional[str] = None
    data_folder_name: Optional[str] = None
    extensions_dir: Optional[str] = None
    launched_from_terminal: bool = False
    is_server: bool = False
    user_data_dir: Optional[str] = None
    profile_name: Optional[str] = None
    profile_flag_unparsed: bool = False
    dak_disabled_by_flag: Optional[bool] = False
    source: str = "none"


@dataclasses.dataclass(frozen=True)
class _ProcInfo:
    pid: int
    ppid: Optional[int]
    exe: Optional[str]
    cmdline: Tuple[str, ...] = ()


_ProcLookup = Callable[[int], Optional[_ProcInfo]]


def _linux_proc_lookup(proc_root: str = "/proc") -> _ProcLookup:
    def lookup(pid: int) -> Optional[_ProcInfo]:
        base = os.path.join(proc_root, str(pid))
        try:
            with open(os.path.join(base, "stat"), "rb") as f:
                stat = f.read().decode("utf-8", "replace")
        except OSError:
            return None
        # The command name is in parentheses and may contain spaces or
        # parentheses, so split after the last ")": "<state> <ppid> ...".
        fields = stat[stat.rfind(")") + 1 :].split()
        ppid = (
            int(fields[1]) if len(fields) > 1 and fields[1].isdigit() else None
        )
        cmdline: Tuple[str, ...] = ()
        try:
            with open(os.path.join(base, "cmdline"), "rb") as f:
                cmdline = tuple(
                    a.decode("utf-8", "replace")
                    for a in f.read().split(b"\0")
                    if a
                )
        except OSError:
            pass
        try:
            exe: Optional[str] = os.readlink(os.path.join(base, "exe"))
            if exe.endswith(" (deleted)"):
                exe = exe[: -len(" (deleted)")]
        except OSError:
            args, _ = _cmdline_args(cmdline) if cmdline else ((), False)
            first = args[0] if args else ""
            exe = first if os.path.isabs(first) else None
        return _ProcInfo(pid, ppid, exe, cmdline)

    return lookup


def _psutil_proc_lookup(psutil) -> _ProcLookup:
    def call(fn, default):
        try:
            return fn()
        except Exception:
            return default

    def lookup(pid: int) -> Optional[_ProcInfo]:
        try:
            p = psutil.Process(pid)
        except Exception:
            return None
        return _ProcInfo(
            pid,
            call(p.ppid, None),
            call(p.exe, None) or None,
            tuple(call(p.cmdline, None) or ()),
        )

    return lookup


def _ps_proc_lookup(run=subprocess.run) -> _ProcLookup:
    """Lookup backed by one ``ps`` call, for macOS without psutil.

    On macOS ``comm`` is the full executable path. It is the last column, so
    spaces in it are unambiguous. Command lines are not available this way.
    """
    table: Optional[Dict[int, _ProcInfo]] = None

    def load() -> Dict[int, _ProcInfo]:
        result: Dict[int, _ProcInfo] = {}
        try:
            out = run(
                ["ps", "-axo", "pid=,ppid=,comm="],
                capture_output=True,
                text=True,
                timeout=_PS_TIMEOUT_SECONDS,
                check=False,
            ).stdout
        except Exception:
            return result
        for line in (out or "").splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 2 or not (
                parts[0].isdigit() and parts[1].isdigit()
            ):
                continue
            pid, ppid = int(parts[0]), int(parts[1])
            exe = parts[2] if len(parts) == 3 else None
            result[pid] = _ProcInfo(pid, ppid, exe)
        return result

    def lookup(pid: int) -> Optional[_ProcInfo]:
        nonlocal table
        if table is None:
            table = load()
        return table.get(pid)

    return lookup


def _default_proc_lookup() -> Optional[_ProcLookup]:
    if sys.platform.startswith("linux") and os.path.isdir("/proc"):
        return _linux_proc_lookup()
    try:
        import psutil  # type: ignore[import-not-found]

        return _psutil_proc_lookup(psutil)
    except Exception:
        pass
    if sys.platform == "darwin":
        return _ps_proc_lookup()
    return None


def _walk_parents(
    lookup: _ProcLookup, start_pid: Optional[int], max_depth: int
) -> Iterator[_ProcInfo]:
    seen = set()
    pid = start_pid
    for _ in range(max_depth):
        if pid is None or pid <= 0 or pid in seen:
            return
        seen.add(pid)
        try:
            info = lookup(pid)
        except Exception:
            return
        if info is None:
            return
        yield info
        pid = info.ppid


def _read_product_json(path: str) -> Optional[dict]:
    try:
        if not os.path.isfile(path):
            return None
        if os.path.getsize(path) > _MAX_PRODUCT_JSON_BYTES:
            return None
        with open(path, "rb") as f:
            product = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(product, dict):
        return None
    if not isinstance(product.get("nameShort"), str):
        return None
    if not any(
        isinstance(product.get(k), str)
        for k in ("urlProtocol", "dataFolderName", "serverDataFolderName")
    ):
        return None
    return product


def _find_product_json(exe: str) -> Optional[Tuple[str, dict, bool]]:
    """Returns (path, product, is_server) for the editor owning ``exe``."""
    # A relative path (e.g. macOS `ps` reports some commands by bare name)
    # would be resolved against our own working directory.
    if not os.path.isabs(exe):
        return None
    exe_dir = os.path.dirname(exe)
    # Server installs (code serve-web, Remote-SSH, tunnels) ship <root>/node
    # with <root>/product.json next to it.
    if os.path.basename(exe).lower() in _SERVER_NODE_NAMES:
        path = os.path.join(exe_dir, "product.json")
        product = _read_product_json(path)
        if product is not None:
            return path, product, True
    # Try each layout over all ancestors before the next layout, so that on
    # case-insensitive macOS file systems the macOS layout is found under its
    # canonical spelling rather than as <App>.app/Contents/resources/app.
    for rel in _PRODUCT_JSON_RELPATHS:
        d = exe_dir
        for _ in range(_MAX_EXE_ANCESTORS):
            path = os.path.join(d, rel)
            product = _read_product_json(path)
            if product is not None:
                return path, product, False
            parent = os.path.dirname(d)
            if not parent or parent == d:
                break
            d = parent
    return None


def _cmdline_args(cmdline: Sequence[str]) -> Tuple[Sequence[str], bool]:
    """Returns (args, joined).

    On Linux, Chromium rewrites the process title of Electron processes, so
    /proc/<pid>/cmdline is one space-joined string, with all positional
    arguments (including detached switch values) moved after the switches.
    Such a cmdline is split on whitespace and reported as joined; values
    containing spaces then come out truncated and fail validation.
    """
    if len(cmdline) == 1 and " " in cmdline[0]:
        return cmdline[0].split(), True
    return cmdline, False


def _flag_lookup(
    cmdlines: Iterable[Sequence[str]], flag: str
) -> Tuple[bool, Optional[str]]:
    """Returns (present, value) for a command-line flag.

    ``--flag=value`` is reliable. ``--flag value`` is taken only from a
    regular (not joined) cmdline and only when the next argument is not
    another flag; in a joined Electron title the next token may be any
    positional. Callers must validate the value and treat a present flag
    without a usable value as unknown rather than falling back to defaults.
    """
    present = False
    prefix = flag + "="
    for cmdline in cmdlines:
        args, joined = _cmdline_args(cmdline)
        for i, arg in enumerate(args):
            if arg.startswith(prefix):
                present = True
                if len(arg) > len(prefix):
                    return True, arg[len(prefix) :]
            elif arg == flag:
                present = True
                if (
                    not joined
                    and i + 1 < len(args)
                    and not args[i + 1].startswith("-")
                ):
                    return True, args[i + 1]
    return present, None


def _str_or_none(value) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def _existing_dir(path: Optional[str], marker: Optional[str] = None):
    """``path`` if it is a directory (containing ``marker``), else None."""
    if not path or not os.path.isabs(path) or not os.path.isdir(path):
        return None
    if marker and not os.path.exists(os.path.join(path, marker)):
        return None
    return path


def _resolve_extensions_dir(
    product: dict,
    is_server: bool,
    cmdlines: Sequence[Sequence[str]],
    env: Mapping[str, str],
    home: str,
) -> Tuple[Optional[str], Optional[str]]:
    """Returns (extensions_dir, data_folder_name).

    Mirrors src/vs/platform/environment/common/environmentService.ts
    (desktop) and src/vs/server/node/server.main.ts (server).
    """
    folder = _str_or_none(
        product.get("serverDataFolderName" if is_server else "dataFolderName")
    )
    present, explicit = _flag_lookup(cmdlines, "--extensions-dir")
    if present:
        return _existing_dir(explicit, "extensions.json"), folder
    if is_server:
        present, server_data_dir = _flag_lookup(cmdlines, "--server-data-dir")
        if present:
            if not _existing_dir(server_data_dir, "extensions"):
                return None, folder
            return os.path.join(server_data_dir, "extensions"), folder
        if env.get("VSCODE_AGENT_FOLDER"):
            return (
                os.path.join(env["VSCODE_AGENT_FOLDER"], "extensions"),
                folder,
            )
    else:
        if env.get("VSCODE_EXTENSIONS"):
            return env["VSCODE_EXTENSIONS"], folder
        if env.get("VSCODE_PORTABLE"):
            return os.path.join(env["VSCODE_PORTABLE"], "extensions"), folder
    if folder and home:
        return os.path.join(home, folder, "extensions"), folder
    return None, folder


def _resolve_user_data_dir(
    product: dict,
    cmdlines: Sequence[Sequence[str]],
    env: Mapping[str, str],
    home: str,
    platform: str,
) -> Optional[str]:
    """The desktop editor's user data directory.

    VSCODE_CODE_CACHE_PATH is <userData>/CachedData/<commit> (main.ts).
    Otherwise mirrors src/vs/platform/environment/node/userDataPath.ts.
    """
    cache_path = env.get("VSCODE_CODE_CACHE_PATH", "")
    if cache_path:
        cached_data = os.path.dirname(os.path.normpath(cache_path))
        if os.path.basename(cached_data).lower() == "cacheddata":
            return os.path.dirname(cached_data)
    present, explicit = _flag_lookup(cmdlines, "--user-data-dir")
    if present:
        # A real user data dir has a User subdirectory.
        return _existing_dir(explicit, "User")
    if env.get("VSCODE_PORTABLE"):
        return os.path.join(env["VSCODE_PORTABLE"], "user-data")
    name_short = _str_or_none(product.get("nameShort"))
    if not name_short:
        return None
    if env.get("VSCODE_APPDATA"):
        return os.path.join(env["VSCODE_APPDATA"], name_short)
    if platform == "win32":
        appdata = env.get("APPDATA")
        return os.path.join(appdata, name_short) if appdata else None
    if not home:
        return None
    if platform == "darwin":
        return os.path.join(home, "Library", "Application Support", name_short)
    config = env.get("XDG_CONFIG_HOME") or os.path.join(home, ".config")
    return os.path.join(config, name_short)


def _dak_disabled_by_flag(cmdlines: Sequence[Sequence[str]]) -> Optional[bool]:
    """True if DAK is disabled on the command line, None if unparseable."""
    unparsed = False
    for cmdline in cmdlines:
        args, joined = _cmdline_args(cmdline)
        for i, arg in enumerate(args):
            if arg == "--disable-extensions":
                return True
            value = None
            if arg.startswith("--disable-extension="):
                value = arg[len("--disable-extension=") :]
            elif arg == "--disable-extension":
                if (
                    not joined
                    and i + 1 < len(args)
                    and not args[i + 1].startswith("-")
                ):
                    value = args[i + 1]
                else:
                    unparsed = True
            if value and value.lower() == DAK_EXTENSION_ID:
                return True
    return None if unparsed else False


def _info_from_product(
    product: dict,
    is_server: bool,
    cmdlines: Sequence[Sequence[str]],
    env: Mapping[str, str],
    home: str,
    source: str,
) -> IdeInfo:
    extensions_dir, folder = _resolve_extensions_dir(
        product, is_server, cmdlines, env, home
    )
    user_data_dir = None
    if not is_server:
        user_data_dir = _resolve_user_data_dir(
            product, cmdlines, env, home, sys.platform
        )
    profile_present, profile_name = _flag_lookup(cmdlines, "--profile")
    return IdeInfo(
        family=VSCODE_FAMILY,
        flavor=_str_or_none(product.get("applicationName")),
        name=_str_or_none(product.get("nameLong"))
        or _str_or_none(product.get("nameShort")),
        url_protocol=_str_or_none(product.get("urlProtocol")),
        data_folder_name=folder,
        extensions_dir=extensions_dir,
        launched_from_terminal=env.get("TERM_PROGRAM") == _TERM_PROGRAM_VSCODE,
        is_server=is_server,
        user_data_dir=user_data_dir,
        profile_name=profile_name,
        profile_flag_unparsed=profile_present and not profile_name,
        dak_disabled_by_flag=_dak_disabled_by_flag(cmdlines),
        source=source,
    )


def _from_process_tree(
    lookup: Optional[_ProcLookup],
    start_pid: Optional[int],
    env: Mapping[str, str],
    home: str,
) -> Optional[IdeInfo]:
    if lookup is None:
        return None
    chain: List[_ProcInfo] = []
    walk = _walk_parents(lookup, start_pid, _MAX_PARENT_DEPTH)
    for proc in walk:
        chain.append(proc)
        if not proc.exe:
            continue
        found = _find_product_json(proc.exe)
        if found is None:
            continue
        path, product, is_server = found
        # The editor's launch flags may be on an ancestor (e.g. the main
        # process), so collect a few more command lines.
        cmdlines = [proc.cmdline]
        for _, ancestor in zip(range(_MAX_FLAG_DEPTH), walk):
            cmdlines.append(ancestor.cmdline)
        return _info_from_product(
            product,
            is_server,
            cmdlines,
            env,
            home,
            source=f"process:{proc.pid}:{path}",
        )
    # The kernel may have been reparented (e.g. by a daemonizing wrapper), so
    # also try the editor's main process.
    vscode_pid = env.get("VSCODE_PID", "")
    if vscode_pid.isdigit():
        try:
            proc = lookup(int(vscode_pid))
        except Exception:
            proc = None
        if proc is not None and proc.exe:
            found = _find_product_json(proc.exe)
            if found is not None:
                path, product, is_server = found
                return _info_from_product(
                    product,
                    is_server,
                    [proc.cmdline],
                    env,
                    home,
                    source=f"VSCODE_PID:{path}",
                )
    return None


def _from_app_root_env(env: Mapping[str, str], home: str) -> Optional[IdeInfo]:
    """Reads product.json via app-root paths found in the environment."""
    candidates: List[Tuple[str, str]] = []
    try:
        nls = json.loads(env.get("VSCODE_NLS_CONFIG", "") or "null")
    except ValueError:
        nls = None
    if isinstance(nls, dict):
        messages = _str_or_none(nls.get("defaultMessagesFile"))
        if messages:
            # <appRoot>/out/nls.messages.json
            app_root = os.path.dirname(os.path.dirname(messages))
            candidates.append(("VSCODE_NLS_CONFIG", app_root))
    if env.get("ANTIGRAVITY_EDITOR_APP_ROOT"):
        candidates.append(
            ("ANTIGRAVITY_EDITOR_APP_ROOT", env["ANTIGRAVITY_EDITOR_APP_ROOT"])
        )
    for var, app_root in candidates:
        path = os.path.join(app_root, "product.json")
        product = _read_product_json(path)
        if product is None:
            continue
        # A server's app root holds the server product.json and node.
        is_server = isinstance(product.get("serverDataFolderName"), str) and (
            any(
                os.path.isfile(os.path.join(app_root, n))
                for n in _SERVER_NODE_NAMES
            )
        )
        return _info_from_product(
            product, is_server, [], env, home, source=f"env:{var}:{path}"
        )
    return None


def _path_segments(path: str) -> List[str]:
    return [s for s in path.replace("\\", "/").split("/") if s]


def _from_identifying_env(env: Mapping[str, str]) -> Optional[IdeInfo]:
    """Names the editor from environment variables. Never sets url_protocol."""
    terminal = env.get("TERM_PROGRAM") == _TERM_PROGRAM_VSCODE
    cache_path = env.get("VSCODE_CODE_CACHE_PATH", "")
    segments = _path_segments(cache_path)
    lowered = [s.lower() for s in segments]
    if "cacheddata" in lowered:
        i = lowered.index("cacheddata")
        if i > 0:
            flavor = _NAME_SHORT_TO_FLAVOR.get(lowered[i - 1])
            if flavor:
                return IdeInfo(
                    family=VSCODE_FAMILY,
                    flavor=flavor,
                    name=segments[i - 1],
                    launched_from_terminal=terminal,
                    source="env:VSCODE_CODE_CACHE_PATH",
                )
    bundle = env.get("__CFBundleIdentifier", "").lower()
    if bundle in _BUNDLE_ID_TO_FLAVOR:
        return IdeInfo(
            family=VSCODE_FAMILY,
            flavor=_BUNDLE_ID_TO_FLAVOR[bundle],
            launched_from_terminal=terminal,
            source="env:__CFBundleIdentifier",
        )
    if env.get(_DAK_IDE_ENV):
        return IdeInfo(
            family=VSCODE_FAMILY,
            launched_from_terminal=terminal,
            source=f"env:{_DAK_IDE_ENV}",
        )
    return None


def _detect(
    env: Mapping[str, str],
    lookup: Optional[_ProcLookup],
    start_pid: Optional[int],
    home: str,
) -> IdeInfo:
    for detector in (
        lambda: _from_process_tree(lookup, start_pid, env, home),
        lambda: _from_app_root_env(env, home),
        lambda: _from_identifying_env(env),
    ):
        try:
            info = detector()
        except Exception:
            logger.debug("IDE detector failed", exc_info=True)
            continue
        if info is not None:
            return info
    return IdeInfo()


@functools.lru_cache(maxsize=1)
def detect_ide() -> IdeInfo:
    """Detects the VS Code-family editor hosting this process.

    The result is cached for the life of the process. Never raises; returns
    ``IdeInfo()`` (``family=None``) if no editor is found.
    """
    try:
        try:
            lookup = _default_proc_lookup()
        except Exception:
            lookup = None
        return _detect(
            os.environ,
            lookup,
            os.getppid(),
            os.path.expanduser("~"),
        )
    except Exception:
        logger.debug("IDE detection failed", exc_info=True)
        return IdeInfo(source="error")


def _dak_in_extensions_dir(
    extensions_dir: str, extensions_json: Optional[str] = None
) -> Optional[bool]:
    """Whether DAK is in extensions.json, minus entries in ``.obsolete``.

    ``extensions_json`` overrides ``<extensions_dir>/extensions.json``, e.g.
    for a non-default profile; ``.obsolete`` is always in the extensions dir.
    """
    try:
        with open(
            extensions_json or os.path.join(extensions_dir, "extensions.json"),
            "rb",
        ) as f:
            entries = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(entries, list):
        return None
    obsolete: dict = {}
    try:
        with open(os.path.join(extensions_dir, ".obsolete"), "rb") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            obsolete = loaded
    except (OSError, ValueError):
        pass
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        identifier = entry.get("identifier")
        ext_id = identifier.get("id") if isinstance(identifier, dict) else None
        if not isinstance(ext_id, str) or ext_id.lower() != DAK_EXTENSION_ID:
            continue
        # .obsolete is keyed by extension directory name, not by ID.
        folder = _str_or_none(entry.get("relativeLocation"))
        location = entry.get("location")
        if not folder and isinstance(location, dict):
            loc_path = _str_or_none(location.get("path"))
            if loc_path:
                folder = os.path.basename(loc_path.rstrip("/"))
        if folder and obsolete.get(folder):
            continue
        return True
    return False


DAK_ENABLED = "enabled"
DAK_DISABLED = "disabled"
DAK_NOT_INSTALLED = "not-installed"
DAK_UNKNOWN = "unknown"

_DEFAULT_PROFILE_ID = "__default__profile__"
_DISABLED_KEY = "extensionsIdentifiers/disabled"
_ENABLED_KEY = "extensionsIdentifiers/enabled"
_SQLITE_TIMEOUT_SECONDS = 0.2
_MAX_WORKSPACE_STORAGE_DIRS = 5000


class _Unknown(Exception):
    """The enablement state cannot be determined."""


@dataclasses.dataclass(frozen=True)
class _Profile:
    # Directory under <userData>/User/profiles, or None for the default.
    location: Optional[str] = None
    use_default_extensions: bool = False
    use_default_global_state: bool = False


def _notebook_path() -> Optional[str]:
    """The notebook path set by the Jupyter extension, if any.

    Only consults IPython if it is already loaded, i.e. in a kernel.
    """
    ipython = sys.modules.get("IPython")
    get_ipython = getattr(ipython, "get_ipython", None)
    shell = get_ipython() if callable(get_ipython) else None
    value = getattr(shell, "user_ns", {}).get(_VSC_IPYNB_FILE)
    return value if isinstance(value, str) and os.path.isabs(value) else None


def _file_uri_to_path(uri: str) -> Optional[str]:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        return None
    if os.name == "nt":
        from urllib.request import url2pathname

        path = url2pathname(parsed.path)
    else:
        path = unquote(parsed.path)
    return path if os.path.isabs(path) else None


def _is_within(path: str, folder: str) -> bool:
    path = os.path.normcase(os.path.normpath(path))
    folder = os.path.normcase(os.path.normpath(folder))
    return path == folder or path.startswith(folder.rstrip(os.sep) + os.sep)


def _longest_containing(
    path: str, candidates: Iterable[Tuple[str, str]]
) -> Optional[str]:
    """Returns the value whose folder URI most deeply contains ``path``."""
    best: Optional[Tuple[int, str]] = None
    for uri, value in candidates:
        folder = _file_uri_to_path(uri) if isinstance(uri, str) else None
        if folder and _is_within(path, folder):
            if best is None or len(folder) > best[0]:
                best = (len(folder), value)
    return best[1] if best else None


def _read_json_or_unknown(path: str):
    """Parsed JSON, None if the file is missing; raises _Unknown otherwise."""
    try:
        with open(path, "rb") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:
        raise _Unknown(path) from e


def _resolve_profile(info: IdeInfo, notebook: Optional[str]) -> _Profile:
    """Mirrors userDataProfile.ts: storage.json associations or --profile."""
    if info.profile_flag_unparsed:
        raise _Unknown("unparsed --profile")
    storage = _read_json_or_unknown(
        os.path.join(
            info.user_data_dir, "User", "globalStorage", "storage.json"
        )
    )
    if storage is None:
        if info.profile_name:
            raise _Unknown("--profile without storage.json")
        return _Profile()
    if not isinstance(storage, dict):
        raise _Unknown("storage.json")
    profiles = storage.get("userDataProfiles") or []
    if not isinstance(profiles, list):
        raise _Unknown("userDataProfiles")
    profile_id: Optional[str] = None
    if info.profile_name:
        for p in profiles:
            if isinstance(p, dict) and p.get("name") == info.profile_name:
                profile_id = _str_or_none(p.get("location"))
                break
        else:
            raise _Unknown(f"profile {info.profile_name!r}")
    elif notebook:
        associations = storage.get("profileAssociations") or {}
        workspaces = (
            associations.get("workspaces")
            if isinstance(associations, dict)
            else None
        )
        if isinstance(workspaces, dict):
            profile_id = _longest_containing(notebook, workspaces.items())
    if not profile_id or profile_id == _DEFAULT_PROFILE_ID:
        return _Profile()
    for p in profiles:
        if isinstance(p, dict) and p.get("location") == profile_id:
            flags = p.get("useDefaultFlags")
            flags = flags if isinstance(flags, dict) else {}
            return _Profile(
                location=profile_id,
                use_default_extensions=bool(flags.get("extensions")),
                use_default_global_state=bool(flags.get("globalState")),
            )
    raise _Unknown(f"profile id {profile_id!r}")


def _read_id_list(db_path: str, key: str) -> List[str]:
    """Lower-cased extension IDs stored under ``key`` in a state.vscdb.

    Opens the database read-only; VS Code uses the default rollback journal
    and short write transactions, so a concurrent read is safe. A missing
    database or key means an empty list. Raises _Unknown on any error.
    """
    if not os.path.isfile(db_path):
        return []
    try:
        import sqlite3
        import pathlib

        uri = pathlib.Path(db_path).as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=_SQLITE_TIMEOUT_SECONDS)
        try:
            row = conn.execute(
                "SELECT value FROM ItemTable WHERE key = ?", (key,)
            ).fetchone()
        finally:
            conn.close()
    except Exception as e:
        raise _Unknown(db_path) from e
    if row is None or row[0] is None:
        return []
    value = row[0]
    try:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        entries = json.loads(value)
    except (UnicodeDecodeError, ValueError) as e:
        raise _Unknown(db_path) from e
    if not isinstance(entries, list):
        raise _Unknown(db_path)
    return [
        e["id"].lower()
        for e in entries
        if isinstance(e, dict) and isinstance(e.get("id"), str)
    ]


def _workspace_storage_db(
    user_data_dir: str, notebook: Optional[str]
) -> Optional[str]:
    """state.vscdb of the workspace folder containing the notebook.

    Found by scanning workspaceStorage/*/workspace.json rather than
    recomputing the folder hash (src/vs/platform/workspaces/node/
    workspaces.ts), which needs the folder's inode or birth time.
    """
    if not notebook:
        return None
    root = os.path.join(user_data_dir, "User", "workspaceStorage")
    candidates: List[Tuple[str, str]] = []
    try:
        with os.scandir(root) as entries:
            for i, entry in enumerate(entries):
                if i >= _MAX_WORKSPACE_STORAGE_DIRS:
                    raise _Unknown("too many workspaces")
                try:
                    with open(
                        os.path.join(entry.path, "workspace.json"), "rb"
                    ) as f:
                        meta = json.load(f)
                except (OSError, ValueError):
                    continue
                folder = meta.get("folder") if isinstance(meta, dict) else None
                if isinstance(folder, str):
                    candidates.append((folder, entry.path))
    except FileNotFoundError:
        return None
    except OSError as e:
        raise _Unknown(root) from e
    found = _longest_containing(notebook, candidates)
    return os.path.join(found, "state.vscdb") if found else None


def _dak_desktop_state(info: IdeInfo, notebook: Optional[str]) -> str:
    """Installed/enabled state from the desktop editor's own files.

    Mirrors _getUserEnablementState in src/vs/workbench/services/
    extensionManagement/browser/extensionEnablementService.ts: workspace
    enabled, then workspace disabled, then the profile's global disabled
    list. Workspace trust, virtual workspaces and policies are not checked.

    Known gaps: multi-root (.code-workspace) windows are not matched to their
    workspace storage, so only the global state applies there; and without
    the notebook path (``__vsc_ipynb_file__``) the default profile is assumed
    and workspace-level state is not seen.
    """
    if not info.user_data_dir or not info.extensions_dir:
        raise _Unknown("no user data dir")
    profile = _resolve_profile(info, notebook)
    user_dir = os.path.join(info.user_data_dir, "User")
    profile_dir = (
        os.path.join(user_dir, "profiles", profile.location)
        if profile.location
        else None
    )
    extensions_json = None
    if profile_dir and not profile.use_default_extensions:
        extensions_json = os.path.join(profile_dir, "extensions.json")
    listed = _dak_in_extensions_dir(info.extensions_dir, extensions_json)
    if listed is None:
        raise _Unknown("extensions.json")
    if not listed:
        return DAK_NOT_INSTALLED
    if info.dak_disabled_by_flag is None:
        raise _Unknown("unparsed --disable-extension")
    if info.dak_disabled_by_flag:
        return DAK_DISABLED
    workspace_db = _workspace_storage_db(info.user_data_dir, notebook)
    if workspace_db:
        if DAK_EXTENSION_ID in _read_id_list(workspace_db, _ENABLED_KEY):
            return DAK_ENABLED
        if DAK_EXTENSION_ID in _read_id_list(workspace_db, _DISABLED_KEY):
            return DAK_DISABLED
    global_dir = (
        os.path.join(profile_dir, "globalStorage")
        if profile_dir and not profile.use_default_global_state
        else os.path.join(user_dir, "globalStorage")
    )
    global_db = os.path.join(global_dir, "state.vscdb")
    if DAK_EXTENSION_ID in _read_id_list(global_db, _DISABLED_KEY):
        return DAK_DISABLED
    return DAK_ENABLED


def _has_editor_ancestor(info: IdeInfo) -> bool:
    return info.family == VSCODE_FAMILY and info.source.startswith(
        ("process:", "VSCODE_PID:")
    )


def dak_state(info: Optional[IdeInfo] = None) -> str:
    """Whether Data Agent Kit is enabled in the hosting editor.

    Returns DAK_ENABLED, DAK_DISABLED, DAK_NOT_INSTALLED or DAK_UNKNOWN.
    Never raises. Not cached, so installing or enabling DAK is picked up.

    - DAK sets DATA_CLOUD_CURR_IDE_NAME in its extension host once it is
      running, and kernels inherit it. With an editor among our ancestors
      that means enabled. DAK also sets it in integrated terminals, from
      which it can leak into another editor, so it is not trusted when that
      editor's extensions.json does not list DAK.
    - Otherwise, for desktop editors, the editor's extensions.json and
      enablement state (per profile and workspace) are read.
    - Server installs keep enablement on the client, so a listed DAK is
      DAK_UNKNOWN there.
    """
    try:
        if info is None:
            info = detect_ide()
        if info.family is None:
            return DAK_UNKNOWN
        dak_env = bool(os.environ.get(_DAK_IDE_ENV))
        listed: Optional[bool] = None
        if info.extensions_dir:
            listed = _dak_in_extensions_dir(info.extensions_dir)
        if dak_env:
            if listed is False:
                return DAK_UNKNOWN
            if listed or _has_editor_ancestor(info):
                return DAK_ENABLED
            return DAK_UNKNOWN
        if info.is_server:
            return DAK_NOT_INSTALLED if listed is False else DAK_UNKNOWN
        try:
            return _dak_desktop_state(info, _notebook_path())
        except _Unknown:
            logger.debug("DAK enablement unknown", exc_info=True)
            return DAK_UNKNOWN
    except Exception:
        logger.debug("DAK detection failed", exc_info=True)
        return DAK_UNKNOWN


def is_dak_installed(info: Optional[IdeInfo] = None) -> Optional[bool]:
    """Whether Data Agent Kit is installed and enabled; None if unknown.

    See dak_state(). Never raises.
    """
    return {
        DAK_ENABLED: True,
        DAK_DISABLED: False,
        DAK_NOT_INSTALLED: False,
    }.get(dak_state(info))


def is_rendered_by_editor(info: Optional[IdeInfo] = None) -> bool:
    """Whether notebook output is shown by a VS Code-family editor.

    True if the ms-toolsai.jupyter extension has marked this kernel, or if the
    editor was found in the kernel's parent processes and the kernel was not
    started from an integrated terminal (where the frontend may be a browser).
    Never raises.
    """
    try:
        if _notebook_path() is not None:
            return True
        if info is None:
            info = detect_ide()
        return (
            info.family == VSCODE_FAMILY
            and not info.launched_from_terminal
            and info.source.startswith(("process:", "VSCODE_PID:"))
        )
    except Exception:
        return False


DAK_SESSION_LABEL = "Managed Spark Session (Data Agent Kit)"
DAK_INSTALL_LABEL = "Install Data Agent Kit"
DAK_ENABLE_LABEL = "Enable Data Agent Kit"
# For DAK_UNKNOWN, where DAK is usually installed but its state unconfirmed.
DAK_OPEN_EXTENSION_LABEL = "Open Data Agent Kit in Extensions"
# Follows the install/enable call-to-action link.
DAK_CALL_TO_ACTION_SUFFIX = "to open sessions in your editor"
# Opens the Extensions view searching for DAK. workbench.extensions.search is
# on the notebook output command allowlist (backLayerWebView.ts).
DAK_INSTALL_URL = (
    "command:workbench.extensions.search?%5B%22" + DAK_EXTENSION_ID + "%22%5D"
)

# Link styles for opening a session in DAK:
# - "resource": dataproc-resource:/<project>/<location>/<id>.spark.session,
#   DAK's own file system. Works whatever the editor's URL scheme, but only
#   inside the editor's notebook output and only when DAK is installed.
# - "url-handler": <urlProtocol>://googlecloudtools.datacloud/dataproc/
#   projects/<project>/locations/<location>/sessions/<id>, routed through the
#   OS/editor URL handler to DAK's URI handler. Notebook output drops query
#   strings, so everything is in the path.
DAK_LINK_STYLE_RESOURCE = "resource"
DAK_LINK_STYLE_URL_HANDLER = "url-handler"
DAK_LINK_STYLE = DAK_LINK_STYLE_RESOURCE

_DAK_RESOURCE_SCHEME = "dataproc-resource"
# Schemes that never reach the URL handler from notebook output. VS Code core
# registers a file system provider for "vscode" (SettingsFileSystemProvider),
# so such links open as an (unreadable) file instead.
_SCHEMES_WITHOUT_URL_HANDLER = frozenset({"vscode"})


def _quote_segment(segment: str) -> str:
    # Keep ":" for domain-scoped projects such as "google.com:my-project".
    return quote(segment, safe=":")


def _dak_resource_url(
    info: IdeInfo, project_id: str, location: str, session_id: str
) -> Optional[str]:
    return (
        f"{_DAK_RESOURCE_SCHEME}:/{_quote_segment(project_id)}/"
        f"{_quote_segment(location)}/{_quote_segment(session_id)}"
        ".spark.session"
    )


def _dak_url_handler_url(
    info: IdeInfo, project_id: str, location: str, session_id: str
) -> Optional[str]:
    scheme = info.url_protocol
    if not scheme or scheme.lower() in _SCHEMES_WITHOUT_URL_HANDLER:
        return None
    return (
        f"{scheme}://{DAK_EXTENSION_ID}/dataproc/projects/"
        f"{_quote_segment(project_id)}/locations/{_quote_segment(location)}"
        f"/sessions/{_quote_segment(session_id)}"
    )


@dataclasses.dataclass(frozen=True)
class CallToAction:
    """An invitation to install, enable or check Data Agent Kit.

    Attributes:
        state: The dak_state() it responds to.
        label: Link text.
        url: The ``command:`` link opening the Extensions view on DAK.
        suffix: Plain text following the link (may be empty).
        icon: Decorative prefix for rich output; omitted from plain text.
    """

    state: str
    label: str
    url: str
    suffix: str = ""
    icon: str = ""


# Decorative icons for calls to action in rich output. DAK itself uses no
# emoji; set this to {} to drop them.
CALL_TO_ACTION_ICONS = {
    DAK_NOT_INSTALLED: "🧩",
    DAK_DISABLED: "🔌",
    DAK_UNKNOWN: "🧩",
}


def _call_to_action(state: str) -> Optional[CallToAction]:
    if state == DAK_NOT_INSTALLED:
        label, suffix = DAK_INSTALL_LABEL, DAK_CALL_TO_ACTION_SUFFIX
    elif state == DAK_DISABLED:
        label, suffix = DAK_ENABLE_LABEL, DAK_CALL_TO_ACTION_SUFFIX
    elif state == DAK_UNKNOWN:
        label, suffix = DAK_OPEN_EXTENSION_LABEL, ""
    else:
        return None
    return CallToAction(
        state=state,
        label=label,
        url=DAK_INSTALL_URL,
        suffix=suffix,
        icon=CALL_TO_ACTION_ICONS.get(state, ""),
    )


@dataclasses.dataclass(frozen=True)
class DakLinks:
    """Data Agent Kit links for one session.

    Attributes:
        session: (label, url) opening the session in DAK, to be shown INSTEAD
            of the Cloud Console link; None to show the Console link.
        call_to_action: Invites the user to install, enable or check DAK;
            shown next to the session link, not as a session link.
    """

    session: Optional[Tuple[str, str]] = None
    call_to_action: Optional[CallToAction] = None


# What to show when DAK's state is DAK_UNKNOWN (server installs, or DAK
# listed but its enablement unreadable):
# - "console": the Console link, plus the install call to action.
# - "dak": the DAK link, plus the install call to action.
DAK_UNKNOWN_SHOWS_CONSOLE = "console"
DAK_UNKNOWN_SHOWS_DAK = "dak"
DAK_UNKNOWN_STATE_LINK = DAK_UNKNOWN_SHOWS_CONSOLE


def dak_links(
    project_id: Optional[str],
    location: Optional[str],
    session_id: Optional[str],
    html: bool,
    style: Optional[str] = None,
    info: Optional[IdeInfo] = None,
) -> DakLinks:
    """Decides which Data Agent Kit links to show for a session.

    Empty unless running under a VS Code-family editor. ``html`` selects
    links for rich notebook output; plain-text output gets only links that
    work as plain text, and never a call to action (``command:`` links only
    work in the editor's notebook output). Never raises.
    """
    try:
        if not (project_id and location and session_id):
            return DakLinks()
        if info is None:
            info = detect_ide()
        if info.family != VSCODE_FAMILY:
            return DakLinks()
        style = style or DAK_LINK_STYLE
        rendered_by_editor = is_rendered_by_editor(info)
        state = dak_state(info)
        show_dak = state == DAK_ENABLED or (
            state == DAK_UNKNOWN
            and DAK_UNKNOWN_STATE_LINK == DAK_UNKNOWN_SHOWS_DAK
        )
        session = None
        if show_dak:
            url = None
            if style == DAK_LINK_STYLE_RESOURCE:
                # Only resolvable inside the editor's notebook output.
                if html and rendered_by_editor:
                    url = _dak_resource_url(
                        info, project_id, location, session_id
                    )
            elif style == DAK_LINK_STYLE_URL_HANDLER:
                url = _dak_url_handler_url(
                    info, project_id, location, session_id
                )
            if url:
                session = (DAK_SESSION_LABEL, url)
        call_to_action = None
        if html and rendered_by_editor:
            call_to_action = _call_to_action(state)
        return DakLinks(session=session, call_to_action=call_to_action)
    except Exception:
        logger.debug("Building DAK links failed", exc_info=True)
        return DakLinks()


def dak_session_links(
    project_id: Optional[str],
    location: Optional[str],
    session_id: Optional[str],
    html: bool,
    style: Optional[str] = None,
    info: Optional[IdeInfo] = None,
) -> List[Tuple[str, str]]:
    """dak_links() as a list of (label, url): session link and/or CTA."""
    links = dak_links(project_id, location, session_id, html, style, info)
    result = [links.session] if links.session else []
    if links.call_to_action:
        result.append((links.call_to_action.label, links.call_to_action.url))
    return result


def _reset_cache_for_testing() -> None:
    detect_ide.cache_clear()
