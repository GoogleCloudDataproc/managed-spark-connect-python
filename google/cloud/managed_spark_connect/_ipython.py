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

from dataclasses import dataclass, field
import importlib.util
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from traitlets import Bool, default, observe
from traitlets.config import Config, Configurable

logger = logging.getLogger(__name__)

ENV_ENABLE_EXTRAS = "MANAGED_SPARK_CONNECT_ENABLE_EXTRAS"

_ENV_FALSE_VALUES = frozenset(("0", "false", "no", "off"))

# Module that must be installed for explore_dataframe() to be injected.
_COLABSQLVIZ_MODULE = "google.colabsqlviz"

# (extension module, ((magic kind, magic name), ...)). Each extension is only
# loaded if its module is installed.
_EXTRAS_EXTENSIONS = (
    (
        "google.cloud.managed_spark_magics",
        (("line", "dpip"), ("line", "sparksql"), ("cell", "sparksql")),
    ),
)


def _is_module_available(name: str) -> bool:
    """Whether `name` is installed, without importing it.

    All of the extras' dependencies are optional (they come from the
    [interactive] extra), so a module that isn't installed means that extra is
    deliberately absent, not broken. Anything other than a clean "not found" is
    reported as available so that the subsequent import surfaces the real error.
    """
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        # A parent package is missing.
        return False
    except Exception:
        return True


def _is_env_extras_enabled() -> bool:
    val = os.getenv(ENV_ENABLE_EXTRAS)
    if val is None or not val.strip():
        # Treat an empty/whitespace-only value the same as an unset variable,
        # so that `MANAGED_SPARK_CONNECT_ENABLE_EXTRAS=` is not silently
        # interpreted as "enabled".
        return True
    return val.strip().lower() not in _ENV_FALSE_VALUES


class ManagedSparkConnect(Configurable):
    """IPython configuration for google.cloud.managed_spark_connect extras."""

    # No static default_value: the dynamic `_default_enable_extras` below
    # always wins.
    enable_extras = Bool(
        help=(
            "Whether to automatically load the installed interactive extras "
            "(explore_dataframe, %dpip, %%sparksql)."
        ),
    ).tag(config=True)

    def __init__(self, shell: Any = None, **kwargs: Any) -> None:
        self._initialized = False
        self._shell = shell
        super().__init__(**kwargs)
        self._initialized = True

    @default("enable_extras")
    def _default_enable_extras(self) -> bool:
        return _is_env_extras_enabled()

    @observe("enable_extras")
    def _observe_enable_extras(self, change: Dict[str, Any]) -> None:
        # Traitlets applies file-based config inside `super().__init__()`, and
        # the resulting notification fires before the shell has been wired up.
        # Skip it: `_init_extras` loads the extras once construction is done.
        if not getattr(self, "_initialized", False):
            return
        shell = self._shell or self.parent
        if shell is None:
            return
        if change["new"]:
            _load_extras_for_shell(shell)
        else:
            _unload_extras_for_shell(shell)


@dataclass
class _ShellExtrasState:
    shell: Any
    injected_explore_dataframe: Any = None
    loaded_extensions: Set[str] = field(default_factory=set)
    # Per extension we loaded, the (kind, name) magics it added. Magics that
    # already existed under the same name were put back and are not listed.
    added_magics: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)
    config_instance: Optional[ManagedSparkConnect] = None


# Keyed by id() rather than being a WeakKeyDictionary. A weak key would not
# release anything here: the value reaches the key via
# `config_instance._shell` and traitlets' own `Configurable.parent`, and
# `InteractiveShell.__init__` hands a bound method to `atexit`, so a shell is
# retained for the life of the process regardless. `state.shell` is what makes
# id() keying safe, since CPython reuses addresses; see `_get_shell_state`.
_SHELL_STATES: Dict[int, _ShellExtrasState] = {}


def _get_shell_state(ip: Any) -> _ShellExtrasState:
    shell_id = id(ip)
    state = _SHELL_STATES.get(shell_id)
    if state is None or state.shell is not ip:
        state = _ShellExtrasState(shell=ip)
        _SHELL_STATES[shell_id] = state
    return state


def _is_same_class(obj: Any, cls: type) -> bool:
    """Compares by module+qualname so that reloaded classes still match."""
    obj_cls = type(obj)
    return (
        obj_cls.__module__ == cls.__module__
        and obj_cls.__qualname__ == cls.__qualname__
    )


def _get_or_create_config(ip: Any) -> ManagedSparkConnect:
    state = _get_shell_state(ip)
    ip_config = getattr(ip, "config", None)

    if state.config_instance is None:
        parent = ip if isinstance(ip, Configurable) else None
        config = (
            ip_config
            if (parent is None and isinstance(ip_config, Config))
            else None
        )
        # Precedence is left entirely to traitlets: an explicit
        # `c.ManagedSparkConnect.enable_extras` from the shell's config wins,
        # otherwise `_default_enable_extras` consults the environment.
        cfg = ManagedSparkConnect(shell=ip, parent=parent, config=config)
        state.config_instance = cfg
    else:
        cfg = state.config_instance
        cfg._shell = ip

    configurables = getattr(ip, "configurables", None)
    if isinstance(configurables, list):
        # Remove any stale ManagedSparkConnect instances (e.g. across module
        # reloads)
        configurables[:] = [
            c
            for c in configurables
            if c is cfg or not _is_same_class(c, ManagedSparkConnect)
        ]
        if cfg not in configurables:
            configurables.append(cfg)

    return cfg


def _import_explore_dataframe() -> Any:
    """Returns colabsqlviz's explore_dataframe(), or None if unavailable."""
    try:
        from google.colabsqlviz.explore_dataframe import explore_dataframe

        return explore_dataframe
    except Exception:
        logger.debug("Failed to import explore_dataframe", exc_info=True)
        return None


def _safe_repr(value: Any, max_len: int = 60) -> str:
    """repr() that tolerates objects whose __repr__ raises."""
    try:
        val_repr = repr(value)
    except Exception:
        return f"<{type(value).__name__} with a failing __repr__>"
    if len(val_repr) > max_len:
        return f"{val_repr[:max_len]}..."
    return val_repr


def _print_extras_failure() -> None:
    """Tells the user something went wrong, without diagnosing what.

    Every individual failure below is logged at debug level and otherwise
    swallowed, which leaves a broken install looking exactly like extras being
    switched off. One line, the same regardless of cause, is enough to point
    somebody at the logs.
    """
    print(
        "\033[94m⚠️  [google.cloud.managed_spark_connect]\033[0m"
        " Failed to load interactive extras."
        " For details, enable debug logging and restart the kernel."
    )


def _shell_magics(ip: Any) -> Dict[str, Dict[str, Any]]:
    magics_mgr = getattr(ip, "magics_manager", None)
    magics = getattr(magics_mgr, "magics", None)
    return magics if isinstance(magics, dict) else {}


def _present_magics(
    ip: Any, magics: Sequence[Tuple[str, str]]
) -> Dict[Tuple[str, str], Any]:
    """The subset of `magics` the shell already has, with their callables."""
    shell_magics = _shell_magics(ip)
    present = {}
    for kind, name in magics:
        kind_dict = shell_magics.get(kind)
        if isinstance(kind_dict, dict) and name in kind_dict:
            present[(kind, name)] = kind_dict[name]
    return present


def _is_from_module(fn: Any, module: str) -> bool:
    """Whether a registered magic was defined in `module` or a submodule."""
    mod = getattr(fn, "__module__", None)
    return isinstance(mod, str) and (
        mod == module or mod.startswith(module + ".")
    )


def _is_extension_loaded(
    ip: Any, ext_name: str, magics: Sequence[Tuple[str, str]]
) -> bool:
    mod = sys.modules.get(ext_name)
    if mod is not None and getattr(
        getattr(mod, "__spec__", None), "_initializing", False
    ):
        return True

    ext_mgr = getattr(ip, "extension_manager", None)
    loaded = getattr(ext_mgr, "loaded", None)
    if isinstance(loaded, (set, dict, list, tuple)) and ext_name in loaded:
        return True

    # Every magic it would add is already there, so it has nothing to offer.
    return len(_present_magics(ip, magics)) == len(magics)


def _print_extras_loaded(loaded: List[str]) -> None:
    """Lists the extras that were just loaded, in a single line.

    Which extras get loaded depends on what happens to be installed, which the
    user may not know (or may have forgotten), so name them explicitly rather
    than leaving them to be discovered.
    """
    names = ", ".join(f"\033[1m{name}\033[0m" for name in loaded)
    print(
        "\033[94m👉 [google.cloud.managed_spark_connect]\033[0m"
        f" Loaded interactive extras: {names}."
        " To disable: %config ManagedSparkConnect.enable_extras = False"
    )


def _magic_display_name(magic_kind: str, magic_name: str) -> str:
    prefix = "%%" if magic_kind == "cell" else "%"
    return f"{prefix}{magic_name}"


def _magics_display_names(magics: Sequence[Tuple[str, str]]) -> List[str]:
    """Display names, listing a magic that is both line and cell once."""
    return [
        _magic_display_name(kind, name)
        for kind, name in magics
        if not (kind == "line" and ("cell", name) in magics)
    ]


def _load_extras_for_shell(ip: Any) -> None:
    state = _get_shell_state(ip)
    target_name = "explore_dataframe"
    failed = False
    # Only what this call loaded; anything that was already present, whether
    # from us or the user, is not news.
    loaded: List[str] = []

    # 1. Inject explore_dataframe from google-colabsqlviz
    try:
        user_ns = getattr(ip, "user_ns", None)
        if not _is_module_available(_COLABSQLVIZ_MODULE):
            logger.debug(
                "Not injecting %s: %s is not installed",
                target_name,
                _COLABSQLVIZ_MODULE,
            )
        elif isinstance(user_ns, dict):
            if (
                state.injected_explore_dataframe is not None
                and user_ns.get(target_name) is state.injected_explore_dataframe
            ):
                pass
            elif target_name not in user_ns:
                from google.colabsqlviz.explore_dataframe import (
                    explore_dataframe,
                )

                ip.push({target_name: explore_dataframe})
                state.injected_explore_dataframe = explore_dataframe
                loaded.append(f"{target_name}()")
            else:
                current_val = user_ns[target_name]
                ours = _import_explore_dataframe()
                if ours is not None and current_val is ours:
                    # Already bound to the exact function we would have
                    # injected, e.g. because the user imported it themselves.
                    # Warning about that would be pure noise.
                    pass
                else:
                    val_repr = _safe_repr(current_val)
                    print(
                        "\033[94m⚠️  [google.cloud.managed_spark_connect]\033[0m"
                        f" Did not inject \033[1m{target_name}()\033[0m"
                        f" because it is already defined as: {val_repr}"
                    )
    except Exception:
        logger.debug(
            "Failed to inject %s into IPython user_ns",
            target_name,
            exc_info=True,
        )
        failed = True

    # 2. Load magic extensions
    ext_mgr = getattr(ip, "extension_manager", None)
    if ext_mgr is not None and hasattr(ext_mgr, "load_extension"):
        for ext_name, magics in _EXTRAS_EXTENSIONS:
            try:
                if ext_name in state.loaded_extensions or (
                    _is_extension_loaded(ip, ext_name, magics)
                ):
                    continue
                if not _is_module_available(ext_name):
                    logger.debug(
                        "Not loading IPython extension %s: not installed",
                        ext_name,
                    )
                    continue
                # A magic of the same name from somewhere else (e.g. the
                # sparksql-magic package) must survive our extension loading.
                existing = _present_magics(ip, magics)
                res = ext_mgr.load_extension(ext_name)
                if res is None:
                    shell_magics = _shell_magics(ip)
                    for (kind, name), fn in existing.items():
                        shell_magics.setdefault(kind, {})[name] = fn
                    added = [m for m in magics if m not in existing]
                    state.loaded_extensions.add(ext_name)
                    state.added_magics[ext_name] = added
                    loaded.extend(_magics_display_names(added))
            except Exception:
                logger.debug(
                    "Failed to load IPython extension %s",
                    ext_name,
                    exc_info=True,
                )
                failed = True

    if loaded:
        _print_extras_loaded(loaded)
    if failed:
        _print_extras_failure()


def _unload_extras_for_shell(ip: Any) -> None:
    state = _get_shell_state(ip)
    target_name = "explore_dataframe"

    # 1. Remove explore_dataframe only if we injected it and it wasn't overwritten
    try:
        if state.injected_explore_dataframe is not None:
            user_ns = getattr(ip, "user_ns", None)
            if (
                isinstance(user_ns, dict)
                and user_ns.get(target_name) is state.injected_explore_dataframe
            ):
                del user_ns[target_name]
            state.injected_explore_dataframe = None
    except Exception:
        logger.debug(
            "Failed to remove %s from IPython user_ns",
            target_name,
            exc_info=True,
        )

    # 2. Unload only the extensions that we loaded
    ext_mgr = getattr(ip, "extension_manager", None)
    loaded = getattr(ext_mgr, "loaded", None)

    for ext_name, magics in _EXTRAS_EXTENSIONS:
        if ext_name in state.loaded_extensions:
            added = state.added_magics.pop(ext_name, ())
            # Magics from this extension that were already present before we
            # loaded (e.g. from `%load_ext ...sparksql`) must survive unload.
            preserved = _present_magics(
                ip, [m for m in magics if m not in added]
            )
            try:
                if ext_mgr is not None and hasattr(ext_mgr, "unload_extension"):
                    ext_mgr.unload_extension(ext_name)
            except Exception:
                logger.debug(
                    "Failed to unload IPython extension %s",
                    ext_name,
                    exc_info=True,
                )
            try:
                shell_magics = _shell_magics(ip)
                for (kind, name), fn in preserved.items():
                    shell_magics.setdefault(kind, {})[name] = fn
                for kind, name in added:
                    kind_dict = shell_magics.get(kind)
                    if isinstance(kind_dict, dict) and _is_from_module(
                        kind_dict.get(name), ext_name
                    ):
                        del kind_dict[name]
                if isinstance(loaded, set):
                    loaded.discard(ext_name)
            except Exception:
                logger.debug(
                    "Failed to clean up magics for extension %s",
                    ext_name,
                    exc_info=True,
                )
            state.loaded_extensions.discard(ext_name)


def _init_extras() -> None:
    """Initialize interactive extras on package import unless opted out."""
    ip = None
    try:
        ip_mod = sys.modules.get("IPython")
        get_ipython = getattr(ip_mod, "get_ipython", None)
        if not callable(get_ipython):
            return

        ip = get_ipython()
        if ip is None:
            return

        # `cfg.enable_extras` already folds in both the environment variable
        # (via the trait's dynamic default) and any explicit configuration, and
        # it is the same value `%config` toggles later, so it is the only thing
        # that should be consulted here.
        cfg = _get_or_create_config(ip)
        if cfg.enable_extras:
            _load_extras_for_shell(ip)
    except Exception:
        logger.debug(
            "Failed to initialize IPython extras on import", exc_info=True
        )
        if ip is not None:
            # Only worth saying inside a shell: outside one there are no extras
            # to miss, and printing would be noise in ordinary scripts.
            _print_extras_failure()
