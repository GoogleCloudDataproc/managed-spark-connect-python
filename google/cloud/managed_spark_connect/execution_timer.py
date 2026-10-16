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

import logging
import threading
import time

logger = logging.getLogger(__name__)

_MIB = 1024 * 1024
_KIB = 1024

# How many of the slowest round trips to name individually in the log
# line before collapsing the rest into a "+N more" tail.
_MAX_ROUND_TRIPS_SHOWN = 3

_ZERO_TOTALS = {
    "session_creation_seconds": 0.0,
    "bytes_down": 0,
    "bytes_up": 0,
    "transport_blocked": 0.0,
}

_lock = threading.Lock()
_cell_start = None
_registered = False
_totals = dict(_ZERO_TOTALS)
# (kind, seconds) for every round trip to the Spark Connect endpoint in
# the current cell, where kind is "execute", "fetch", or "analyze".
# Bounded by one cell's worth of activity: start_cell() clears it, and
# nothing is appended at all outside IPython.
_round_trips = []


def start_cell():
    """Marks the start of a cell, discarding any prior measurements."""
    global _cell_start
    with _lock:
        _cell_start = time.monotonic()
        _totals.update(_ZERO_TOTALS)
        _round_trips.clear()


def record(kind, start, end):
    """Adds one Managed Spark round trip to the current cell.

    ``kind`` is one of ``"execute"``, ``"fetch"``, or ``"analyze"`` and
    is recorded verbatim alongside the duration; it is not validated,
    since every caller is this module's own package. Dropped when no
    cell is in progress, which keeps this inert and bounded outside
    IPython, where nothing ever resets the totals.
    """
    with _lock:
        if _cell_start is None or end <= _cell_start:
            return
        _round_trips.append((kind, end - max(start, _cell_start)))


def record_session_creation(seconds):
    """Adds cold session provisioning time to the current cell.

    Takes an already-computed duration rather than a start/end pair,
    since the caller measures it around a long provisioning loop.
    Dropped when no cell is in progress.
    """
    with _lock:
        if _cell_start is None:
            return
        _totals["session_creation_seconds"] += seconds


def record_transport(direction, nbytes, blocked):
    """Adds one websocket frame's transport accounting to the cell.

    Called from the bridge's hot path once per frame, so this stays
    cheap: one lock acquire and dict updates, nothing else. An
    unknown ``direction`` is ignored rather than raising. Dropped
    when no cell is in progress.
    """
    with _lock:
        if _cell_start is None:
            return
        if direction == "down":
            _totals["bytes_down"] += nbytes
        elif direction == "up":
            _totals["bytes_up"] += nbytes
        else:
            return
        _totals["transport_blocked"] += blocked


def summary():
    """Returns a dict of every accumulated total plus ``cell_seconds``.

    ``managed_spark_seconds`` is the sum of every round trip's
    duration plus ``session_creation_seconds``: provisioning a
    Managed Spark session is time spent in Managed Spark, same as
    any round trip. Both ``managed_spark_seconds`` and
    ``transport_blocked`` are clamped to at most ``cell_seconds``.
    When no cell is in progress, every value is zero and
    ``round_trips`` is empty.
    """
    with _lock:
        if _cell_start is None:
            result = dict(_ZERO_TOTALS)
            result["round_trips"] = []
            result["managed_spark_seconds"] = 0.0
            result["cell_seconds"] = 0.0
            return result
        cell_seconds = time.monotonic() - _cell_start
        result = dict(_totals)
        result["round_trips"] = list(_round_trips)
    managed_spark_seconds = (
        sum(seconds for _, seconds in result["round_trips"])
        + result["session_creation_seconds"]
    )
    result["managed_spark_seconds"] = min(managed_spark_seconds, cell_seconds)
    # The bridge forwards bytes on multiple daemon threads, and each
    # one accumulates its blocked time into this same counter, so
    # concurrent blocking double counts and can exceed the wall time
    # of the cell that contains it. Clamp for the same reason as
    # managed_spark_seconds above.
    result["transport_blocked"] = min(result["transport_blocked"], cell_seconds)
    result["cell_seconds"] = cell_seconds
    return result


def register_cell_timing():
    """Registers IPython cell hooks that log per-cell Managed Spark timing.

    Returns False (and registers nothing) outside of IPython, or if
    called more than once. Handlers never raise into the notebook.
    """
    global _registered

    if _registered:
        return False

    try:
        from IPython import get_ipython

        shell = get_ipython()
    except ImportError:
        return False

    if shell is None:
        return False

    shell.events.register("pre_run_cell", _on_pre_run_cell)
    shell.events.register("post_run_cell", _on_post_run_cell)
    _registered = True

    if _cell_start is None:
        # Registration happens mid-cell on the very first cell of a
        # notebook: getOrCreate() calls this before pre_run_cell has
        # ever fired, since no handler existed yet to fire it into.
        # Without starting the cell here, the session creation cost
        # about to be recorded would find no cell in progress and be
        # silently dropped. Known imprecision: cell_seconds for that
        # first cell is measured from registration rather than the
        # cell's true start, so it slightly under-reports the cell's
        # own wall time -- an acceptable trade for capturing the
        # session-creation cost, which is the dominant term.
        start_cell()

    return True


def _on_pre_run_cell(info=None):
    try:
        start_cell()
    except Exception:
        pass


def _format_round_trips_clause(round_trips):
    """Names up to the three slowest round trips, slowest first.

    Any remaining round trips are collapsed into a trailing "+N more"
    rather than named individually, so a cell with many small fetches
    doesn't turn the log line into a wall of text.
    """
    slowest_first = sorted(round_trips, key=lambda rt: rt[1], reverse=True)
    shown = slowest_first[:_MAX_ROUND_TRIPS_SHOWN]
    text = ", ".join("%s %.2fs" % (kind, seconds) for kind, seconds in shown)
    extra = len(slowest_first) - len(shown)
    if extra > 0:
        text += ", +%d more" % extra
    return text


def _format_detail_clauses(data):
    """Builds the parenthesised detail clauses for the cell log line.

    Each clause is omitted when its underlying value is empty/zero,
    so a cell with no session creation, round trips, or transport
    activity yields an empty list.
    """
    clauses = []

    if data["session_creation_seconds"]:
        clauses.append(
            "session creation %.2fs" % data["session_creation_seconds"]
        )

    if data["round_trips"]:
        clauses.append(_format_round_trips_clause(data["round_trips"]))

    bytes_down = data["bytes_down"]
    if bytes_down:
        blocked = data["transport_blocked"]
        if bytes_down >= _MIB:
            # Big enough for the rate to mean something.
            mib_down = bytes_down / _MIB
            rate = mib_down / blocked if blocked else 0.0
            clauses.append(
                "transport %.1f MiB down in %.2fs, %.1f MiB/s"
                % (mib_down, blocked, rate)
            )
        else:
            # A few KiB of control traffic would round to "0.0 MiB"
            # and "0.0 MiB/s", which reads as broken. Report the
            # smaller unit instead, and drop the rate: it would be
            # meaningless at this scale, while the blocked time
            # itself is still the useful signal (e.g. a long wait on
            # a cold cluster that moved almost no data).
            kib_down = bytes_down / _KIB
            clauses.append(
                "transport %.1f KiB down in %.2fs" % (kib_down, blocked)
            )

    return clauses


def _on_post_run_cell(result=None):
    try:
        data = summary()
        round_trips = data["round_trips"]
        # Session creation deliberately does not require any round
        # trips (it happens before the Spark session exists), so
        # without this a cell that only calls getOrCreate() would
        # still stay silent.
        if not round_trips and data["session_creation_seconds"] == 0:
            return
        message = "Cell took %.2fs, %.2fs in Managed Spark" % (
            data["cell_seconds"],
            data["managed_spark_seconds"],
        )
        if round_trips:
            count = len(round_trips)
            unit = "round trip" if count == 1 else "round trips"
            message += " across %d %s" % (count, unit)
        clauses = _format_detail_clauses(data)
        if clauses:
            message = "%s (%s)" % (message, "; ".join(clauses))
        logger.info(message)
    except Exception:
        pass
