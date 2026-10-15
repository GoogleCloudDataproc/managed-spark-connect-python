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

"""Implementation of the %sparksql / %%sparksql magic.

Everything here works with any pyspark SparkSession, classic or Spark Connect,
and only uses public pyspark APIs. pyspark and sqlparse are imported lazily so
that loading the extension (and with it %dpip) needs neither.

This module is structured as a self-contained IPython extension (with its own
SparkSqlMagics class and load/unload_ipython_extension entry points) so it can
be cleanly split out into a separate project later, leaving a thin re-export
shim at this module path.
"""

import dataclasses
import html
import re
import uuid
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
)

from IPython.core.error import UsageError
from IPython.core.magic import (
    Magics,
    line_cell_magic,
    magics_class,
    no_var_expand,
)
from traitlets import Bool, CaselessStrEnum, Int, TraitError, validate
from traitlets.config import Configurable

USAGE = """\
%%sparksql [-s SESSION] [-d DISPLAY] [-l LIMIT] [-v VIEW] [-c] [-e] [-q] [--explain [MODE]] [variable]
<SQL statements>

df = %sparksql [-s SESSION] [-v VIEW] [-c] [-e] [--explain [MODE]] <SQL statement>
"""

EXPLAIN_MODES = ("simple", "extended", "codegen", "cost", "formatted")
DISPLAY_MODES = ("colabsqlviz", "viz", "html")
DEFAULT_HTML_LIMIT = 20
DEFAULT_VARIABLE = "_df"

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_PLACEHOLDER_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)\Z")

# Rough width at which a value wraps in the HTML table, and is cut off in the
# plain-text one.
_MAX_TEXT_WIDTH = 40


class SparkSql(Configurable):
    """%config options for %%sparksql.

    Named SparkSql so that `%config SparkSql.limit = N` lines written for the
    third-party sparksql-magic package keep working.
    """

    display = CaselessStrEnum(
        DISPLAY_MODES,
        default_value="colabsqlviz",
        help=(
            "How %%sparksql displays results: 'colabsqlviz' (or 'viz') for an"
            " interactive table, or 'html' for a plain HTML table (-d"
            " overrides)."
        ),
    ).tag(config=True)
    limit = Int(
        None,
        allow_none=True,
        help=(
            "Maximum number of rows %%sparksql displays (-l overrides). By"
            " default, colabsqlviz paginates all rows and html shows"
            f" {DEFAULT_HTML_LIMIT}."
        ),
    ).tag(config=True)
    full_traceback = Bool(
        False,
        help=(
            "Show Spark's full exception and traceback for failed queries"
            " instead of a short summary."
        ),
    ).tag(config=True)

    @validate("display")
    def _validate_display(self, proposal: Mapping[str, Any]) -> str:
        val = proposal["value"]
        return "colabsqlviz" if val == "viz" else val

    @validate("limit")
    def _validate_limit(self, proposal: Mapping[str, Any]) -> Optional[int]:
        # Same rule as -l, which parse_options enforces.
        val = proposal["value"]
        if val is not None and val < 1:
            raise TraitError("SparkSql.limit must be at least 1, or None")
        return val


def _is_sparksql_config(obj: Any) -> bool:
    """Compares by module+qualname so that a reloaded SparkSql still matches."""
    cls = type(obj)
    return (
        cls.__module__ == SparkSql.__module__
        and cls.__qualname__ == SparkSql.__qualname__
    )


def get_config(shell: Any) -> SparkSql:
    """Returns the shell's SparkSql config, registering one if needed.

    Registering it in shell.configurables is what makes `%config SparkSql.x`
    take effect immediately rather than only for configurables created later.
    """
    for c in getattr(shell, "configurables", ()):
        if _is_sparksql_config(c):
            return c
    # Without a Configurable parent there is no shell config to inherit. An
    # invalid value in the shell's config still raises TraitError.
    cfg = SparkSql(parent=shell if isinstance(shell, Configurable) else None)
    configurables = getattr(shell, "configurables", None)
    if isinstance(configurables, list):
        configurables.append(cfg)
    return cfg


# ---------------------------------------------------------------------------
# Option parsing


@dataclasses.dataclass
class Options:
    session: Optional[str] = None
    display: Optional[str] = None
    limit: Optional[int] = None
    view: Optional[str] = None
    cache: bool = False
    eager: bool = False
    quiet: bool = False
    explain: Optional[str] = None
    variable: Optional[str] = None


_VALUE_OPTS = {
    "-s": "session",
    "--session": "session",
    "-d": "display",
    "--display": "display",
    "-l": "limit",
    "--limit": "limit",
    "-v": "view",
    "--view": "view",
}
_FLAG_OPTS = {
    "-c": "cache",
    "--cache": "cache",
    "-e": "eager",
    "--eager": "eager",
    "-q": "quiet",
    "--quiet": "quiet",
}
_SHORT_FLAGS = {k[1]: v for k, v in _FLAG_OPTS.items() if len(k) == 2}


def _usage_error(msg: str) -> UsageError:
    return UsageError(f"{msg}\n\nUsage:\n{USAGE}")


def parse_options(line: str, line_form: bool) -> Tuple[Options, str]:
    """Parses magic options.

    For the cell form, `line` holds only options and an optional variable name
    (defaulting to `_df`), and the returned rest is always empty. For the line
    form, options end at the first token that is not an option, and everything
    from there on is returned verbatim as the SQL.

    This is hand-rolled rather than built on argparse or
    IPython.core.magic_arguments for two reasons:

    - The line form's SQL has to come back exactly as written. Using those
      means first splitting the line with shlex (as magic_arguments does),
      which strips quotes (`'it''s'` comes back as `its`) and loses the
      spacing and character offsets needed to slice the SQL out of `line`
      verbatim.
    - `--explain [MODE]` takes an optional value. With argparse's
      `nargs='?'` it would greedily take the next token, i.e. the
      `[variable]` name or the first word of the SQL, as the mode. Here the
      next token is only taken if it is an explain mode.
    """
    opts = Options()
    tokens = [(m.group(), m.start()) for m in re.finditer(r"\S+", line)]
    i = 0
    rest = ""

    def take_value(flag: str) -> str:
        nonlocal i
        if i + 1 >= len(tokens):
            raise _usage_error(f"{flag} needs a value")
        i += 1
        return tokens[i][0]

    while i < len(tokens):
        tok, pos = tokens[i]
        if not tok.startswith("-") or tok == "-":
            if line_form:
                rest = line[pos:]
                break
            if opts.variable is not None:
                raise _usage_error(f"Unexpected argument: {tok}")
            opts.variable = tok
            i += 1
            continue

        flag, eq, inline_value = tok.partition("=")
        if flag == "--explain":
            if eq:
                mode = inline_value
            elif (
                i + 1 < len(tokens)
                and tokens[i + 1][0].lower() in EXPLAIN_MODES
            ):
                i += 1
                mode = tokens[i][0]
            else:
                mode = "formatted"
            if mode.lower() not in EXPLAIN_MODES:
                raise _usage_error(
                    f"Unknown explain mode {mode!r}; expected one of"
                    f" {', '.join(EXPLAIN_MODES)}"
                )
            opts.explain = mode.lower()
        elif flag in _VALUE_OPTS:
            if eq and not inline_value:
                raise _usage_error(f"{flag} needs a value")
            value = inline_value if eq else take_value(flag)
            setattr(opts, _VALUE_OPTS[flag], value)
        elif flag in _FLAG_OPTS and not eq:
            setattr(opts, _FLAG_OPTS[flag], True)
        elif (
            not flag.startswith("--")
            and len(flag) > 2
            and flag[1] in "sdlv"
            and not eq
        ):
            # Attached short value, e.g. -l50.
            setattr(opts, _VALUE_OPTS[flag[:2]], flag[2:])
        elif (
            not flag.startswith("--")
            and not eq
            and all(ch in _SHORT_FLAGS for ch in flag[1:])
        ):
            # Combined short flags, e.g. -ce.
            for ch in flag[1:]:
                setattr(opts, _SHORT_FLAGS[ch], True)
        else:
            raise _usage_error(f"Unknown option: {tok}")
        i += 1

    if opts.display is not None:
        mode = opts.display.lower()
        if mode not in DISPLAY_MODES:
            raise _usage_error(
                f"Unknown display mode {opts.display!r}; expected one of"
                f" {', '.join(DISPLAY_MODES)}"
            )
        opts.display = "colabsqlviz" if mode == "viz" else mode
    if opts.limit is not None:
        try:
            limit = int(opts.limit)
        except ValueError:
            raise _usage_error(
                f"--limit must be an integer, got {opts.limit!r}"
            )
        if limit < 1:
            raise _usage_error("--limit must be at least 1")
        opts.limit = limit
    for attr in ("session", "variable"):
        value = getattr(opts, attr)
        if value is not None and not _IDENT_RE.match(value):
            raise _usage_error(f"Not a valid Python identifier: {value!r}")
    if opts.view is not None and not _IDENT_RE.match(opts.view):
        # Temporary views are unqualified; catching a bad name here beats a
        # Spark error after the query has already run.
        raise _usage_error(f"Not a valid temporary view name: {opts.view!r}")
    if line_form:
        if opts.display is not None or opts.limit is not None:
            # The line form never displays anything, so these would be
            # silently ignored.
            raise _usage_error(
                "-d and -l only apply to the cell form (%%sparksql)"
            )
        if not rest.strip():
            raise _usage_error("No SQL given")
        opts.quiet = True
    elif opts.variable is None:
        opts.variable = DEFAULT_VARIABLE
    return opts, rest


# ---------------------------------------------------------------------------
# Statement splitting and parameter binding


def _import_sqlparse() -> Any:
    try:
        import sqlparse
    except ImportError:
        raise UsageError(
            "%%sparksql needs the sqlparse package. Install it with:\n"
            "  pip install 'google-cloud-spark-connect[interactive]'"
        ) from None
    return sqlparse


@dataclasses.dataclass
class Statement:
    """One SQL statement from a cell, plus where it came from."""

    text: str
    # Offset of `text` within the cell, for mapping error positions back.
    offset: int
    # Flattened sqlparse tokens making up exactly `text`, as (ttype, value).
    tokens: List[Tuple[Any, str]]


def _is_code(token: Tuple[Any, str]) -> bool:
    """Whether a token is significant: not whitespace, a comment or `;`."""
    from sqlparse import tokens as T

    ttype, value = token
    return (
        value.strip() not in ("", ";")
        and ttype not in T.Comment
        and ttype not in T.Text.Whitespace
    )


def _make_statement(
    flat: List[Tuple[Any, str]], start: int
) -> Optional[Statement]:
    """Trims leading whitespace and everything after the last code token."""
    code = [i for i, tok in enumerate(flat) if _is_code(tok)]
    if not code:
        return None
    first = next(i for i, (_, v) in enumerate(flat) if v.strip())
    kept = flat[first : code[-1] + 1]
    offset = start + sum(len(v) for _, v in flat[:first])
    return Statement("".join(v for _, v in kept), offset, kept)


def split_statements(sql: str) -> List[Statement]:
    """Splits a cell into statements, keeping each one's offset in the cell.

    A cell whose first keyword is BEGIN is a SQL script (a single compound
    statement), and is never split.
    """
    sqlparse = _import_sqlparse()

    parsed = []
    offset = 0
    for stmt in sqlparse.parse(sql):
        raw = str(stmt)
        parsed.append((offset, [(t.ttype, t.value) for t in stmt.flatten()]))
        offset += len(raw)

    all_tokens = [t for _, flat in parsed for t in flat]
    first_code = next((v for tt, v in all_tokens if _is_code((tt, v))), None)
    if first_code is None:
        return []
    if first_code.upper() == "BEGIN":
        return [_make_statement(all_tokens, 0)]
    statements = (_make_statement(flat, start) for start, flat in parsed)
    return [s for s in statements if s is not None]


def _brace_ident(
    toks: Sequence[Tuple[Any, str]], i: int
) -> Optional[Tuple[int, str]]:
    """If `toks[i]` opens a `{name}` reference, returns (closing_index, name).

    Matches on token values alone, not types: sqlparse has no rule for braces
    and emits each as a one-character Error token, but that is incidental,
    and its lexer is process-wide and extensible. A brace inside a string
    literal, quoted name or comment is part of that longer token, so it never
    makes up a token's whole value.
    """
    if toks[i][1] != "{":
        return None
    j = i + 1
    while j < len(toks) and toks[j][1] not in ("{", "}"):
        j += 1
    if j < len(toks) and toks[j][1] == "}":
        inner = "".join(v for _, v in toks[i + 1 : j])
        if _IDENT_RE.match(inner):
            return j, inner
    return None


def find_parameters(statement: Statement) -> Tuple[List[str], List[str]]:
    """Returns the (`:name` marker names, `{name}` DataFrame names) in a statement.

    Both are only recognized outside string literals and comments.
    """
    from sqlparse import tokens as T

    markers: List[str] = []
    fields: List[str] = []
    toks = statement.tokens
    i = 0
    while i < len(toks):
        ttype, value = toks[i]
        if ttype in T.Name.Placeholder:
            m = _PLACEHOLDER_RE.match(value)
            if m and m.group(1) not in markers:
                markers.append(m.group(1))
        else:
            brace = _brace_ident(toks, i)
            if brace is not None:
                j, name = brace
                if name not in fields:
                    fields.append(name)
                i = j
        i += 1
    return markers, fields


def escape_braces(statement: Statement) -> str:
    """Returns the statement text with braces escaped for str.format().

    Spark formats the whole query with str.format() when it is given keyword
    arguments, so every brace that is not part of a `{name}` reference, such
    as one inside a string literal or a non-identifier `{...}`, has to be
    doubled first.
    """
    out = []
    toks = statement.tokens
    i = 0
    while i < len(toks):
        brace = _brace_ident(toks, i)
        if brace is not None:
            j, name = brace
            out.append(f"{{{name}}}")
            i = j + 1
        else:
            _, value = toks[i]
            out.append(value.replace("{", "{{").replace("}", "}}"))
            i += 1
    return "".join(out)


def _dataframe_classes() -> Tuple[type, ...]:
    classes = []
    try:
        from pyspark.sql import DataFrame

        classes.append(DataFrame)
    except ImportError:
        pass
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        if ConnectDataFrame not in classes:
            classes.append(ConnectDataFrame)
    except ImportError:
        pass
    return tuple(classes)


def _type_name(val: Any) -> str:
    cls = type(val)
    if cls.__name__ == "DataFrame":
        return f"{cls.__module__.split('.')[0]}.DataFrame"
    return cls.__name__


def bind_parameters(
    statement: Statement, user_ns: Mapping[str, Any]
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Returns (sql, args, kwargs) to pass to spark.sql() for a statement."""
    markers, fields = find_parameters(statement)
    missing = [n for n in markers + fields if n not in user_ns]
    if missing:
        names = ", ".join(dict.fromkeys(missing))
        raise UsageError(
            f"Not defined in the notebook: {names}. SQL parameters (:name) and"
            " DataFrame references ({name}) are bound from notebook variables."
        )
    df_classes = _dataframe_classes() if (markers or fields) else ()
    for n in markers:
        if isinstance(user_ns[n], df_classes):
            raise UsageError(
                f":{n}: expected a SQL value, got a Spark DataFrame."
                f" Use {{{n}}} to query a DataFrame."
            )
    for n in fields:
        val = user_ns[n]
        if not isinstance(val, df_classes):
            raise UsageError(
                f"{{{n}}}: expected a Spark DataFrame, got {_type_name(val)}."
                f" Use :{n} for SQL values, or IDENTIFIER(:{n}) for table and"
                " column names."
            )
    args = {n: user_ns[n] for n in markers}
    kwargs = {n: user_ns[n] for n in fields}
    sql = escape_braces(statement) if kwargs else statement.text
    return sql, args, kwargs


# ---------------------------------------------------------------------------
# Session resolution


def _session_classes() -> Tuple[type, ...]:
    classes = []
    try:
        from pyspark.sql import SparkSession

        classes.append(SparkSession)
    except ImportError:
        pass
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession

        classes.append(ConnectSession)
    except ImportError:
        pass
    return tuple(classes)


def _active_session() -> Any:
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession

        return ConnectSession.active()
    except Exception:
        pass
    try:
        from pyspark.sql import SparkSession

        return SparkSession.active()
    except Exception:
        return None


def resolve_session(user_ns: Mapping[str, Any], name: Optional[str]) -> Any:
    """Finds the SparkSession to run against.

    In order: the -s variable, the active session, and finally the only
    SparkSession in the notebook namespace.
    """
    classes = _session_classes()
    if not classes:
        raise UsageError("%%sparksql needs pyspark, which is not installed.")
    if name is not None:
        if name not in user_ns:
            raise UsageError(f"-s {name}: no such variable")
        session = user_ns[name]
        if not isinstance(session, classes):
            raise UsageError(
                f"-s {name}: expected a SparkSession, got"
                f" {type(session).__name__}"
            )
        return session

    session = _active_session()
    if session is not None:
        return session

    found = [
        k
        for k, v in user_ns.items()
        if not k.startswith("_") and isinstance(v, classes)
    ]
    if len(found) == 1:
        return user_ns[found[0]]
    if found:
        raise UsageError(
            f"Found several SparkSessions ({', '.join(sorted(found))}) and"
            " none is active; pick one with -s NAME."
        )
    raise UsageError(
        "No SparkSession found. Create one first (for example with"
        " ManagedSparkSession.builder.getOrCreate()), or pass -s NAME."
    )


# ---------------------------------------------------------------------------
# Errors


class SparkSqlError(Exception):
    """A failed SQL statement, rendered without the Python traceback.

    The traceback would only show the magic's and pyspark's internals; the
    error message and the position in the SQL are what matter. The original
    exception is kept as __cause__.

    Not a subclass of managed_spark_connect's exception type: importing that
    package pulls in pyspark, which loading this extension must not require.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def _render_traceback_(self) -> List[str]:
        return self.message.splitlines()


def _condition(exc: Any) -> Optional[str]:
    # getErrorClass() is the pre-4.0 name, deprecated since.
    fn = getattr(exc, "getCondition", None) or getattr(
        exc, "getErrorClass", None
    )
    if fn is None:
        return None
    try:
        return fn() or None
    except Exception:
        return None


def _sql_context(exc: Any, sql: str) -> Optional[Tuple[int, int]]:
    """Returns the (start, stop) offsets in `sql` Spark blamed, if it said.

    A context only counts if its fragment is the text at those offsets in
    `sql`: an error raised inside a view, for example, carries offsets into
    the view's own SQL, which would put the caret somewhere unrelated.
    """
    try:
        contexts = exc.getQueryContext()
    except Exception:
        return None
    from pyspark.errors import QueryContextType

    for ctx in contexts or ():
        try:
            if ctx.contextType() != QueryContextType.SQL:
                continue
            start, stop = ctx.startIndex(), ctx.stopIndex()
            get_fragment = getattr(ctx, "fragment", None)
            fragment = get_fragment() if get_fragment is not None else None
        except Exception:
            continue
        if start is None or stop is None or not 0 <= start <= stop:
            continue
        if fragment and sql[start : stop + 1] != fragment:
            continue
        return start, stop
    return None


# The position Spark writes into parse errors, which have no query context:
# "Syntax error at or near 'SELEC'. SQLSTATE: 42601 (line 1, pos 0)".
_PARSE_POSITION_RE = re.compile(r"\(line (\d+), pos (\d+)\)")


def _parse_position(exc: Any, sql: str) -> Optional[Tuple[int, int]]:
    """Returns the (start, stop) of the token a parse error points at."""
    m = _PARSE_POSITION_RE.search(str(exc))
    if not m:
        return None
    line, pos = int(m.group(1)), int(m.group(2))
    start = 0
    for _ in range(line - 1):
        start = sql.find("\n", start) + 1
        if start == 0:
            return None
    start += pos
    if start >= len(sql):
        return None
    token = re.match(r"\w+|\S", sql[start:])
    return start, start + (len(token.group()) if token else 1) - 1


# Where Spark's own message ends and the extras it appends begin: the JVM
# stack trace (classic, and Connect's str()), an "== SQL ==" excerpt, and on
# Connect the analyzed plan after "; line 2 pos 2;" or a parse error's
# " (line 1, pos 0)".
_MESSAGE_END_RE = re.compile(
    r"\n\s*\n|\n== SQL|\nJVM stacktrace:|;\s*line \d+ pos \d+;?"
    r"| \(line \d+, pos \d+\)"
)


def _first_paragraph(exc: Any, condition: Optional[str]) -> str:
    """Spark's message, without the context and JVM stack it appends."""
    text = None
    get_message = getattr(exc, "getMessage", None)
    if get_message is not None:
        try:
            text = get_message()
        except Exception:
            text = None
    if not text:
        text = str(exc)
    text = _MESSAGE_END_RE.split(text.strip(), maxsplit=1)[0].rstrip()
    if condition and not text.startswith("["):
        text = f"[{condition}] {text}"
    return text


def format_error(
    exc: Any,
    cell: str,
    statement: Statement,
    index: int,
    count: int,
    offsets_match: bool,
) -> str:
    """Formats a Spark SQL error from `statement` for display."""
    condition = _condition(exc)
    lines = [_first_paragraph(exc, condition)]
    sqlstate = None
    get_state = getattr(exc, "getSqlState", None)
    if get_state is not None:
        try:
            sqlstate = get_state()
        except Exception:
            sqlstate = None
    if sqlstate and f"SQLSTATE: {sqlstate}" not in lines[0]:
        lines[0] += f" SQLSTATE: {sqlstate}"

    where = []
    if count > 1:
        where.append(f"statement {index + 1} of {count}")
        if index > 0:
            where.append("earlier statements already ran")
    span = None
    if offsets_match:
        span = _sql_context(exc, statement.text) or _parse_position(
            exc, statement.text
        )
    if span is not None and span[0] < len(statement.text):
        start = statement.offset + span[0]
        stop = statement.offset + min(span[1], len(statement.text) - 1)
        line_start = cell.rfind("\n", 0, start) + 1
        line_end = cell.find("\n", start)
        if line_end < 0:
            line_end = len(cell)
        line_no = cell.count("\n", 0, start) + 1
        where.insert(0, f"line {line_no}")
        src = cell[line_start:line_end]
        width = max(1, min(stop, line_end - 1) - start + 1)
        caret = " " * (start - line_start) + "^" * width
        excerpt = [f"  {src.rstrip()}", f"  {caret}"]
    else:
        excerpt = []
    if where:
        lines.append(f"({'; '.join(where)})")
    lines.extend(excerpt)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Rendering

_NUMERIC_TYPES = (
    "ByteType",
    "ShortType",
    "IntegerType",
    "LongType",
    "FloatType",
    "DoubleType",
    "DecimalType",
)


@dataclasses.dataclass
class Table:
    names: List[str]
    types: List[str]
    numeric: List[bool]
    rows: List[List[Optional[str]]]
    truncated: bool


def fetch_table(df: Any, limit: int) -> Table:
    """Fetches up to `limit` rows, formatted to strings by Spark.

    Formatting server-side with CAST(... AS STRING) means values look the way
    Spark itself prints them (timestamps in the session time zone, nested
    types, decimals), rather than however they convert to Python. Fetching one
    extra row tells us whether there are more without a separate count.
    """
    # SQL expressions rather than pyspark.sql.functions: which implementation
    # those dispatch to, classic or Connect, depends on process-wide state
    # rather than on `df`.
    fields = df.schema.fields
    # Positional names sidestep duplicate or dotted column names.
    tmp = [f"_c{i}" for i in range(len(fields))]
    exprs = []
    for name, field in zip(tmp, fields):
        expr = name
        if field.dataType.typeName() == "binary":
            # Same "[62 69 6E]" form df.show() uses.
            expr = (
                f"concat('[', regexp_replace(hex({name}), '(..)(?=.)', '$1 '),"
                " ']')"
            )
        exprs.append(f"CAST({expr} AS STRING) AS {name}")
    rows = df.toDF(*tmp).selectExpr(*exprs).limit(limit + 1).collect()
    return Table(
        names=[f.name for f in fields],
        types=[f.dataType.simpleString() for f in fields],
        numeric=[type(f.dataType).__name__ in _NUMERIC_TYPES for f in fields],
        rows=[list(r) for r in rows[:limit]],
        truncated=len(rows) > limit,
    )


def _footer(table: Table) -> str:
    n = len(table.rows)
    if table.truncated:
        return f"Showing the first {n} rows; more are available (-l to change)."
    return f"{n} row{'' if n == 1 else 's'}"


def render_html(table: Table) -> str:
    cls = f"sparksql-{uuid.uuid4().hex[:8]}"
    out = [
        f'<div class="{cls}"><style>',
        f".{cls} table {{border-collapse:collapse}}",
        f".{cls} th, .{cls} td {{text-align:left;vertical-align:top;"
        f"white-space:pre-wrap;max-width:{_MAX_TEXT_WIDTH}ch;"
        "overflow-wrap:anywhere}",
        f".{cls} th .t {{display:block;font-weight:normal;opacity:.6;"
        "font-family:monospace}",
        f".{cls} td.n {{text-align:right;font-variant-numeric:tabular-nums}}",
        f".{cls} td.null {{opacity:.45;font-style:italic}}",
        f".{cls} .foot {{font-size:smaller;opacity:.7;margin-top:4px}}",
        "</style><table><thead><tr>",
    ]
    for name, typ in zip(table.names, table.types):
        out.append(
            f"<th>{html.escape(name)}"
            f'<span class="t">{html.escape(typ)}</span></th>'
        )
    out.append("</tr></thead><tbody>")
    for row in table.rows:
        out.append("<tr>")
        for value, numeric in zip(row, table.numeric):
            classes = []
            if numeric:
                classes.append("n")
            if value is None:
                classes.append("null")
                value = "NULL"
            attr = f' class="{" ".join(classes)}"' if classes else ""
            out.append(f"<td{attr}>{html.escape(value)}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    out.append(f'<div class="foot">{html.escape(_footer(table))}</div></div>')
    return "".join(out)


def _cut(value: str) -> str:
    value = value.replace("\n", "\\n")
    if len(value) > _MAX_TEXT_WIDTH:
        return value[: _MAX_TEXT_WIDTH - 3] + "..."
    return value


def render_text(table: Table) -> str:
    """Renders the table the way df.show() would, for terminals."""
    cells = [
        [_cut("NULL" if v is None else v) for v in row] for row in table.rows
    ]
    widths = [
        max([len(name)] + [len(row[i]) for row in cells])
        for i, name in enumerate(table.names)
    ]
    sep = "+" + "+".join("-" * w for w in widths) + "+"

    def fmt(values: Sequence[str]) -> str:
        return "|" + "|".join(v.rjust(w) for v, w in zip(values, widths)) + "|"

    lines = [sep, fmt(table.names), sep]
    lines.extend(fmt(row) for row in cells)
    lines.append(sep)
    lines.append(_footer(table))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Running


def run(shell: Any, line: str, cell: Optional[str]) -> Any:
    """Entry point for both forms of the magic."""
    line_form = cell is None
    opts, rest = parse_options(line, line_form)
    sql = rest if line_form else cell

    statements = split_statements(sql)
    if not statements:
        raise _usage_error("No SQL given")

    config = get_config(shell)
    user_ns = shell.user_ns
    session = resolve_session(user_ns, opts.session)

    # Bind every statement before running any, so that a missing variable in a
    # later statement fails the cell before earlier ones have side effects.
    bound = [bind_parameters(s, user_ns) for s in statements]
    df = None
    for index, (statement, (bound_sql, args, kwargs)) in enumerate(
        zip(statements, bound)
    ):
        try:
            df = session.sql(bound_sql, args=args or None, **kwargs)
        except Exception as e:
            _reraise(e, config, sql, statement, index, len(statements), kwargs)
    # Errors from here on, e.g. at collect time, still carry positions in the
    # last statement's SQL as Spark saw it.
    last_kwargs = bound[-1][2]

    try:
        if opts.eager:
            # The checkpoint is already materialized and stored, so caching
            # it as well (-c -e) would only keep a second copy.
            df = df.localCheckpoint(eager=True)
        elif opts.cache:
            df = df.cache()
        if opts.view is not None:
            df.createOrReplaceTempView(opts.view)
        if opts.explain is not None:
            df.explain(mode=opts.explain)
        elif not opts.quiet:
            limit = opts.limit if opts.limit is not None else config.limit
            display_mode = opts.display or config.display
            _display(
                df,
                limit=limit,
                display_mode=display_mode,
                explicit_display=opts.display is not None,
            )
        # Only once everything has succeeded, so that a failure does not
        # leave the variable bound to a DataFrame that does not work.
        if opts.variable is not None:
            user_ns[opts.variable] = df
    except Exception as e:
        _reraise(
            e,
            config,
            sql,
            statements[-1],
            len(statements) - 1,
            len(statements),
            last_kwargs,
        )

    return df if line_form else None


def _reraise(
    exc: Exception,
    config: SparkSql,
    cell: str,
    statement: Statement,
    index: int,
    count: int,
    kwargs: Mapping[str, Any],
) -> NoReturn:
    try:
        from pyspark.errors import PySparkException
    except ImportError:
        raise exc
    if not isinstance(exc, PySparkException):
        raise exc
    if config.full_traceback:
        if count > 1:
            add_note = getattr(exc, "add_note", None)
            if add_note is not None:
                add_note(f"In %%sparksql statement {index + 1} of {count}")
        raise exc
    # With {name} references Spark rewrites the SQL before parsing it, so its
    # error positions no longer line up with the cell.
    message = format_error(
        exc, cell, statement, index, count, offsets_match=not kwargs
    )
    raise SparkSqlError(message) from exc


def _import_explore_dataframe() -> Any:
    try:
        from google.colabsqlviz.explore_dataframe import explore_dataframe

        return explore_dataframe
    except ImportError:
        return None


def _display(
    df: Any,
    limit: Optional[int] = None,
    display_mode: str = "colabsqlviz",
    explicit_display: bool = False,
) -> None:
    from IPython.display import display

    if not df.schema.fields:
        # DDL and most DML: there is nothing to show but that it worked.
        display({"text/plain": "OK"}, raw=True)
        return
    if display_mode in ("colabsqlviz", "viz"):
        explore_dataframe = _import_explore_dataframe()
        if explore_dataframe is not None:
            if limit is not None:
                df = df.limit(limit)
            # Relies on explore_dataframe() only returning the widget, which
            # it does while colabsqlviz's anywidget_enabled experiment is on
            # (the default in 0.4.0). With it off, explore_dataframe() also
            # displays the widget itself, and the table would render twice.
            display(explore_dataframe(df, chart_type="table"))
            return
        if explicit_display:
            raise UsageError(
                "%%sparksql -d colabsqlviz needs the google-colabsqlviz"
                " package. Install it with:\n"
                "  pip install 'google-cloud-spark-connect[interactive]'"
            )
    table = fetch_table(df, limit if limit is not None else DEFAULT_HTML_LIMIT)
    display(
        {"text/html": render_html(table), "text/plain": render_text(table)},
        raw=True,
    )


# ---------------------------------------------------------------------------
# IPython extension
#
# Kept in this module (rather than bundled into ManagedSparkMagics) so that
# %%sparksql can be cleanly split out into a standalone project later.


@magics_class
class SparkSqlMagics(Magics):

    def __init__(self, shell: Any, **kwargs: Any) -> None:
        super().__init__(shell, **kwargs)
        # Registered up front so `%config SparkSql.limit = N` works before the
        # first %%sparksql cell runs.
        get_config(shell)

    @no_var_expand
    @line_cell_magic
    def sparksql(self, line: str, cell: Optional[str] = None) -> Any:
        """Runs Spark SQL against the active SparkSession.

        Cell form: displays the result of the last statement and stores the
        DataFrame in `variable` (default: `_df`).

            %%sparksql [-s SESSION] [-d DISPLAY] [-l LIMIT] [-v VIEW] [-c] [-e]
                       [-q] [--explain [MODE]] [variable]
            SELECT * FROM t WHERE x = :x

        Line form: always --quiet; returns the DataFrame directly.

            df = %sparksql [-s SESSION] [-v VIEW] [-c] [-e]
                           [--explain [MODE]] SELECT * FROM {other_df}

        Options:
          variable          Store the un-truncated DataFrame in this variable
                            (cell form only; default: _df; -l only limits
                            displayed rows).
          -s, --session N   Use the SparkSession in variable N. Default: the
                            active session, else the only one in the notebook.
          -d, --display M   How to display results: colabsqlviz (or viz) for an
                            interactive table, or html for a plain HTML table
                            (default: %config SparkSql.display, initially
                            colabsqlviz; falls back to html if
                            google-colabsqlviz is not installed).
          -l, --limit N     Maximum rows to display (default: %config
                            SparkSql.limit; when unset, colabsqlviz paginates
                            all rows and html shows 20).
          -v, --view V      Also register the result as temp view V.
          -c, --cache       Cache the result (lazily). Redundant with -e.
          -e, --eager       Materialize the result now, with
                            localCheckpoint(eager=True).
          -q, --quiet       Don't display anything.
          --explain [MODE]  Print the query plan instead of results. MODE is
                            simple, extended, codegen, cost or formatted
                            (default).

        Python values are bound from notebook variables:
          :name             A typed SQL literal (e.g. WHERE id = :id). For a
                            table or column name, use IDENTIFIER(:name).
          {name}            A Spark DataFrame to query as a table (e.g.
                            SELECT * FROM {df}).
          Literals and comments: :name and {name} inside string literals and
                            comments are preserved verbatim.

        A cell can hold several statements separated by ';'. They run in
        order, and only the last one's result is displayed or stored. A cell
        starting with BEGIN is run whole, as a SQL script.
        """
        return run(self.shell, line, cell)


def load_ipython_extension(ipython: Any) -> None:
    ipython.register_magics(SparkSqlMagics)


def unload_ipython_extension(ipython: Any) -> None:
    magics_mgr = getattr(ipython, "magics_manager", None)
    magics = getattr(magics_mgr, "magics", None)
    if not isinstance(magics, dict):
        return
    for kind in ("line", "cell"):
        kind_magics = magics.get(kind)
        if not isinstance(kind_magics, dict):
            continue
        # Leave alone a magic of the same name that someone else
        # registered, e.g. %%sparksql from the sparksql-magic package.
        owner = getattr(kind_magics.get("sparksql"), "__self__", None)
        if isinstance(owner, SparkSqlMagics):
            del kind_magics["sparksql"]
