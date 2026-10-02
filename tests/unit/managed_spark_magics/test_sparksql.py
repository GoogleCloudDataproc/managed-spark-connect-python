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

"""Tests for %%sparksql that need neither a JVM nor a Spark server.

End-to-end behaviour against real classic and Connect sessions is covered by
test_sparksql_local.py.
"""

import unittest
from unittest import mock

from IPython.core.error import UsageError
from IPython.core.interactiveshell import InteractiveShell
from pyspark.errors import PySparkException, QueryContextType
from traitlets import TraitError
from traitlets.config import Config, Configurable

from google.cloud.managed_spark_magics import sparksql


class ParseOptionsTest(unittest.TestCase):

    def test_cell_defaults(self):
        opts, rest = sparksql.parse_options("", line_form=False)
        self.assertEqual(opts, sparksql.Options())
        self.assertEqual(rest, "")

    def test_cell_all_options(self):
        opts, _ = sparksql.parse_options(
            "-s spark -d html -l 5 -v tmp -c -e -q --explain cost df",
            line_form=False,
        )
        self.assertEqual(
            opts,
            sparksql.Options(
                session="spark",
                display="html",
                limit=5,
                view="tmp",
                cache=True,
                eager=True,
                quiet=True,
                explain="cost",
                variable="df",
            ),
        )

    def test_long_and_attached_forms(self):
        opts, _ = sparksql.parse_options(
            "--session=s --display=VIZ --limit=7 --view v --cache -ce -l9",
            line_form=False,
        )
        self.assertEqual(opts.session, "s")
        self.assertEqual(opts.display, "colabsqlviz")
        self.assertEqual(opts.limit, 9)
        self.assertEqual(opts.view, "v")
        self.assertTrue(opts.cache)
        self.assertTrue(opts.eager)

        opts, _ = sparksql.parse_options("-dhtml", line_form=False)
        self.assertEqual(opts.display, "html")

    def test_explain_mode_is_optional(self):
        opts, _ = sparksql.parse_options("--explain df", line_form=False)
        self.assertEqual(opts.explain, "formatted")
        self.assertEqual(opts.variable, "df")
        opts, _ = sparksql.parse_options("--explain=EXTENDED", line_form=False)
        self.assertEqual(opts.explain, "extended")

    def test_errors(self):
        for line, msg in [
            ("--explain=bogus", "Unknown explain mode"),
            ("--display=bogus", "Unknown display mode"),
            ("-d", "needs a value"),
            ("-l", "needs a value"),
            ("-l 0", "at least 1"),
            ("-l x", "must be an integer"),
            ("-x", "Unknown option"),
            ("a b", "Unexpected argument"),
            ("my-df", "Unknown option|Not a valid"),
            ("1df", "Not a valid Python identifier"),
            ("-s 1x", "Not a valid Python identifier"),
            ("--view=", "needs a value"),
            ("-v=", "needs a value"),
            ("--session=", "needs a value"),
            ("-v my.view", "Not a valid temporary view name"),
            ("-v 1x", "Not a valid temporary view name"),
        ]:
            with self.subTest(line=line):
                with self.assertRaisesRegex(UsageError, msg):
                    sparksql.parse_options(line, line_form=False)

    def test_usage_in_errors(self):
        with self.assertRaisesRegex(UsageError, "Usage:"):
            sparksql.parse_options("-x", line_form=False)

    def test_line_form(self):
        opts, rest = sparksql.parse_options(
            "-c SELECT  -1 AS x  -- c", line_form=True
        )
        self.assertTrue(opts.cache)
        self.assertTrue(opts.quiet)
        # The SQL is everything from the first non-option token, verbatim.
        self.assertEqual(rest, "SELECT  -1 AS x  -- c")

    def test_line_form_needs_sql(self):
        with self.assertRaisesRegex(UsageError, "No SQL given"):
            sparksql.parse_options("-c", line_form=True)

    def test_line_form_rejects_display_options(self):
        # The line form never displays, so these would be silently ignored.
        for line in ("-l 3 SELECT 1", "-d html SELECT 1"):
            with self.subTest(line=line):
                with self.assertRaisesRegex(
                    UsageError, "only apply to the cell form"
                ):
                    sparksql.parse_options(line, line_form=True)


class SplitStatementsTest(unittest.TestCase):

    def test_offsets_and_stripping(self):
        cell = "SELECT 1;\n\n  SELECT ';' AS s ;\n"
        stmts = sparksql.split_statements(cell)
        self.assertEqual(
            [s.text for s in stmts], ["SELECT 1", "SELECT ';' AS s"]
        )
        for s in stmts:
            self.assertEqual(cell[s.offset : s.offset + len(s.text)], s.text)

    def test_comments_only_and_empty(self):
        self.assertEqual(sparksql.split_statements(""), [])
        self.assertEqual(sparksql.split_statements("-- hi\n/* x */ ;\n"), [])

    def test_comment_only_statement_is_dropped(self):
        stmts = sparksql.split_statements("SELECT 1; -- trailing\n")
        self.assertEqual([s.text for s in stmts], ["SELECT 1"])

    def test_script_is_not_split(self):
        cell = "-- a script\nBEGIN\n  SELECT 1;\n  SELECT 2;\nEND;\n"
        stmts = sparksql.split_statements(cell)
        self.assertEqual(len(stmts), 1)
        self.assertTrue(stmts[0].text.startswith("-- a script"))
        self.assertTrue(stmts[0].text.endswith("END"))


class ParametersTest(unittest.TestCase):

    def setUp(self):
        mock.patch.object(
            sparksql, "_dataframe_classes", return_value=(FakeDataFrame,)
        ).start()
        self.addCleanup(mock.patch.stopall)
        self.df = FakeDataFrame("SELECT 1", None, {})

    def _stmt(self, sql):
        (stmt,) = sparksql.split_statements(sql)
        return stmt

    def test_find_parameters(self):
        stmt = self._stmt(
            "SELECT :a, :b, :a, ':c', x::int, {df}, {col.name} -- :d {e}\n"
            "FROM IDENTIFIER(:t) /* :f */"
        )
        markers, fields = sparksql.find_parameters(stmt)
        self.assertEqual(markers, ["a", "b", "t"])
        self.assertEqual(fields, ["df"])

    def test_braces_in_strings_and_non_identifiers_are_escaped(self):
        stmt = self._stmt("SELECT '{x}', \"}\", {a.b}, {0} FROM {df}")
        self.assertEqual(
            sparksql.escape_braces(stmt),
            "SELECT '{{x}}', \"}}\", {{a.b}}, {{0}} FROM {df}",
        )

    def test_braces_found_whatever_their_token_type(self):
        # sqlparse currently emits braces as Error tokens; nothing should
        # depend on that, e.g. if a lexer change made them Punctuation.
        import dataclasses

        from sqlparse import tokens as T

        stmt = self._stmt(
            "SELECT '{x}', `{y}`, {a.b}, :n FROM {df} -- {z}\nWHERE {df2}.c"
        )
        self.assertIn((T.Error, "{"), stmt.tokens)
        stmt = dataclasses.replace(
            stmt,
            tokens=[
                (T.Punctuation if v in ("{", "}") else ttype, v)
                for ttype, v in stmt.tokens
            ],
        )
        self.assertEqual(sparksql.find_parameters(stmt), (["n"], ["df", "df2"]))
        self.assertEqual(
            sparksql.escape_braces(stmt),
            "SELECT '{{x}}', `{{y}}`, {{a.b}}, :n FROM {df} -- {{z}}\n"
            "WHERE {df2}.c",
        )

    def test_bind(self):
        stmt = self._stmt("SELECT :n, '{lit}' FROM {df};")
        sql, args, kwargs = sparksql.bind_parameters(
            stmt, {"n": 1, "df": self.df, "lit": "unused"}
        )
        self.assertEqual(sql, "SELECT :n, '{{lit}}' FROM {df}")
        self.assertEqual(args, {"n": 1})
        self.assertEqual(kwargs, {"df": self.df})

    def test_bind_without_kwargs_keeps_text(self):
        stmt = self._stmt("SELECT '{x}', :n")
        sql, args, kwargs = sparksql.bind_parameters(stmt, {"n": 2})
        self.assertEqual(sql, "SELECT '{x}', :n")
        self.assertEqual(args, {"n": 2})
        self.assertEqual(kwargs, {})

    def test_bind_missing(self):
        stmt = self._stmt("SELECT :a, :b FROM {c}")
        with self.assertRaisesRegex(UsageError, "Not defined.*: b, c"):
            sparksql.bind_parameters(stmt, {"a": 1})

    def test_bind_rejects_non_dataframe_in_braces(self):
        stmt = self._stmt("SELECT * FROM {t}")
        with self.assertRaisesRegex(
            UsageError,
            r"\{t\}: expected a Spark DataFrame, got str\."
            r" Use :t .* or IDENTIFIER\(:t\)",
        ):
            sparksql.bind_parameters(stmt, {"t": "my_table"})

        # A non-Spark class also named DataFrame (e.g. pandas.DataFrame)
        # includes its package name so the message isn't "got DataFrame".
        FakePandasDF = type(
            "DataFrame", (), {"__module__": "pandas.core.frame"}
        )
        with self.assertRaisesRegex(
            UsageError, r"expected a Spark DataFrame, got pandas\.DataFrame"
        ):
            sparksql.bind_parameters(stmt, {"t": FakePandasDF()})

    def test_bind_rejects_dataframe_in_marker(self):
        stmt = self._stmt("SELECT :df")
        with self.assertRaisesRegex(
            UsageError,
            r":df: expected a SQL value, got a Spark DataFrame\. Use \{df\}",
        ):
            sparksql.bind_parameters(stmt, {"df": self.df})


class ResolveSessionTest(unittest.TestCase):

    class FakeSession:
        pass

    def setUp(self):
        patcher = mock.patch.object(
            sparksql, "_session_classes", return_value=(self.FakeSession,)
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.active = mock.patch.object(
            sparksql, "_active_session", return_value=None
        ).start()
        self.addCleanup(mock.patch.stopall)

    def test_named(self):
        s = self.FakeSession()
        self.active.return_value = self.FakeSession()
        self.assertIs(sparksql.resolve_session({"s": s}, "s"), s)

    def test_named_errors(self):
        with self.assertRaisesRegex(UsageError, "no such variable"):
            sparksql.resolve_session({}, "s")
        with self.assertRaisesRegex(UsageError, "expected a SparkSession"):
            sparksql.resolve_session({"s": 1}, "s")

    def test_active_wins_over_namespace(self):
        active = self.FakeSession()
        self.active.return_value = active
        self.assertIs(
            sparksql.resolve_session({"other": self.FakeSession()}, None),
            active,
        )

    def test_only_session_in_namespace(self):
        s = self.FakeSession()
        self.assertIs(
            sparksql.resolve_session({"s": s, "_s": self.FakeSession()}, None),
            s,
        )

    def test_ambiguous_and_missing(self):
        with self.assertRaisesRegex(UsageError, r"several.*\(a, b\)"):
            sparksql.resolve_session(
                {"b": self.FakeSession(), "a": self.FakeSession()}, None
            )
        with self.assertRaisesRegex(UsageError, "No SparkSession found"):
            sparksql.resolve_session({}, None)

    def test_no_pyspark_recommends_install(self):
        with mock.patch.object(sparksql, "_session_classes", return_value=()):
            with self.assertRaisesRegex(
                UsageError,
                r"pip install 'google-cloud-spark-connect\[client,interactive\]'",
            ):
                sparksql.resolve_session({}, None)


class FakeContext:

    def __init__(self, start, stop, fragment=""):
        self._start, self._stop, self._fragment = start, stop, fragment

    def contextType(self):
        return QueryContextType.SQL

    def startIndex(self):
        return self._start

    def stopIndex(self):
        return self._stop

    def fragment(self):
        # Empty, like a context that does not say, unless a test sets it.
        return self._fragment


class FakeSparkError(PySparkException):

    def __init__(self, message, condition=None, sqlstate=None, span=None):
        """`span` is (start, stop) or (start, stop, fragment)."""
        super().__init__(message=message)
        self._msg, self._cond, self._state = message, condition, sqlstate
        self._ctx = [FakeContext(*span)] if span else []

    def getMessage(self):
        return self._msg

    def getCondition(self):
        return self._cond

    def getSqlState(self):
        return self._state

    def getQueryContext(self):
        return self._ctx


class FormatErrorTest(unittest.TestCase):

    def test_caret_in_second_statement(self):
        cell = "SELECT 1;\nSELECT\n  nope FROM t"
        stmts = sparksql.split_statements(cell)
        exc = FakeSparkError(
            "[UNRESOLVED_COLUMN] Column `nope` cannot be resolved."
            " SQLSTATE: 42703\n== SQL (line 2, position 3) ==\n...",
            condition="UNRESOLVED_COLUMN",
            sqlstate="42703",
            span=(9, 12),
        )
        msg = sparksql.format_error(exc, cell, stmts[1], 1, 2, True)
        self.assertEqual(
            msg.splitlines(),
            [
                "[UNRESOLVED_COLUMN] Column `nope` cannot be resolved."
                " SQLSTATE: 42703",
                "(line 3; statement 2 of 2; earlier statements already ran)",
                "    nope FROM t",
                "    ^^^^",
            ],
        )

    def test_adds_condition_and_sqlstate(self):
        (stmt,) = sparksql.split_statements("SELECT x")
        exc = FakeSparkError("Something broke.", "SOME_CONDITION", "XX000")
        msg = sparksql.format_error(exc, "SELECT x", stmt, 0, 1, True)
        self.assertEqual(
            msg, "[SOME_CONDITION] Something broke. SQLSTATE: XX000"
        )

    def test_no_caret_when_offsets_do_not_match(self):
        (stmt,) = sparksql.split_statements("SELECT x FROM {df}")
        exc = FakeSparkError("Bad.", span=(7, 7))
        msg = sparksql.format_error(
            exc, "SELECT x FROM {df}", stmt, 0, 1, False
        )
        self.assertEqual(msg, "Bad.")

    def test_caret_when_fragment_matches(self):
        (stmt,) = sparksql.split_statements("SELECT 1/0 FROM t")
        exc = FakeSparkError("Bad.", span=(7, 9, "1/0"))
        msg = sparksql.format_error(exc, "SELECT 1/0 FROM t", stmt, 0, 1, True)
        self.assertEqual(
            msg.splitlines(),
            ["Bad.", "(line 1)", "  SELECT 1/0 FROM t", "         ^^^"],
        )

    def test_no_caret_when_fragment_is_from_elsewhere(self):
        # E.g. an error inside a view: the offsets are into the view's SQL.
        (stmt,) = sparksql.split_statements("SELECT * FROM some_view")
        exc = FakeSparkError("Bad.", span=(7, 9, "1/0"))
        msg = sparksql.format_error(
            exc, "SELECT * FROM some_view", stmt, 0, 1, True
        )
        self.assertEqual(msg, "Bad.")

    def test_drops_jvm_stacktrace(self):
        (stmt,) = sparksql.split_statements("SELECT x")
        exc = FakeSparkError("Bad.\n\nJVM stacktrace:\norg.apache...")
        self.assertEqual(
            sparksql.format_error(exc, "SELECT x", stmt, 0, 1, True), "Bad."
        )

    def test_drops_connect_plan(self):
        (stmt,) = sparksql.split_statements("SELECT * FROM t")
        exc = FakeSparkError(
            "[TABLE_OR_VIEW_NOT_FOUND] Not found.\nVerify the spelling."
            " SQLSTATE: 42P01; line 1 pos 14;\n'Project [*]\n+- 'Unresolved"
        )
        self.assertEqual(
            sparksql.format_error(exc, "SELECT * FROM t", stmt, 0, 1, True),
            "[TABLE_OR_VIEW_NOT_FOUND] Not found.\nVerify the spelling."
            " SQLSTATE: 42P01",
        )

    def test_parse_error_position(self):
        cell = "SELECT 1;\nSELECT\n  1 +* 2"
        stmts = sparksql.split_statements(cell)
        exc = FakeSparkError(
            "\n[PARSE_SYNTAX_ERROR] Syntax error at or near '*'. SQLSTATE:"
            " 42601 (line 2, pos 5)\n\n== SQL ==\nSELECT\n  1 +* 2\n-----^^^\n"
        )
        self.assertEqual(
            sparksql.format_error(exc, cell, stmts[1], 1, 2, True).splitlines(),
            [
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '*'."
                " SQLSTATE: 42601",
                "(line 3; statement 2 of 2; earlier statements already ran)",
                "    1 +* 2",
                "       ^",
            ],
        )

    def test_render_traceback(self):
        err = sparksql.SparkSqlError("a\nb")
        self.assertEqual(err._render_traceback_(), ["a", "b"])


class RenderTest(unittest.TestCase):

    def setUp(self):
        self.table = sparksql.Table(
            names=["id", "<name>"],
            types=["bigint", "string"],
            numeric=[True, False],
            rows=[["1", "a&b"], ["22", None]],
            truncated=False,
        )

    def test_html(self):
        out = sparksql.render_html(self.table)
        self.assertIn("&lt;name&gt;", out)
        self.assertIn('<span class="t">bigint</span>', out)
        self.assertIn('<td class="n">22</td>', out)
        self.assertIn('<td class="null">NULL</td>', out)
        self.assertIn("a&amp;b", out)
        self.assertIn("2 rows", out)
        self.assertNotIn("<name>", out)

    def test_html_style_is_scoped(self):
        out = sparksql.render_html(self.table)
        cls = out.split('class="', 1)[1].split('"', 1)[0]
        self.assertTrue(cls.startswith("sparksql-"))
        self.assertIn(f".{cls} td.n", out)

    def test_truncation_footer(self):
        self.table.truncated = True
        self.assertIn(
            "Showing the first 2 rows; more are available",
            sparksql.render_html(self.table),
        )
        self.table.rows = self.table.rows[:1]
        self.table.truncated = False
        self.assertIn("1 row<", sparksql.render_html(self.table))

    def test_text(self):
        self.table.rows.append(["3", "x" * 50 + "\ny"])
        self.assertEqual(
            sparksql.render_text(self.table).splitlines(),
            [
                "+--+----------------------------------------+",
                "|id|                                  <name>|",
                "+--+----------------------------------------+",
                "| 1|                                     a&b|",
                "|22|                                    NULL|",
                "| 3|" + "x" * 37 + "...|",
                "+--+----------------------------------------+",
                "3 rows",
            ],
        )


class FakeQueryDataFrame:
    """Records the toDF/selectExpr/limit chain fetch_table() builds."""

    def __init__(self, fields, rows):
        from pyspark.sql.types import StructType

        self.schema = StructType(fields)
        self._rows = rows
        self.renamed = self.exprs = self.limit_n = None

    def toDF(self, *names):
        self.renamed = list(names)
        return self

    def selectExpr(self, *exprs):
        self.exprs = list(exprs)
        return self

    def limit(self, n):
        self.limit_n = n
        return self

    def collect(self):
        return [tuple(r) for r in self._rows[: self.limit_n]]


class FetchTableTest(unittest.TestCase):

    def setUp(self):
        from pyspark.sql.types import (
            BinaryType,
            DecimalType,
            LongType,
            StringType,
            StructField,
        )

        self.fields = [
            StructField("id", LongType()),
            StructField("id", StringType()),  # Duplicate names are fine.
            StructField("b", BinaryType()),
            StructField("d", DecimalType(10, 2)),
        ]
        self.rows = [[str(i), f"s{i}", "[01]", None] for i in range(5)]

    def test_formats_server_side_with_positional_names(self):
        df = FakeQueryDataFrame(self.fields, self.rows)
        table = sparksql.fetch_table(df, 10)
        self.assertEqual(df.renamed, ["_c0", "_c1", "_c2", "_c3"])
        self.assertEqual(
            df.exprs,
            [
                "CAST(_c0 AS STRING) AS _c0",
                "CAST(_c1 AS STRING) AS _c1",
                # Binary shows as hex bytes, like df.show().
                "CAST(concat('[', regexp_replace(hex(_c2), '(..)(?=.)',"
                " '$1 '), ']') AS STRING) AS _c2",
                "CAST(_c3 AS STRING) AS _c3",
            ],
        )
        self.assertEqual(table.names, ["id", "id", "b", "d"])
        self.assertEqual(
            table.types, ["bigint", "string", "binary", "decimal(10,2)"]
        )
        self.assertEqual(table.numeric, [True, False, False, True])
        self.assertEqual(table.rows, self.rows)
        self.assertFalse(table.truncated)

    def test_fetches_one_extra_row_to_detect_truncation(self):
        df = FakeQueryDataFrame(self.fields, self.rows)
        table = sparksql.fetch_table(df, 3)
        self.assertEqual(df.limit_n, 4)
        self.assertEqual(table.rows, self.rows[:3])
        self.assertTrue(table.truncated)

        df = FakeQueryDataFrame(self.fields, self.rows)
        table = sparksql.fetch_table(df, 5)
        self.assertEqual(table.rows, self.rows)
        self.assertFalse(table.truncated)


class FakeDataFrame:

    def __init__(self, sql, args, kwargs, fields=("x",)):
        self.sql, self.args, self.kwargs = sql, args, kwargs
        self.schema = mock.Mock(fields=list(fields))
        self.calls = []

    def _record(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))
        return self

    def limit(self, n):
        return self._record("limit", n)

    def localCheckpoint(self, **kwargs):
        return self._record("localCheckpoint", **kwargs)

    def cache(self):
        return self._record("cache")

    def createOrReplaceTempView(self, name):
        self._record("createOrReplaceTempView", name)

    def explain(self, **kwargs):
        self._record("explain", **kwargs)


class DisplayTest(unittest.TestCase):

    def setUp(self):
        self.df = FakeDataFrame("SELECT 1", None, {})
        self.ipython_display = mock.patch("IPython.display.display").start()
        self.explore = mock.Mock(return_value="VIZ")
        self.import_explore = mock.patch.object(
            sparksql, "_import_explore_dataframe", return_value=self.explore
        ).start()
        self.table = sparksql.Table(
            names=["x"],
            types=["bigint"],
            numeric=[True],
            rows=[["1"]],
            truncated=False,
        )
        self.fetch_table = mock.patch.object(
            sparksql, "fetch_table", return_value=self.table
        ).start()
        self.addCleanup(mock.patch.stopall)

    def test_empty_schema_displays_ok(self):
        empty = FakeDataFrame("CREATE VIEW v AS SELECT 1", None, {}, fields=())
        sparksql._display(empty)
        self.ipython_display.assert_called_once_with(
            {"text/plain": "OK"}, raw=True
        )
        self.explore.assert_not_called()
        self.fetch_table.assert_not_called()

    def test_colabsqlviz_default_is_unlimited(self):
        sparksql._display(self.df)
        self.assertEqual(self.df.calls, [])
        self.explore.assert_called_once_with(self.df, chart_type="table")
        self.ipython_display.assert_called_once_with("VIZ")
        self.fetch_table.assert_not_called()

    def test_colabsqlviz_with_limit(self):
        sparksql._display(self.df, limit=5)
        self.assertEqual(self.df.calls, [("limit", (5,), {})])
        self.explore.assert_called_once_with(self.df, chart_type="table")
        self.ipython_display.assert_called_once_with("VIZ")

    def test_colabsqlviz_falls_back_to_html_when_not_installed(self):
        self.import_explore.return_value = None
        sparksql._display(self.df)
        self.fetch_table.assert_called_once_with(
            self.df, sparksql.DEFAULT_HTML_LIMIT
        )
        self.ipython_display.assert_called_once()
        bundle = self.ipython_display.call_args[0][0]
        self.assertIn("text/html", bundle)
        self.assertIn("text/plain", bundle)

    def test_colabsqlviz_explicit_raises_when_not_installed(self):
        self.import_explore.return_value = None
        with self.assertRaisesRegex(
            UsageError,
            r"pip install 'google-cloud-spark-connect\[interactive\]'",
        ):
            sparksql._display(
                self.df, display_mode="colabsqlviz", explicit_display=True
            )
        self.fetch_table.assert_not_called()

    def test_html_mode_default_and_custom_limit(self):
        sparksql._display(self.df, display_mode="html")
        self.fetch_table.assert_called_once_with(
            self.df, sparksql.DEFAULT_HTML_LIMIT
        )
        self.explore.assert_not_called()

        self.fetch_table.reset_mock()
        sparksql._display(self.df, limit=7, display_mode="html")
        self.fetch_table.assert_called_once_with(self.df, 7)


class FakeSession:

    def __init__(self, fail_on=None):
        self.queries = []
        self.fail_on = fail_on

    def sql(self, sql, args=None, **kwargs):
        self.queries.append(sql)
        if self.fail_on is not None and self.fail_on in sql:
            raise FakeSparkError("Nope.", "BAD", "42000", span=(0, 5))
        return FakeDataFrame(sql, args, kwargs)


class MagicTest(unittest.TestCase):
    """Drives the magic through SparkSqlMagics with a fake session."""

    def setUp(self):
        self.shell = mock.create_autospec(InteractiveShell, instance=True)
        self.shell.user_ns = {}
        self.shell.config = Config()
        self.shell.configurables = []
        self.magics = sparksql.SparkSqlMagics(shell=self.shell)
        self.session = FakeSession()
        mock.patch.object(
            sparksql, "_active_session", return_value=self.session
        ).start()
        mock.patch.object(
            sparksql, "_session_classes", return_value=(FakeSession,)
        ).start()
        mock.patch.object(
            sparksql, "_dataframe_classes", return_value=(FakeDataFrame,)
        ).start()
        self.display = mock.patch.object(sparksql, "_display").start()
        self.addCleanup(mock.patch.stopall)

    def test_config_is_registered(self):
        cfgs = [
            c
            for c in self.shell.configurables
            if isinstance(c, sparksql.SparkSql)
        ]
        self.assertEqual(len(cfgs), 1)
        cfg = cfgs[0]
        self.assertIs(sparksql.get_config(self.shell), cfg)
        self.assertEqual(cfg.display, "colabsqlviz")
        self.assertIsNone(cfg.limit)
        self.assertFalse(cfg.full_traceback)
        cfg.display = "VIZ"
        self.assertEqual(cfg.display, "colabsqlviz")
        cfg.display = "HTML"
        self.assertEqual(cfg.display, "html")

    def test_config_limit_must_be_positive(self):
        cfg = sparksql.get_config(self.shell)
        for bad in (0, -1):
            with self.subTest(limit=bad):
                with self.assertRaisesRegex(TraitError, "at least 1"):
                    cfg.limit = bad
        cfg.limit = 1
        cfg.limit = None
        self.assertIsNone(cfg.limit)

    def test_get_config_matches_config_from_reloaded_module(self):
        # A SparkSql created by an earlier copy of the module, as after
        # importlib.reload(): same module and name, but a different class.
        StaleSparkSql = type(
            "SparkSql", (Configurable,), {"__module__": sparksql.__name__}
        )
        stale = StaleSparkSql()
        self.assertNotIsInstance(stale, sparksql.SparkSql)
        self.shell.configurables[:] = [stale]
        self.assertIs(sparksql.get_config(self.shell), stale)
        self.assertEqual(len(self.shell.configurables), 1)

    def test_get_config_without_configurable_parent(self):
        shell = mock.Mock(spec=["user_ns", "configurables"])
        shell.configurables = []
        cfg = sparksql.get_config(shell)
        self.assertIsNone(cfg.parent)
        self.assertEqual(shell.configurables, [cfg])

    def test_get_config_reports_bad_shell_config(self):
        ip = InteractiveShell.instance()
        self.addCleanup(InteractiveShell.clear_instance)
        ip.configurables[:] = [
            c for c in ip.configurables if not isinstance(c, sparksql.SparkSql)
        ]
        ip.config.SparkSql.limit = 0
        self.addCleanup(ip.config.pop, "SparkSql", None)
        with self.assertRaises(TraitError):
            sparksql.get_config(ip)

    def test_cell_runs_all_and_displays_last(self):
        input_df = FakeDataFrame("SELECT 0", None, {})
        self.shell.user_ns.update(n=3, df=input_df)
        result = self.magics.sparksql(
            "out", "CREATE VIEW v AS SELECT 1;\nSELECT :n FROM {df}"
        )
        self.assertIsNone(result)
        self.assertEqual(
            self.session.queries,
            ["CREATE VIEW v AS SELECT 1", "SELECT :n FROM {df}"],
        )
        df = self.shell.user_ns["out"]
        self.assertEqual(df.args, {"n": 3})
        self.assertEqual(df.kwargs, {"df": input_df})
        self.display.assert_called_once_with(
            df,
            limit=None,
            display_mode="colabsqlviz",
            explicit_display=False,
        )

    def test_limit_option_and_config(self):
        self.magics.sparksql("-l 5", "SELECT 1")
        self.display.assert_called_with(
            mock.ANY,
            limit=5,
            display_mode="colabsqlviz",
            explicit_display=False,
        )
        sparksql.get_config(self.shell).limit = 7
        self.magics.sparksql("", "SELECT 1")
        self.display.assert_called_with(
            mock.ANY,
            limit=7,
            display_mode="colabsqlviz",
            explicit_display=False,
        )

    def test_display_option_and_config(self):
        self.magics.sparksql("-d html", "SELECT 1")
        self.display.assert_called_with(
            mock.ANY,
            limit=None,
            display_mode="html",
            explicit_display=True,
        )
        sparksql.get_config(self.shell).display = "html"
        self.magics.sparksql("", "SELECT 1")
        self.display.assert_called_with(
            mock.ANY,
            limit=None,
            display_mode="html",
            explicit_display=False,
        )
        self.magics.sparksql("-d viz", "SELECT 1")
        self.display.assert_called_with(
            mock.ANY,
            limit=None,
            display_mode="colabsqlviz",
            explicit_display=True,
        )

    def test_view_cache_eager_explain(self):
        self.magics.sparksql("-e -c -v tmp --explain d", "SELECT 1")
        df = self.shell.user_ns["d"]
        self.assertEqual(
            df.calls,
            [
                # No cache(): the checkpoint is already stored.
                ("localCheckpoint", (), {"eager": True}),
                ("createOrReplaceTempView", ("tmp",), {}),
                ("explain", (), {"mode": "formatted"}),
            ],
        )
        self.display.assert_not_called()

    def test_cache_without_eager(self):
        self.magics.sparksql("-c -q d", "SELECT 1")
        self.assertEqual(self.shell.user_ns["d"].calls, [("cache", (), {})])

    def test_quiet(self):
        self.magics.sparksql("-q d", "SELECT 1")
        self.display.assert_not_called()
        self.assertIn("d", self.shell.user_ns)

    def test_line_form_returns_dataframe(self):
        input_df = FakeDataFrame("SELECT 0", None, {})
        self.shell.user_ns["x"] = input_df
        df = self.magics.sparksql("-c SELECT * FROM {x}")
        self.assertIsInstance(df, FakeDataFrame)
        self.assertEqual(df.sql, "SELECT * FROM {x}")
        self.assertEqual(df.kwargs, {"x": input_df})
        self.display.assert_not_called()

    def test_missing_variable(self):
        with self.assertRaisesRegex(UsageError, "Not defined.*: x"):
            self.magics.sparksql("SELECT {x}")
        self.assertEqual(self.session.queries, [])

    def test_missing_variable_in_later_statement_runs_nothing(self):
        with self.assertRaisesRegex(UsageError, "Not defined.*: y"):
            self.magics.sparksql("", "CREATE VIEW v AS SELECT 1;\nSELECT :y")
        self.assertEqual(self.session.queries, [])

    def test_display_error_has_caret(self):
        self.display.side_effect = FakeSparkError(
            "Bad.", "BAD", "42000", span=(7, 9, "1/0")
        )
        with self.assertRaises(sparksql.SparkSqlError) as cm:
            self.magics.sparksql("", "SELECT 1/0")
        self.assertIn("  SELECT 1/0\n         ^^^", cm.exception.message)

    def test_display_error_after_dataframe_reference_has_no_caret(self):
        # Spark rewrote {d} before running it, so its positions are not the
        # cell's.
        self.shell.user_ns["d"] = FakeDataFrame("SELECT 0", None, {})
        self.display.side_effect = FakeSparkError(
            "Bad.", "BAD", "42000", span=(0, 5)
        )
        with self.assertRaises(sparksql.SparkSqlError) as cm:
            self.magics.sparksql("", "SELECT * FROM {d}")
        self.assertEqual(cm.exception.message, "[BAD] Bad. SQLSTATE: 42000")

    def test_variable_not_bound_when_display_fails(self):
        self.display.side_effect = FakeSparkError("Bad.")
        with self.assertRaises(sparksql.SparkSqlError):
            self.magics.sparksql("out", "SELECT 1")
        self.assertNotIn("out", self.shell.user_ns)

    def test_error_in_later_statement(self):
        self.session.fail_on = "boom"
        with self.assertRaises(sparksql.SparkSqlError) as cm:
            self.magics.sparksql("", "SELECT 1;\nboom;\nSELECT 3")
        self.assertEqual(self.session.queries, ["SELECT 1", "boom"])
        msg = str(cm.exception)
        self.assertIn("[BAD] Nope. SQLSTATE: 42000", msg)
        self.assertIn("statement 2 of 3", msg)
        self.assertIn("  boom;\n  ^^^^", msg)
        self.assertIsInstance(cm.exception.__cause__, FakeSparkError)

    def test_full_traceback(self):
        self.session.fail_on = "boom"
        sparksql.get_config(self.shell).full_traceback = True
        with self.assertRaises(FakeSparkError):
            self.magics.sparksql("", "boom")

    def test_non_spark_errors_propagate(self):
        self.session.sql = mock.Mock(side_effect=KeyError("k"))
        with self.assertRaises(KeyError):
            self.magics.sparksql("", "SELECT 1")

    def test_empty_cell(self):
        with self.assertRaisesRegex(UsageError, "No SQL given"):
            self.magics.sparksql("", "-- nothing\n")


class ExtensionTest(unittest.TestCase):

    _EXTENSIONS = (
        "google.cloud.managed_spark_magics",
        "google.cloud.managed_spark_magics.sparksql",
    )

    def _reset(self):
        magics = self.ip.magics_manager.magics
        for kind in ("line", "cell"):
            magics.get(kind, {}).pop("sparksql", None)
        magics.get("line", {}).pop("dpip", None)
        for ext in self._EXTENSIONS:
            self.ip.extension_manager.loaded.discard(ext)

    def setUp(self):
        self.ip = InteractiveShell.instance()
        self._reset()

    def tearDown(self):
        self._reset()
        InteractiveShell.clear_instance()

    def test_standalone_load_and_unload_ext(self):
        magics = self.ip.magics_manager.magics
        self.ip.run_line_magic(
            "load_ext", "google.cloud.managed_spark_magics.sparksql"
        )
        self.assertIn("sparksql", magics["line"])
        self.assertIn("sparksql", magics["cell"])
        self.assertNotIn("dpip", magics["line"])

        self.ip.run_line_magic(
            "unload_ext", "google.cloud.managed_spark_magics.sparksql"
        )
        self.assertNotIn("sparksql", magics["line"])
        self.assertNotIn("sparksql", magics["cell"])

    def test_package_unload_keeps_separately_loaded_sparksql(self):
        magics = self.ip.magics_manager.magics
        self.ip.run_line_magic(
            "load_ext", "google.cloud.managed_spark_magics.sparksql"
        )
        self.ip.run_line_magic("load_ext", "google.cloud.managed_spark_magics")
        self.ip.run_line_magic(
            "unload_ext", "google.cloud.managed_spark_magics"
        )
        self.assertNotIn("dpip", magics["line"])
        # Still loaded in its own right, so still registered.
        self.assertIn("sparksql", magics["line"])
        self.assertIn("sparksql", magics["cell"])

        self.ip.run_line_magic(
            "unload_ext", "google.cloud.managed_spark_magics.sparksql"
        )
        self.assertNotIn("sparksql", magics["cell"])

    def test_package_unload_removes_its_own_sparksql(self):
        magics = self.ip.magics_manager.magics
        self.ip.run_line_magic("load_ext", "google.cloud.managed_spark_magics")
        self.ip.run_line_magic(
            "unload_ext", "google.cloud.managed_spark_magics"
        )
        self.assertNotIn("dpip", magics["line"])
        self.assertNotIn("sparksql", magics["line"])
        self.assertNotIn("sparksql", magics["cell"])


_MISSING_DEPS_PROBE = """
import sys

for name in ("sqlparse", "pyspark"):
    sys.modules[name] = None

from IPython.core.error import UsageError
from IPython.core.interactiveshell import InteractiveShell

ip = InteractiveShell.instance()
ip.run_line_magic("load_ext", "google.cloud.managed_spark_magics")
print("dpip" in ip.magics_manager.magics["line"])
try:
    ip.run_cell_magic("sparksql", "", "SELECT 1")
except UsageError as e:
    print("interactive]" in str(e))
"""


class MissingDependenciesTest(unittest.TestCase):

    def test_extension_loads_without_sqlparse_or_pyspark(self):
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", _MISSING_DEPS_PROBE],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(), ["True", "True"])


if __name__ == "__main__":
    unittest.main()
