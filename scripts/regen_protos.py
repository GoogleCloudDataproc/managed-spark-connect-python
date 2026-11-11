#!/usr/bin/env python3
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
"""Regenerates vendored Spark Connect GetStatus and Cell Monitor proto stubs.

Usage:
  # Regenerate *_pb2.py from the checked-in .proto files:
  python scripts/regen_protos.py

  # Verify checked-in *_pb2.py files match the checked-in .proto files:
  python scripts/regen_protos.py --check

  # Refresh .proto files, NOTICE, METADATA, and *_pb2.py from a Git ref/SHA on
  # bigdataoss-internal/third_party/apache/spark (e.g. once CL 196087 merges):
  python scripts/regen_protos.py --ref refs/heads/dataproc-branch-4.2.0
"""

import argparse
import datetime
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Iterable, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
VENDOR_DIR = REPO_ROOT / "third_party" / "apache_spark"
TARGET_PACKAGE = "google.cloud.managed_spark_connect._third_party.apache_spark"
DEFAULT_SPARK_REMOTE = "sso://bigdataoss-internal/third_party/apache/spark"

UPSTREAM_BASE_PROTO_PATH = (
    "sql/connect/common/src/main/protobuf/spark/connect/base.proto"
)
UPSTREAM_CELL_MONITOR_PROTO_PATH = (
    "sql/connect/common/src/main/protobuf/dataproc/cellmonitor/v1/"
    "cell_monitor.proto"
)

ASF_PYTHON_HEADER = """\
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications copyright 2026 Google LLC:
# Generated and post-processed by scripts/regen_protos.py for packaging under
# google.cloud.managed_spark_connect._third_party.apache_spark.
"""

STATUS_PROTO_HEADER = """\
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright 2026 Google LLC:
 * Extracted minimal UserContext, GetStatusRequest, GetStatusResponse, and
 * SparkConnectService.GetStatus definitions from
 * sql/connect/common/src/main/protobuf/spark/connect/base.proto so clients on
 * PySpark 4.0+ can invoke GetStatus without pulling in full plan protos.
 */

syntax = 'proto3';

package spark.connect;

import "google/protobuf/any.proto";

option java_multiple_files = true;
option java_package = "org.apache.spark.connect.proto";
option go_package = "internal/generated";
"""

STATUS_SERVICE_BLOCK = """\
// Minimal SparkConnectService subset for GetStatus RPC.
service SparkConnectService {
  // Get status information of different types.
  rpc GetStatus(GetStatusRequest) returns (GetStatusResponse) {}
}
"""


def _extract_proto_block(proto_text: str, message_name: str) -> str:
    """Extracts a top-level message block with its leading comment lines."""
    lines = proto_text.splitlines()
    decl_re = re.compile(rf"^message\s+{re.escape(message_name)}\s*\{{")
    start_idx = None
    for idx, line in enumerate(lines):
        if decl_re.match(line):
            start_idx = idx
            break
    if start_idx is None:
        raise ValueError(f"Message {message_name!r} not found in base.proto")

    comment_start = start_idx
    while comment_start > 0 and lines[comment_start - 1].lstrip().startswith(
        "//"
    ):
        comment_start -= 1

    depth = 0
    end_idx = None
    for idx in range(start_idx, len(lines)):
        # Strip // line comments before counting braces.
        code_part = lines[idx].split("//", 1)[0]
        depth += code_part.count("{") - code_part.count("}")
        if depth == 0:
            end_idx = idx
            break
    if end_idx is None:
        raise ValueError(
            f"Unbalanced braces while extracting {message_name!r} from"
            " base.proto"
        )

    return "\n".join(lines[comment_start : end_idx + 1])


def build_minimal_status_proto(base_proto_text: str) -> str:
    """Builds spark_connect_status.proto from upstream base.proto."""
    blocks = [
        _extract_proto_block(base_proto_text, name)
        for name in ("UserContext", "GetStatusRequest", "GetStatusResponse")
    ]
    return (
        STATUS_PROTO_HEADER
        + "\n"
        + "\n\n".join(blocks)
        + "\n\n"
        + STATUS_SERVICE_BLOCK
    )


def _run_git(repo_dir: Path, args: Sequence[str]) -> str:
    git_env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    res = subprocess.run(
        ["git", "-C", str(repo_dir), *args],
        check=False,
        capture_output=True,
        text=True,
        env=git_env,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed (exit {res.returncode}):\n"
            f"{res.stderr.strip()}"
        )
    return res.stdout


def _git_show_file(repo_dir: Path, commit: str, rel_path: str) -> str:
    return _run_git(repo_dir, ["show", f"{commit}:{rel_path}"])


def refresh_from_remote(remote: str, ref: str) -> str:
    """Fetches `ref` from `remote` and updates .proto, NOTICE, and metadata."""
    with tempfile.TemporaryDirectory(prefix="spark_proto_sync_") as tmp:
        tmp_repo = Path(tmp) / "repo"
        tmp_repo.mkdir()
        _run_git(tmp_repo, ["init"])
        _run_git(tmp_repo, ["remote", "add", "origin", remote])
        _run_git(tmp_repo, ["config", "remote.origin.promisor", "true"])
        _run_git(
            tmp_repo,
            ["config", "remote.origin.partialclonefilter", "blob:none"],
        )
        _run_git(
            tmp_repo,
            [
                "fetch",
                "--depth=1",
                "--filter=blob:none",
                "--no-tags",
                "origin",
                ref,
            ],
        )
        rev = _run_git(tmp_repo, ["rev-parse", "FETCH_HEAD"]).strip()

        base_proto = _git_show_file(tmp_repo, rev, UPSTREAM_BASE_PROTO_PATH)
        cell_monitor_proto = _git_show_file(
            tmp_repo, rev, UPSTREAM_CELL_MONITOR_PROTO_PATH
        )
        notice_text = _git_show_file(tmp_repo, rev, "NOTICE")

    (VENDOR_DIR / "spark_connect_status.proto").write_text(
        build_minimal_status_proto(base_proto), encoding="utf-8"
    )
    (VENDOR_DIR / "cell_monitor.proto").write_text(
        cell_monitor_proto, encoding="utf-8"
    )
    (VENDOR_DIR / "NOTICE").write_text(notice_text, encoding="utf-8")

    today = datetime.date.today()
    metadata_path = VENDOR_DIR / "METADATA"
    if metadata_path.exists():
        meta = metadata_path.read_text(encoding="utf-8")
        meta = re.sub(
            r"last_upgrade_date\s*\{\s*year:\s*\d+\s+month:\s*\d+\s+day:\s*\d+\s*\}",
            (
                f"last_upgrade_date {{ year: {today.year} "
                f"month: {today.month} day: {today.day} }}"
            ),
            meta,
        )
        meta = re.sub(
            r'local_modifications:\s*(?:"[^"]*"\s*)+',
            (
                "local_modifications:\n"
                '      "Extracted minimal UserContext, GetStatusRequest, '
                'GetStatusResponse, and "\n'
                '      "SparkConnectService.GetStatus definitions into '
                'spark_connect_status.proto "\n'
                '      "and added Dataproc cell_monitor.proto from "\n'
                '      "bigdataoss-internal/third_party/apache/spark "\n'
                f'      "(ref {ref}, commit {rev}), then generated Python "\n'
                '      "protobuf stubs (*_pb2.py) via scripts/regen_protos.py. '
                'See README.google.md."\n'
            ),
            meta,
        )
        metadata_path.write_text(meta, encoding="utf-8")

    readme_path = VENDOR_DIR / "README.google.md"
    if readme_path.exists():
        readme = readme_path.read_text(encoding="utf-8")
        readme = re.sub(
            r"(?m)^- \*\*Internal Dataproc Source Commit:\*\* .*$",
            f"- **Internal Dataproc Source Commit:** `{rev}` (`{ref}`)",
            readme,
        )
        readme_path.write_text(readme, encoding="utf-8")

    return rev


def _postprocess_pb2(raw_code: str, module_basename: str) -> str:
    """Applies cross-version compatibility and DescriptorPool isolation."""
    code = raw_code

    # Remove protoc's strict Gencode/Runtime version pin so generated stubs
    # load cleanly across protobuf 4.21+, 5.x, 6.x, and 7.x runtimes.
    code = re.sub(
        r"^# Protobuf Python Version: .*\n",
        "",
        code,
        flags=re.MULTILINE,
    )
    code = re.sub(
        r"^from google\.protobuf import runtime_version as _runtime_version\n",
        "",
        code,
        flags=re.MULTILINE,
    )
    code = re.sub(
        r"_runtime_version\.ValidateProtobufRuntimeVersion\(\n(?:.*?\n)*?\)\n",
        "",
        code,
    )

    full_module_name = f"{TARGET_PACKAGE}.{module_basename}"

    if module_basename == "spark_connect_status_pb2":
        # Isolate spark.connect.* symbols in a private DescriptorPool and
        # SymbolDatabase so loading spark_connect_status_pb2 alongside
        # pyspark.sql.connect.proto.base_pb2 never raises duplicate-symbol
        # errors on PySpark 4.0, 4.1, or 4.2+ (under upb/cpp or pure-Python).
        old_add = "DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile("
        if old_add not in code:
            raise RuntimeError(
                "Expected Default().AddSerializedFile in"
                " spark_connect_status_pb2"
            )
        code = code.replace(
            old_add,
            "_POOL = _descriptor_pool.DescriptorPool()\n"
            "_ANY_DESCRIPTOR = _POOL.AddSerializedFile(\n"
            "    google_dot_protobuf_dot_any__pb2.DESCRIPTOR.serialized_pb\n"
            ")\n"
            "DESCRIPTOR = _POOL.AddSerializedFile(",
            1,
        )
        old_build = (
            "_builder.BuildTopDescriptorsAndMessages("
            f"DESCRIPTOR, '{module_basename}', _globals)"
        )
        if old_build not in code:
            raise RuntimeError(
                "Expected BuildTopDescriptorsAndMessages in"
                " spark_connect_status_pb2"
            )
        code = code.replace(
            old_build,
            "_orig_sym_db = _builder._sym_db\n"
            "try:\n"
            "    _builder._sym_db = _symbol_database.SymbolDatabase("
            "pool=_POOL)\n"
            "    _builder.BuildTopDescriptorsAndMessages(\n"
            f"        _ANY_DESCRIPTOR, '{full_module_name}', {{}}\n"
            "    )\n"
            "    _builder.BuildTopDescriptorsAndMessages(\n"
            f"        DESCRIPTOR, '{full_module_name}', _globals\n"
            "    )\n"
            "finally:\n"
            "    _builder._sym_db = _orig_sym_db",
            1,
        )
    else:
        old_build = (
            "_builder.BuildTopDescriptorsAndMessages("
            f"DESCRIPTOR, '{module_basename}', _globals)"
        )
        if old_build not in code:
            raise RuntimeError(
                f"Expected BuildTopDescriptorsAndMessages in {module_basename}"
            )
        code = code.replace(
            old_build,
            "_builder.BuildTopDescriptorsAndMessages("
            f"DESCRIPTOR, '{full_module_name}', _globals)",
            1,
        )

    return ASF_PYTHON_HEADER + code


def _format_with_pyink(paths: Iterable[Path]) -> None:
    path_list = [str(p) for p in paths]
    if not path_list:
        return
    pyink_bin = shutil.which("pyink")
    cmd = None
    if pyink_bin:
        cmd = [pyink_bin, "--config", str(REPO_ROOT / "pyproject.toml")]
    else:
        probe = subprocess.run(
            [sys.executable, "-c", "import pyink"],
            capture_output=True,
            check=False,
        )
        if probe.returncode == 0:
            cmd = [
                sys.executable,
                "-m",
                "pyink",
                "--config",
                str(REPO_ROOT / "pyproject.toml"),
            ]
    if cmd is not None:
        subprocess.run(
            [*cmd, *path_list],
            check=True,
            capture_output=True,
            text=True,
        )


def compile_protos(check_only: bool = False) -> int:
    """Compiles .proto files into *_pb2.py and optionally checks freshness."""
    try:
        import grpc_tools  # type: ignore[import-not-found]
        from grpc_tools import protoc  # type: ignore[import-not-found]
    except ImportError as exc:
        raise SystemExit(
            "grpcio-tools is required to regenerate proto stubs. "
            "Install it with: pip install grpcio-tools"
        ) from exc

    include_dir = Path(grpc_tools.__file__).parent / "_proto"
    proto_files = [
        VENDOR_DIR / "spark_connect_status.proto",
        VENDOR_DIR / "cell_monitor.proto",
    ]

    with tempfile.TemporaryDirectory(prefix="spark_pb2_gen_") as tmp:
        tmp_dir = Path(tmp)
        rc = protoc.main(
            [
                "grpc_tools.protoc",
                f"-I{VENDOR_DIR}",
                f"-I{include_dir}",
                f"--python_out={tmp_dir}",
                *[str(p) for p in proto_files],
            ]
        )
        if rc != 0:
            raise SystemExit(f"protoc failed with exit code {rc}")

        generated_files = []
        for module_basename in ("spark_connect_status_pb2", "cell_monitor_pb2"):
            out_file = tmp_dir / f"{module_basename}.py"
            processed = _postprocess_pb2(
                out_file.read_text(encoding="utf-8"), module_basename
            )
            out_file.write_text(processed, encoding="utf-8")
            generated_files.append(out_file)

        _format_with_pyink(generated_files)

        if check_only:
            stale = []
            for gen_file in generated_files:
                target = VENDOR_DIR / gen_file.name
                if not target.exists() or (
                    target.read_text(encoding="utf-8")
                    != gen_file.read_text(encoding="utf-8")
                ):
                    stale.append(gen_file.name)
            if stale:
                sys.stderr.write(
                    "Stale or missing generated proto stubs: "
                    + ", ".join(stale)
                    + "\nRun `python scripts/regen_protos.py` to update.\n"
                )
                return 1
            return 0

        for gen_file in generated_files:
            shutil.copy2(gen_file, VENDOR_DIR / gen_file.name)

    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ref",
        help=(
            "Optional Git ref or commit SHA on --remote to refresh "
            "spark_connect_status.proto, cell_monitor.proto, NOTICE, and "
            "METADATA before regenerating Python stubs."
        ),
    )
    parser.add_argument(
        "--remote",
        default=DEFAULT_SPARK_REMOTE,
        help=f"Git remote URL for --ref (default: {DEFAULT_SPARK_REMOTE}).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify checked-in *_pb2.py stubs match the .proto files.",
    )
    args = parser.parse_args(argv)

    if args.ref and args.check:
        parser.error("--ref and --check cannot be used together.")

    if args.ref:
        rev = refresh_from_remote(args.remote, args.ref)
        print(f"Refreshed .proto definitions from {args.ref} ({rev}).")

    return compile_protos(check_only=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
