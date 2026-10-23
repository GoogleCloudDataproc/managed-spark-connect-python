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

from . import sparksql
from .magics import ManagedSparkMagics


def load_ipython_extension(ipython):
    ipython.register_magics(ManagedSparkMagics)
    # %%sparksql is implemented as a standalone extension in `.sparksql` so it
    # can be cleanly split out into a separate project later; delegate to it
    # here so `%load_ext google.cloud.managed_spark_magics` loads both.
    sparksql.load_ipython_extension(ipython)


def unload_ipython_extension(ipython):
    magics_mgr = getattr(ipython, "magics_manager", None)
    magics = getattr(magics_mgr, "magics", None)
    if isinstance(magics, dict):
        line_magics = magics.get("line")
        if isinstance(line_magics, dict):
            owner = getattr(line_magics.get("dpip"), "__self__", None)
            if isinstance(owner, ManagedSparkMagics):
                del line_magics["dpip"]
    # If %%sparksql was also loaded as an extension in its own right, it stays
    # until that is unloaded. Removing it here would leave IPython believing
    # it is still loaded, so a later `%load_ext` of it would do nothing.
    ext_mgr = getattr(ipython, "extension_manager", None)
    loaded = getattr(ext_mgr, "loaded", None)
    if not (
        isinstance(loaded, (set, dict, list, tuple))
        and sparksql.__name__ in loaded
    ):
        sparksql.unload_ipython_extension(ipython)
