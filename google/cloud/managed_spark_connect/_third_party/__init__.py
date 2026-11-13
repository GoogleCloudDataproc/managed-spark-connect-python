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
"""Private namespace for remapped third_party dependencies."""

from pathlib import Path

# In an installed wheel, `setup.py` places `apache_spark/` directly beside this
# file. When imported directly from a source checkout (e.g. `PYTHONPATH=.` or
# pytest's rootdir shadowing a non-editable install), expose `<repo>/third_party`
# on `__path__` as well.
_HERE = Path(__file__).resolve().parent
_REPO_THIRD_PARTY = _HERE.parents[3] / "third_party"
if (
    not (_HERE / "apache_spark").is_dir()
    and (_REPO_THIRD_PARTY / "apache_spark").is_dir()
):
    __path__.append(str(_REPO_THIRD_PARTY))
