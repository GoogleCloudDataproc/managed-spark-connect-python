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

__version__ = "0.1.0"


from google.cloud import storage
from ._internal import magic


_original_pip = None


def load_ipython_extension(ipython):
    """Called by IPython when this module is loaded as an IPython ext."""
    global _original_pip
    _original_pip = ipython.find_magic("pip")

    if _original_pip:
        magics = magic.DataprocMagics(
            shell=ipython,
            original_pip=_original_pip,
            gcs_client=storage.Client(),
        )
        ipython.register_magics(magics)


def unload_ipython_extension(ipython):
    """Called by IPython when this module is unloaded as an IPython ext."""
    global _original_pip
    if _original_pip:
        ipython.register_magic_function(
            _original_pip, magic_kind="line", magic_name="pip"
        )
        _original_pip = None


__all__ = [
    "__version__",
    "load_ipython_extension",
    "unload_ipython_extension",
]
