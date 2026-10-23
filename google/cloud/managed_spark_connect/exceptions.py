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


class ManagedSparkConnectException(Exception):
    """A custom exception class to only print the error messages.
    This would be used for exceptions where the stack trace
    doesn't provide any additional information.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(message)

    def _render_traceback_(self):
        return [self.message]


class PySparkNotInstalledError(ManagedSparkConnectException, ImportError):
    """Raised on import when pyspark is missing or incomplete.

    Subclasses ImportError so that the usual optional-dependency guard,
    ``try: import ... except ImportError:``, still works.
    """

    def __init__(self, message):
        super().__init__(message)
        self.name = "pyspark"


class UnsupportedPySparkVersionError(PySparkNotInstalledError):
    """Raised on import when the installed pyspark is older than 4.0.

    A PySparkNotInstalledError, and so an ImportError, too: a pyspark this
    old is as unusable as a missing one.
    """
