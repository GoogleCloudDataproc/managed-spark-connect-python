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
