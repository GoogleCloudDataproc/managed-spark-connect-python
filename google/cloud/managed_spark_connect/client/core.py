# Copyright 2024 Google LLC
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

import google
import grpc
from pyspark.sql.connect.client import DefaultChannelBuilder

from . import proxy

logger = logging.getLogger(__name__)


class ManagedSparkChannelBuilder(DefaultChannelBuilder):
    """
    This is a helper class that is used to create a GRPC channel based on the given
    connection string per the documentation of Spark Connect.

    This implementation of ChannelBuilder uses `secure_authorized_channel` from the
    `google.auth.transport.grpc` package for authenticating secure channel.

    Examples
    --------
    >>> cb =  ChannelBuilder("sc://localhost")
    ... cb.endpoint

    >>> cb = ChannelBuilder("sc://localhost/;use_ssl=true;token=aaa")
    ... cb.secure
    True
    """

    def __init__(self, url, is_active_callback=None):
        self._is_active_callback = is_active_callback
        super().__init__(url)

    def toChannel(self) -> grpc.Channel:
        """
        Applies the parameters of the connection string and creates a new
        GRPC channel according to the configuration. Passes optional channel options to
        construct the channel.

        Returns
        -------
        GRPC Channel instance.
        """
        # TODO: Replace with a direct channel once all compatibility issues with
        # grpc have been resolved.
        return self._proxied_channel()

    def _proxied_channel(self) -> grpc.Channel:
        return ProxiedChannel(self.host, self._is_active_callback)

    def _direct_channel(self) -> grpc.Channel:
        destination = f"{self.host}:{self.port}"

        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        # Get an HTTP request function to refresh credentials.
        request = google.auth.transport.requests.Request()
        # Create a channel.

        return google.auth.transport.grpc.secure_authorized_channel(
            credentials,
            request,
            destination,
            None,
            None,
            options=self._channel_options,
        )


class ProxiedChannel(grpc.Channel):

    def __init__(self, target_host, is_active_callback):
        self._is_active_callback = is_active_callback
        self._proxy = proxy.ManagedSparkSessionProxy(0, target_host)
        self._proxy.start()
        self._proxied_connect_url = f"sc://localhost:{self._proxy.port}"
        self._wrapped = DefaultChannelBuilder(
            self._proxied_connect_url
        ).toChannel()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        ret = self._wrapped.__exit__(*args)
        self._proxy.stop()
        return ret

    def close(self):
        ret = self._wrapped.close()
        self._proxy.stop()
        return ret

    def _wrap_method(self, wrapped_method):
        if self._is_active_callback is None:
            return wrapped_method

        def checked_method(*margs, **mkwargs):
            if (
                self._is_active_callback is not None
                and not self._is_active_callback()
            ):
                logger.warning(f"Session is no longer active")
                raise RuntimeError(
                    "Session not active. Please create a new session"
                )
            return wrapped_method(*margs, **mkwargs)

        return checked_method

    def stream_stream(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.stream_stream(*args, **kwargs))

    def stream_unary(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.stream_unary(*args, **kwargs))

    def subscribe(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.subscribe(*args, **kwargs))

    def unary_stream(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.unary_stream(*args, **kwargs))

    def unary_unary(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.unary_unary(*args, **kwargs))

    def unsubscribe(self, *args, **kwargs):
        return self._wrap_method(self._wrapped.unsubscribe(*args, **kwargs))
