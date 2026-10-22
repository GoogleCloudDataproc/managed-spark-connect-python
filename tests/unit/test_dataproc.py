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

import datetime
import os
import threading
import time
from unittest import mock

import pytest
from google.api_core.client_options import ClientOptions
from google.auth import credentials as google_auth_credentials
from google.cloud.managed_spark_connect import session as session_mod
from google.cloud.managed_spark_connect.client import proxy as proxy_mod
from google.cloud.managed_spark_connect.instrumentation import (
    InMemorySink,
    Instrumentation,
)


@pytest.fixture(autouse=True)
def _reset_caches():
    session_mod.clear_dataproc_caches()
    yield
    session_mod.clear_dataproc_caches()


class _FakeRealCredentials:
    """Minimal stand-in for google.auth.credentials.Credentials with bool .valid."""

    def __init__(self, valid: bool = False, token: str | None = None):
        self.valid = valid
        self.token = token
        self.refresh_calls = []

    def refresh(self, request):
        self.refresh_calls.append(request)
        self.valid = True
        self.token = "refreshed-bearer-token"


class _RealSubclassedCredentials(google_auth_credentials.Credentials):
    """Concrete subclass of google.auth.credentials.Credentials for expiry lifecycle testing."""

    def __init__(self):
        super().__init__()
        self.refresh_calls = []

    def refresh(self, request):
        self.refresh_calls.append(request)
        self.token = f"token-{len(self.refresh_calls)}"
        self.expiry = datetime.datetime.now(datetime.timezone.utc).replace(
            tzinfo=None
        ) + datetime.timedelta(hours=1)


def _set_unpatched_client_cls(monkeypatch, cls):
    monkeypatch.setattr(session_mod.dataproc_v1, "SessionControllerClient", cls)
    monkeypatch.setattr(session_mod, "SessionControllerClient", cls)
    monkeypatch.setattr(session_mod, "_ORIG_SESSION_CONTROLLER_CLIENT", cls)


def test_get_default_credentials_caches_by_scopes_and_env(monkeypatch):
    calls = []
    fake_creds = _FakeRealCredentials(valid=True, token="tok")

    def fake_default(scopes=None):
        calls.append(tuple(scopes))
        return fake_creds, "proj-1"

    monkeypatch.setattr(proxy_mod.googleauth, "default", fake_default)

    c1, p1 = proxy_mod.get_default_credentials()
    c2, p2 = proxy_mod.get_default_credentials()
    assert c1 is fake_creds and c2 is fake_creds
    assert p1 == "proj-1" and p2 == "proj-1"
    assert calls == [proxy_mod._DEFAULT_SCOPES]

    # Distinct scopes tuple populates a separate cache key.
    custom_scopes = ["https://www.googleapis.com/auth/userinfo.email"]
    proxy_mod.get_default_credentials(scopes=custom_scopes)
    proxy_mod.get_default_credentials(scopes=custom_scopes)
    assert len(calls) == 2

    # Changing GOOGLE_CLOUD_PROJECT invalidates the cache key.
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "other-proj")
    proxy_mod.get_default_credentials()
    assert len(calls) == 3

    # Clearing caches forces a fresh lookup.
    session_mod.clear_dataproc_caches()
    proxy_mod.get_default_credentials()
    assert len(calls) == 4


def test_get_default_credentials_bypasses_cache_when_side_effect_set():
    m1 = (mock.Mock(), "p1")
    m2 = (mock.Mock(), "p2")
    with mock.patch("google.auth.default", side_effect=[m1, m2]) as mock_def:
        assert proxy_mod.get_default_credentials() == m1
        assert proxy_mod.get_default_credentials() == m2
        assert mock_def.call_count == 2
        assert proxy_mod._CREDENTIALS_CACHE == {}


def test_get_refreshed_credentials_skips_refresh_when_valid_real_creds(
    monkeypatch,
):
    creds = _FakeRealCredentials(valid=False, token=None)

    def fake_default(scopes=None):
        return creds, "proj"

    monkeypatch.setattr(proxy_mod.googleauth, "default", fake_default)

    req_mod = mock.Mock()
    monkeypatch.setattr(proxy_mod, "googleauthrequests", req_mod)
    inst = Instrumentation([InMemorySink()])

    # First call: valid=False, token=None -> refreshes once and sets valid=True.
    out1 = proxy_mod.get_refreshed_credentials(recorder=inst.recorder)
    assert out1 is creds
    assert creds.valid is True
    assert creds.token == "refreshed-bearer-token"
    assert len(creds.refresh_calls) == 1

    # Second call: valid=True (bool) and token is non-empty -> skips refresh,
    # but still records both auth.default and auth.refresh spans.
    out2 = proxy_mod.get_refreshed_credentials(recorder=inst.recorder)
    assert out2 is creds
    assert len(creds.refresh_calls) == 1

    span_names = [r["name"] for r in inst.records if r["type"] == "span"]
    assert span_names == [
        "auth.default",
        "auth.refresh",
        "auth.default",
        "auth.refresh",
    ]


def test_real_subclassed_credentials_refresh_expiry_lifecycle(monkeypatch):
    creds = _RealSubclassedCredentials()
    monkeypatch.setattr(
        proxy_mod.googleauth, "default", lambda scopes=None: (creds, "proj")
    )
    monkeypatch.setattr(proxy_mod, "googleauthrequests", mock.Mock())

    # 1. Initially unrefreshed -> refreshes once.
    proxy_mod.get_refreshed_credentials()
    assert len(creds.refresh_calls) == 1
    assert creds.token == "token-1"

    # 2. Still valid -> skips refresh.
    proxy_mod.get_refreshed_credentials()
    assert len(creds.refresh_calls) == 1

    # 3. Expire the token -> refreshes again.
    creds.expiry = datetime.datetime.now(datetime.timezone.utc).replace(
        tzinfo=None
    ) - datetime.timedelta(minutes=5)
    proxy_mod.get_refreshed_credentials()
    assert len(creds.refresh_calls) == 2
    assert creds.token == "token-2"

    # 4. Credentials with expiry=None (e.g. UserAccessTokenCredentials) always refresh.
    creds.expiry = None
    proxy_mod.get_refreshed_credentials()
    assert len(creds.refresh_calls) == 3


def test_get_refreshed_credentials_evicts_cache_on_refresh_failure(monkeypatch):
    bad_creds = _FakeRealCredentials(valid=False, token=None)

    def failing_refresh(_request):
        raise RuntimeError("refresh failed")

    bad_creds.refresh = failing_refresh
    monkeypatch.setattr(
        proxy_mod.googleauth, "default", lambda scopes=None: (bad_creds, "proj")
    )
    monkeypatch.setattr(proxy_mod, "googleauthrequests", mock.Mock())

    with pytest.raises(RuntimeError, match="refresh failed"):
        proxy_mod.get_refreshed_credentials()
    assert proxy_mod._CREDENTIALS_CACHE == {}


@pytest.mark.parametrize(
    "valid,token",
    [
        (False, "existing-token"),
        (True, None),
        (True, ""),
    ],
)
def test_get_refreshed_credentials_refreshes_when_invalid_or_missing_token(
    monkeypatch, valid, token
):
    creds = _FakeRealCredentials(valid=valid, token=token)

    def fake_default(scopes=None):
        return creds, "proj"

    monkeypatch.setattr(proxy_mod.googleauth, "default", fake_default)
    monkeypatch.setattr(proxy_mod, "googleauthrequests", mock.Mock())

    proxy_mod.get_refreshed_credentials()
    assert len(creds.refresh_calls) == 1


def test_get_refreshed_credentials_refreshes_mock_credentials(monkeypatch):
    """unittest.mock.Mock(token='tok') has a Mock .valid (not bool) and must refresh."""
    mock_creds = mock.Mock(token="tok")
    req_mod = mock.Mock()
    monkeypatch.setattr(proxy_mod, "googleauthrequests", req_mod)
    with mock.patch("google.auth.default", return_value=(mock_creds, None)):
        out = proxy_mod.get_refreshed_credentials()
    assert out is mock_creds
    mock_creds.refresh.assert_called_once_with(req_mod.Request.return_value)


def test_create_session_controller_client_caches_by_options_when_unpatched(
    monkeypatch,
):
    fake_creds = _FakeRealCredentials(valid=True, token="tok")
    default_calls = []

    def fake_default(scopes=None):
        default_calls.append(scopes)
        return fake_creds, "proj"

    created_clients = []

    class FakeClient:

        def __init__(self, client_options=None, credentials=None):
            self.client_options = client_options
            self.credentials = credentials
            created_clients.append(self)

    monkeypatch.setattr(proxy_mod.googleauth, "default", fake_default)
    _set_unpatched_client_cls(monkeypatch, FakeClient)

    opts_a1 = ClientOptions(api_endpoint="us-central1-dataproc.googleapis.com")
    opts_a2 = ClientOptions(api_endpoint="us-central1-dataproc.googleapis.com")
    opts_b = ClientOptions(api_endpoint="us-east4-dataproc.googleapis.com")
    opts_dict = {"api_endpoint": "us-central1-dataproc.googleapis.com"}
    opts_custom_creds = ClientOptions(
        api_endpoint="us-central1-dataproc.googleapis.com",
        credentials_file="/path/to/key.json",
    )

    c1 = session_mod._create_session_controller_client(client_options=opts_a1)
    c2 = session_mod._create_session_controller_client(client_options=opts_a2)
    c_dict = session_mod._create_session_controller_client(
        client_options=opts_dict
    )
    c3 = session_mod._create_session_controller_client(client_options=opts_b)
    c4 = session_mod._create_session_controller_client(client_options=None)
    c5 = session_mod._create_session_controller_client(
        client_options=opts_custom_creds
    )

    assert c1 is c2
    assert c1 is c_dict
    assert c1 is not c3
    assert c1 is not c4
    assert c1 is not c5
    assert c1.credentials is fake_creds
    assert c3.credentials is fake_creds
    # Custom credentials_file omits shared ADC credentials to avoid DuplicateCredentialArgs.
    assert c5.credentials is None
    assert len(created_clients) == 4
    assert len(default_calls) == 1


@pytest.mark.parametrize(
    "patch_target",
    [
        "google.cloud.dataproc_v1.SessionControllerClient",
        "google.cloud.managed_spark_connect.session.SessionControllerClient",
    ],
)
def test_create_session_controller_client_bypasses_cache_and_omits_creds_when_patched(
    patch_target,
):
    opts = ClientOptions(api_endpoint="us-central1-dataproc.googleapis.com")
    with mock.patch(patch_target) as mock_cls:
        mock_cls.side_effect = [mock.Mock(name="c1"), mock.Mock(name="c2")]
        c1 = session_mod._create_session_controller_client(client_options=opts)
        c2 = session_mod._create_session_controller_client(client_options=opts)
        assert c1 is not c2
        assert mock_cls.call_count == 2
        mock_cls.assert_called_with(client_options=opts)
        assert session_mod._CLIENT_CACHE == {}


def test_thread_safety_under_concurrent_callers(monkeypatch):
    creds = _FakeRealCredentials(valid=False, token=None)
    default_count = 0
    client_count = 0
    counter_lock = threading.Lock()

    def slow_default(scopes=None):
        nonlocal default_count
        time.sleep(0.01)
        with counter_lock:
            default_count += 1
        return creds, "proj"

    class SlowClient:

        def __init__(self, client_options=None, credentials=None):
            nonlocal client_count
            time.sleep(0.01)
            with counter_lock:
                client_count += 1
            self.client_options = client_options
            self.credentials = credentials

    monkeypatch.setattr(proxy_mod.googleauth, "default", slow_default)
    monkeypatch.setattr(proxy_mod, "googleauthrequests", mock.Mock())
    _set_unpatched_client_cls(monkeypatch, SlowClient)

    barrier = threading.Barrier(16)
    results_client = [None] * 16
    results_creds = [None] * 16
    opts = ClientOptions(api_endpoint="us-east4-dataproc.googleapis.com")

    def worker(idx: int):
        barrier.wait()
        results_creds[idx] = proxy_mod.get_refreshed_credentials()
        results_client[idx] = session_mod._create_session_controller_client(
            client_options=opts
        )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert default_count == 1
    assert len(creds.refresh_calls) == 1
    assert client_count == 1
    assert len({id(c) for c in results_client}) == 1
    assert all(c is creds for c in results_creds)
