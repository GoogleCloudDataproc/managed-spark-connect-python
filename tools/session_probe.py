#!/usr/bin/env python3
"""Standalone Managed Spark session-start / endpoint-reachability probe.

Does NOT import google.cloud.managed_spark_connect. Uses google-auth,
google-cloud-dataproc, socket/ssl, websockets (and optionally pyspark).

Modes:
  trial   create N sessions, wait ACTIVE, probe endpoint in layers, terminate
  host    probe an existing endpoint host (for cross-host checks on a stuck session)

Output: JSONL (one record per trial / per probe round) + human log on stderr.
"""
import argparse
import base64
import concurrent.futures
import datetime as dt
import json
import os
import platform
import random
import re
import socket
import ssl
import string
import subprocess
import sys
import threading
import time
import traceback
import uuid as uuidlib

import google.auth
import google.auth.transport.requests
from google.api_core import client_options as client_options_lib
from google.api_core import exceptions as core_exceptions
from google.cloud import dataproc_v1
from google.protobuf import duration_pb2
import websockets
import websockets.sync.client as wsclient

BRIDGE_PATH = "tcp-over-websocket-bridge/35218cb7-1201-4940-89e8-48d8f03fed96"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
_log_lock = threading.Lock()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def log(msg):
    with _log_lock:
        sys.stderr.write(f"{now()} {msg}\n")
        sys.stderr.flush()


class Creds:
    def __init__(self):
        self.creds, self.project = google.auth.default(scopes=SCOPES)
        self._lock = threading.Lock()

    def token(self, force=False):
        with self._lock:
            if force or not (self.creds.valid and self.creds.token):
                self.creds.refresh(google.auth.transport.requests.Request())
            return self.creds.token


# --------------------------------------------------------------------------
# Layered probes
# --------------------------------------------------------------------------


def probe_dns(host):
    t0 = time.perf_counter()
    rec = {"layer": "dns", "host": host}
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        addrs = []
        for fam, _, _, _, sa in infos:
            addrs.append({"family": "v6" if fam == socket.AF_INET6 else "v4", "addr": sa[0]})
        rec["addrs"] = addrs
        rec["ok"] = True
    except Exception as e:
        rec["ok"] = False
        rec["error"] = f"{type(e).__name__}: {e}"
    rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
    # dig for TTL / CNAME chain (best effort)
    try:
        p = subprocess.run(["dig", "+noall", "+answer", "+tries=1", "+time=3", host, "A"], capture_output=True, text=True, timeout=5)
        rec["dig"] = p.stdout.strip().splitlines()
    except Exception as e:
        rec["dig_error"] = f"{type(e).__name__}: {e}"
    return rec


def probe_tcp_tls(host, addr, family, timeout=10.0, alpn=("http/1.1",)):
    rec = {"layer": "tcp_tls", "addr": addr, "family": family}
    t0 = time.perf_counter()
    sock = None
    try:
        sock = socket.socket(socket.AF_INET6 if family == "v6" else socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((addr, 443))
        rec["tcp_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        rec["local"] = sock.getsockname()[0]
        ctx = ssl.create_default_context()
        if alpn:
            ctx.set_alpn_protocols(list(alpn))
        t1 = time.perf_counter()
        ss = ctx.wrap_socket(sock, server_hostname=host)
        rec["tls_ms"] = round((time.perf_counter() - t1) * 1000, 1)
        rec["tls_version"] = ss.version()
        rec["cipher"] = ss.cipher()[0] if ss.cipher() else None
        rec["alpn"] = ss.selected_alpn_protocol()
        cert = ss.getpeercert()
        rec["cert_subject"] = dict(x[0] for x in cert.get("subject", ()))
        rec["cert_not_after"] = cert.get("notAfter")
        sans = [v for k, v in cert.get("subjectAltName", ()) if k == "DNS"]
        rec["cert_sans"] = sans[:5] + (["..."] if len(sans) > 5 else [])
        rec["ok"] = True
        return rec, ss
    except Exception as e:
        rec["ok"] = False
        rec["error"] = f"{type(e).__name__}: {e}"
        rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return rec, None


def probe_raw_upgrade(host, ss, token, read_timeout=30.0, label="raw_upgrade"):
    """Send an HTTP/1.1 WebSocket upgrade over an established TLS socket."""
    rec = {"layer": label}
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        f"GET /{BRIDGE_PATH} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        f"User-Agent: session-probe/{websockets.__version__}\r\n"
    )
    if token:
        req += f"Authorization: Bearer {token}\r\n"
    req += "\r\n"
    t0 = time.perf_counter()
    try:
        ss.settimeout(read_timeout)
        ss.sendall(req.encode())
        buf = b""
        first = None
        while b"\r\n\r\n" not in buf and len(buf) < 65536:
            chunk = ss.recv(4096)
            if first is None:
                first = time.perf_counter()
            if not chunk:
                rec["eof_after_bytes"] = len(buf)
                break
            buf += chunk
        rec["ttfb_ms"] = round((first - t0) * 1000, 1) if first else None
        rec["total_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        head = buf.split(b"\r\n\r\n", 1)[0].decode("latin1")
        lines = head.split("\r\n")
        rec["status_line"] = lines[0] if lines else None
        hdrs = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                hdrs[k.strip().lower()] = v.strip()
        rec["headers"] = hdrs
        try:
            rec["status"] = int(lines[0].split()[1])
        except Exception:
            rec["status"] = None
        body = buf.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in buf else b""
        rec["body_prefix"] = body[:200].decode("latin1")
        rec["ok"] = rec["status"] == 101
    except Exception as e:
        rec["ok"] = False
        rec["error"] = f"{type(e).__name__}: {e}"
        rec["total_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    finally:
        try:
            ss.close()
        except Exception:
            pass
    return rec


def probe_ws_connect(host, token, open_timeout=30):
    """websockets.sync.client.connect with the same args as connect_tcp_bridge."""
    rec = {"layer": "ws_connect", "websockets": websockets.__version__}
    t0 = time.perf_counter()
    try:
        conn = wsclient.connect(
            f"wss://{host}/{BRIDGE_PATH}",
            additional_headers={"Authorization": f"Bearer {token}"},
            open_timeout=open_timeout,
            close_timeout=3,
            compression=None,
            max_size=64 * 1024 * 1024,
            max_queue=None,
            ping_timeout=60,
        )
        rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
        try:
            rec["peer"] = conn.socket.getpeername()[0]
            rec["local"] = conn.socket.getsockname()[0]
        except Exception:
            pass
        rec["response_status"] = getattr(getattr(conn, "response", None), "status_code", None)
        rec["ok"] = True
        try:
            conn.close()
        except Exception:
            pass
    except Exception as e:
        rec["ok"] = False
        rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
        rec["error"] = f"{type(e).__name__}: {e}"
        resp = getattr(e, "response", None)
        if resp is not None:
            rec["response_status"] = getattr(resp, "status_code", None)
            try:
                rec["response_headers"] = dict(resp.headers)
            except Exception:
                pass
    return rec


class MiniBridge:
    """Minimal local TCP -> hex-over-WebSocket bridge (a re-implementation of
    the library's bridged_socket, used only to run a real gRPC RPC)."""

    def __init__(self, host, token_fn):
        self.host = host
        self.token_fn = token_fn
        self.srv = socket.create_server(("127.0.0.1", 0))
        self.port = self.srv.getsockname()[1]
        self.errors = []
        self.bytes_c2s = 0
        self.bytes_s2c = 0
        self.conns = 0
        self._stop = False
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while not self._stop:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            self.conns += 1
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            ws = wsclient.connect(
                f"wss://{self.host}/{BRIDGE_PATH}",
                additional_headers={"Authorization": f"Bearer {self.token_fn()}"},
                open_timeout=30, close_timeout=3, compression=None,
                max_size=64 * 1024 * 1024, max_queue=None, ping_timeout=60,
            )
        except Exception as e:
            self.errors.append(f"{type(e).__name__}: {e}")
            conn.close()
            return

        def c2s():
            try:
                while True:
                    b = conn.recv(65536)
                    if not b:
                        break
                    self.bytes_c2s += len(b)
                    ws.send(b.hex())
            except Exception:
                pass
            finally:
                try:
                    ws.close()
                except Exception:
                    pass

        def s2c():
            try:
                while True:
                    m = ws.recv()
                    b = bytes.fromhex(m)
                    self.bytes_s2c += len(b)
                    conn.sendall(b)
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        threading.Thread(target=c2s, daemon=True).start()
        s2c()

    def close(self):
        self._stop = True
        try:
            self.srv.close()
        except Exception:
            pass


def probe_grpc_inproc(host, session_uuid, token_fn, timeout=90):
    """Run a real Spark Connect RPC through MiniBridge via pyspark (this process)."""
    rec = {"layer": "grpc"}
    t0 = time.perf_counter()
    bridge = MiniBridge(host, token_fn)
    try:
        from pyspark.sql.connect.session import SparkSession as CSession

        result = {}

        def run():
            try:
                t = time.perf_counter()
                spark = CSession.builder.remote(f"sc://localhost:{bridge.port}/;session_id={session_uuid}").create()
                result["version"] = spark.version
                result["version_ms"] = round((time.perf_counter() - t) * 1000, 1)
                result["count"] = spark.range(10).count()
                try:
                    spark.client.close()
                except Exception:
                    pass
            except BaseException as e:
                result["error"] = f"{type(e).__name__}: {str(e)[:300]}"

        th = threading.Thread(target=run, daemon=True)
        th.start()
        th.join(timeout)
        if th.is_alive():
            rec["ok"] = False
            rec["error"] = f"timeout after {timeout}s"
        else:
            rec.update(result)
            rec["ok"] = "error" not in result
        rec["bridge_errors"] = bridge.errors[:5]
        rec["bridge_bytes_c2s"] = bridge.bytes_c2s
        rec["bridge_bytes_s2c"] = bridge.bytes_s2c
        rec["bridge_conns"] = bridge.conns
    finally:
        bridge.close()
    rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return rec


def probe_grpc(host, session_uuid, token_fn, timeout=90):
    """Run the gRPC probe in a fresh interpreter: several pyspark Connect
    clients in one process (parallel trials) were seen to wedge each other,
    which must not be mistaken for a service problem."""
    t0 = time.perf_counter()
    cmd = [sys.executable, os.path.abspath(__file__), "grpc-once", host, session_uuid, "--timeout", str(timeout)]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
        last = [ln for ln in p.stdout.splitlines() if ln.startswith("{")]
        rec = json.loads(last[-1]) if last else {"layer": "grpc", "ok": False, "error": f"no result (rc={p.returncode}): {p.stderr[-300:]}"}
    except subprocess.TimeoutExpired:
        rec = {"layer": "grpc", "ok": False, "error": f"subprocess timeout after {timeout + 30}s"}
    rec["ms"] = round((time.perf_counter() - t0) * 1000, 1)
    rec["subprocess"] = True
    return rec


def classify_stall(rnd):
    """Which layer failed in a probe round (IPv6 failures ignored if IPv4 worked)."""
    layers = rnd["layers"]
    dns = next((l for l in layers if l["layer"] == "dns"), None)
    if dns is not None and not dns["ok"]:
        return "dns"
    tcp = [l for l in layers if l["layer"] == "tcp_tls"]
    v4 = [l for l in tcp if l["family"] == "v4"]
    if tcp and not any(l["ok"] for l in tcp):
        return "tcp_tls:" + (v4[0].get("error", "?") if v4 else tcp[0].get("error", "?"))
    up = next((l for l in layers if l["layer"] == "raw_upgrade"), None)
    if up is not None and not up.get("ok"):
        return f"upgrade:{up.get('status') or up.get('error')}"
    ws = next((l for l in layers if l["layer"] == "ws_connect"), None)
    if ws is not None and not ws["ok"]:
        return f"ws:{ws.get('error')}"
    g = next((l for l in layers if l["layer"] == "grpc"), None)
    if g is not None and not g["ok"]:
        return f"grpc:{g.get('error')} c2s={g.get('bridge_bytes_c2s')} s2c={g.get('bridge_bytes_s2c')}"
    return "unknown"


def long_upgrade(host, token, wait_s=150):
    """Send one Upgrade request and wait up to wait_s for whatever comes back."""
    dns = probe_dns(host)
    v4 = [a for a in dns.get("addrs", []) if a["family"] == "v4"]
    if not v4:
        return {"error": "no v4 addr"}
    rec, ss = probe_tcp_tls(host, v4[0]["addr"], "v4")
    if ss is None:
        return rec
    return probe_raw_upgrade(host, ss, token, read_timeout=wait_s, label="raw_upgrade_long")


def probe_round(host, creds, session_uuid=None, do_grpc=False, families=("v4", "v6"), fresh_token=True):
    out = {"t": now(), "layers": []}
    dns = probe_dns(host)
    out["layers"].append(dns)
    token = creds.token(force=fresh_token)
    for a in dns.get("addrs", []):
        if a["family"] not in families:
            continue
        rec, ss = probe_tcp_tls(host, a["addr"], a["family"])
        out["layers"].append(rec)
        if ss is not None:
            out["layers"].append(probe_raw_upgrade(host, ss, token))
    # raw upgrade without token, v4 only (expect 401/403 quickly if frontend alive)
    for a in dns.get("addrs", []):
        if a["family"] == "v4":
            rec, ss = probe_tcp_tls(host, a["addr"], a["family"])
            if ss is not None:
                r = probe_raw_upgrade(host, ss, None, label="raw_upgrade_no_token")
                out["layers"].append(r)
            break
    out["layers"].append(probe_ws_connect(host, token))
    if do_grpc and session_uuid:
        out["layers"].append(probe_grpc(host, session_uuid, lambda: creds.token()))
    return out


def summarize_round(r):
    parts = []
    for l in r["layers"]:
        if l["layer"] == "dns":
            parts.append("dns=" + ("ok(" + ",".join(a["addr"] for a in l.get("addrs", [])) + ")" if l["ok"] else "FAIL:" + l.get("error", "")))
        elif l["layer"] == "tcp_tls":
            parts.append(f"tls[{l['addr']}]=" + (f"ok({l.get('tcp_ms')}/{l.get('tls_ms')}ms,{l.get('alpn')})" if l["ok"] else "FAIL:" + l.get("error", "")))
        elif l["layer"].startswith("raw_upgrade"):
            parts.append(f"{l['layer']}=" + (f"{l.get('status')}({l.get('ttfb_ms')}ms)" if "status" in l else "FAIL:" + l.get("error", "")))
        elif l["layer"] == "ws_connect":
            parts.append("ws=" + (f"ok({l['ms']}ms)" if l["ok"] else f"FAIL({l['ms']}ms):" + l.get("error", "")))
        elif l["layer"] == "grpc":
            parts.append("grpc=" + (f"ok(v{l.get('version')},{l['ms']}ms)" if l["ok"] else f"FAIL:{l.get('error')} bridge={l.get('bridge_errors')}"))
    return " ".join(parts)


# --------------------------------------------------------------------------
# Session lifecycle
# --------------------------------------------------------------------------


def session_client(region):
    opts = client_options_lib.ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com")
    return dataproc_v1.SessionControllerClient(client_options=opts)


def gen_id(prefix):
    prefix = re.sub(r"[^a-z0-9-]", "-", prefix.lower()).strip("-")[:20] or "probe"
    return f"{prefix}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')}-{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}"


def label_safe(value):
    """Lowercase, [a-z0-9_-], max 63 chars (Dataproc label value rules)."""
    return re.sub(r"[^a-z0-9_-]", "-", value.lower())[:63].strip("-")


def build_session(args, run_id):
    s = dataproc_v1.Session()
    s.runtime_config.version = args.runtime_version
    s.spark_connect_session = dataproc_v1.SparkConnectConfig()
    ec = s.environment_config.execution_config
    ec.ttl = duration_pb2.Duration(seconds=args.ttl)
    ec.idle_ttl = duration_pb2.Duration(seconds=args.idle_ttl)
    if args.subnet:
        ec.subnetwork_uri = args.subnet
    if args.service_account:
        ec.service_account = args.service_account
        ec.authentication_config.user_workload_authentication_type = (
            dataproc_v1.AuthenticationConfig.AuthenticationType.SERVICE_ACCOUNT
        )
    elif args.auth_type:
        ec.authentication_config.user_workload_authentication_type = (
            dataproc_v1.AuthenticationConfig.AuthenticationType[args.auth_type]
        )
    for kv in args.prop or []:
        k, v = kv.split("=", 1)
        s.runtime_config.properties[k] = v
    s.labels["session-probe-run"] = label_safe(run_id)
    s.labels["dataproc-session-client"] = "session-probe"
    return s


def trial(args, creds, run_id, idx, out_fh):
    region = args.region
    client = session_client(region)
    parent = f"projects/{args.project}/locations/{region}"
    sid = gen_id(args.prefix)
    name = f"{parent}/sessions/{sid}"
    rec = {
        "run_id": run_id, "trial": idx, "region": region, "session_id": sid, "host": platform.node(),
        "os": platform.platform(), "python": sys.version.split()[0], "websockets": websockets.__version__,
        "delay_s": args.delay, "t_create_req": now(),
    }
    t0 = time.perf_counter()
    op = None
    try:
        try:
            op = client.create_session(parent=parent, session=build_session(args, run_id), session_id=sid)
        except core_exceptions.GoogleAPICallError as e:
            rec["t_create_err"] = now()
            rec["outcome"] = "503" if isinstance(e, core_exceptions.ServiceUnavailable) else "create_failed"
            rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
            return rec
        rec["t_create_ack"] = now()
        rec["create_ack_s"] = round(time.perf_counter() - t0, 1)
        log(f"[{idx}] created {sid} in {region}")
        # Poll GetSession until ACTIVE (don't wait on the LRO; mirror the library)
        deadline = time.monotonic() + args.active_timeout
        resp = None
        endpoint_seen_at = None
        while time.monotonic() < deadline:
            resp = client.get_session(name=name)
            st = resp.state.name
            ep = dict(resp.runtime_info.endpoints).get("Spark Connect Server") if resp.runtime_info else None
            if ep and endpoint_seen_at is None:
                endpoint_seen_at = now()
                rec["t_endpoint_seen"] = endpoint_seen_at
                rec["endpoint_seen_state"] = st
            if st == "ACTIVE":
                break
            if st in ("FAILED", "TERMINATING", "TERMINATED"):
                rec["outcome"] = "create_failed"
                rec["error"] = f"state={st} msg={resp.state_message[:300]}"
                return rec
            time.sleep(2)
        else:
            rec["outcome"] = "slow_create"
            rec["error"] = f"not ACTIVE after {args.active_timeout}s (state={resp.state.name if resp else None})"
            return rec
        rec["t_active"] = now()
        rec["create_to_active_s"] = round(time.perf_counter() - t0, 1)
        rec["uuid"] = resp.uuid
        url = dict(resp.runtime_info.endpoints)["Spark Connect Server"]
        host = url.split("://", 1)[-1].split("/")[0].split(":")[0]
        rec["endpoint"] = url
        rec["endpoint_host"] = host
        log(f"[{idx}] {sid} ACTIVE after {rec['create_to_active_s']}s, endpoint {host}")
        if args.delay:
            time.sleep(args.delay)
        # Probe rounds
        rounds = []
        rec["rounds"] = rounds
        first_ok = None
        start = time.monotonic()
        n = 0
        while True:
            n += 1
            r = probe_round(host, creds, resp.uuid, do_grpc=args.grpc, families=tuple(args.families.split(",")))
            r["n"] = n
            rounds.append(r)
            ws_ok = any(l["layer"] == "ws_connect" and l["ok"] for l in r["layers"])
            grpc_ok = any(l["layer"] == "grpc" and l["ok"] for l in r["layers"])
            log(f"[{idx}] {sid} round {n}: {summarize_round(r)}")
            success = grpc_ok if args.grpc else ws_ok
            if success and first_ok is None:
                first_ok = time.monotonic() - start
                rec["first_ok_s"] = round(first_ok, 1)
                rec["first_ok_round"] = n
            elapsed = time.monotonic() - start
            if first_ok is not None and not args.keep_probing:
                break
            if elapsed >= args.probe_duration:
                break
            time.sleep(args.interval)
        if first_ok is None:
            rec["outcome"] = "active_unreachable"
            rec["stall"] = classify_stall(rounds[-1])
            # Capture what the frontend eventually says to an Upgrade request
            # when websockets gave up after 30 s.
            if rec["stall"].startswith("ws") or rec["stall"].startswith("upgrade"):
                rec["long_upgrade"] = long_upgrade(host, creds.token())
            log(f"[{idx}] {sid} ACTIVE_UNREACHABLE stall={rec['stall']} (keep={args.keep_unreachable})")
            if args.keep_unreachable:
                rec["kept"] = True
                return rec
        else:
            rec["outcome"] = "ok" if first_ok <= args.ok_deadline else "slow_connect"
        return rec
    except Exception as e:
        rec["outcome"] = "other"
        rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        rec["traceback"] = traceback.format_exc()[-1500:]
        return rec
    finally:
        rec["t_end"] = now()
        if not rec.get("kept") and op is not None:
            try:
                client.terminate_session(name=name)
                rec["terminated"] = True
            except Exception as e:
                rec["terminate_error"] = f"{type(e).__name__}: {str(e)[:200]}"
        with _log_lock:
            out_fh.write(json.dumps(rec, default=str) + "\n")
            out_fh.flush()
        log(f"[{idx}] {sid} outcome={rec.get('outcome')} {rec.get('error','')}")


def sweep(args, run_id=None):
    """List sessions created by this probe (any run) still alive."""
    client = session_client(args.region)
    parent = f"projects/{args.project}/locations/{args.region}"
    alive = []
    req = dataproc_v1.ListSessionsRequest(parent=parent, filter="state = ACTIVE OR state = CREATING")
    for s in client.list_sessions(request=req):
        if s.labels.get("dataproc-session-client") == "session-probe":
            alive.append((s.name.split("/")[-1], s.state.name, s.labels.get("session-probe-run")))
    return alive


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    t = sub.add_parser("trial")
    t.add_argument("--project", default="datagravity-demo")
    t.add_argument("--region", default="us-east4")
    t.add_argument("--trials", type=int, default=1)
    t.add_argument("--parallel", type=int, default=1)
    t.add_argument("--prefix", default="probe")
    t.add_argument("--runtime-version", default="3.0")
    t.add_argument("--ttl", type=int, default=1800)
    t.add_argument("--idle-ttl", type=int, default=600)
    t.add_argument("--subnet", default=None)
    t.add_argument("--service-account", default=None)
    t.add_argument("--auth-type", default=None, help="END_USER_CREDENTIALS or SERVICE_ACCOUNT")
    t.add_argument("--prop", action="append")
    t.add_argument("--delay", type=float, default=0, help="seconds between ACTIVE and first probe")
    t.add_argument("--interval", type=float, default=10)
    t.add_argument("--probe-duration", type=float, default=540)
    t.add_argument("--ok-deadline", type=float, default=120)
    t.add_argument("--active-timeout", type=float, default=600)
    t.add_argument("--grpc", action="store_true")
    t.add_argument("--families", default="v4,v6")
    t.add_argument("--keep-probing", action="store_true")
    t.add_argument("--keep-unreachable", action="store_true")
    t.add_argument("--out", required=True)
    h = sub.add_parser("host")
    h.add_argument("host")
    h.add_argument("--uuid", default=None)
    h.add_argument("--grpc", action="store_true")
    h.add_argument("--rounds", type=int, default=1)
    h.add_argument("--interval", type=float, default=10)
    h.add_argument("--families", default="v4,v6")
    h.add_argument("--out", default=None)
    sw = sub.add_parser("sweep")
    sw.add_argument("--project", default="datagravity-demo")
    sw.add_argument("--region", default="us-east4")
    sw.add_argument("--terminate", action="store_true")
    g1 = sub.add_parser("grpc-once")
    g1.add_argument("host")
    g1.add_argument("uuid")
    g1.add_argument("--timeout", type=float, default=90)
    args = ap.parse_args()

    creds = Creds()
    if args.mode == "grpc-once":
        rec = probe_grpc_inproc(args.host, args.uuid, lambda: creds.token(), timeout=args.timeout)
        print(json.dumps(rec, default=str))
        return
    if args.mode == "host":
        fh = open(args.out, "a") if args.out else None
        for i in range(args.rounds):
            r = probe_round(args.host, creds, args.uuid, do_grpc=args.grpc, families=tuple(args.families.split(",")))
            r["probe_host"] = platform.node()
            log(f"round {i+1}: {summarize_round(r)}")
            print(json.dumps(r, default=str))
            if fh:
                fh.write(json.dumps(r, default=str) + "\n"); fh.flush()
            if i + 1 < args.rounds:
                time.sleep(args.interval)
        return
    if args.mode == "sweep":
        alive = sweep(args)
        print(json.dumps(alive))
        if args.terminate and alive:
            client = session_client(args.region)
            for sid, _, _ in alive:
                client.terminate_session(name=f"projects/{args.project}/locations/{args.region}/sessions/{sid}")
                log(f"terminated {sid}")
        return

    run_id = f"{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')}-{platform.node().split('.')[0]}"
    log(f"run_id={run_id} project={args.project} region={args.region} trials={args.trials} parallel={args.parallel} delay={args.delay} grpc={args.grpc}")
    pre = sweep(args)
    if pre:
        log(f"WARNING: pre-existing probe sessions alive: {pre}")
    with open(args.out, "a") as fh:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as ex:
            futs = [ex.submit(trial, args, creds, run_id, i, fh) for i in range(args.trials)]
            results = [f.result() for f in futs]
    import collections
    c = collections.Counter(r.get("outcome") for r in results)
    log(f"SUMMARY run_id={run_id}: {dict(c)}")
    post = sweep(args)
    kept = [r["session_id"] for r in results if r.get("kept")]
    leaked = [s for s in post if s[0] not in kept]
    if leaked:
        log(f"LEAK CHECK: still alive and not intentionally kept: {leaked}")
    else:
        log("LEAK CHECK: clean" + (f" (kept for cross-host probing: {kept})" if kept else ""))


if __name__ == "__main__":
    main()
