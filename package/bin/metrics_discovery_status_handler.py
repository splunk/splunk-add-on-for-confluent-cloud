#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics_discovery_status_handler.py ― Persistent REST handler for:

    GET /servicesNS/-/confluent_addon_for_splunk/confluent_metrics/discovery_status

Returns the latest metrics discovery session statistics, persisted in KV store.
"""

from __future__ import annotations

# ────── stdlib ─────────────────────────────────────────────────────────────
import json
import logging
import os
import sys
import traceback
from urllib.parse import urlparse
# ────── add-on lib path bootstrap (bin/../lib) ────────────────────────────
BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, '..', 'lib'))
for _p in (BIN_DIR, LIB_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ────── Splunk / 3rd-party libs ────────────────────────────────────────────
from solnlib import splunk_rest_client
from solnlib.log import Logs
from splunk.persistconn.application import PersistentServerConnectionApplication
from kv_ops import KVStoreManager

# ────── Constants ──────────────────────────────────────────────────────────
APPNAME    = "confluent_addon_for_splunk"
COLLECTION = "confluent_metrics_discovery_status"
OWNER      = "nobody"

# ────── Logging setup ──────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APPNAME,
    log_level=logging.DEBUG,
)
LOG = Logs().get_logger("metrics_discovery_status_handler")


class MetricsDiscoveryStatusHandler(PersistentServerConnectionApplication):
    """
    Handles GET /confluent_metrics/discovery_status
    """
    def __init__(self, command_line, command_arg):
        super().__init__()
        self.command_line = command_line
        self.command_arg = command_arg
        LOG.debug("Handler started, cmd=%s arg=%s", command_line, command_arg)

    def handle(self, raw_request: str) -> dict:
        try:
            envelope = json.loads(raw_request or "{}")
            rest_uri = (
                envelope.get("server", {}).get("rest_uri")
                or os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")
            )
            uri = urlparse(rest_uri)
            scheme, host, port = uri.scheme, uri.hostname, uri.port

            token = envelope.get("session", {}).get("authtoken")
            app = envelope.get("ns", {}).get("app") or APPNAME

            if not token:
                raise RuntimeError("Missing sessionKey – cannot auth to splunkd")

            client = splunk_rest_client.SplunkRestClient(
                session_key=token,
                app=app,
                owner=OWNER,
                scheme=scheme,
                host=host,
                port=port,
                verify=False,
                retry=True,
                retry_count=3,
            )
            LOG.debug("SplunkRestClient connected to %s:%s app=%s", host, port, app)

            kv_mgr = KVStoreManager(client, COLLECTION)
            docs = kv_mgr.get_all()
            LOG.debug("Fetched %d discovery status docs", len(docs))

            # Return the latest document (assume only one doc in collection)
            if docs:
                return {
                    "status": 200,
                    "payload": docs[0]
                }
            else:
                return {
                    "status": 404,
                    "payload": {"error": "not_found", "message": "No discovery status found"}
                }
        except Exception:
            LOG.exception("Error in MetricsDiscoveryStatusHandler")
            return {
                "status": 500,
                "payload": {"error": "internal_error", "traceback": traceback.format_exc()}
            }


# ────── CLI test helper ────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Quick manual test:

    export SPLUNK_AUTH_TOKEN='Splunk <token>'
    python3 metrics_discovery_status_handler.py
    """
    fake_env = {
        "method": "GET",
        "server": {"rest_uri": os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")},
        "session": {"authtoken": os.getenv("SPLUNK_AUTH_TOKEN", "")},
        "ns": {"app": APPNAME},
        "payload": json.dumps({}),
    }
    response = MetricsDiscoveryStatusHandler("", "").handle(json.dumps(fake_env))
    print(json.dumps(response, indent=2))