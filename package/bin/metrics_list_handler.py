#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics_list_handler.py  –  Persistent REST handler for the endpoint:

    GET  /servicesNS/-/confluent_addon_for_splunk/confluent_metrics/list

Returns **all** discovered metrics from the KV-store, grouped by the namespace
(the portion of the metric name *before* the first "/").

If a 'metric' parameter is provided, returns details for that specific metric.
If metric=all, returns all metrics with full details in a single call.

Examples:

1. List all metrics (names only):
    curl -k -u admin:XzXz123! \
         "https://127.0.0.1:8089/servicesNS/-/confluent_addon_for_splunk/confluent_metrics/list?output_mode=json"

2. Get specific metric details for a given metric from a dataset:
    curl -k -u admin:XzXz123! \
         "https://127.0.0.1:8089/servicesNS/-/confluent_addon_for_splunk/confluent_metrics/list?output_mode=json&metric=io.confluent.kafka.server/response_bytes&dataset=cloud"

3. Get ALL metrics with full details:
    curl -k -u admin:XzXz123! \
         "https://127.0.0.1:8089/servicesNS/-/confluent_addon_for_splunk/confluent_metrics/list?output_mode=json&metric=all"

Response payload for list all (names only):

{
  "namespaces": {
    "io.confluent.kafka.server": [
      "io.confluent.kafka.server/request_bytes",
      "io.confluent.kafka.server/response_bytes"
    ]
  },
  "total_metrics": 104,
  "metrics_by_status": {
    "enabled": 5,
    "disabled": 99
  }
}

Response payload for specific metric:

{
  "metric": {
    "_key": "6862a815b5ad26917c0d3d71",
    "name": "io.confluent.kafka.server/response_bytes",
    "description": "The delta count of...",
    "type": "COUNTER_INT64",
    "enabled": false,
    ...
  }
}

Response payload for metric=all:

{
  "metrics": [
    {
      "_key": "6862a815b5ad26917c0d3d71",
      "name": "io.confluent.kafka.server/response_bytes",
      "description": "The delta count of...",
      "type": "COUNTER_INT64",
      "enabled": false,
      ...
    },
    ...
  ],
  "total_metrics": 104,
  "metrics_by_status": {
    "enabled": 5,
    "disabled": 99
  },
  "namespaces_summary": {
    "io.confluent.kafka.server": 41,
    "io.confluent.kafka.connect": 22,
    ...
  }
}
"""

from __future__ import annotations

# ────── stdlib ─────────────────────────────────────────────────────────────
import json
import logging
import os
import re
import sys
import traceback
from collections import defaultdict
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

# ────── add-on lib path bootstrap (bin/../lib) ────────────────────────────
BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, '..', 'lib'))
for _p in (BIN_DIR, LIB_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ────── Splunk / 3rd-party libs ────────────────────────────────────────────
from solnlib import splunk_rest_client
from solnlib.log import Logs
from solnlib import conf_manager 
from splunk.persistconn.application import PersistentServerConnectionApplication
from kv_ops import KVStoreManager, wiretap

# ────── Constants ──────────────────────────────────────────────────────────
APPNAME    = "confluent_addon_for_splunk"
COLLECTION = "confluent_metrics_definitions"
OWNER      = "nobody"

# Metric name validation pattern: namespace/metric_name
METRIC_NAME_PATTERN = re.compile(r'^io\.confluent\.[a-z_]+(\.[a-z_]+)*\/[a-z0-9_\/]+$')

# ────── Logging setup ──────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APPNAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("metrics_list_handler")


# ═══════════════════════════════════════════════════════════════════════════
class MetricsListHandler(PersistentServerConnectionApplication):
    """
    Handler class registered via *restmap.conf* :

        [script:metrics_list]
        script          = metrics_list_handler.py
        scripttype      = persist
        python.version  = python3.9
        handler         = metrics_list_handler.MetricsListHandler
        output_modes    = json
        match           = /confluent_metrics/list
        requireAuthentication = true
        passSession     = true
        passHttpHeaders = true
    """

    def __init__(self,
                 command_line: Optional[str] = None,
                 command_arg:  Optional[str] = None):
        super().__init__()
        self.token: Optional[str] = None
        self.app:   Optional[str] = None

        if command_arg:
            try:
                arg = json.loads(command_arg)
                self.token = arg.get("sessionKey") or arg.get("session_key")
                self.app   = arg.get("appName") or arg.get("app")
            except Exception:
                LOG.exception("Bad JSON in command_arg")
        
        LOG.debug("INIT – token=%s  app=%s", bool(self.token), self.app)

    def _validate_metric_name(self, metric_name: str) -> bool:
        """
        Validate metric name format.
        
        Expected format: io.confluent.<service>/metric_name
        Examples:
        - io.confluent.kafka.server/response_bytes
        - io.confluent.kafka.connect/sent_records
        - io.confluent.flink/num_records_in
        """
        if not metric_name:
            return False
        
        # Special case: 'all' is a valid parameter value
        if metric_name.lower() == "all":
            return True
        
        return bool(METRIC_NAME_PATTERN.match(metric_name))

    def _extract_query_params(self, envelope: Dict[str, Any]) -> Dict[str, str]:
        """Extract query parameters from the request envelope."""
        query_params = {}
        
        LOG.debug("Full envelope: %s", envelope)
        
        # Method 1: Handle Splunk's query format - list of [key, value] pairs
        query_data = envelope.get("query", [])
        if query_data:
            LOG.debug("Found query data: %s (type: %s)", query_data, type(query_data))
            
            if isinstance(query_data, list):
                # Handle list of [key, value] pairs: [['metric', 'value'], ['other', 'value2']]
                for item in query_data:
                    if isinstance(item, list) and len(item) >= 2:
                        key, value = item[0], item[1]
                        if isinstance(key, str) and isinstance(value, str):
                            query_params[key] = value
                            LOG.debug("Extracted query param: %s = %s", key, value)
                    elif isinstance(item, tuple) and len(item) >= 2:
                        key, value = item[0], item[1]
                        if isinstance(key, str) and isinstance(value, str):
                            query_params[key] = value
                            LOG.debug("Extracted query param: %s = %s", key, value)
        
        # Method 2: Check form data (fallback)
        form_data = envelope.get("form", {})
        if form_data:
            LOG.debug("Found form data: %s", form_data)
            for key, value in form_data.items():
                if key not in query_params:  # Don't override query params
                    if isinstance(value, list) and value:
                        query_params[key] = value[0]
                    elif isinstance(value, str):
                        query_params[key] = value
        
        # Method 3: Check if query is a string (alternative format)
        if isinstance(query_data, str) and query_data:
            LOG.debug("Query is string format: %s", query_data)
            try:
                parsed = parse_qs(query_data)
                for key, values in parsed.items():
                    if values and key not in query_params:
                        query_params[key] = values[0]
            except Exception as e:
                LOG.warning("Failed to parse query string '%s': %s", query_data, e)
        
        # Method 4: Check query_args directly
        query_args = envelope.get("query_args", {})
        if query_args:
            LOG.debug("Found query_args: %s", query_args)
            for key, value in query_args.items():
                if key not in query_params:
                    if isinstance(value, list) and value:
                        query_params[key] = value[0]
                    elif isinstance(value, str):
                        query_params[key] = value
        
        # Method 5: Check the rest_uri for embedded parameters (last resort)
        rest_uri = envelope.get("rest_uri", "")
        if rest_uri and "?" in rest_uri and not query_params:
            try:
                _, query_part = rest_uri.split("?", 1)
                parsed = parse_qs(query_part)
                for key, values in parsed.items():
                    if values and key not in query_params:
                        query_params[key] = values[0]
            except Exception as e:
                LOG.warning("Failed to parse rest_uri query '%s': %s", rest_uri, e)
        
        LOG.debug("Final extracted query params: %s", query_params)
        return query_params

    def _handle_specific_metric(self, metric_name: str, docs: List[Dict[str, Any]], dataset: Optional[str] = None) -> Dict[str, Any]:
        """Handle request for a specific metric, optionally filtered by dataset."""
        matches = [doc for doc in docs if doc.get("name") == metric_name]
        if dataset:
            matches = [doc for doc in matches if doc.get("dataset") == dataset]
        if len(matches) == 1:
            LOG.info("Found metric details for: %s (dataset: %s)", metric_name, matches[0].get("dataset"))
            return {
                "status": 200,
                "payload": {
                    "metric": matches[0]
                }
            }
        elif len(matches) > 1:
            LOG.warning("Ambiguous metric name: '%s' found in multiple datasets: %s", metric_name, [doc.get("dataset") for doc in matches])
            return {
                "status": 400,
                "payload": {
                    "error": "ambiguous_metric",
                    "message": f"Multiple metrics found with name '{metric_name}'. Specify dataset parameter.",
                    "available_datasets": [doc.get("dataset") for doc in matches],
                    "matching_metrics": [
                        {
                            "name": doc.get("name"),
                            "dataset": doc.get("dataset"),
                            "_key": doc.get("_key")
                        } for doc in matches
                    ]
                }
            }
        else:
            LOG.warning("Metric not found: %s", metric_name)
            return {
                "status": 404,
                "payload": {
                    "error": "metric_not_found",
                    "message": f"Metric doesn't exist: '{metric_name}'"
                }
            }

    def _handle_all_metrics_with_details(self, docs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Handle request for all metrics with full details (metric=all)."""
        
        # Count enabled/disabled metrics and group by namespace for summary
        enabled_count = 0
        disabled_count = 0
        namespace_counts: Dict[str, int] = defaultdict(int)
        
        for doc in docs:
            # Count enabled/disabled
            if doc.get("enabled", False):
                enabled_count += 1
            else:
                disabled_count += 1
            
            # Count by namespace
            metric_name = doc.get("name", "")
            if "/" in metric_name:
                namespace = metric_name.split("/", 1)[0]
                namespace_counts[namespace] += 1

        # Build response payload with all metric details
        payload = {
            "metrics": docs,  # All metrics with full details
            "total_metrics": len(docs),
            "metrics_by_status": {
                "enabled": enabled_count,
                "disabled": disabled_count
            },
            "namespaces_summary": dict(sorted(namespace_counts.items()))
        }
        
        LOG.info("Returning ALL %d metrics with full details (%d enabled, %d disabled)", 
                len(docs), enabled_count, disabled_count)
        
        return {"status": 200, "payload": payload}

    def _handle_list_all_metrics(self, docs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Handle request to list all metrics grouped by namespace (names only)."""
        
        # ── Group by "namespace" (= part before first "/") ────────────────────────
        groups: Dict[str, List[str]] = defaultdict(list)
        enabled_count = 0
        disabled_count = 0
        
        for doc in docs:
            metric_name = doc.get("name")
            if not metric_name:
                LOG.warning("Document found without name field: %s", doc.get("_key", "NO_KEY"))
                continue
            
            # Extract namespace (part before first "/")
            namespace = metric_name.split("/", 1)[0]
            groups[namespace].append(metric_name)
            
            # Count enabled/disabled metrics
            if doc.get("enabled", False):
                enabled_count += 1
            else:
                disabled_count += 1

        # Sort namespaces and metrics within each namespace
        grouped_sorted = {
            ns: sorted(metric_list) 
            for ns, metric_list in sorted(groups.items())
        }

        # Build response payload
        payload = {
            "namespaces": grouped_sorted,
            "total_metrics": len(docs),
            "metrics_by_status": {
                "enabled": enabled_count,
                "disabled": disabled_count
            },
            "namespace_summary": {
                ns: len(metrics) 
                for ns, metrics in grouped_sorted.items()
            }
        }
        
        LOG.info("Returning %d metric names across %d namespaces (%d enabled, %d disabled)", 
                len(docs), len(grouped_sorted), enabled_count, disabled_count)
        
        return {"status": 200, "payload": payload}

    # ------------------------------------------------------------- handle
    def handle(self, raw_request: str) -> Dict[str, Any]:
        """
        Handle GET requests to list all metrics or get specific metric details.
        
        Query parameters:
        - metric: (optional) specific metric name to get details for, or "all" for all details
        
        Returns either:
        1. All metrics grouped by namespace (if no metric parameter) - names only
        2. Specific metric details (if metric parameter with specific name)
        3. All metrics with full details (if metric=all)
        4. Error response (if metric name is invalid or not found)
        """
        try:
            envelope = json.loads(raw_request or "{}")
            if envelope.get("method", "").upper() != "GET":
                return {"status": 405, "payload": {"error": "Only GET supported"}}

            # Extract query parameters
            query_params = self._extract_query_params(envelope)
            metric_name = query_params.get("metric")
            
            LOG.debug("Request - metric parameter: %s", metric_name)

            # ── Early validation: if metric name provided, validate format FIRST ──────
            if metric_name:
                if not self._validate_metric_name(metric_name):
                    LOG.warning("Invalid metric name format (early validation): %s", metric_name)
                    return {
                        "status": 400,
                        "payload": {
                            "error": "metric_name_format_error",
                            "message": f"Invalid metric name format: '{metric_name}'. Expected format: io.confluent.<service>/metric_name or 'all'",
                            "examples": [
                                "io.confluent.kafka.server/response_bytes",
                                "io.confluent.kafka.connect/sent_records",
                                "io.confluent.flink/num_records_in",
                                "all"
                            ]
                        }
                    }
                LOG.debug("Metric name format is valid: %s", metric_name)

            # ── splunkd connection (only after validation passes) ────────────────────
            rest_uri = (
                envelope.get("server", {}).get("rest_uri")
                or os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")
            )
            uri = urlparse(rest_uri)
            scheme, host, port = uri.scheme, uri.hostname, uri.port

            # fallbacks for token / app
            if not self.token:
                self.token = envelope.get("session", {}).get("authtoken")
            if not self.app:
                self.app = envelope.get("ns", {}).get("app") or APPNAME

            if not self.token:
                raise RuntimeError("Missing sessionKey – cannot auth to splunkd")

            try:
                level_name = conf_manager.get_log_level(
                    logger=LOG,
                    session_key=self.token,
                    app_name=self.app,
                    conf_name=f"{self.app}_settings",
                )
                LOG.setLevel(getattr(logging, str(level_name).upper(), logging.INFO))
                LOG.info("Dynamic log level set to %s (%s).", level_name, LOG.getEffectiveLevel())
            except Exception as e:
                LOG.warning("Unable to set dynamic log level: %s", e)
                LOG.setLevel(logging.INFO)

            client = splunk_rest_client.SplunkRestClient(
                session_key=self.token,
                app=self.app,
                owner=OWNER,
                scheme=scheme,
                host=host,
                port=port,
                verify=False,
                retry=True,
                retry_count=3,
            )
            LOG.debug("SplunkRestClient connected to %s:%s app=%s", host, port, self.app)
            
            try:
                wiretap(client)
            except AttributeError:
                LOG.debug("wiretap not available in this context")

            # ── KV-store: get all docs (only after validation) ───────────────────────
            kv_mgr = KVStoreManager(client, COLLECTION)
            docs: List[Dict[str, Any]] = kv_mgr.get_all()
            
            LOG.info("Retrieved %d metrics from KV store", len(docs))

            # ── Route based on metric parameter ───────────────────────────────────────
            if metric_name:
                if metric_name.lower() == "all":
                    # Return all metrics with full details
                    return self._handle_all_metrics_with_details(docs)
                else:
                    # Return specific metric details
                    dataset = query_params.get("dataset")
                    return self._handle_specific_metric(metric_name, docs, dataset)
            else:
                # Return metric names grouped by namespace (lightweight)
                return self._handle_list_all_metrics(docs)

        # ─────── error path ────────────────────────────────────────────────
        except Exception:
            LOG.exception("Failed to process metrics request")
            return {
                "status": 500,
                "payload": {
                    "error": "internal-error",
                    "traceback": traceback.format_exc(),
                },
            }

    # ----------------------------------------------------- unused stream API
    def handleStream(self, handle, _in):
        raise NotImplementedError

    def done(self):
        pass


# ────── CLI test helper ────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Quick manual test:

    export SPLUNK_AUTH_TOKEN='Splunk <token>'
    
    # Test list all metrics (names only):
    python3 metrics_list_handler.py
    
    # Test specific metric:
    METRIC_NAME=io.confluent.kafka.server/response_bytes python3 metrics_list_handler.py
    
    # Test all metrics with details:
    METRIC_NAME=all python3 metrics_list_handler.py
    """
    
    # Check if specific metric was requested via environment variable
    metric_name = os.getenv("METRIC_NAME")
    
    fake_env = {
        "method": "GET",
        "server": {"rest_uri": os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")},
        "session": {"authtoken": os.getenv("SPLUNK_AUTH_TOKEN", "")},
        "ns": {"app": APPNAME},
    }
    
    # Add metric parameter if provided
    if metric_name:
        fake_env["form"] = {"metric": metric_name}
    
    response = MetricsListHandler().handle(json.dumps(fake_env))
    print(json.dumps(response, indent=2))