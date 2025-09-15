#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics_update_handler.py ― Persistent REST handler for:

    PUT  /servicesNS/nobody/confluent_addon_for_splunk/confluent_metrics/update

Allows updating editable fields of a specific metric in the KV store.

Example curl request:

    curl -k -u admin:XzXz123! -X PUT \
         -H "Content-Type: application/json" \
         -d '{
           "metric": "io.confluent.kafka.server/request_bytes",
           "granularity": "PT5M",
           "intervals": ["now-15m|m/now-1m|m"],
           "group_by": ["metric.type"],
           "limit": 500,
           "filter": {"field":"type","op":"EQ","value":"Produce"},
           "enabled": true
         }' \
         "https://127.0.0.1:8089/servicesNS/nobody/confluent_addon_for_splunk/confluent_metrics/update?output_mode=json"

Only editable fields will be updated. Non-editable descriptor fields
(name, type, labels, description, etc.) are ignored if present in the payload.

Response examples:

Success (200):
{
  "updated": "io.confluent.kafka.server/request_bytes",
  "fields_updated": ["granularity", "enabled", "limit"]
}

Error (400 - Invalid format):
{
  "error": "validation_error",
  "message": "Request validation failed",
  "details": ["Invalid granularity format: 'PT99M'. Supported values: PT1M, PT5M, PT15M, PT30M, PT1H, PT4H, PT6H, PT12H, P1D, ALL"]
}

Error (404 - Not found):
{
  "error": "metric_not_found", 
  "message": "Metric doesn't exist: 'io.confluent.kafka.server/nonexistent'"
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
from typing import Any, Dict, List, Optional, Set, Union
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
from solnlib import conf_manager 
from splunk.persistconn.application import PersistentServerConnectionApplication
from kv_ops import KVStoreManager, wiretap

# ────── Constants ──────────────────────────────────────────────────────────
APPNAME    = "confluent_addon_for_splunk"
COLLECTION = "confluent_metrics_definitions"
OWNER      = "nobody"

# Metric name validation pattern (same as metrics_list_handler)
METRIC_NAME_PATTERN = re.compile(r'^io\.confluent\.[a-z_]+(\.[a-z_]+)*\/[a-z0-9_\/]+$')

# Fields that can be modified by users (removed 'aggregation')
EDITABLE_FIELDS: Set[str] = {
    "group_by", 
    "granularity",
    "intervals",
    "limit",
    "filter",
    "enabled",
}

# Valid granularity values according to Confluent Metrics API
VALID_GRANULARITIES: Set[str] = {
    "PT1M",   # 1 minute
    "PT5M",   # 5 minutes
    "PT15M",  # 15 minutes
    "PT30M",  # 30 minutes
    "PT1H",   # 1 hour
    "PT4H",   # 4 hours
    "PT6H",   # 6 hours
    "PT12H",  # 12 hours
    "P1D",    # 1 day
    "ALL"     # All intervals
}

# Valid filter operators
VALID_FILTER_OPS: Set[str] = {"EQ", "GT", "GTE", "AND", "OR", "NOT"}

# Interval pattern validation (simplified)
INTERVAL_PATTERN = re.compile(
    r'^(now|[\d\-T:Z+\-]+)([+\-]\d+[mhd])*(\|[mhd])?/(now|[\d\-T:Z+\-]+)([+\-]\d+[mhd])*(\|[mhd])?$|'
    r'^PT\d+[MH]/now$|'
    r'^now[+\-]\d+[mhd](\|[mhd])?/now(\|[mhd])?$'
)

MAX_INTERVALS = 4

# ────── Logging setup ──────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APPNAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("metrics_update_handler")


# ═══════════════════════════════════════════════════════════════════════════
class MetricsUpdateHandler(PersistentServerConnectionApplication):
    """
    Handler class registered via *restmap.conf* :

        [script:metrics_update]
        script          = metrics_update_handler.py
        scripttype      = persist
        python.version  = python3.9
        handler         = metrics_update_handler.MetricsUpdateHandler
        output_modes    = json
        match           = /confluent_metrics/update
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
        Validate metric name format (same validation as metrics_list_handler).
        
        Expected format: io.confluent.<service>/metric_name
        Examples:
        - io.confluent.kafka.server/response_bytes
        - io.confluent.kafka.connect/sent_records
        - io.confluent.flink/num_records_in
        """
        if not metric_name:
            return False
        
        return bool(METRIC_NAME_PATTERN.match(metric_name))

    def _validate_group_by(self, group_by: Any) -> List[str]:
        """Validate group_by field - should be list of label strings."""
        errors = []
        
        if not isinstance(group_by, list):
            errors.append("Field 'group_by' must be an array of label strings")
            return errors
        
        for i, label in enumerate(group_by):
            if not isinstance(label, str):
                errors.append(f"Field 'group_by[{i}]' must be a string, got {type(label).__name__}")
            elif not label.strip():
                errors.append(f"Field 'group_by[{i}]' cannot be empty string")
        
        return errors

    def _validate_granularity(self, granularity: Any) -> List[str]:
        """Validate granularity field - must be valid ISO-8601 duration or ALL."""
        errors = []
        
        if not isinstance(granularity, str):
            errors.append("Field 'granularity' must be a string")
            return errors
        
        if granularity not in VALID_GRANULARITIES:
            errors.append(
                f"Invalid granularity format: '{granularity}'. "
                f"Supported values: {', '.join(sorted(VALID_GRANULARITIES))}"
            )
        
        return errors

    def _validate_intervals(self, intervals: Any) -> List[str]:
        """Validate intervals field - should be array with exactly one interval string."""
        errors: List[str] = []
        
        if not isinstance(intervals, list):
            errors.append("Field 'intervals' must be an array of ISO‑8601 interval strings")
            return errors

        if not intervals:
            errors.append("Field 'intervals' cannot be empty")
            return errors
        
        if len(intervals) > MAX_INTERVALS:
            errors.append(f"Field 'intervals' may contain at most {MAX_INTERVALS} items")
            return errors
        
        for idx, interval in enumerate(intervals):
            if not isinstance(interval, str):
                errors.append(f"intervals[{idx}] must be a string, got {type(interval).__name__}")
                continue
            if not interval.strip():
                errors.append(f"intervals[{idx}] cannot be empty string")
                continue
            if not INTERVAL_PATTERN.match(interval):
                errors.append(
                    f"intervals[{idx}] has invalid format: '{interval}'. "
                    "Expected ISO‑8601 interval like "
                    "'now-1h|h/now|h' or '2019-12-19T11:00:00Z/2019-12-19T11:05:00Z'"
                )
                
        return errors

    def _validate_filter_recursive(self, filter_obj: Any, path: str = "filter") -> List[str]:
        """Recursively validate filter object structure."""
        errors = []
        
        if not isinstance(filter_obj, dict):
            errors.append(f"Field '{path}' must be an object")
            return errors
        
        if "op" not in filter_obj:
            errors.append(f"Field '{path}' missing required 'op' field")
            return errors
        
        op = filter_obj.get("op")
        if not isinstance(op, str):
            errors.append(f"Field '{path}.op' must be a string")
            return errors
        
        if op not in VALID_FILTER_OPS:
            errors.append(f"Field '{path}.op' has invalid value: '{op}'. Valid operators: {', '.join(sorted(VALID_FILTER_OPS))}")
            return errors
        
        # Validate based on operator type
        if op in ["EQ", "GT", "GTE"]:  # Field Filter
            if "field" not in filter_obj:
                errors.append(f"Field '{path}' with op '{op}' missing required 'field'")
            elif not isinstance(filter_obj["field"], str):
                errors.append(f"Field '{path}.field' must be a string")
            
            if "value" not in filter_obj:
                errors.append(f"Field '{path}' with op '{op}' missing required 'value'")
            elif not isinstance(filter_obj["value"], (str, int)):
                errors.append(f"Field '{path}.value' must be a string or integer")
        
        elif op in ["AND", "OR"]:  # Compound Filter
            if "filters" not in filter_obj:
                errors.append(f"Field '{path}' with op '{op}' missing required 'filters' array")
            elif not isinstance(filter_obj["filters"], list):
                errors.append(f"Field '{path}.filters' must be an array")
            elif not filter_obj["filters"]:
                errors.append(f"Field '{path}.filters' cannot be empty for op '{op}'")
            else:
                for i, sub_filter in enumerate(filter_obj["filters"]):
                    errors.extend(self._validate_filter_recursive(sub_filter, f"{path}.filters[{i}]"))
        
        elif op == "NOT":  # Unary Filter
            if "filter" not in filter_obj:
                errors.append(f"Field '{path}' with op 'NOT' missing required 'filter' object")
            else:
                errors.extend(self._validate_filter_recursive(filter_obj["filter"], f"{path}.filter"))
        
        return errors

    def _validate_filter(self, filter_obj: Any) -> List[str]:
        """Validate filter field - must be valid non-empty filter object structure."""
        errors = []
        
        if filter_obj is None or filter_obj == {}:
            errors.append("Field 'filter' cannot be empty - must be a valid filter object")
            return errors
        
        return self._validate_filter_recursive(filter_obj)

    def _validate_payload(self, payload: Dict[str, Any]) -> tuple[str, Dict[str, Any], List[str]]:
        """
        Validate and extract update payload.
        
        Returns:
            tuple: (metric_name, filtered_updates, validation_errors)
        """
        errors = []
        
        # Extract and validate metric name
        metric_name = payload.get("metric", "").strip()
        if not metric_name:
            errors.append("Missing required field: 'metric'")
        elif not self._validate_metric_name(metric_name):
            errors.append(
                f"Invalid metric name format: '{metric_name}'. "
                f"Expected format: io.confluent.<service>/metric_name"
            )
        
        # Check for deprecated aggregation field
        if "aggregation" in payload:
            LOG.warning("Field 'aggregation' is deprecated and will be ignored")
        
        # Filter only editable fields and validate
        updates = {}
        for key, value in payload.items():
            if key == "metric":
                continue  # Skip the metric name field
            elif key == "aggregation":
                continue  # Skip deprecated aggregation field
            elif key in EDITABLE_FIELDS:
                updates[key] = value
            else:
                LOG.debug("Ignoring non-editable field: %s", key)
        
        if not updates and not errors:
            errors.append(
                f"No editable fields provided. Editable fields are: {sorted(EDITABLE_FIELDS)}"
            )
        
        # Validate specific field types/values
        if "enabled" in updates:
            if not isinstance(updates["enabled"], bool):
                errors.append("Field 'enabled' must be a boolean (true/false)")
        
        if "limit" in updates:
            try:
                limit_val = int(updates["limit"])
                if limit_val < 1:
                    errors.append("Field 'limit' must be a positive integer")
                updates["limit"] = limit_val  # Ensure it's an integer
            except (ValueError, TypeError):
                errors.append("Field 'limit' must be a valid integer")
        
        if "group_by" in updates:
            errors.extend(self._validate_group_by(updates["group_by"]))
        
        if "granularity" in updates:
            errors.extend(self._validate_granularity(updates["granularity"]))
        
        if "intervals" in updates:
            errors.extend(self._validate_intervals(updates["intervals"]))
        
        if "filter" in updates:
            errors.extend(self._validate_filter(updates["filter"]))
        
        return metric_name, updates, errors

    # ------------------------------------------------------------- handle
    def handle(self, raw_request: str) -> Dict[str, Any]:
        """
        Handle PUT requests to update a specific metric's editable fields.
        
        Expected payload:
        {
          "metric": "io.confluent.kafka.server/request_bytes",
          "granularity": "PT5M",
          "enabled": true,
          ...
        }
        """
        try:
            envelope = json.loads(raw_request or "{}")
            if envelope.get("method", "").upper() != "PUT":
                return {"status": 405, "payload": {"error": "Only PUT supported"}}

            LOG.debug("Full envelope: %s", envelope)

            # Parse the request payload - try multiple locations where Splunk might put it
            payload = {}
            
            # Method 1: Check envelope["form"] (most common for Splunk REST handlers)
            form_data = envelope.get("form", {})
            if form_data:
                LOG.debug("Found form data: %s", form_data)
                # Form data might have JSON as string values
                for key, value in form_data.items():
                    if isinstance(value, list) and value:
                        payload[key] = value[0]
                    else:
                        payload[key] = value
            
            # Method 2: Check envelope["payload"] as JSON string
            payload_str = envelope.get("payload", "")
            if payload_str and not payload:
                try:
                    payload = json.loads(payload_str)
                    LOG.debug("Parsed payload from envelope.payload: %s", payload)
                except json.JSONDecodeError as e:
                    LOG.warning("Failed to parse envelope.payload as JSON: %s", e)
            
            # Method 3: Check if the JSON data is in envelope["query"] (form-like data)
            query_data = envelope.get("query", [])
            if query_data and not payload:
                LOG.debug("Trying to extract from query data: %s", query_data)
                temp_payload = {}
                if isinstance(query_data, list):
                    for item in query_data:
                        if isinstance(item, list) and len(item) >= 2:
                            key, value = item[0], item[1]
                            # Try to parse JSON values
                            if isinstance(value, str):
                                try:
                                    # Try parsing as JSON (for complex values)
                                    temp_payload[key] = json.loads(value)
                                except json.JSONDecodeError:
                                    # Treat as string value
                                    temp_payload[key] = value
                            else:
                                temp_payload[key] = value
                if temp_payload:
                    payload = temp_payload
                    LOG.debug("Extracted payload from query data: %s", payload)
            
            # Method 4: For PUT requests, check if entire request body is in envelope root
            if not payload:
                # Sometimes the JSON payload fields are directly in the envelope
                potential_fields = {"metric", "enabled", "granularity", "intervals", "limit", "filter", "group_by"}
                found_fields = {k: v for k, v in envelope.items() if k in potential_fields}
                if found_fields:
                    payload = found_fields
                    LOG.debug("Found payload fields in envelope root: %s", payload)
            
            # If still no payload, try to extract from raw POST body simulation
            if not payload:
                LOG.warning("No payload found in envelope. Available keys: %s", list(envelope.keys()))
                return {
                    "status": 400,
                    "payload": {
                        "error": "missing_payload",
                        "message": "No JSON payload found in request",
                        "debug_info": {
                            "envelope_keys": list(envelope.keys()),
                            "form_data": envelope.get("form", {}),
                            "payload_str": envelope.get("payload", "")[:200] if envelope.get("payload") else None
                        }
                    }
                }

            LOG.debug("Final extracted payload: %s", payload)

            # Validate payload and extract updates (early validation)
            metric_name, updates, validation_errors = self._validate_payload(payload)
            
            if validation_errors:
                LOG.warning("Validation errors for update request: %s", validation_errors)
                return {
                    "status": 400,
                    "payload": {
                        "error": "validation_error",
                        "message": "Request validation failed",
                        "details": validation_errors
                    }
                }

            LOG.debug("Updating metric '%s' with fields: %s", metric_name, list(updates.keys()))

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
                settings_conf = f"{self.app}_settings"
                level_name = conf_manager.get_log_level(
                    logger=LOG,
                    session_key=self.token,
                    app_name=self.app,
                    conf_name=settings_conf,
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

            # ── KV-store operations ───────────────────────────────────────────────────
            kv_mgr = KVStoreManager(client, COLLECTION)
            document_key = payload.get("_key")
            if not document_key:
                LOG.warning("No _key provided in update request for metric: %s", metric_name)
                return {
                    "status": 400,
                    "payload": {
                    "error": "missing_key",
                    "message": "Document _key is required for metric updates"
                    }
                }

            try:
                # Direct document lookup by _key
                target_doc = kv_mgr.find_metric_by_key(document_key)
                LOG.debug("Found metric by _key: %s (name: %s)", document_key, target_doc.get("name"))
                # Validate that the _key matches the expected metric name (optional safety check)
                if target_doc.get("name") != metric_name:
                    LOG.warning("Metric name mismatch: _key '%s' points to '%s', but request was for '%s'", 
                                document_key, target_doc.get("name"), metric_name)
            except RuntimeError as e:
                LOG.warning("Metric lookup by _key failed: %s", str(e))
                return {
                    "status": 404,
                    "payload": {
                        "error": "metric_not_found",
                        "message": f"Metric with _key '{document_key}' not found"
                    }
                }
            LOG.info("Found target metric for update: %s (dataset: %s, _key: %s)",
                    target_doc.get("name"), target_doc.get("dataset"), document_key)

            # Remove deprecated aggregation field if it exists in the document
            if "aggregation" in target_doc:
                del target_doc["aggregation"]
                LOG.info("Removed deprecated 'aggregation' field from metric: %s", metric_name)

            # Apply updates to the document
            updated_fields = []
            for field, new_value in updates.items():
                old_value = target_doc.get(field)
                if old_value != new_value:
                    target_doc[field] = new_value
                    updated_fields.append(field)
                    LOG.debug("Updated %s.%s: %s -> %s", metric_name, field, old_value, new_value)

            if not updated_fields:
                LOG.info("No changes needed for metric: %s", metric_name)
                return {
                    "status": 200,
                    "payload": {
                        "updated": metric_name,
                        "fields_updated": [],
                        "message": "No changes were necessary"
                    }
                }

            # Save the updated document
            kv_mgr.batch_upsert([target_doc])

            LOG.info("Successfully updated metric '%s' with fields: %s", metric_name, updated_fields)
            return {
                "status": 200,
                "payload": {
                    "updated": metric_name,
                    "fields_updated": updated_fields
                }
            }

        # ─────── error path ────────────────────────────────────────────────
        except Exception:
            LOG.exception("Failed to update metric")
            return {
                "status": 500,
                "payload": {
                    "error": "internal_error",
                    "traceback": traceback.format_exc(),
                },
            }

    # ----------------------------------------------------- unused stream API
    def handleStream(self, handle, _in):  # noqa: N802
        raise NotImplementedError

    def done(self):  # noqa: D401
        pass


# ────── CLI test helper ────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Manual test examples:

    export SPLUNK_AUTH_TOKEN='Splunk <token>'
    
    # Test metric update:
    echo '{"metric":"io.confluent.kafka.server/response_bytes","enabled":true,"limit":200,"granularity":"PT5M"}' | \
         python3 metrics_update_handler.py
         
    # Test invalid granularity:
    echo '{"metric":"io.confluent.kafka.server/response_bytes","granularity":"INVALID"}' | \
         python3 metrics_update_handler.py
    """
    stdin_body = sys.stdin.read() or '{"metric":"dummy"}'
    fake_env = {
        "method": "PUT",
        "server": {"rest_uri": os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")},
        "session": {"authtoken": os.getenv("SPLUNK_AUTH_TOKEN", "")},
        "ns": {"app": APPNAME},
        "payload": stdin_body,
    }
    result = MetricsUpdateHandler().handle(json.dumps(fake_env))
    print(json.dumps(result, indent=2))