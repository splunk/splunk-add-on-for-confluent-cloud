#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cloud_metrics_input.py

Modular Input for Confluent Cloud metrics with multi-dataset support and end-to-
end DEBUG logging.
"""

import import_declare_test

# ─────────────────────────── path bootstrap ────────────────────────────
import os, sys
BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, "..", "lib"))
for _p in (BIN_DIR, LIB_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# stdlib
import json, logging, traceback
from datetime import datetime, timezone
from urllib.parse import urlsplit
from typing import List, Dict, Optional, Any

# Splunk libs
from splunklib import modularinput as smi
from solnlib.log import Logs
from solnlib.splunk_rest_client import SplunkRestClient
from solnlib import conf_manager
from solnlib import log

# local libs
import confluent_api
from confluent_api import (
    ConfluentTelemetryClient,
    ConfluentAPIError,
    ConfluentRateLimitError,
    ConfluentValidationError,
)
import kv_ops
from kv_ops import KVStoreManager, wiretap

# ───────────────────────────── constants ───────────────────────────────
APP_NAME       = "confluent_addon_for_splunk"
ACCOUNT_CONF   = "confluent_addon_for_splunk_account"
ACCOUNT_REALM  = f"__REST_CREDENTIAL__#{APP_NAME}#configs/conf-{ACCOUNT_CONF}"
SETTINGS_CONF = f"{APP_NAME}_settings"

# ───────────────────────────── logging  ────────────────────────────────
Logs.set_context(
    directory=os.path.join(os.environ.get("SPLUNK_HOME", "/opt/splunk"), 
                           "var", "log", "splunk"),
    namespace=APP_NAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("cloud_metrics_input")

# ──────────────────────────── helpers ───────────────────────────────────

def _get_account_creds(session_key: str, account: str):
    """Return (api_key, api_secret) decrypted via ConfManager."""
    cm = conf_manager.ConfManager(session_key, APP_NAME, realm=ACCOUNT_REALM)
    conf = cm.get_conf(ACCOUNT_CONF)
    if not conf.stanza_exist(account):
        raise KeyError(f"Account stanza '{account}' not found in {ACCOUNT_CONF}.conf")
    st = conf.get(account)
    return st.get("api_key"), st.get("api_secret")

def extract_interval_parts(interval_str):
    """Extract start and end times from an interval string."""
    if not interval_str or '/' not in interval_str:
        return None, None
    
    parts = interval_str.split('/')
    if len(parts) != 2:
        return None, None
        
    return parts[0].strip(), parts[1].strip()

def parse_iso8601_datetime(datetime_str):
    """
    Parse ISO 8601 datetime string using native Python datetime methods.
    
    This replaces ciso8601.parse_datetime() with native Python functionality
    for Splunk Cloud compatibility.
    
    Args:
        datetime_str: ISO 8601 datetime string (e.g., "2021-02-24T10:00:00Z")
        
    Returns:
        datetime object or None if parsing fails
    """
    if not datetime_str:
        return None
        
    try:
        # Handle common ISO 8601 formats
        # First try: standard format with 'Z' suffix
        if datetime_str.endswith('Z'):
            # Replace 'Z' with '+00:00' for fromisoformat compatibility
            iso_str = datetime_str[:-1] + '+00:00'
            return datetime.fromisoformat(iso_str)
        
        # Second try: use fromisoformat directly (handles +00:00, +01:00, etc.)
        return datetime.fromisoformat(datetime_str)
        
    except ValueError:
        # Fallback: try manual parsing for edge cases
        try:
            # Handle 'Z' suffix with strptime
            if datetime_str.endswith('Z'):
                return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            
            # Handle microseconds with Z
            if 'T' in datetime_str and datetime_str.endswith('Z'):
                return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            
            # Basic ISO format without timezone
            if 'T' in datetime_str and '+' not in datetime_str and '-' not in datetime_str[-6:]:
                return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
                
        except ValueError as e:
            LOG.warning("Failed to parse datetime string '%s': %s", datetime_str, e)
            
    return None

# ───────────────────────── main class ──────────────────────────────────
class CLOUD_METRICS_INPUT(smi.Script):
    def __init__(self) -> None:
        super().__init__()
        LOG.debug("Initialized CLOUD_METRICS_INPUT")

    # ─────────────────── scheme ───────────────────
    def get_scheme(self):
        LOG.debug("Building modular-input scheme")
        s = smi.Scheme("cloud_metrics_input")
        s.description            = "Ingest Confluent Cloud metrics into Splunk."
        s.use_external_validation = True
        s.streaming_mode_xml      = True
        s.use_single_instance     = False

        add = s.add_argument
        # core
        add(smi.Argument("account",                 title="Account",   required_on_create=True))
        add(smi.Argument("datasets",                title="Datasets",  required_on_create=True))
        add(smi.Argument("use_individual_settings", title="Use Individual Settings", required_on_create=True))

        # common settings
        add(smi.Argument("common_settings_resource",    title="Resource"))
        add(smi.Argument("common_settings_filter",      title="Filter"))
        add(smi.Argument("common_settings_granularity", title="Granularity"))
        add(smi.Argument("common_settings_interval",    title="Intervals"))
        add(smi.Argument("common_settings_limit",       title="Limit"))
        return s

    # ─────────────── validate_input ───────────────
    def validate_input(self, definition):
        LOG.debug("Validating modular-input parameters")
        raw_filter = definition.parameters.get("common_settings_filter")
        if raw_filter:
            try:
                json.loads(raw_filter)
            except Exception as ex:
                LOG.error("Invalid JSON in common_settings_filter: %s", ex)
                log.log_configuration_error(LOG, ex)
                raise

    # ───────────────── stream_events ──────────────
    def stream_events(self, inputs: smi.InputDefinition, ew: smi.EventWriter):
        LOG.info("=== cloud_metrics_input run started ===")
        LOG.debug("Input metadata: %s", inputs.metadata)

        sess_key = inputs.metadata.get("session_key")
        srv_uri  = inputs.metadata.get("server_uri")
        if not sess_key or not srv_uri:
            LOG.error("Missing session_key or server_uri - aborting modular-input")
            log.log_configuration_error(LOG, ValueError("Missing session_key or server_uri"))
            return
        level_name = conf_manager.get_log_level(
            logger=LOG,
            session_key=sess_key,
            app_name=APP_NAME,
            conf_name=SETTINGS_CONF,
        )
        LOG.setLevel(getattr(logging, str(level_name).upper(), logging.INFO))
        confluent_api.apply_dynamic_log_level(sess_key)
        kv_ops.apply_dynamic_log_level(sess_key)
        LOG.info("Dynamic log level set to %s (%s).", level_name, LOG.getEffectiveLevel())

        # REST & KV
        uri = urlsplit(srv_uri)
        srest = SplunkRestClient(
            session_key=sess_key, app=APP_NAME, owner="nobody",
            scheme=uri.scheme, host=uri.hostname, port=uri.port,
            verify=False, retry=True, retry_count=3,
        )
        wiretap(srest)
        kv_mgr = KVStoreManager(srest)
        LOG.debug("SplunkRestClient + KVStoreManager initialised")

        metric_defs = kv_mgr.get_all()
        LOG.info("Loaded %d metric definitions", len(metric_defs))
        LOG.debug("Metric names: %s", [m.get("name") for m in metric_defs])

        # ─── per-stanza loop ───
        for stanza, params in inputs.inputs.items():
            LOG.info("── Processing stanza '%s' ──", stanza)
            LOG.debug("Raw params: %s", params)
            try:
                self._run_stanza(stanza, params, metric_defs, kv_mgr, sess_key, ew)
            except Exception as e:
                LOG.error("UNHANDLED EXCEPTION in stanza '%s':\n%s",
                          stanza, traceback.format_exc())
                log.log_exception(LOG, e, "unknown error")

        LOG.info("=== cloud_metrics_input run complete ===")

    # ───────────────── stanza worker ──────────────
    def _run_stanza(
        self,
        stanza_name: str,
        params: dict,
        metric_defs: list,
        kv_mgr: KVStoreManager,
        session_key: str,
        ew: smi.EventWriter,
    ) -> None:

        account         = params.get("account")
        datasets_str    = params.get("datasets", "cloud")
        use_indiv       = str(params.get("use_individual_settings", "0")).lower() in ("1", "true")
        resource_selected = params.get("common_settings_resource")
        target_index    = params.get("index", "_metrics")

        # Parse comma-separated datasets string
        selected_datasets = [d.strip() for d in datasets_str.split(",") if d.strip()]
        if not selected_datasets:
            selected_datasets = ["cloud"]  # Default fallback

        LOG.debug("Parsed params: account=%s datasets=%s use_indiv=%s resource=%s index=%s",
                  account, selected_datasets, use_indiv, resource_selected, target_index)

        # ─── Confluent account credentials ───
        if not account:
            LOG.error("Missing account parameter; skipping stanza '%s'", stanza_name)
            return
        try:
            api_key, api_secret = _get_account_creds(session_key, account)
        except Exception as e:
            LOG.error("Account credential lookup failed: %s", e)
            log.log_configuration_error(LOG, e)
            return
        LOG.debug("Account creds: api_key=%s…", api_key[:4] if api_key else None)
        if not (api_key and api_secret):
            LOG.error("Incomplete credentials - skipping stanza")
            log.log_configuration_error(LOG, ValueError("Incomplete credentials"))
            return

        # ─── metric selection with dataset filtering ───
        to_query: List[Dict[str, Any]] = []
        for md in metric_defs:
            metric_name   = md.get("name")
            metric_dataset = md.get("dataset")

            # Skip metrics without proper metadata
            if not metric_name:
                LOG.warning("Metric definition missing 'name' field, skipping")
                continue
            if not metric_dataset:
                LOG.warning("Metric '%s' missing 'dataset' field, skipping", metric_name)
                continue

            # Filter by selected datasets
            if metric_dataset not in selected_datasets:
                LOG.debug("Skipping metric '%s' - dataset '%s' not in selected datasets: %s",
                          metric_name, metric_dataset, selected_datasets)
                continue

            # Individual settings mode: check if metric is enabled
            if use_indiv and not md.get("enabled", False):
                LOG.debug("Skipping disabled metric: %s (dataset: %s)", metric_name, metric_dataset)
                continue

            # Common settings mode: check resource filter
            if not use_indiv:
                res_set = [r.strip() for r in md.get("resources", "").split(",") if r.strip()]
                if resource_selected and resource_selected not in res_set:
                    LOG.debug("Skipping metric '%s' - resource '%s' not in metric's resources: %s",
                              metric_name, resource_selected, res_set)
                    continue

            to_query.append(md)
            LOG.debug("Selected metric '%s' from dataset '%s'", metric_name, metric_dataset)

        if not to_query:
            LOG.warning("No metrics selected - skipping stanza")
            return

        # Build in-memory lookup from the metric_defs we already loaded
        kv_by_key = {d.get("_key"): d for d in metric_defs if d.get("_key")}
        last_ts_by_key = {k: v.get("last_timestamp") for k, v in kv_by_key.items()}

        # Map KV _key -> metric name for later lookups (event building, logs)
        name_by_key = {md.get("_key"): md.get("name") for md in to_query if md.get("_key")}
        # Log summary by dataset
        metrics_by_dataset = {}
        for md in to_query:
            dataset = md.get("dataset")
            metrics_by_dataset.setdefault(dataset, []).append(md.get("name"))
        for dataset, metric_names in metrics_by_dataset.items():
            LOG.info("Dataset '%s': selected %d metrics: %s",
                     dataset, len(metric_names), ", ".join(metric_names[:5]))

        LOG.info("Querying %d metrics across %d datasets (%s settings)",
                 len(to_query), len(metrics_by_dataset), "individual" if use_indiv else "common")

        # build base common settings (same for every metric)
        common_settings_tpl: Optional[Dict[str, Any]] = None
        if not use_indiv:
            try:
                base_filter = json.loads(params["common_settings_filter"])
            except Exception:
                LOG.error("Invalid JSON in common_settings_filter - skipping stanza")
                return
            
            # Parse original interval to get end boundary
            original_interval = params["common_settings_interval"]
            _, end_time = extract_interval_parts(original_interval)
            if not end_time:
                LOG.warning("Invalid interval format: %s, using as-is", original_interval)
                dynamic_interval = original_interval
            else:
                # Get checkpoints for all metrics being queried
                last_timestamps = {}
                for md in to_query:
                    metric_name = md.get("name")
                    metric_key = md.get("_key")
                    if metric_key:
                        try:
                            checkpoint_ts = last_ts_by_key.get(metric_key)
                            if checkpoint_ts:
                                last_timestamps[metric_name] = checkpoint_ts
                        except Exception as e:
                            LOG.debug("Could not find metric %s: %s", metric_name, str(e))

                # If we have checkpoints, use earliest one for start time
                if last_timestamps:
                     earliest_ts = min(last_timestamps.values())
                     dynamic_interval = f"{earliest_ts}/{end_time}"
                     LOG.debug("Using dynamic interval from checkpoint: %s to %s", earliest_ts, end_time)
                else:
                    # No checkpoints - use original interval
                    dynamic_interval = original_interval
                    LOG.debug("No checkpoints found, using original interval: %s", original_interval)
            
            common_settings_tpl = {
                "granularity": params["common_settings_granularity"],
                "intervals":   [dynamic_interval],
                "filter":      base_filter,
            }
            lim = params.get("common_settings_limit")
            if lim not in (None, ""):
                common_settings_tpl["limit"] = int(lim) if str(lim).isdigit() else lim
            LOG.debug("Common-settings template: %s", common_settings_tpl)

        # ─── query Confluent with multi-dataset support ────────────────────
        results: Dict[str, List[Dict[str, Any]]] = {}
        try:
            with ConfluentTelemetryClient(api_key, api_secret) as cclient:
                if use_indiv:
                    # Individual settings mode: each metric uses its own settings
                    LOG.debug("Using individual metric settings for dataset-aware querying")
                    modified_metric_defs = []
                    for md in to_query:
                        md_copy = md.copy()
                        metric_name = md_copy.get("name")
                        metric_key = md_copy.get("_key")

                        if metric_name and metric_key and "intervals" in md_copy and md_copy["intervals"]:
                            # Get original interval and extract end time
                            original_interval = md_copy["intervals"][0] if isinstance(md_copy["intervals"], list) else md_copy["intervals"]
                            _, end_time = extract_interval_parts(original_interval)

                            # Get checkpoint for this specific metric
                            try:
                                checkpoint_ts = last_ts_by_key.get(metric_key)
                                if checkpoint_ts and end_time:
                                     # Create dynamic interval from checkpoint to original end time
                                     md_copy["intervals"] = [f"{checkpoint_ts}/{end_time}"]
                                     LOG.debug("Metric %s: using dynamic interval %s to %s", 
                                                  metric_name, checkpoint_ts, end_time)
                            except Exception as e:
                                LOG.debug("Could not find metric %s: %s", metric_name, str(e))
                    
                        modified_metric_defs.append(md_copy)

                    # Replace original definitions with modified ones
                    to_query = modified_metric_defs
                    results = cclient.query_metrics_data(
                        metric_definitions=to_query,
                        use_individual_settings=True,
                        format_type="FLAT"
                    )
                else:
                    # Common settings mode: build group_by from metric labels for each metric
                    LOG.debug("Using common settings with metric-specific group_by labels")
                    results = cclient.query_metrics_data(
                        metric_definitions=to_query,
                        use_individual_settings=False,
                        common_settings=common_settings_tpl,
                        format_type="FLAT"
                    )
        except ConfluentRateLimitError as e:
            LOG.error("Rate-limit error: %s", e);
            log.log_server_error(LOG, e)
            return
        except ConfluentValidationError as e:
            LOG.error("Confluent validation error: %s", e)
            log.log_configuration_error(LOG, e)
            return
        
        except ConfluentAPIError as e:
            LOG.error("Confluent API error: %s", e)
            msg = str(e).lower()
            if "401" in msg or "unauthorized" in msg:
                log.log_authentication_error(LOG, e)
            elif "403" in msg or "forbidden" in msg:
                log.log_permission_error(LOG, e)
            elif "http 5" in msg or " 5" in msg:
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "unknown error")
            return

        LOG.info("Confluent returned %d metric groups", len(results))
        total_pts = sum(len(v) for v in results.values())
        LOG.info("Total points fetched: %d", total_pts)
        if not total_pts:
            LOG.info("No data points - nothing to index"); return

        # ─── build Splunk events ───
        events: List[Dict[str, Any]] = []
        for doc_key, pts in results.items():
            metric_name = name_by_key.get(doc_key, doc_key)
            mfield = metric_name.replace("/", ".") if metric_name else "unknown_metric"
            for p in pts:
                # Skip events with invalid or NaN values
                raw_value = p.get("value")
                if raw_value is None:
                    LOG.debug("Skipping point with missing value for metric %s", metric_name)
                    continue
                if (isinstance(raw_value, str) and raw_value.lower() in ("nan", "null", "none", "")) or \
                   (isinstance(raw_value, float) and raw_value != raw_value):
                    LOG.debug("Skipping point with NaN/invalid value for metric %s: %s", metric_name, raw_value)
                    continue

                # Convert value to float for proper metric ingestion
                try:
                    metric_value = float(raw_value)
                    if not (-float('inf') < metric_value < float('inf')):
                        LOG.debug("Skipping point with infinite value for metric %s: %s", metric_name, raw_value)
                        continue
                except (ValueError, TypeError, OverflowError):
                    LOG.debug("Skipping point with non-numeric value for metric %s: %s", metric_name, raw_value)
                    continue

                # Parse timestamp
                ts = p.get("timestamp")
                if not isinstance(ts, str) or not ts:
                    LOG.warning("Missing or invalid timestamp for metric %s, using current time", metric_name)
                    epoch = int(datetime.now(timezone.utc).timestamp())
                else:
                    try:
                        dt = parse_iso8601_datetime(ts)
                        if dt is None:
                            raise ValueError("Unable to parse timestamp")
                        # Should always be tz-aware for Confluent's "Z" format
                        epoch = int(dt.timestamp())
                    except Exception:
                        LOG.warning("Invalid timestamp for metric %s, using current time", metric_name)
                        epoch = int(datetime.now(timezone.utc).timestamp())

                # Build fields dictionary (excluding timestamp and value)
                fields = {k: v for k, v in p.items() if k not in ("timestamp", "value")}

                # Create single-metric event (each metric as a separate event. Multi-metic format support in later releases)
                fields[f"metric_name:{mfield}"] = metric_value
                events.append({
                    "time": epoch,
                    "event": "metric",
                    "source": stanza_name,
                    "sourcetype": "confluent_cloud:metrics",
                    "host": "confluent_cloud",
                    "index": target_index,
                    "fields": fields,
                })

        LOG.info("Built %d Splunk metric events", len(events))
        if events:
            LOG.debug("Example event:\n%s", json.dumps(events[0], indent=2))
        else:
            LOG.warning("No valid metric events created - all values were NaN or invalid")

        # ─── output to Splunk ───
        ok = True
        if events:
            try:
                written = 0
                total = len(events)
                LOG.debug("Streaming %d events to Splunk via provided EventWriter", total)
                def _kv(v):
                    if isinstance(v, str):
                        return '"' + v.replace('\\','\\\\').replace('"','\\"') + '"'
                    return v

                for i, ev in enumerate(events, 1):
                    # build one-line KV string: all fields (dimensions + metric_name:<metric>=<value>)
                    # ev["fields"] already contains keys like "resource.connector.id", "metric_name:my.metric", etc.
                    raw = " ".join(f"{k}={_kv(v)}" for k, v in ev["fields"].items())
                    
                    if i <= 3:
                        LOG.debug("Sample event %d/%d: %s", i, total, raw)
                    
                    evt = smi.Event(
                        data=raw,
                        time=ev["time"],
                        index=ev.get("index", "_metrics"),
                        host=ev["host"],
                        source=ev["source"],
                        sourcetype=ev["sourcetype"],
                        stanza=stanza_name
                    )
                    ew.write_event(evt)
                    written += 1
                
                LOG.info("Successfully wrote %d/%d events via provided EventWriter", written, total)
            
            except Exception as e:
                LOG.error("Failed to write events via provided EventWriter: %s", e)
                LOG.debug("Exception traceback: %s", traceback.format_exc())
                log.log_exception(LOG, e, "write error")
                ok = False
        
        if not ok:
            LOG.error("Event writing failed - skipping checkpoint"); return

        # ─── checkpoint last_timestamp ───
        LOG.info("Updating last_timestamp in KV store")
            
        def _latest_ts(points: List[Dict[str, Any]]) -> Optional[str]:
            ts = [p.get("timestamp") for p in points if p.get("timestamp") is not None]
            ts = [t for t in ts if t is not None]
            return max(ts) if ts else None
        
        if use_indiv:
            # Individual settings: update each metric's timestamp separately
            for md in to_query:
                metric_key = md.get("_key")
                if not metric_key:
                    LOG.warning("Skipping timestamp update for metric with no key: %s", metric_key)
                    continue
                latest = _latest_ts(results.get(metric_key, []))
                if latest:
                    kv_mgr.patch_by_key(metric_key, {"last_timestamp": latest})
                    LOG.debug("Patched '%s' last_timestamp=%s", metric_key, latest)

        else:
            # Common settings: find the single most-recent point across all metrics
            ts_candidates = [_latest_ts(pts) for pts in results.values()]
            ts_candidates = [ts for ts in ts_candidates if ts]
            if ts_candidates:
                max_ts = max(ts_candidates)
                for md in to_query:
                    metric_key = md.get("_key")
                    if metric_key:
                        kv_mgr.patch_by_key(metric_key, {"last_timestamp": max_ts})
                LOG.debug("Patched common last_timestamp=%s for %d queried metrics", max_ts, len(to_query))
            else:
                LOG.warning("No valid timestamps returned - KV checkpoint unchanged")

        log.events_ingested(
            LOG,
            stanza_name,                     # full input name like cloud_metrics_input://<name>
            "confluent_cloud:metrics",       # sourcetype used for the events
            len(events),                     # number of events ingested
            target_index,                    # index where events were written
            account=account,                 # account context (adds visibility in dashboard)
            host="confluent_cloud"           # host used for events (adds visibility)
        )

        LOG.info("✓ stanza '%s' complete - %d events indexed", stanza_name, len(events))

# ───────────────────────────── entrypoint ──────────────────────────────
if __name__ == "__main__":
    sys.exit(CLOUD_METRICS_INPUT().run(sys.argv))