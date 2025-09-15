#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics_refresh_handler.py ― Persistent REST handler for:

    POST  /servicesNS/-/confluent_addon_for_splunk/confluent_metrics/refresh

It performs a **full refresh** of Confluent Cloud metric-descriptors:

1. Reads the first account stanza (or the stanza supplied in the JSON body).
2. Calls Confluent Metrics V2 API (via `ConfluentMetricsClient`) to fetch
   *GA* metric-descriptors + resource-descriptors from both 'cloud' and 'cloud-custom' datasets.
3. Merges the results with the existing KV-store documents, **preserving
   editable fields** set by admins.
4. Bulk-upserts the collection *confluent_metrics_definitions*.
5. Responds with JSON statistics.

The handler is wired in *restmap.conf* as:

    [script:metrics_refresh]
    script          = metrics_refresh_handler.py
    scripttype      = persist
    python.version  = python3.9
    handler         = metrics_refresh_handler.MetricsRefreshHandler
    output_modes    = json
    match           = /confluent_metrics/refresh
    requireAuthentication = true
    passSession     = true
    passHttpHeaders = true
"""

from __future__ import annotations

# ────── stdlib ─────────────────────────────────────────────────────────────
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

# ────── add-on lib path bootstrap (bin/../lib) ────────────────────────────
BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, '..', 'lib'))
for _p in (BIN_DIR, LIB_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ────── Splunk / 3rd-party libs ────────────────────────────────────────────
from solnlib import splunk_rest_client, conf_manager
from solnlib.log import Logs
from solnlib import conf_manager 
from splunk.persistconn.application import PersistentServerConnectionApplication
from kv_ops import KVStoreManager, wiretap
from confluent_api import ConfluentTelemetryClient

# ────── Constants ──────────────────────────────────────────────────────────
APPNAME    = "confluent_addon_for_splunk"
COLLECTION = "confluent_metrics_definitions"
DISCOVERY_STATUS_COLLECTION = "confluent_metrics_discovery_status"
OWNER      = "nobody"

# ────── Logging setup ──────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APPNAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("metrics_refresh_handler")


def build_prefixed_labels(metric_labels: List[str], resource_labels_by_type: Dict[str, List[str]]) -> str:
    """
    Build properly prefixed labels string for storage.
    
    Args:
        metric_labels: List of metric label keys (e.g., ['topic', 'partition'])
        resource_labels_by_type: Dict mapping resource types to their label keys
                                 (e.g., {'kafka': ['id', 'cluster'], 'connector': ['id']})
    
    Returns:
        Comma-separated string of prefixed labels
        (e.g., 'metric.topic,metric.partition,resource.kafka.id,resource.kafka.cluster')
    """
    all_labels = []
    
    # Add metric labels with 'metric.' prefix
    for label_key in metric_labels:
        prefixed_label = f"metric.{label_key}"
        all_labels.append(prefixed_label)
        LOG.debug("Added metric label: %s", prefixed_label)
    
    # Add resource labels with 'resource.' prefix (labels already include type)
    for resource_type, label_keys in resource_labels_by_type.items():
        for label_key in label_keys:
            # Resource labels already include the type (e.g., 'kafka.id', 'connector.id')
            # Just add 'resource.' prefix, don't duplicate the type
            prefixed_label = f"resource.{label_key}"
            all_labels.append(prefixed_label)
            LOG.debug("Added resource label: %s (from type: %s)", prefixed_label, resource_type)
    
    # Sort and deduplicate
    unique_labels = sorted(set(all_labels))
    labels_string = ",".join(unique_labels)
    
    LOG.debug("Final prefixed labels: %s", labels_string)
    return labels_string


# ═══════════════════════════════════════════════════════════════════════════
class MetricsRefreshHandler(PersistentServerConnectionApplication):
    """
    Handles POST /confluent_metrics/refresh
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


        LOG.debug("INIT – token=%s  app=%s", bool(self.token), self.app)

    # ------------------------------------------------------------- handle
    def handle(self, raw_request: str) -> Dict[str, Any]:
        """
        POST body (optional):

            { "account": "my_account" }
        """
        try:
            envelope = json.loads(raw_request or "{}")
            if envelope.get("method", "").upper() != "POST":
                return {"status": 405, "payload": {"error": "Only POST supported"}}

            body = {}
            if envelope.get("payload"):
                try:
                    body = json.loads(envelope["payload"])
                except Exception:
                    LOG.warning("Payload is not valid JSON – ignored")

            # ── splunkd connection ---------------------------------------------------------
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

            # ── pick account stanza --------------------------------------------------------
            cmgr = conf_manager.ConfManager(
                self.token,
                self.app or APPNAME,
                realm=f"__REST_CREDENTIAL__#{self.app}#configs/conf-{self.app}_account",
            )
            all_accounts = cmgr.get_conf(f"{self.app}_account").get_all()
            if not all_accounts:
                raise RuntimeError("No account stanzas configured")

            acct_name = body.get("account") or next(iter(all_accounts.keys()))
            if acct_name not in all_accounts:
                raise RuntimeError(f"Account '{acct_name}' not found")

            acct      = all_accounts[acct_name]
            api_key   = acct["api_key"]
            api_sec   = acct["api_secret"]
            api_root  = acct.get("api_root", "https://api.telemetry.confluent.cloud")

            LOG.info("Refreshing metrics with account=%s", acct_name)

            # ── Confluent Metrics API for both datasets ───────────────────────────────
            cmc = ConfluentTelemetryClient(api_key, api_sec, base_url=api_root)
            
            # Fetch from both 'cloud' and 'cloud-custom' datasets
            datasets = ["cloud", "cloud-custom"]
            all_metrics_ga = []
            all_resources = {}  # dataset -> resource_type -> [label_keys]
            
            for dataset in datasets:
                LOG.info("Fetching metrics and resources from dataset: %s", dataset)
                
                # Fetch metrics for this dataset
                dataset_metrics = [
                    m for m in cmc.fetch_metrics(dataset)
                    if m.get("lifecycle_stage") == "GENERAL_AVAILABILITY"
                ]
                
                # Add dataset field to each metric
                for metric in dataset_metrics:
                    metric["dataset"] = dataset
                
                all_metrics_ga.extend(dataset_metrics)
                LOG.info("Fetched %d GA metrics from dataset '%s'", len(dataset_metrics), dataset)
                
                # Fetch resources for this dataset
                dataset_resources = cmc.fetch_resources(dataset)
                
                # Build resource mapping for this dataset: resource_type -> [label_keys]
                dataset_res_map = {}
                for r in dataset_resources:
                    resource_type = r["type"]
                    label_keys = [l["key"] for l in r.get("labels", [])]
                    dataset_res_map[resource_type] = label_keys
                    LOG.debug("Dataset '%s' resource type '%s' has labels: %s", 
                             dataset, resource_type, label_keys)
                
                all_resources[dataset] = dataset_res_map
                LOG.info("Fetched %d resource types from dataset '%s'", len(dataset_res_map), dataset)
            
            LOG.info("Total fetched: %d GA metrics across %d datasets", len(all_metrics_ga), len(datasets))

            # ── current KV docs -----------------------------------------------------------
            kv_mgr = KVStoreManager(client, COLLECTION)
            current_docs = kv_mgr.get_all()
            
            # Build mapping using composite key: (name, dataset) (not _key since it's auto-generated)
            current = {}
            for doc in current_docs:
                metric_name = doc.get("name")
                dataset = doc.get("dataset", "cloud")  # Default to 'cloud' for backward compatibility
                if metric_name:
                    composite_key = (metric_name, dataset)
                    current[composite_key] = doc
                    LOG.debug("Found existing metric: %s (dataset: %s)", metric_name, dataset)
                else:
                    LOG.warning("Document found without name field: %s", doc.get("_key", "NO_KEY"))

            LOG.debug("Found %d existing documents in KV store", len(current))

            # ── Metric classification analysis ─────────────────────────────────────────
            existing_metric_keys: Set[tuple] = set(current.keys())
            current_ga_metric_keys: Set[tuple] = {(m["name"], m["dataset"]) for m in all_metrics_ga}
            
            # Classify metrics
            new_metrics: Set[tuple] = current_ga_metric_keys - existing_metric_keys
            potentially_updatable: Set[tuple] = current_ga_metric_keys & existing_metric_keys
            deprecated_metrics: Set[tuple] = existing_metric_keys - current_ga_metric_keys
            
            # Debug logging for metric classification
            LOG.debug("=== METRIC CLASSIFICATION ANALYSIS ===")
            LOG.debug("Total existing metrics in KV store: %d", len(existing_metric_keys))
            LOG.debug("Total GA metrics from Confluent API: %d", len(current_ga_metric_keys))

            if new_metrics:
                LOG.debug("NEW metrics (%d): %s", len(new_metrics), 
                        [f"{name} ({dataset})" for name, dataset in sorted(new_metrics)])
            else:
                LOG.debug("NEW metrics: None")
                
            if potentially_updatable:
                LOG.debug("POTENTIALLY UPDATABLE metrics (%d): %s", len(potentially_updatable), 
                        [f"{name} ({dataset})" for name, dataset in sorted(potentially_updatable)])
            else:
                LOG.debug("POTENTIALLY UPDATABLE metrics: None")
                
            if deprecated_metrics:
                LOG.debug("DEPRECATED/REMOVED metrics (%d): %s", len(deprecated_metrics), 
                        [f"{name} ({dataset})" for name, dataset in sorted(deprecated_metrics)])
            else:
                LOG.debug("DEPRECATED/REMOVED metrics: None")
            
            LOG.debug("=== END METRIC CLASSIFICATION ===")

            new_docs: List[Dict[str, Any]] = []
            inserted = updated = unchanged = 0

            for meta in all_metrics_ga:
                name    = meta["name"]
                desc    = meta.get("description", "")
                typ     = meta.get("type", "")
                unit    = meta.get("unit", "")
                stage   = meta.get("lifecycle_stage", "")
                rtypes  = meta.get("resources", [])
                res     = ",".join(rtypes)
                dataset = meta["dataset"]  # Now included in each metric

                # ── Build properly prefixed labels using dataset-specific resources ──────
                LOG.debug("Processing labels for metric: %s (dataset: %s)", name, dataset)
                
                # Extract metric labels (add 'metric.' prefix)
                metric_labels = [l["key"] for l in meta.get("labels", [])]
                LOG.debug("Raw metric labels: %s", metric_labels)
                
                # Extract resource labels by type using dataset-specific resource map
                resource_labels_by_type = {}
                dataset_res_map = all_resources.get(dataset, {})
                for resource_type in rtypes:
                    if resource_type in dataset_res_map:
                        resource_labels_by_type[resource_type] = dataset_res_map[resource_type]
                        LOG.debug("Dataset '%s' resource type '%s' labels: %s", 
                                 dataset, resource_type, dataset_res_map[resource_type])
                    else:
                        LOG.warning("Resource type '%s' not found in dataset '%s' resource map for metric '%s'", 
                                   resource_type, dataset, name)
                
                # Build the final prefixed labels string
                labels = build_prefixed_labels(metric_labels, resource_labels_by_type)
                
                LOG.debug("Final labels for metric '%s' (dataset: %s): %s", name, dataset, labels)

                composite_key = (name, dataset)
                existing = current.get(composite_key)
                if existing:
                    # Only check frequently changing fields
                    api_fields_changed = (
                        existing.get("description") != desc or
                        existing.get("lifecycle_stage") != stage or
                        existing.get("labels") != labels
                    )
                    
                    if api_fields_changed:
                        # Build document for update (preserve _key and editable fields)
                        doc = {
                            # Preserve existing _key
                            "_key":            existing.get("_key"),
                            "name":            name,
                            "description":     desc,
                            "type":            typ,
                            "unit":            unit,
                            "lifecycle_stage": stage,
                            "resources":       res,
                            "labels":          labels,
                            "dataset":         dataset,  # New field
                            # Preserve existing editable fields
                            "group_by":        existing.get("group_by", ""),
                            "granularity":     existing.get("granularity", "PT1M"),
                            "intervals":       existing.get("intervals", "now-6m|m/now-1m|m"),
                            "limit":           existing.get("limit", 100),
                            "filter":          existing.get("filter", ""),
                            "enabled":         existing.get("enabled", False),
                        }
                        
                        updated += 1
                        new_docs.append(doc)
                        
                        # Log which specific field(s) changed for debugging
                        changed_fields = []
                        if existing.get("description") != desc:
                            changed_fields.append("description")
                        if existing.get("lifecycle_stage") != stage:
                            changed_fields.append("lifecycle_stage")
                        if existing.get("labels") != labels:
                            changed_fields.append("labels")
                            LOG.debug("Labels changed for '%s' (dataset: %s): old='%s' new='%s'", 
                         name, dataset, existing.get("labels", ""), labels)
                        if existing.get("dataset") != dataset:
                            changed_fields.append("dataset")
                            LOG.debug("Dataset changed for '%s': old='%s' new='%s'", 
                         name, existing.get("dataset", ""), dataset)
                        LOG.debug("Metric '%s' changed fields: %s", name, ", ".join(changed_fields))
                    else:
                        unchanged += 1
                        LOG.debug("Metric '%s' (dataset: %s) unchanged", name, dataset)
                else:
                    # Create new document (let Splunk auto-generate _key)
                    doc = {
                        "name":            name,
                        "description":     desc,
                        "type":            typ,
                        "unit":            unit,
                        "lifecycle_stage": stage,
                        "resources":       res,
                        "labels":          labels,
                        "dataset":         dataset,
                        # editable defaults
                        "group_by":        "",
                        "granularity":     "PT1M",
                        "intervals":       "now-6m|m/now-1m|m",
                        "limit":           100,
                        "filter":          "",
                        "enabled":         False,
                    }
                    inserted += 1
                    new_docs.append(doc)
                    LOG.debug("New metric '%s' (dataset: %s) with labels: %s", name, dataset, labels)

            # Only call batch_upsert if there are actual changes
            if inserted > 0 or updated > 0:
                kv_mgr.batch_upsert(new_docs)
                LOG.info("KV store updated with %d documents", len(new_docs))
            else:
                LOG.info("No changes detected, skipping KV store update")

            payload = {
                "inserted":     inserted,
                "updated":      updated,
                "unchanged":    unchanged,
                "total_ga":     len(all_metrics_ga),
                "deprecated":   len(deprecated_metrics),
                "last_run":     datetime.now(timezone.utc).isoformat(),
            }
            
            discovery_status_mgr = KVStoreManager(client, DISCOVERY_STATUS_COLLECTION)
            discovery_status_mgr.ensure_collection()
            status_doc = {
                "last_run":    payload["last_run"],
                "inserted":    payload["inserted"],
                "updated":     payload["updated"],
                "unchanged":   payload["unchanged"],
                "total_ga":    payload["total_ga"],
                "deprecated":  payload["deprecated"],
            }
            existing_docs = discovery_status_mgr.get_all()
            if existing_docs:
                # Update the first (and only) doc using its _key
                status_doc["_key"] = existing_docs[0]["_key"]
                discovery_status_mgr.batch_upsert([status_doc])
            else:
                # Insert new doc
                discovery_status_mgr.batch_upsert([status_doc])
            
            LOG.info("Discovery status persisted to KV store")
            LOG.info("Refresh OK – inserted=%d updated=%d unchanged=%d deprecated=%d", 
                    inserted, updated, unchanged, len(deprecated_metrics))
            return {"status": 200, "payload": payload}

        # ─────── error path ────────────────────────────────────────────────
        except Exception:
            LOG.exception("Metric refresh failed")
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
    export CONFLUENT_KEY=...
    export CONFLUENT_SECRET=...
    python3 metrics_refresh_handler.py
    """
    fake_env = {
        "method": "POST",
        "server": {"rest_uri": os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")},
        "session": {"authtoken": os.getenv("SPLUNK_AUTH_TOKEN", "")},
        "ns": {"app": APPNAME},
        "payload": json.dumps({}),  # or {"account": "my_account"}
    }
    response = MetricsRefreshHandler().handle(json.dumps(fake_env))
    print(json.dumps(response, indent=2))