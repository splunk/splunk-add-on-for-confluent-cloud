#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
resources_for_dataset_handler.py - REST handler for:

    GET  /servicesNS/-/confluent_addon_for_splunk/confluent_addon_for_splunk/resources_for_dataset

Provides resource options for the dependent dropdown based on selected dataset.
This handler supports the UCC dependent dropdown feature for the common_settings_resource field.

The handler is wired in *restmap.conf* as:

    [script:resources_for_dataset]
    script            = resources_for_dataset_handler.py
    scripttype        = persist
    python.version    = python3.9
    handler           = resources_for_dataset_handler.ResourcesForDatasetHandler
    output_modes      = json
    match             = /confluent_addon_for_splunk/resources_for_dataset
    requireAuthentication = true
    passHttpHeaders   = true
    passSession       = true
"""

from __future__ import annotations

# ────── stdlib ─────────────────────────────────────────────────────────────
import json
import logging
import os
import sys
import traceback
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

# ────── add-on lib path bootstrap (bin/../lib) ────────────────────────────
BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, '..', 'lib'))
for _p in (BIN_DIR, LIB_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ────── Splunk / 3rd-party libs ────────────────────────────────────────────
from solnlib.log import Logs
from solnlib import log
from solnlib import conf_manager 
from splunk.persistconn.application import PersistentServerConnectionApplication

# ────── Constants ──────────────────────────────────────────────────────────
APPNAME = "confluent_addon_for_splunk"

# ────── Logging setup ──────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APPNAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("resources_for_dataset_handler")

# ────── Resource Definitions ───────────────────────────────────────────────
# These mappings define which resources are available for each dataset
DATASET_RESOURCES = {
    "cloud": [
        {"value": "kafka", "label": "Kafka"},
        {"value": "schema_registry", "label": "Schema Registry"},
        {"value": "connector", "label": "Connector"},
        {"value": "compute_pool", "label": "Compute Pool"},
        {"value": "flink_statement", "label": "Flink Statement"},
        {"value": "ksql", "label": "Ksql"},
    ],
    "cloud-custom": [
        {"value": "custom_connector", "label": "Custom Connector"}
    ]
}

# Default resources if dataset is not specified or unknown
DEFAULT_RESOURCES = DATASET_RESOURCES["cloud"]


# ═══════════════════════════════════════════════════════════════════════════
class ResourcesForDatasetHandler(PersistentServerConnectionApplication):
    """
    Handles GET /confluent_addon_for_splunk/resources_for_dataset?datasets=<dataset>
    
    This handler provides resource options for the UCC dependent dropdown feature.
    It returns different resource options based on the selected dataset.
    """

    def __init__(self,
                 command_line: Optional[str] = None,
                 command_arg: Optional[str] = None):
        super().__init__()
        self.token: Optional[str] = None
        self.app: Optional[str] = None

        if command_arg:
            try:
                arg = json.loads(command_arg)
                self.token = arg.get("sessionKey") or arg.get("session_key")
                self.app = arg.get("appName") or arg.get("app")
            except Exception:
                LOG.exception("Bad JSON in command_arg")

        LOG.debug("INIT - token=%s  app=%s", bool(self.token), self.app)

    # ------------------------------------------------------------- handle
    def handle(self, raw_request: str) -> Dict[str, Any]:
        """
        Handle the REST request for resource options.
        
        Expected query parameters:
        - datasets: The selected dataset (e.g., "cloud" or "cloud-custom")
        
        Returns:
        UCC-compatible response format with resource options for the specified dataset.
        """
        try:
            LOG.info("=== Handler called with raw_request ===")
            LOG.info("Raw request length: %d", len(raw_request or ""))
            LOG.debug("Raw request content: %s", raw_request)

            envelope = json.loads(raw_request or "{}")
            method = envelope.get("method", "").upper()

            if not self.token:
                self.token = envelope.get("session", {}).get("authtoken")
            if not self.app:
                self.app = envelope.get("ns", {}).get("app") or APPNAME
            
            if not self.token:
                LOG.error("Missing sessionKey – cannot auth to splunkd")
                return {"status": 401, "payload": {"error": "unauthorized", "message": "Missing sessionKey"}}
            
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

            LOG.info("HTTP Method: %s", method)
            LOG.debug("Full envelope: %s", json.dumps(envelope, indent=2))
                      
            if method != "GET":
                return {"status": 405, "payload": {"error": "Only GET supported"}}

            # Extract query parameters
            query_params = self._extract_query_params(envelope)
            dataset = query_params.get("datasets", ["cloud"])[0]  # Get first value if multiple
            
            LOG.info("=== EXTRACTED PARAMETERS ===")
            LOG.debug("Processing request for dataset: %s", dataset)
            LOG.debug("All query parameters: %s", query_params)
            LOG.info("================================")

            # Get resources for the specified dataset
            resources = self._get_resources_for_dataset(dataset)
            
            # Format response in UCC-compatible format
            response_data = {
                "entry": [
                    {
                        "name": resource["value"],
                        "content": {
                            "label": resource["label"],
                            "value": resource["value"]
                        }
                    }
                    for resource in resources
                ]
            }
            
            LOG.info("Returning %d resources for dataset '%s'", len(resources), dataset)
            LOG.debug("Response data: %s", json.dumps(response_data, indent=2))
            
            return {"status": 200, "payload": response_data}

        # ─────── error path ────────────────────────────────────────────────
        except Exception:
            LOG.exception("Resources lookup failed")
            return {
                "status": 500,
                "payload": {
                    "error": "internal-error",
                    "traceback": traceback.format_exc(),
                },
            }

    def _extract_query_params(self, envelope: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Extract query parameters from the request envelope.
        
        Args:
            envelope: The request envelope containing query information
            
        Returns:
            Dictionary mapping parameter names to lists of values
        """
        query_params = {}
        LOG.debug("=== EXTRACTING QUERY PARAMS ===")
        LOG.debug("Envelope keys: %s", list(envelope.keys()))
        
        try:
            # Try to get query string from various possible locations in the envelope
            # query_string = None
            
            # Method 1: Handle Splunk's list of lists format for query parameters
            if "query" in envelope:
                query_data = envelope["query"]
                LOG.debug("Found query data: %s (type: %s)", query_data, type(query_data))
            
                # Splunk passes query params as list of [key, value] pairs
                if isinstance(query_data, list):
                    for item in query_data:
                        if isinstance(item, list) and len(item) >= 2:
                            key, value = item[0], item[1]
                            if key not in query_params:
                                query_params[key] = []
                            query_params[key].append(value)
                            LOG.debug("Added query param: %s = %s", key, value)
            
                # Fallback: try to parse as query string if it's a string
                elif isinstance(query_data, str):
                    LOG.debug("Parsing query as string: %s", query_data)
                    parsed_params = parse_qs(query_data, keep_blank_values=True)
                    query_params.update(parsed_params)
            
            # Method 2: Check if it's in the form data
            elif "form" in envelope:
                LOG.debug("Found form data: %s", envelope["form"])
                for key, values in envelope["form"].items():
                    if isinstance(values, list):
                        query_params[key] = values
                    else:
                        query_params[key] = [str(values)]
            
            # Method 3: Parse from URL if available
            for possible_location in ["restmap", "args", "params", "query_params"]:
                if possible_location in envelope:
                    LOG.debug("Found %s: %s", possible_location, envelope[possible_location])
            
            LOG.debug("Final extracted query params: %s", query_params)
            
        except Exception as e:
            log.log_configuration_error(LOG, e)
            LOG.warning("Failed to extract query parameters: %s", e)
            LOG.debug("Request envelope: %s", json.dumps(envelope, indent=2))
        
        return query_params

    def _get_resources_for_dataset(self, dataset: str) -> List[Dict[str, str]]:
        """
        Get the list of available resources for the specified dataset.
        
        Args:
            dataset: The dataset name (e.g., "cloud", "cloud-custom")
            
        Returns:
            List of resource dictionaries with 'value' and 'label' keys
        """
        # Normalize dataset name
        dataset = dataset.strip().lower() if dataset else "cloud"
        
        # Get resources for the dataset
        resources = DATASET_RESOURCES.get(dataset, DEFAULT_RESOURCES)
        
        LOG.debug("Dataset '%s' has %d available resources: %s", 
                 dataset, len(resources), [r["value"] for r in resources])
        
        return resources

    # ----------------------------------------------------- unused stream API
    def handleStream(self, handle, _in):
        raise NotImplementedError

    def done(self):
        pass


# ────── CLI test helper ────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Quick manual test:
    
    python3 resources_for_dataset_handler.py cloud
    python3 resources_for_dataset_handler.py cloud-custom
    """
    
    # Test with command line argument
    dataset = sys.argv[1] if len(sys.argv) > 1 else "cloud"
    
    fake_env = {
        "method": "GET",
        "server": {"rest_uri": os.getenv("SPLUNKD_URI", "https://127.0.0.1:8089")},
        "session": {"authtoken": "fake-token"},
        "ns": {"app": APPNAME},
        "query": f"datasets={dataset}",
    }
    
    handler = ResourcesForDatasetHandler()
    response = handler.handle(json.dumps(fake_env))
    
    print(f"Testing dataset: {dataset}")
    print(f"Status: {response['status']}")
    print(f"Response:")
    print(json.dumps(response["payload"], indent=2))
    
    # Test both datasets
    if len(sys.argv) == 1:
        print("\n" + "="*50)
        print("Testing all datasets:")
        
        for test_dataset in ["cloud", "cloud-custom"]:
            print(f"\n--- Dataset: {test_dataset} ---")
            fake_env["query"] = f"datasets={test_dataset}"
            response = handler.handle(json.dumps(fake_env))
            
            if response["status"] == 200:
                resources = response["payload"]["entry"]
                print(f"Resources ({len(resources)}):")
                for resource in resources:
                    print(f"  - {resource['content']['label']} ({resource['content']['value']})")
            else:
                print(f"Error: {response}")