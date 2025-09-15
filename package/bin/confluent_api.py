#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
confluent_api.py

Confluent Cloud Telemetry API client focused on API communication with multi-dataset support.

Responsibilities:
• HTTP plumbing (basic-auth, retry, rate-limit)
• Descriptor fetching: fetch_metrics(), fetch_resources()
• Metric data querying: query_metrics_data() with dataset-aware routing
• Support for both individual metric settings and common settings
• Request validation against API constraints
• Comprehensive interval parsing with 'now' modifiers and 'ALL' granularity support
• Multi-dataset architecture: groups metrics by dataset for efficient querying
"""

# ────── Path setup ──────────────────────────────────────────
import os
import sys

BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, '..', 'lib'))
for _p in (BIN_DIR, LIB_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Standard library imports
import json
import logging
import re
import ssl
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict

# Third-party imports
import httpx
from httpx_ratelimiter import LimiterTransport
from httpx_retries import Retry, RetryTransport

# Splunk library imports
from solnlib import log
from solnlib.log import Logs
from solnlib import conf_manager

# ────── Constants ────────────────────────────────────────────────────────────
# Granularity to max interval mapping (in hours)
GRANULARITY_LIMITS = {
    "PT1M": 6,      # 1 minute -> 6 hours max
    "PT5M": 24,     # 5 minutes -> 1 day max
    "PT15M": 96,    # 15 minutes -> 4 days max
    "PT30M": 168,   # 30 minutes -> 7 days max
    "PT1H": None,   # 1 hour -> Any
    "PT4H": None,   # 4 hours -> Any
    "PT6H": None,   # 6 hours -> Any
    "PT12H": None,  # 12 hours -> Any
    "P1D": None,    # 1 day -> Any
    "ALL": None,    # ALL -> Any (special single bucket)
}
# ───────────────────────────── constants ───────────────────────────────
APP_NAME = globals().get("APP_NAME", "confluent_addon_for_splunk")

# ────── Logging setup ────────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APP_NAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("confluent_api")

def apply_dynamic_log_level(session_key: str, app_name: str = APP_NAME) -> None:
    """
    Set this module's logger level from [logging] in {app_name}_settings.conf.
    Call this from the caller (e.g., cloud_metrics_input) once you have a session key.
    """
    try:
        level_name = conf_manager.get_log_level(
            logger=LOG,
            session_key=session_key,
            app_name=app_name,
            conf_name=f"{app_name}_settings",
        )
        LOG.setLevel(getattr(logging, str(level_name).upper(), logging.INFO))
        LOG.info("Dynamic log level set to %s (%s).", level_name, LOG.getEffectiveLevel())
    except Exception as e:
        LOG.warning("Unable to set dynamic log level: %s", e)
        LOG.setLevel(logging.INFO)

# ────── SSL Configuration ────────────────────────────────────────────────────
def _get_ssl_verify():
    """Get SSL verification setting that works in Splunk environment."""
    # Clear SSL_CERT_FILE env var if it points to non-existent file
    cert_file = os.environ.get("SSL_CERT_FILE")
    if cert_file and not os.path.exists(cert_file):
        LOG.debug("Clearing invalid SSL_CERT_FILE: %s", cert_file)
        os.environ.pop("SSL_CERT_FILE", None)
    
    # Try to create SSL context to test if it works
    try:
        ssl.create_default_context()
        return True  # Default SSL verification works
    except (FileNotFoundError, ssl.SSLError) as e:
        LOG.warning("Default SSL context failed: %s", e)
        
        # Try alternative CA bundle locations
        ca_locations = [
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt", 
            "/usr/share/ssl/certs/ca-bundle.crt",
            "/etc/ssl/ca-bundle.pem",
        ]
        
        for ca_path in ca_locations:
            if os.path.exists(ca_path):
                LOG.info("Using CA bundle: %s", ca_path)
                return ca_path
        
        LOG.warning("No CA bundle found, disabling SSL verification")
        return False

# ────── Exception Classes ────────────────────────────────────────────────────
class ConfluentAPIError(Exception):
    """Base exception for Confluent API errors."""
    pass

class ConfluentValidationError(ConfluentAPIError):
    """Raised when request validation fails."""
    pass

class ConfluentRateLimitError(ConfluentAPIError):
    """Raised when rate limit is exceeded."""
    pass

# ────────────────────────────────────────────────────────────────────────────
class ConfluentTelemetryClient:
    """
    Confluent Telemetry API client with multi-dataset support.

    Features:
    • Descriptor fetching (metrics, resources) for any dataset
    • Multi-dataset metric data querying with automatic dataset routing
    • Support for individual and common metric settings
    • Request validation against API limits
    • Automatic label prefix handling
    • Comprehensive interval parsing with 'now' modifiers
    • Special 'ALL' granularity support
    
    Usage:
        # Initialize with credentials (no default dataset)
        client = ConfluentTelemetryClient(
            api_key="<key>",
            api_secret="<secret>"
        )
        
        # Fetch descriptors for specific dataset
        metrics = client.fetch_metrics("cloud")
        custom_metrics = client.fetch_metrics("cloud-custom")
        
        # Query metric data with automatic dataset routing
        data = client.query_metrics_data(
            metric_definitions=[...],  # From KV store (includes 'dataset' field)
            use_individual_settings=True
        )
        
        # Query with common settings
        data = client.query_metrics_data(
            metric_definitions=[...],
            use_individual_settings=False,
            common_settings={
                "granularity": "PT1H",
                "intervals": ["PT1H/now"],
                "group_by": ["metric.topic"],
                "filter": {...},
                "limit": 100
            }
        )
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        base_url: str = "https://api.telemetry.confluent.cloud",
        timeout: float = 30.0,
    ):
        # Credentials - prefer parameters over env vars
        self.api_key = api_key or os.environ.get("CONFLUENT_KEY")
        self.api_secret = api_secret or os.environ.get("CONFLUENT_SECRET")
        
        if not (self.api_key and self.api_secret):
            LOG.error("Missing API credentials. Provide api_key/api_secret or set CONFLUENT_KEY/CONFLUENT_SECRET env vars")
            log.log_configuration_error(LOG, ConfluentAPIError("Missing API credentials"))
            raise ConfluentAPIError("Missing API credentials. Provide api_key/api_secret or set CONFLUENT_KEY/CONFLUENT_SECRET env vars")
        
        self.base_url = base_url.rstrip("/")
        self._client = self._make_httpx_client(self.api_key, self.api_secret, timeout)
        
        LOG.debug("Confluent API client initialized - base_url=%s", self.base_url)

    # ────── Descriptor Methods ──────────────────────────────────────────────────
    def fetch_metrics(self, dataset: str) -> List[Dict[str, Any]]:
        """
        GET /v2/metrics/<dataset>/descriptors/metrics
        Returns **all** pages for the specified dataset.
        
        Args:
            dataset: Dataset name (e.g., 'cloud', 'cloud-custom')
        """
        url = f"{self.base_url}/v2/metrics/{dataset}/descriptors/metrics"
        metrics = self._fetch_pages("GET", url)
        
        if LOG.isEnabledFor(logging.DEBUG):
            metric_names = [m.get("name", "unnamed") for m in metrics]
            LOG.debug("Fetched %d metric descriptors from dataset '%s': %s", 
                     len(metrics), dataset, ", ".join(metric_names[:10]))
        
        return metrics

    def fetch_resources(self, dataset: str) -> List[Dict[str, Any]]:
        """
        GET /v2/metrics/<dataset>/descriptors/resources
        Returns **all** pages for the specified dataset.
        
        Args:
            dataset: Dataset name (e.g., 'cloud', 'cloud-custom')
        """
        url = f"{self.base_url}/v2/metrics/{dataset}/descriptors/resources"
        resources = self._fetch_pages("GET", url)
        
        if LOG.isEnabledFor(logging.DEBUG):
            resource_types = [r.get("type", "unknown") for r in resources]
            LOG.debug("Fetched %d resource descriptors from dataset '%s': %s", 
                     len(resources), dataset, ", ".join(set(resource_types)))
        
        return resources

    # ────── Metric Data Querying Methods ────────────────────────────────────────
    def query_metrics_data(
        self,
        metric_definitions: List[Dict[str, Any]],
        use_individual_settings: bool = True,
        common_settings: Optional[Dict[str, Any]] = None,
        format_type: str = "FLAT"
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Query metric data for multiple metrics with automatic dataset routing.
        
        Args:
            metric_definitions: List of metric definitions from KV store (must include 'dataset' field)
            use_individual_settings: If True, use each metric's own settings.
                                   If False, use common_settings for all metrics.
            common_settings: Common settings to apply to all metrics when 
                           use_individual_settings=False
            format_type: Response format ("FLAT" or "GROUPED")
        
        Returns:
            Dict mapping metric names to their data points
        """
        if not metric_definitions:
            LOG.warning("No metric definitions provided")
            return {}
        
        if not use_individual_settings and not common_settings:
            raise ConfluentValidationError("common_settings required when use_individual_settings=False")
        
        # Group metrics by dataset for efficient querying
        metrics_by_dataset = self._group_metrics_by_dataset(metric_definitions, use_individual_settings)
        
        # Query each dataset separately and merge results
        all_results = {}
        
        for dataset, dataset_metrics in metrics_by_dataset.items():
            LOG.debug("Querying %d metrics from dataset '%s'", len(dataset_metrics), dataset)
            
            try:
                dataset_results = self._query_dataset_metrics(
                    dataset=dataset,
                    metric_definitions=dataset_metrics,
                    use_individual_settings=use_individual_settings,
                    common_settings=common_settings,
                    format_type=format_type
                )
                
                # Merge results
                for metric_def in dataset_metrics:
                    metric_key = metric_def.get("_key")
                    metric_name = metric_def.get("name")
                    if metric_key and metric_name:
                        all_results[metric_key] = dataset_results.get(metric_name, [])
                
                LOG.info("Successfully queried %d metrics from dataset '%s'", 
                        len(dataset_results), dataset)
                
            except Exception as e:
                LOG.error("Failed to query metrics from dataset '%s': %s", dataset, e)
                # Continue with other datasets rather than failing completely
                # Add empty results for failed metrics
                for metric_def in dataset_metrics:
                    metric_key = metric_def.get("_key")
                    metric_name = metric_def.get("name")
                    if metric_key and metric_name:
                        all_results[metric_key] = []
        
        return all_results

    def _group_metrics_by_dataset(
        self,
        metric_definitions: List[Dict[str, Any]],
        use_individual_settings: bool
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group metrics by their dataset field for efficient querying.
        
        Args:
            metric_definitions: List of metric definitions from KV store
            use_individual_settings: If True, filter out disabled metrics
            
        Returns:
            Dict mapping dataset names to lists of metric definitions
        """
        grouped = defaultdict(list)
        
        for metric_def in metric_definitions:
            metric_name = metric_def.get("name")
            dataset = metric_def.get("dataset")
            
            if not metric_name:
                LOG.warning("Metric definition missing 'name' field, skipping")
                continue
                
            if not dataset:
                LOG.warning("Metric '%s' missing 'dataset' field, skipping", metric_name)
                continue
            
            # Skip disabled metrics when using individual settings
            if use_individual_settings and not metric_def.get("enabled", False):
                LOG.debug("Skipping disabled metric: %s (dataset: %s)", metric_name, dataset)
                continue
            
            grouped[dataset].append(metric_def)
            LOG.debug("Grouped metric '%s' into dataset '%s'", metric_name, dataset)
        
        # Log summary
        for dataset, metrics in grouped.items():
            metric_names = [m.get("name") for m in metrics]
            LOG.debug("Dataset '%s' contains %d metrics: %s", 
                     dataset, len(metrics), ", ".join(metric_names[:5]))
        
        return dict(grouped)

    def _query_dataset_metrics(
        self,
        dataset: str,
        metric_definitions: List[Dict[str, Any]],
        use_individual_settings: bool,
        common_settings: Optional[Dict[str, Any]],
        format_type: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Query metrics from a specific dataset.
        
        Args:
            dataset: Dataset name
            metric_definitions: Metrics belonging to this dataset
            use_individual_settings: Whether to use individual metric settings
            common_settings: Common settings if not using individual
            format_type: Response format
            
        Returns:
            Dict mapping metric names to their data points
        """
        results = {}
        
        for metric_def in metric_definitions:
            metric_name = metric_def.get("name")
            if not metric_name:
                continue
            
            try:
                # Determine settings to use
                if use_individual_settings:
                    settings = self._extract_metric_settings(metric_def)
                else:
                    settings = dict(common_settings) if common_settings else {}
                    if "group_by" not in settings:
                        group_by_labels = [
                            lab.strip() for lab in metric_def.get("labels", "").split(",") if lab.strip()
                        ]
                        if group_by_labels:
                            settings["group_by"] = group_by_labels
                
                # Query data for this metric using the specific dataset
                data = self._query_single_metric_with_dataset(
                    dataset=dataset,
                    metric_name=metric_name,
                    settings=settings,
                    format_type=format_type
                )
                results[metric_name] = data
                
            except Exception as e:
                LOG.error("Failed to query metric %s from dataset %s: %s", 
                         metric_name, dataset, e)
                
                if isinstance(e, httpx.HTTPStatusError):
                    sc = e.response.status_code
                    if sc == 401:
                        log.log_authentication_error(LOG, e)
                    elif sc == 403:
                        log.log_permission_error(LOG, e)
                    elif sc == 429:
                        log.log_server_error(LOG, e)
                    elif 400 <= sc < 500:
                        log.log_configuration_error(LOG, e)
                    else:
                        log.log_server_error(LOG, e)
                elif isinstance(e, httpx.RequestError):
                    log.log_connection_error(LOG, e)
                elif isinstance(e, ConfluentRateLimitError):
                    log.log_server_error(LOG, e)
                elif isinstance(e, ConfluentValidationError):
                    log.log_configuration_error(LOG, e)
                else:
                    log.log_exception(LOG, e, "unknown error")

                # Continue with other metrics rather than failing completely
                results[metric_def.get("_key") or metric_name] = []
        
        return results

    def _query_single_metric_with_dataset(
        self,
        dataset: str,
        metric_name: str,
        settings: Dict[str, Any],
        format_type: str = "FLAT"
    ) -> List[Dict[str, Any]]:
        """
        Query data for a single metric from a specific dataset.
        
        Args:
            dataset: Dataset name (e.g., 'cloud', 'cloud-custom')
            metric_name: Name of the metric to query
            settings: Query settings (granularity, intervals, filter, etc.)
            format_type: Response format
        
        Returns:
            List of data points for this metric
        """
        LOG.debug("Querying metric: %s from dataset: %s with settings: %s", 
                 metric_name, dataset, settings)
        
        # Validate settings
        self._validate_query_settings(metric_name, settings)
        
        # Build query payload
        payload = self._build_query_payload(metric_name, settings, format_type)
        
        try:
            # Execute query with dataset-specific URL
            url = f"{self.base_url}/v2/metrics/{dataset}/query"
            data = self._fetch_pages("POST", url, body=payload)
            
            LOG.info("Successfully queried %d data points for metric: %s (dataset: %s)", 
                    len(data), metric_name, dataset)
            return data
            
        
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 429:
                log.log_server_error(LOG, e)
                raise ConfluentRateLimitError(f"Rate limit exceeded for metric {metric_name} (dataset: {dataset})")
            elif code == 400:
                log.log_configuration_error(LOG, e)

                try:
                    error_details = e.response.json()
                    raise ConfluentValidationError(f"Validation failed for metric {metric_name} (dataset: {dataset}): {error_details}")
                except:
                    log.log_configuration_error(LOG, e)
                    raise ConfluentValidationError(f"Bad request for metric {metric_name} (dataset: {dataset}): {e.response.text}")
            elif code == 401:
                log.log_authentication_error(LOG, e)
                raise ConfluentAPIError(f"HTTP 401 for metric {metric_name} (dataset: {dataset}): {e.response.text}")
            elif code == 403:
                log.log_permission_error(LOG, e)
                raise ConfluentAPIError(f"HTTP 403 for metric {metric_name} (dataset: {dataset}): {e.response.text}")
            elif 500 <= code <= 599:
                log.log_server_error(LOG, e)
                raise ConfluentAPIError(f"HTTP {code} for metric {metric_name} (dataset: {dataset}): {e.response.text}")
            else:
                log.log_exception(LOG, e, "http status error")
                raise ConfluentAPIError(f"HTTP {code} for metric {metric_name} (dataset: {dataset}): {e.response.text}")
        
        except httpx.TimeoutException as e:
            log.log_connection_error(LOG, e)
            raise ConfluentAPIError(f"Timeout for metric {metric_name} (dataset: {dataset}): {e}")
        
        except httpx.InvalidURL as e:
            log.log_configuration_error(LOG, e)
            raise ConfluentAPIError(f"Invalid URL for dataset {dataset}: {e}")
        
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.NetworkError, httpx.TransportError) as e:
            log.log_connection_error(LOG, e)
            raise ConfluentAPIError(f"Network error for metric {metric_name} (dataset: {dataset}): {e}")

        except httpx.RequestError as e:
            log.log_connection_error(LOG, e)
            raise ConfluentAPIError(f"Request error for metric {metric_name} (dataset: {dataset}): {e}")

        

    # ────── Legacy Compatibility Method ─────────────────────────────────────────
    def query_single_metric(
        self,
        metric_name: str,
        settings: Dict[str, Any],
        format_type: str = "FLAT",
        dataset: str = "cloud"
    ) -> List[Dict[str, Any]]:
        """
        Query data for a single metric (legacy compatibility method).
        
        Args:
            metric_name: Name of the metric to query
            settings: Query settings (granularity, intervals, filter, etc.)
            format_type: Response format
            dataset: Dataset to query (defaults to 'cloud' for backward compatibility)
        
        Returns:
            List of data points for this metric
        """
        LOG.debug("Legacy query_single_metric called for: %s (dataset: %s)", metric_name, dataset)
        return self._query_single_metric_with_dataset(dataset, metric_name, settings, format_type)

    def _extract_metric_settings(self, metric_def: Dict[str, Any]) -> Dict[str, Any]:
        """Extract query settings from metric definition."""
        settings = {}
        
        # Required fields
        for field in ["granularity", "intervals", "filter"]:
            if field in metric_def:
                settings[field] = metric_def[field]
            else:
                raise ConfluentValidationError(f"Metric {metric_def.get('name')} missing required field: {field}")
        
        # Optional fields
        for field in ["group_by", "limit"]:
            if field in metric_def:
                settings[field] = metric_def[field]
        
        return settings

    def _validate_query_settings(self, metric_name: str, settings: Dict[str, Any]) -> None:
        """Validate query settings against API constraints."""
        granularity = settings.get("granularity")
        intervals = settings.get("intervals", [])
        filter_obj = settings.get("filter")
        
        # Step 1: Check for required fields
        if not granularity:
            raise ConfluentValidationError(f"Missing granularity for metric {metric_name}")
        
        if not intervals:
            raise ConfluentValidationError(f"Missing intervals for metric {metric_name}")
        
        # Step 2: Validate mandatory filter field
        if not filter_obj:
            raise ConfluentValidationError(f"Missing filter for metric {metric_name} - at least a resource filter is required")
        
        # Basic filter validation - ensure it's a valid filter object
        if not isinstance(filter_obj, dict):
            raise ConfluentValidationError(f"Invalid filter format for metric {metric_name} - must be a valid filter object")
        
        # Check for basic filter structure (field filters or compound filters)
        has_field_op = filter_obj.get("field") and filter_obj.get("op")
        has_compound_op = filter_obj.get("op") in ["AND", "OR", "NOT"]
        
        if not (has_field_op or has_compound_op):
            raise ConfluentValidationError(f"Invalid filter structure for metric {metric_name} - must have field+op or be a compound filter")
        
        # Step 3: Validate granularity value
        if granularity not in GRANULARITY_LIMITS:
            raise ConfluentValidationError(f"Invalid granularity '{granularity}' for metric {metric_name}")
        
        # Step 4: Validate interval length against granularity constraints
        # Special case: 'ALL' granularity has no interval restrictions
        if granularity == "ALL":
            LOG.debug("Granularity 'ALL' detected - skipping interval length validation for metric %s", metric_name)
            return
        
        max_hours = GRANULARITY_LIMITS[granularity]
        if max_hours is not None:
            for interval in intervals:
                interval_hours = self._calculate_interval_hours(interval)
                if interval_hours > max_hours:
                    raise ConfluentValidationError(
                        f"Interval '{interval}' ({interval_hours:.1f}h) exceeds maximum {max_hours} hours for granularity '{granularity}' in metric {metric_name}"
                    )
        
        LOG.debug("Query settings validation passed for metric %s", metric_name)

    def _parse_iso8601_datetime(self, datetime_str: str) -> Optional[datetime]:
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

    def _calculate_interval_hours(self, interval: str) -> float:
        """
        Calculate the duration of an interval in hours with comprehensive format support.
        
        Handles Confluent's supported ISO-8601 interval formats:
        - "2021-02-24T10:00:00Z/2021-02-24T11:00:00Z" (absolute timestamps)
        - "PT6H/now" (duration/relative)
        - "now-6h|h/now|h" (Splunk-style relative with modifiers)
        - "now-2m|m/now" (now with offset and truncation modifiers)
        """
        LOG.debug("Calculating interval hours for: '%s'", interval)
        
        # Method 1: Handle absolute timestamp intervals (primary Confluent format)
        # "2021-02-24T10:00:00Z/2021-02-24T11:00:00Z"
        if "/" in interval and "T" in interval and ("Z" in interval or "+00:00" in interval):
            try:
                start_str, end_str = interval.split("/", 1)
                
                # Handle end_str which might be "now" or "now" with modifiers
                if end_str.startswith("now"):
                    end_dt = self._parse_now_expression(end_str)
                else:
                    end_dt = self._parse_iso8601_datetime(end_str)
                
                # Handle start_str
                if start_str.startswith("now"):
                    start_dt = self._parse_now_expression(start_str)
                else:
                    start_dt = self._parse_iso8601_datetime(start_str)
                
                if start_dt and end_dt:
                    duration = end_dt - start_dt
                    hours = duration.total_seconds() / 3600
                    LOG.debug("Parsed absolute interval '%s' as %.2f hours", interval, hours)
                    return abs(hours)  # Ensure positive duration
                    
            except Exception as e:
                LOG.debug("Failed to parse absolute interval '%s': %s", interval, e)
        
        # Method 2: Handle ISO-8601 duration format (PT<X>H, PT<X>M, etc.)
        # "PT6H/now", "PT30M/now", "PT1H30M/now"
        if "PT" in interval and "/" in interval:
            try:
                duration_part = interval.split("/")[0]
                
                # Parse PT format: PT6H, PT30M, PT1H30M, P1D, etc.
                hours = self._parse_iso8601_duration(duration_part)
                if hours is not None:
                    LOG.debug("Parsed duration interval '%s' as %.2f hours", interval, hours)
                    return hours
                    
            except Exception as e:
                LOG.debug("Failed to parse duration interval '%s': %s", interval, e)
        
        # Method 3: Handle single duration without endpoint
        # "PT6H", "P1D", "P7D"
        if interval.startswith(("PT", "P")) and "/" not in interval:
            try:
                hours = self._parse_iso8601_duration(interval)
                if hours is not None:
                    LOG.debug("Parsed single duration '%s' as %.2f hours", interval, hours)
                    return hours
            except Exception as e:
                LOG.debug("Failed to parse single duration '%s': %s", interval, e)
        
        # Method 4: Handle Splunk-style relative time expressions with modifiers
        # "now-6h|h/now|h", "now-1d|d/now", "now-2m|m/now"
        if "now" in interval:
            try:
                # Split on "/" to get start and end parts
                if "/" in interval:
                    start_part, end_part = interval.split("/", 1)
                else:
                    start_part = interval
                    end_part = "now"
                
                # Parse both parts and calculate duration
                start_time = self._parse_now_expression(start_part)
                end_time = self._parse_now_expression(end_part)
                
                if start_time and end_time:
                    duration = end_time - start_time
                    hours = abs(duration.total_seconds()) / 3600
                    LOG.debug("Parsed relative interval '%s' as %.2f hours", interval, hours)
                    return hours
                    
            except Exception as e:
                LOG.debug("Failed to parse relative interval '%s': %s", interval, e)
        
        # Conservative fallback
        LOG.warning("Could not parse interval '%s', assuming 24 hours", interval)
        return 24.0

    def _parse_now_expression(self, now_expr: str) -> datetime:
        """
        Parse 'now' expressions with offset and truncation modifiers.
        
        Examples:
        - "now" -> current time
        - "now-2m" -> current time minus 2 minutes  
        - "now-1h" -> current time minus 1 hour
        - "now-1d" -> current time minus 1 day
        - "now|m" -> current time truncated to start of minute
        - "now|h" -> current time truncated to start of hour
        - "now|d" -> current time truncated to start of day
        - "now-2m|m" -> current time minus 2 minutes, truncated to start of minute
        - "now-1d|d" -> current time minus 1 day, truncated to start of day
        """
        # Start with current UTC time
        current_time = datetime.now(timezone.utc)
        
        # Clean the expression
        expr = now_expr.strip()
        
        # Handle simple "now"
        if expr == "now":
            return current_time
        
        # Split by truncation modifier (|)
        if "|" in expr:
            time_part, truncate_part = expr.split("|", 1)
        else:
            time_part = expr
            truncate_part = None
        
        # Parse offset from "now" in time_part
        # Patterns: now-2m, now-1h, now-1d, now+1h, etc.
        offset_match = re.search(r'now([+-])(\d+)([mhd])', time_part)
        
        result_time = current_time
        
        if offset_match:
            sign = offset_match.group(1)
            amount = int(offset_match.group(2))
            unit = offset_match.group(3)
            
            # Calculate offset
            if unit == 'm':  # minutes
                offset_seconds = amount * 60
            elif unit == 'h':  # hours
                offset_seconds = amount * 3600
            elif unit == 'd':  # days
                offset_seconds = amount * 86400
            else:
                offset_seconds = 0
            
            # Apply offset
            if sign == '-':
                result_time = current_time - timedelta(seconds=offset_seconds)
            else:  # '+'
                result_time = current_time + timedelta(seconds=offset_seconds)
        
        # Apply truncation if specified
        if truncate_part:
            if truncate_part == 'm':  # truncate to start of minute
                result_time = result_time.replace(second=0, microsecond=0)
            elif truncate_part == 'h':  # truncate to start of hour
                result_time = result_time.replace(minute=0, second=0, microsecond=0)
            elif truncate_part == 'd':  # truncate to start of day
                result_time = result_time.replace(hour=0, minute=0, second=0, microsecond=0)
        
        LOG.debug("Parsed now expression '%s' to %s", now_expr, result_time.isoformat())
        return result_time

    def _parse_iso8601_duration(self, duration_str: str) -> Optional[float]:
        """
        Parse ISO-8601 duration strings to hours.
        
        Examples:
        - "PT6H" -> 6.0
        - "PT30M" -> 0.5  
        - "PT1H30M" -> 1.5
        - "P1D" -> 24.0
        - "P7D" -> 168.0
        - "P1DT6H" -> 30.0
        """
        if not duration_str.startswith("P"):
            return None
        
        # Remove 'P' prefix
        duration = duration_str[1:]
        
        hours = 0.0
        
        try:
            # Handle days (before T)
            if "D" in duration and ("T" not in duration or duration.index("D") < duration.index("T")):
                days_match = re.search(r'(\d+)D', duration)
                if days_match:
                    hours += int(days_match.group(1)) * 24
            
            # Handle time part (after T)
            if "T" in duration:
                time_part = duration.split("T", 1)[1]
                
                # Hours
                if "H" in time_part:
                    hours_match = re.search(r'(\d+)H', time_part)
                    if hours_match:
                        hours += int(hours_match.group(1))
                
                # Minutes
                if "M" in time_part:
                    minutes_match = re.search(r'(\d+)M', time_part)
                    if minutes_match:
                        hours += int(minutes_match.group(1)) / 60
                
                # Seconds (rare, but possible)
                if "S" in time_part:
                    seconds_match = re.search(r'(\d+(?:\.\d+)?)S', time_part)
                    if seconds_match:
                        hours += float(seconds_match.group(1)) / 3600
            
            return hours if hours > 0 else None
            
        except Exception as e:
            LOG.debug("Failed to parse ISO-8601 duration '%s': %s", duration_str, e)
            return None

    def _build_query_payload(
        self,
        metric_name: str,
        settings: Dict[str, Any],
        format_type: str
    ) -> Dict[str, Any]:
        """Build the request payload for metric query."""
        payload = {
            "aggregations": [{"metric": metric_name}],  # Omit 'agg' field as recommended
            "granularity": settings["granularity"],
            "intervals": settings["intervals"],
            "filter": settings["filter"],
            "format": format_type
        }
        
        # Add optional fields
        if "group_by" in settings and settings["group_by"]:
            payload["group_by"] = settings["group_by"]
        
        if "limit" in settings and settings["limit"]:
            payload["limit"] = settings["limit"]
        
        # Add payload construction logging
        LOG.debug("Built query payload for metric '%s': %s", metric_name, json.dumps(payload, indent=2))
        
        return payload

    def extract_latest_timestamp(self, data: List[Dict[str, Any]]) -> Optional[str]:
        """
        Extract the latest timestamp from query results.
        Public method for use by modular input scripts for checkpointing.
        """
        if not data:
            return None
        
        timestamps = []
        for point in data:
            if "timestamp" in point:
                timestamps.append(point["timestamp"])
        
        if timestamps:
            return max(timestamps)
        
        return None

    # ────── HTTP Client Methods ─────────────────────────────────────────────────
    @staticmethod
    def _make_httpx_client(key: str, secret: str, timeout: Union[float, int]) -> httpx.Client:
        """Build an httpx.Client with retry and rate limiting."""
        retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],  # Removed 400 for query endpoint
            backoff_jitter=1.0,
        )

        ssl_verify = _get_ssl_verify()

        transport = LimiterTransport(
            per_second=1,
            max_delay=1000,
            transport=RetryTransport(
                retry=retry,
                transport=httpx.HTTPTransport(verify=ssl_verify)
            ),
        )
        LOG.debug("httpx client created - timeout=%ss, rps=1", timeout)
        return httpx.Client(auth=(key, secret), timeout=timeout, transport=transport)

    def _fetch_pages(
        self,
        verb: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Enhanced cursor-pagination helper for both GET and POST endpoints.
        Includes proper error handling for query endpoint with detailed request/response logging.
        """
        out: List[Dict[str, Any]] = []
        token: Optional[str] = None
        page = 1

        LOG.debug("Fetching paginated %s %s", verb, url)

        while True:
            q = {"page_size": 1000, **(params or {})}
            if token:
                q["page_token"] = token

            try:
                if verb.upper() == "GET":
                    LOG.debug("GET request params: %s", q)
                    r = self._client.get(url, params=q)
                else:  # POST
                    payload = dict(body or {})
                    if token:
                        payload["page_token"] = token
                    
                    # Add detailed request logging for POST requests
                    LOG.debug("POST request URL: %s", url)
                    LOG.debug("POST request params: %s", q)
                    LOG.debug("POST request body: %s", json.dumps(payload, indent=2))
                    
                    r = self._client.post(url, params=q, json=payload)

                # Add response logging
                LOG.debug("Response status: %d", r.status_code)
                LOG.debug("Response headers: %s", dict(r.headers))
                
                r.raise_for_status()
                data = r.json()
                
                # Add response body logging (truncated for large responses)
                if LOG.isEnabledFor(logging.DEBUG):
                    response_str = json.dumps(data, indent=2)
                    if len(response_str) > 1000:
                        LOG.debug("Response body (truncated): %s...", response_str[:1000])
                    else:
                        LOG.debug("Response body: %s", response_str)
                
                # Handle both descriptor and query response formats
                page_data = data.get("data", [])
                out.extend(page_data)

                token = (
                    data.get("meta", {})
                    .get("pagination", {})
                    .get("next_page_token")
                )
                
                LOG.debug("Page %d → %d items (next=%s)", page, len(out), bool(token))
                page += 1

                if not token:
                    break

            except httpx.HTTPStatusError as e:
                # Add detailed error logging
                LOG.error("HTTP error %d on page %d", e.response.status_code, page)
                LOG.error("Error response headers: %s", dict(e.response.headers))
                
                try:
                    error_body = e.response.json()
                    LOG.error("Error response body: %s", json.dumps(error_body, indent=2))
                except:
                    LOG.error("Error response text: %s", e.response.text[:500])
                
                if e.response.status_code == 429:
                    LOG.warning("Rate limit hit on page %d, response: %s", page, e.response.text[:200])
                    log.log_server_error(LOG, e)
                    raise ConfluentRateLimitError("Rate limit exceeded during pagination")
                elif e.response.status_code == 400:
                    LOG.error("Bad request on page %d, response: %s", page, e.response.text[:500])
                    try:
                        error_data = e.response.json()
                        if "errors" in error_data:
                            error_details = "; ".join([err.get("detail", str(err)) for err in error_data["errors"]])
                            log.log_configuration_error(LOG, e)
                            raise ConfluentValidationError(f"API validation failed: {error_details}")
                    except (json.JSONDecodeError, KeyError):
                        pass
                    raise ConfluentValidationError(f"Bad request: {e.response.text}")
                elif e.response.status_code == 401:
                    log.log_authentication_error(LOG, e)
                    raise ConfluentAPIError(f"HTTP 401: {e.response.text}")
                elif e.response.status_code == 403:
                    log.log_permission_error(LOG, e)
                    raise ConfluentAPIError(f"HTTP 403: {e.response.text}")
                elif 500 <= e.response.status_code <= 599:
                    log.log_server_error(LOG, e)
                    raise ConfluentAPIError(f"HTTP {e.response.status_code}: {e.response.text}")
                
                else:
                    log.log_exception(LOG, e, "http status error")
                    raise ConfluentAPIError(f"HTTP {e.response.status_code}: {e.response.text}")
                
            except httpx.RequestError as e:  # NEW
                log.log_connection_error(LOG, e)
                raise ConfluentAPIError(f"Network error during pagination: {e}")

        LOG.debug("Completed pagination – %d total items", len(out))
        return out

    # ────── Context Manager Support ─────────────────────────────────────────────
    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        self.close()

    def close(self):
        """Clean up resources."""
        try:
            self._client.close()
        except Exception:
            pass


# ────── CLI Test Helper ──────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Test examples:
    
    # Test descriptors
    export CONFLUENT_KEY=<key>
    export CONFLUENT_SECRET=<secret>
    python3 confluent_api.py descriptors cloud
    python3 confluent_api.py descriptors cloud-custom
    
    # Test query
    python3 confluent_api.py query cloud
    """
    key = os.environ.get("CONFLUENT_KEY")
    secret = os.environ.get("CONFLUENT_SECRET")
    
    if not (key and secret):
        print("Need CONFLUENT_KEY / CONFLUENT_SECRET in env", file=sys.stderr)
        sys.exit(1)

    client = ConfluentTelemetryClient(key, secret)
    
    if len(sys.argv) > 2 and sys.argv[1] == "query":
        dataset = sys.argv[2]
        # Test query functionality
        # Get real cluster ID from environment or use a known one
        cluster_id = os.environ.get("TEST_CLUSTER_ID", "lkc-vz1qz0")  # Use real cluster ID
        
        test_metric = {
            "name": "io.confluent.kafka.server/received_bytes",
            "dataset": dataset,
            "enabled": True,
            "granularity": "PT1H", 
            "intervals": ["PT24H/now"],  # Last 24 hours instead of 1 hour
            "filter": {
                "field": "resource.kafka.id",
                "op": "EQ", 
                "value": cluster_id
            },
            "group_by": ["metric.topic"],
            "limit": 100  # Increased limit
        }
        
        try:
            print(f"Testing query with dataset: {dataset}, cluster ID: {cluster_id}")
            results = client.query_metrics_data([test_metric], use_individual_settings=True)
            data_points = results.get('io.confluent.kafka.server/received_bytes', [])
            print(f"Query results: {len(data_points)} data points")
            
            # Show sample data if available
            if data_points:
                latest = client.extract_latest_timestamp(data_points)
                print(f"Latest timestamp: {latest}")
                print(f"Sample data point: {data_points[0]}")
            else:
                print("No data points returned - check if cluster ID is correct and has recent activity")
                
        except Exception as e:
            print(f"Query test failed: {e}")
    elif len(sys.argv) > 2 and sys.argv[1] == "descriptors":
        dataset = sys.argv[2]
        # Test descriptors
        try:
            metrics = client.fetch_metrics(dataset)
            resources = client.fetch_resources(dataset)
            print(f"Fetched {len(metrics)} metrics, {len(resources)} resources from dataset: {dataset}")
        except Exception as e:
            print(f"Descriptor test failed for dataset {dataset}: {e}")
    else:
        print("Usage: python3 confluent_api.py [descriptors|query] [cloud|cloud-custom]")