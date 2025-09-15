#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Custom REST handler used by SplunkUCC to validate ConfluentCloud **MetricsAPI**
credentials entered on the *Account* tab.

Validation strategy
-------------------
1. Issue **GET** to https://api.telemetry.confluent.cloud/v2/metrics/cloud/export using HTTPBasic-Auth (`api_key` / `api_secret`).
2. **If Confluent responds 401Unauthorized (or 403Forbidden)** → credentials are wrong → raise *RestError* so the UI shows a validation failure.
3. **Any other HTTP status** (200,400"Please specify at least one valid resource", 405, 406, 5xx …) proves authentication succeeded, so the handler returns *OK*.

*Important change:* we **do not send an*Accept* header**.  Confluent replies `406NotAcceptable` only when an unsupportedAccept is supplied; omitting the header yields the predictable 400/401 behaviour that lets us differentiate
valid vs. invalid keys.
"""

import import_declare_test

import os
import sys
import logging
from typing import Any, Optional

APPNAME = "confluent_addon_for_splunk"

BIN_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_DIR = os.path.abspath(os.path.join(BIN_DIR, "..", "lib"))
for _p in {BIN_DIR, LIB_DIR}:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Splunk UCC / solnlib imports
# ---------------------------------------------------------------------------
from splunktaucclib.rest_handler.admin_external import AdminExternalHandler
from splunktaucclib.rest_handler.error import RestError
from solnlib.log import Logs
from solnlib import log
from solnlib import conf_manager 

# ---------------------------------------------------------------------------
# Third-party HTTP client
# ---------------------------------------------------------------------------
import requests
from requests.auth import HTTPBasicAuth

# ---------------------------------------------------------------------------
# Logger setup
# ---------------------------------------------------------------------------
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APPNAME,
    log_level=logging.INFO,
)
_LOGGER = Logs().get_logger("confluent_account_validator")

def _apply_dynamic_log_level(session_key: Optional[str], app_name: str = APPNAME) -> None:
    """
    Set module logger level from [logging] in {app_name}_settings.conf.
    Call this at the start of each handler method.
    """
    try:
        level_name = conf_manager.get_log_level(
            logger=_LOGGER,
            session_key=session_key or "",
            app_name=app_name,
            conf_name=f"{app_name}_settings",
        )
        _LOGGER.setLevel(getattr(logging, str(level_name).upper(), logging.INFO))
        _LOGGER.info("Dynamic log level set to %s (%s).", level_name, _LOGGER.getEffectiveLevel())
    except Exception as e:
        _LOGGER.warning("Unable to set dynamic log level: %s", e)
        _LOGGER.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EXPORT_URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/export"
REQ_TIMEOUT = (5.0, 10.0)  # (connect, read) seconds


# ---------------------------------------------------------------------------
# Helper- live auth check
# ---------------------------------------------------------------------------
def _validate_credentials(api_key: Optional[str], api_secret: Optional[str]) -> None:
    """
    Contact ConfluentCloud and raise *RestError* when credentials are invalid.

    Success⇢ returns *None*.
    Failure⇢ raises *RestError* so UCC shows a toast in the UI.
    """
    if not api_key or not api_secret:
        err = ValueError("Both *APIKey* and *APISecret* are required.")
        log.log_configuration_error(_LOGGER, err)
        raise RestError(400, str(err))

    _LOGGER.debug(
        "Validating Confluent credentials via GET %s (key=%s, secret=***).",
        EXPORT_URL,
        api_key,
    )

    try:
        resp = requests.get(
            EXPORT_URL,
            auth=HTTPBasicAuth(api_key, api_secret),
            timeout=REQ_TIMEOUT,
            headers={
                "User-Agent": "confluent_splunk_addon/validator",
            },
            verify=True,
        )
    
    except requests.exceptions.RequestException as exc:
        log.log_connection_error(_LOGGER, exc)
        _LOGGER.error("Network failure talking to ConfluentAPI: %s", exc, exc_info=True)
        raise RestError(
            502,
            (
                "Unable to reach ConfluentCloud MetricsAPI "
                "(check firewall, DNS and outbound TLS)."
            ),
        ) from exc

    _LOGGER.debug("Confluent response status=%s", resp.status_code)
    _LOGGER.debug("Confluent response body:\n%s", resp.text)

    if resp.status_code == 401:
        # Invalid credentials – show in UCC under Authentication errors
        log.log_authentication_error(_LOGGER, RuntimeError("HTTP 401 from Confluent during validation"))
        raise RestError(401, "ConfluentCloud rejected the supplied APIKey / Secret (HTTP401).")

    if resp.status_code == 403:
        # Forbidden – often permission/entitlement. You currently treat it like auth failure.
        # Categorize as Permission error for dashboards, but keep your 401 surface for UI consistency.
        log.log_permission_error(_LOGGER, RuntimeError("HTTP 403 from Confluent during validation"))
        raise RestError(401, "ConfluentCloud rejected the supplied APIKey / Secret (HTTP403).")

    if 500 <= resp.status_code <= 599:
        log.log_server_error(_LOGGER, RuntimeError(f"HTTP {resp.status_code} from Confluent during validation"))


    # Any other status → auth OK.
    _LOGGER.info(
        "Confluent credentials accepted - server returned HTTP %d.",
        resp.status_code,
    )


# ---------------------------------------------------------------------------
# UCC AdminExternalHandler
# ---------------------------------------------------------------------------
class ConfluentAccountValidator(AdminExternalHandler):
    """
    UCC invokes this handler for CRUD on the *account* conf.
    We hook into CREATE/EDIT to verify credentials before saving.
    """

    def handleList(self, confInfo: Any) -> None:
        super().handleList(confInfo)

    def handleRemove(self, confInfo: Any) -> None:
        super().handleRemove(confInfo)

    def handleCreate(self, confInfo: Any) -> None:
        _validate_credentials(
            self.payload.get("api_key"),
            self.payload.get("api_secret"),
        )
        super().handleCreate(confInfo)

    def handleEdit(self, confInfo: Any) -> None:
        # If the user did not change one of the fields it comes back as None;
        # fall back to existing stanza values so we always validate a full pair.
        api_key = self.payload.get("api_key") or self.existing.get("api_key")
        api_secret = self.payload.get("api_secret") or self.existing.get(
            "api_secret"
        )
        _validate_credentials(api_key, api_secret)
        super().handleEdit(confInfo)