from __future__ import annotations

import logging
import os
import sys
APP_DIR = os.path.dirname(os.path.realpath(__file__))
LIB_PATH = os.path.abspath(os.path.join(APP_DIR, "..", "lib"))
if LIB_PATH not in sys.path:
    sys.path.insert(0, LIB_PATH)

import json
from typing import List, Dict, Any

from solnlib import splunk_rest_client
from solnlib.log import Logs
from solnlib import log
from solnlib import conf_manager
from splunklib import binding


# ────── Constants ──────────────────────────────────────────────────────────
COLLECTION = "confluent_metrics_definitions"     # default collection name
OWNER      = "nobody"       # KV-store config must live here
APP_NAME = globals().get("APP_NAME", "confluent_addon_for_splunk")

# ────── Logging setup ──────────────────────────────────────────────────────
Logs.set_context(
    directory=os.path.join(
        os.environ.get("SPLUNK_HOME", "/opt/splunk"), "var", "log", "splunk"
    ),
    namespace=APP_NAME,
    log_level=logging.INFO,
)
LOG = Logs().get_logger("kv_ops")

def apply_dynamic_log_level(session_key: str, app_name: str = APP_NAME) -> None:
    """
    Set this module's logger level from [logging] in {app_name}_settings.conf.
    Call this from the caller once you have a session key.
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


# ────── SDK HTTP logging hook ──────────────────────────────────────────────
def wiretap(client: splunk_rest_client.SplunkRestClient) -> None:
    """Log every underlying SDK HTTP call with the correct verb."""
    def make_proxy(method: str, fn):
        LOG.debug("Wiretap: Intercepting HTTP %s", method.upper())
        def proxy(path, *args, **kwargs):
            body = kwargs.get("body")
            LOG.debug("SDK %s %s  body=%s",
                      method.upper(), path,
                      body.decode() if body is not None and hasattr(body, "decode") else body)
            return fn(path, *args, **kwargs)
        return proxy

    for method in ("get", "post", "delete"):
        raw_fn = getattr(client.http, method)
        setattr(client.http, method, make_proxy(method, raw_fn))


# ────── KV-Store Manager Class ─────────────────────────────────────────────
class KVStoreManager:
    """
    Thin wrapper around SplunkRestClient to manage KV Store collections.
    """
    def __init__(self, client: splunk_rest_client.SplunkRestClient, collection: str = COLLECTION):
        self.svc = client
        self.collection = collection

    def ensure_collection(self) -> Any:
        """
        Create the collection if it doesn't exist. Return the collection object.
        """
        try:
            kv = self.svc.kvstore
            if self.collection not in kv:
                LOG.debug("Creating KV collection %s", self.collection)
                kv.create(self.collection)  # POST to .../config/_new
                LOG.info("Collection %s created", self.collection)
            else:
                LOG.debug("Collection %s already exists", self.collection)
            return kv[self.collection]
        
        except binding.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (400, 404, 409, 402, 503):
                log.log_configuration_error(LOG, e)
            elif status == 401:
                log.log_authentication_error(LOG, e)
            elif status == 403:
                log.log_permission_error(LOG, e)
            elif status in (500,):
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "kv ensure_collection error")
            raise

        except Exception as e:
            log.log_exception(LOG, e, "kv ensure_collection error")
            raise


    def batch_upsert(self, docs: List[Dict[str, Any]]) -> int:
        """
        Batch upsert a list of documents. Returns the number of docs inserted.
        """
        try:
            coll = self.ensure_collection()
            LOG.debug("Calling batch_save with %d docs", len(docs))
            coll.data.batch_save(*docs)  # requires unpacking
            LOG.info("Upserted %d records into %s", len(docs), self.collection)
            return len(docs)
        except binding.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (400, 404, 409, 402, 503):
                log.log_configuration_error(LOG, e)
            elif status == 401:
                log.log_authentication_error(LOG, e)
            elif status == 403:
                log.log_permission_error(LOG, e)
            elif status in (500,):
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "kv batch_upsert error")
            raise
        except Exception as e:
            log.log_exception(LOG, e, "kv batch_upsert error")
            raise

    def get_all(self) -> List[Dict[str, Any]]:
        """
        Return all documents currently in the collection.
        """
        try:
            coll = self.ensure_collection()
            docs = coll.data.query()
            LOG.debug("Fetched %d records from %s", len(docs), self.collection)
            return docs

        except binding.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (400, 404, 409, 402, 503):
                log.log_configuration_error(LOG, e)
            elif status == 401:
                log.log_authentication_error(LOG, e)
            elif status == 403:
                log.log_permission_error(LOG, e)
            elif status in (500,):
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "kv get_all error")
            raise

        except Exception as e:
            log.log_exception(LOG, e, "kv get_all error")
            raise

    def patch(self, metric_name: str, fields: Dict[str, Any]) -> None:
        """
        Update an existing metric-definition document, identified by its *name*
        field, with the supplied fields.

        • Never alters or fabricates `_key`.
        • Raises RuntimeError if the metric name is not present - because that
          indicates programmer / data-integrity error.
        """
        try:
            coll = self.ensure_collection()

            # --- locate by "name" ----------------------------------------------
            matches = coll.data.query(query=json.dumps({"name": metric_name}))
            if not matches:
                msg = (f"patch(): metric name '{metric_name}' not found in "
                   f"{self.collection}; KV-store out of sync.")
                log.log_configuration_error(LOG, RuntimeError(msg))
                raise RuntimeError(msg)

            # There should be exactly one doc per metric name
            doc     = matches[0]
            doc_key = doc["_key"]
            doc.update(fields)

            # PUT /storage/collections/data/<collection>/<doc_key>
            coll.data.update(doc_key, doc)
            LOG.debug(
                "KV-store PATCH ok - name='%s' (_key=%s) updated with %s",
                metric_name, doc_key, fields,
            )

        except binding.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (400, 404, 409, 402, 503):
                log.log_configuration_error(LOG, e)
            elif status == 401:
                log.log_authentication_error(LOG, e)
            elif status == 403:
                log.log_permission_error(LOG, e)
            elif status in (500,):
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "kv patch error")
            raise

        except Exception as e:
            log.log_exception(LOG, e, "kv patch error")
            raise

    def patch_by_key(self, doc_key: str, fields: Dict[str, Any]) -> None:
        """
        Update a metric-definition document by its KV `_key` with the supplied fields.

        • Never alters or fabricates `_key`.
        • Raises RuntimeError if the document is not found.
        """
        try:
            coll = self.ensure_collection()

            # --- locate by "_key" -------------------------------------------------
            doc = coll.data.query_by_id(doc_key)
            if not doc:
                msg = (f"patch_by_key(): _key '{doc_key}' not found in {self.collection}; "
                   f"KV-store out of sync.")
                log.log_configuration_error(LOG, RuntimeError(msg)) 
                raise RuntimeError(msg)

            # Merge and update
            doc.update(fields)

            # PUT /storage/collections/data/<collection>/<doc_key>
            coll.data.update(doc_key, doc)

            # Logging consistent with patch(): include both name and _key when possible
            LOG.debug(
                "KV-store PATCH ok - _key=%s (name='%s') updated with %s",
                doc_key, doc.get("name"), fields,
            )

        except binding.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (400, 404, 409, 402, 503):
                log.log_configuration_error(LOG, e)
            elif status == 401:
                log.log_authentication_error(LOG, e)
            elif status == 403:
                log.log_permission_error(LOG, e)
            elif status in (500,):
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "kv patch_by_key error")
            raise

        except Exception as e:
            log.log_exception(LOG, e, "kv patch_by_key error")
            raise

    def find_metric_by_key(self, doc_key: str) -> Dict[str, Any]:
        """
        Find a specific metric document by _key (most precise and efficient).
        Args:
            doc_key: The Splunk KV store document _key
        Returns:
            Dict: The metric document if found
        Raises:
            RuntimeError: If metric not found
        """
        try:
            coll = self.ensure_collection()
            # Direct lookup by _key (most efficient)
            doc = coll.data.query_by_id(doc_key)
            if not doc:
                msg = f"Metric with _key '{doc_key}' not found"
                log.log_configuration_error(LOG, RuntimeError(msg))
                raise RuntimeError(msg)
            LOG.debug("Found metric by _key: %s", doc_key)
            return doc
        
        except binding.HTTPError as e:
            status = getattr(e, "status", None)
            if status in (400, 404, 409, 402, 503):
                log.log_configuration_error(LOG, e)
            elif status == 401:
                log.log_authentication_error(LOG, e)
            elif status == 403:
                log.log_permission_error(LOG, e)
            elif status in (500,):
                log.log_server_error(LOG, e)
            else:
                log.log_exception(LOG, e, "kv find_metric_by_key error")
            raise

        except Exception as e:
            log.log_exception(LOG, e, "kv find_metric_by_key error")
            raise
