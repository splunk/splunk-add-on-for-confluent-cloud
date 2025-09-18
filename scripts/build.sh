#!/usr/bin/env bash
# File: scripts/build.sh
# Purpose:
#   - Build with UCC and apply required file edits
#   - Verify a package with Splunk AppInspect REST API
#
# Usage:
#   Build only:          ./scripts/build.sh --build
#   Verify only:         ./scripts/build.sh --verify ./confluent_addon_for_splunk-0.0.29.tar.gz
#   Build a given version and then verify:   ./scripts/build.sh --build 0.0.29 --verify
#
# Optional:
#   --xtrace                       # enable bash xtrace (set -x) for extra verbosity
#
# AppInspect env (for --verify):
#   export SPLUNK_COM_USERNAME=...
#   export SPLUNK_COM_PASSWORD=...
#   # Optional tags:
#   export RUN_CLOUD_VETTING=true         # include tags 'cloud'
#   export INCLUDE_FUTURE_TAG=true        # add 'future' alongside cloud
#
# Behavior:
#   - Runs from <repo>/confluent_addon_for_splunk/scripts
#   - Ensures standard layout exists (globalConfig.json, package/app.manifest, scripts)
#   - ucc-gen build -v --ta-version <VER>  (UI build runs via additional_packaging.py → scripts/build-ui.sh)
#   - Post-build edits (restmap.conf, web.conf, inputs.conf, httpx_ratelimiter.py)
#   - ucc-gen package --path output/confluent_addon_for_splunk  (tar.gz appears in root)
#   - Verify uses AppInspect API; saves HTML+JSON reports next to this script
#   - Prints ERRORS / FAILURES / WARNINGS separately; fails on errors/failures

set -euo pipefail

export PYTHONDONTWRITEBYTECODE=1
export PIP_NO_COMPILE=1
# ---------- Paths ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"      # .../confluent_addon_for_splunk
APP_NAME="confluent_addon_for_splunk"
OUT_DIR="${ROOT_DIR}/output/${APP_NAME}"
PKG_DIR="${ROOT_DIR}/package"
GC_JSON="${ROOT_DIR}/globalConfig.json"
SCRIPTS_DIR="${ROOT_DIR}/scripts"

# ---------- Arg parsing ----------
DO_BUILD=false
BUILD_VERSION=""
DO_VERIFY=false
VERIFY_FILE=""
WANT_XTRACE="${XTRACE:-false}"   # allow env XTRACE=true

print_help() {
  cat <<EOF
Usage:
  Build only:          $0 --build [semver]
  Verify only:         $0 --verify <package.tar.gz|.tgz|.spl|.zip>
  Build then verify:   $0 --build [semver] --verify

Options:
  --build [version]     UCC build (-v) with optional --ta-version, apply file edits, then package.
                        If version is omitted, uses version from globalConfig.json.
  --verify [FILE]       Verify via AppInspect REST API. If used with --build, FILE is ignored.
  --xtrace              Enable bash xtrace (set -x). Password is redacted during login.
  --help                Show this help.

Env for verification:
  SPLUNK_COM_USERNAME, SPLUNK_COM_PASSWORD
  RUN_CLOUD_VETTING=true       # optional (adds included_tags=cloud)
  INCLUDE_FUTURE_TAG=true      # optional (adds 'future' alongside cloud)
EOF
}

# First pass: parse args (so we can enable xtrace early if requested)
ARGS=("$@")
i=0
while [ $i -lt ${#ARGS[@]} ]; do
  a="${ARGS[$i]}"
  case "$a" in
    --xtrace) WANT_XTRACE=true ;;
  esac
  i=$((i+1))
done
if [ "${WANT_XTRACE}" = "true" ]; then
  echo "[INFO] Enabling xtrace (set -x). Credentials will be redacted during login."
  set -x
fi

# Full parse
while (( "$#" )); do
  case "$1" in
    --help|-h) print_help; exit 0 ;;
    --xtrace)  : ;;  # already handled above
    --build)
      DO_BUILD=true
      # Check if next argument exists and is not another flag
      if [ $# -gt 1 ] && [[ ! "$2" =~ ^-- ]]; then
        BUILD_VERSION="$2"
        shift
      fi
      ;;
    --verify)
      DO_VERIFY=true
      if [ $# -gt 1 ] && [[ ! "$2" =~ ^-- ]]; then
        VERIFY_FILE="$2"; shift
      fi
      ;;
    *)
      echo "[ERROR] Unknown argument: $1"
      print_help
      exit 2
      ;;
  esac
  shift
done

if ! $DO_BUILD && ! $DO_VERIFY; then
  echo "[ERROR] No action specified. Use --build <ver> and/or --verify."
  exit 2
fi

# ---------- Utilities ----------
need_tool() { command -v "$1" >/dev/null 2>&1 || { echo "[ERROR] '$1' is required"; exit 1; }; }

get_version_from_global_config() {
  need_tool python3
  [ -f "${GC_JSON}" ] || { echo "[ERROR] globalConfig.json not found: ${GC_JSON}"; exit 1; }
  python3 -c "
import json, sys
try:
    with open('${GC_JSON}', 'r') as f:
        config = json.load(f)
    version = config.get('meta', {}).get('version')
    if not version:
        print('[ERROR] Version not found in globalConfig.json meta.version', file=sys.stderr)
        sys.exit(1)
    print(version)
except Exception as e:
    print(f'[ERROR] Failed to read version from globalConfig.json: {e}', file=sys.stderr)
    sys.exit(1)
"
}

strip_package_ext() {
  local base="$1"
  base="${base##*/}"
  base="${base%.tar.gz}"; base="${base%.tgz}"; base="${base%.spl}"; base="${base%.zip}"
  echo "$base"
}

assert_layout() {
  echo "[INFO] Checking add-on root layout..."
  [ -d "${ROOT_DIR}" ] || { echo "[ERROR] Root not found: ${ROOT_DIR}"; exit 1; }
  [ -f "${GC_JSON}" ] || { echo "[ERROR] Missing ${GC_JSON}"; exit 1; }
  [ -d "${PKG_DIR}" ] || { echo "[ERROR] Missing ${PKG_DIR}/"; exit 1; }
  [ -f "${PKG_DIR}/app.manifest" ] || { echo "[ERROR] Missing ${PKG_DIR}/app.manifest"; exit 1; }
  [ -d "${SCRIPTS_DIR}" ] || { echo "[ERROR] Missing ${SCRIPTS_DIR}/"; exit 1; }
  echo "[INFO] Layout OK."
}

# --- compiled Python pruning & verification ---
prune_compiled_python() {
  echo "[INFO] Removing compiled Python artifacts from ${OUT_DIR}"
  # List (for logs), then delete .pyc/.pyo and __pycache__ directories
  local cnt_before
  cnt_before=$(find "${OUT_DIR}" -type f \( -name '*.pyc' -o -name '*.pyo' -o -name '*$py.class' \) | wc -l | tr -d ' ')
  local dirs_before
  dirs_before=$(find "${OUT_DIR}" -type d -name '__pycache__' | wc -l | tr -d ' ')
  if [ "${cnt_before}" != "0" ] || [ "${dirs_before}" != "0" ]; then
    echo "[INFO]   found ${cnt_before} files and ${dirs_before} __pycache__ dirs to delete"
  fi
  # delete files
  find "${OUT_DIR}" -type f \( -name '*.pyc' -o -name '*.pyo' -o -name '*$py.class' \) -print -delete
  # delete dirs (print first for visibility)
  find "${OUT_DIR}" -type d -name '__pycache__' -print -exec rm -rf {} +
  # report after
  local cnt_after
  cnt_after=$(find "${OUT_DIR}" -type f \( -name '*.pyc' -o -name '*.pyo' -o -name '*$py.class' \) | wc -l | tr -d ' ')
  local dirs_after
  dirs_after=$(find "${OUT_DIR}" -type d -name '__pycache__' | wc -l | tr -d ' ')
  echo "[INFO]   remaining compiled files: ${cnt_after}, remaining __pycache__ dirs: ${dirs_after}"
}

# Remove arch-specific native extensions and fail if any remain.
scrub_native_extensions() {
  local app_dir="${ROOT_DIR}/output/${APP_NAME}"
  local app_lib="${app_dir}/lib"

  echo "[INFO] Scrubbing native extensions to satisfy aarch64_compatibility…"

  # (A) Remove optional speedups we can safely drop (charset_normalizer speedups)
  if [ -d "${app_lib}/charset_normalizer" ]; then
    find "${app_lib}/charset_normalizer" -type f -name '*.so' -print -delete || true
  fi

  # (B) Detect any native binaries that must NOT ship (e.g., orjson, ciso8601, ujson, etc.)
  local natives
  natives="$(find "${app_lib}" -type f \( -name '*.so' -o -name '*.pyd' -o -name '*.dylib' \) 2>/dev/null || true)"

  if [ -n "${natives}" ]; then
    echo "[ERROR] Native binaries found in packaged lib (not Cloud-safe):"
    echo "${natives}" | sed 's/^/  - /'
    echo "[HINT] Remove native deps (e.g., orjson, ciso8601) from package/lib/requirements.txt, or replace with pure-Python alternatives."
    exit 1
  fi
}

verify_no_compiled_in_tarball() {
  # args: <tarball>
  local tarball="$1"
  echo "[INFO] Verifying no compiled Python is present in package: ${tarball}"
  # list contents and look for compiled artifacts
  if tar -tzf "${tarball}" | grep -E '\.py[co]$|(^|/)__pycache__/|/\$py\.class' >/dev/null; then
    echo "[ERROR] Compiled Python detected in final package. Failing the build."
    # show a few offenders for debugging
    tar -tzf "${tarball}" | grep -E '\.py[co]$|(^|/)__pycache__/|/\$py\.class' | head -n 50 >&2
    exit 2
  fi
  echo "[INFO] Package is clean (no compiled Python)."
}

# ---------- Required file edits after UCC build ----------
apply_post_build_edits() {
  echo "[INFO] Applying post-build edits in ${OUT_DIR}"

  # 3.1 Replace httpx_ratelimiter.py
  local src_py="${SCRIPTS_DIR}/httpx_ratelimiter.py"
  local dst_py="${OUT_DIR}/lib/httpx_ratelimiter/httpx_ratelimiter.py"
  [ -f "${src_py}" ] || { echo "[ERROR] Not found: ${src_py}"; exit 1; }
  [ -f "${dst_py}" ] || { echo "[ERROR] Not found (UCC output expected): ${dst_py}"; exit 1; }
  cp -f "${src_py}" "${dst_py}"
  echo "[INFO]   Replaced ${dst_py}"

  # 3.2 restmap.conf appends
  local restmap="${OUT_DIR}/default/restmap.conf"
  [ -f "${restmap}" ] || { echo "[ERROR] Not found: ${restmap}"; exit 1; }
  printf "\n" >> "${restmap}"
  cat >> "${restmap}" <<'RESTMAP_EOF'
[script:metrics_list]
script            = metrics_list_handler.py
scripttype        = persist
python.version    = python3.9
handler           = metrics_list_handler.MetricsListHandler
output_modes      = json
match             = /confluent_metrics/list
requireAuthentication = true
passHttpHeaders   = true
passSession       = true
passConf          = false

[script:metrics_refresh]
script            = metrics_refresh_handler.py
scripttype        = persist
python.version    = python3.9
handler           = metrics_refresh_handler.MetricsRefreshHandler
output_modes      = json
match             = /confluent_metrics/refresh
requireAuthentication = true
passHttpHeaders   = true
passSession       = true
passConf          = false

[script:metrics_update]
script            = metrics_update_handler.py
scripttype        = persist
python.version    = python3.9
handler           = metrics_update_handler.MetricsUpdateHandler
output_modes      = json
match             = /confluent_metrics/update
requireAuthentication = true
passHttpHeaders   = true
passSession       = true
passConf          = false
passPayload       = true

[script:resources_for_dataset]
script            = resources_for_dataset_handler.py
scripttype        = persist
python.version    = python3.9
handler           = resources_for_dataset_handler.ResourcesForDatasetHandler
output_modes      = json
match             = /confluent_metrics/resources_for_dataset
requireAuthentication = true
passHttpHeaders   = true
passSession       = true
passConf          = false

[script:discovery_status]
script            = metrics_discovery_status_handler.py
scripttype        = persist
python.version    = python3.9
handler           = metrics_discovery_status_handler.MetricsDiscoveryStatusHandler
output_modes      = json
match             = /confluent_metrics/discovery_status
requireAuthentication = true
passHttpHeaders   = true
passSession       = true
passConf          = false
RESTMAP_EOF
  echo "[INFO]   Edited ${restmap}"

  # 3.3 web.conf appends
  local webconf="${OUT_DIR}/default/web.conf"
  [ -f "${webconf}" ] || { echo "[ERROR] Not found: ${webconf}"; exit 1; }
  printf "\n" >> "${webconf}"
  cat >> "${webconf}" <<'WEB_EOF'
[expose:metrics_refresh]
pattern = confluent_metrics/refresh
methods = POST, GET

[expose:metrics_update]
pattern = confluent_metrics/update
methods = PUT

[expose:metrics_list]
pattern = confluent_metrics/list
methods = GET

[expose:resources_for_dataset]
pattern = confluent_metrics/resources_for_dataset
methods = GET

[expose:discovery_status]
pattern = confluent_metrics/discovery_status
methods = GET
WEB_EOF
  echo "[INFO]   Edited ${webconf}"

  # 3.4 inputs.conf: line 2 -> python.version = python3.9
  local inputs="${OUT_DIR}/default/inputs.conf"
  [ -f "${inputs}" ] || { echo "[ERROR] Not found: ${inputs}"; exit 1; }
  sed -i '2s/^\(python\.version[[:space:]]*=[[:space:]]*\)python3\(\(\.9\)\)\?$/\1python3.9/' "${inputs}"
  echo "[INFO]   Edited ${inputs} (line 2 -> python3.9)"

  echo "[INFO] Post-build edits complete."
}

# ---------- UCC Build pipeline ----------
run_build() {
  need_tool ucc-gen
  need_tool sed
  assert_layout

  # Determine version to use
  local version_arg=""
  local actual_version=""
  
  if [ -n "${BUILD_VERSION}" ]; then
    actual_version="${BUILD_VERSION}"
    echo "[INFO] Using specified version: ${actual_version}"
  else
    actual_version="$(get_version_from_global_config)"
    echo "[INFO] Using version from globalConfig.json: ${actual_version}"
  fi

  # Always pass --ta-version to prevent UCC from auto-generating versions
  version_arg="--ta-version ${actual_version}"

  echo "[INFO] Running: ucc-gen build -v ${version_arg}"
  ( cd "${ROOT_DIR}" && ucc-gen build -v ${version_arg} )

  echo "[INFO] Applying required edits…"
  apply_post_build_edits
  prune_compiled_python
  scrub_native_extensions

  echo "[INFO] Packaging: ucc-gen package --path output/${APP_NAME}"
  ( cd "${ROOT_DIR}" && ucc-gen package --path "output/${APP_NAME}" )

  local expect="${ROOT_DIR}/${APP_NAME}-${actual_version}.tar.gz"
  if [ -f "${expect}" ]; then
    echo "[INFO] Package created: ${expect}"
    verify_no_compiled_in_tarball "${expect}"
    echo "${expect}"
    return 0
  fi

  local fallback
  fallback="$(ls -1t "${ROOT_DIR}/${APP_NAME}-"*.tar.gz 2>/dev/null | head -n1 || true)"
  if [ -n "${fallback}" ]; then
    echo "[WARN] Expected ${expect} not found; using latest tarball: ${fallback}"
    verify_no_compiled_in_tarball "${fallback}"
    echo "${fallback}"
    return 0
  fi

  echo "[ERROR] Could not locate packaged tarball in ${ROOT_DIR}"
  exit 1
}

# ---------- AppInspect (REST API) ----------
api_login() {  # stdout: JWT (xtrace suppressed to avoid password leak)
  need_tool curl; need_tool python3
  : "${SPLUNK_COM_USERNAME:?[ERROR] Set SPLUNK_COM_USERNAME for verification}"
  : "${SPLUNK_COM_PASSWORD:?[ERROR] Set SPLUNK_COM_PASSWORD for verification}"

  # Temporarily disable xtrace while executing the credentialed request
  local had_xtrace=0
  case "$-" in *x*) had_xtrace=1; set +x ;; esac
  local token_json
  token_json="$(curl -sS -u "${SPLUNK_COM_USERNAME}:${SPLUNK_COM_PASSWORD}" \
    "https://api.splunk.com/2.0/rest/login/splunk")"
  if [ $had_xtrace -eq 1 ]; then set -x; fi

  printf '%s' "${token_json}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["token"])'
}

api_submit() { # args: <jwt> <pkg> [included_tags_string]; stdout: JSON
  local token="$1"; local pkg="$2"; local tags="${3:-}"
  if [ -n "${tags}" ]; then
    curl -sS -X POST \
      -H "Authorization: Bearer ${token}" \
      -H "Cache-Control: no-cache" \
      -F "app_package=@${pkg}" \
      -F "included_tags=${tags}" \
      "https://appinspect.splunk.com/v1/app/validate"
  else
    curl -sS -X POST \
      -H "Authorization: Bearer ${token}" \
      -H "Cache-Control: no-cache" \
      -F "app_package=@${pkg}" \
      "https://appinspect.splunk.com/v1/app/validate"
  fi
}

api_request_id() { python3 -c 'import sys,json; print(json.load(sys.stdin)["request_id"])'; }

api_wait() { # args: <jwt> <request_id>
  local token="$1"; local req="$2"; local status="PROCESSING"
  echo "[INFO] AppInspect: polling status (request_id=${req})"
  until [ "${status}" != "PROCESSING" ]; do
    sleep 5
    status="$(curl -sS -H "Authorization: Bearer ${token}" \
      "https://appinspect.splunk.com/v1/app/validate/status/${req}" \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')"
    echo "[INFO]   status=${status}"
  done
  [ "${status}" = "SUCCESS" ] || { echo "[ERROR] AppInspect status=${status}"; exit 1; }
}

api_save_reports() { # args: <jwt> <request_id> <basename>
  local token="$1"; local req="$2"; local base="$3"
  local html="${SCRIPT_DIR}/${base}.html"
  local json="${SCRIPT_DIR}/${base}.json"
  curl -sS -H "Authorization: Bearer ${token}" -H "Content-Type: text/html" \
       "https://appinspect.splunk.com/v1/app/report/${req}" > "${html}"
  curl -sS -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" \
       "https://appinspect.splunk.com/v1/app/report/${req}" > "${json}"
  echo "[INFO] Saved reports: ${html} | ${json}"
}

print_grouped_findings() { # args: <json_path> <label>
  python3 - "$1" "$2" <<'PY'
import json,sys
p,label = sys.argv[1], sys.argv[2]
with open(p,'r',encoding='utf-8') as f:
    data=json.load(f)

def reps(obj):
    if isinstance(obj.get("reports"), list) and obj["reports"]:
        yield from obj["reports"]
    else:
        yield obj

def msgs(chk):
    out=[]
    for m in chk.get("messages") or []:
        code = m.get("code","")
        fn   = m.get("filename","")
        ln   = m.get("line","")
        msg  = (m.get("message") or "").strip()
        loc = f"{fn}:{ln}".rstrip(":")
        out.append((loc,code,msg))
    return out

b={"error":[],"failure":[],"warning":[]}
for r in reps(data):
    for g in r.get("groups",[]) or []:
        gname=g.get("name","<group>")
        for c in g.get("checks",[]) or []:
            res=(c.get("result") or "").lower()
            if res in b:
                b[res].append((gname, c.get("name","<check>"),
                               (c.get("description") or "").strip(),
                               msgs(c)))

def section(title, items):
    print(f"\n==== {title} ({label}) ====")
    if not items:
        print("(none)")
        return
    for gname, cname, cdesc, imsgs in items:
        print(f"[{title[:-1]}] {cname}  (group: {gname})")
        if cdesc:
            print(f"  desc: {cdesc}")
        for loc, code, msg in imsgs:
            loc_s = f" file={loc}" if loc else ""
            code_s= f" code={code}" if code else ""
            print(f"  -{loc_s}{code_s}")
            if msg:
                print(f"    message: {msg}")

section("ERRORS",   b["error"])
section("FAILURES", b["failure"])
section("WARNINGS", b["warning"])

print(f"\n---- Summary ({label}) ----")
print(f"errors={len(b['error'])}, failures={len(b['failure'])}, warnings={len(b['warning'])}")
PY
}

check_failures() { # args: <json_path> -> exit 0 if no failures/errors, else 2
  python3 - "$1" <<'PY'
import json,sys
p=sys.argv[1]
with open(p,'r',encoding='utf-8') as f:
    data=json.load(f)
def s(obj): return obj.get("summary") or {}
if "summary" in data:
    fail=int(s(data).get("failure",0)); err=int(s(data).get("error",0))
else:
    fail=err=0
    for r in data.get("reports") or []:
        fail+=int(s(r).get("failure",0)); err+=int(s(r).get("error",0))
print(f"failures={fail}, errors={err}")
sys.exit(0 if (fail==0 and err==0) else 2)
PY
}

verify_package() { # args: <abs_package_path>
  local pkg="$1"
  [ -f "${pkg}" ] || { echo "[ERROR] Package not found: ${pkg}"; exit 1; }
  local base; base="$(strip_package_ext "${pkg}")"

  echo "[INFO] AppInspect: authenticate"
  local jwt; jwt="$(api_login)"; [ -n "${jwt}" ] || { echo "[ERROR] Failed to get JWT"; exit 1; }

  # Full scan (default checks)
  echo "[INFO] AppInspect: submit (Full)"
  local req_full; req_full="$(api_submit "${jwt}" "${pkg}" | api_request_id)"
  [ -n "${req_full}" ] || { echo "[ERROR] No request_id (Full)"; exit 1; }
  api_wait "${jwt}" "${req_full}"
  api_save_reports "${jwt}" "${req_full}" "appinspect_${base}_full"
  print_grouped_findings "${SCRIPT_DIR}/appinspect_${base}_full.json" "FULL"
  local rc_full=0; check_failures "${SCRIPT_DIR}/appinspect_${base}_full.json" || rc_full=$?

  # Optional cloud vetting
  local rc_cloud=0
  if [ "${RUN_CLOUD_VETTING:-false}" = "true" ]; then
    local tags="cloud"
    if [ "${INCLUDE_FUTURE_TAG:-false}" = "true" ]; then
      tags="${tags},future"
    fi
    echo "[INFO] AppInspect: submit (Cloud; included_tags=${tags})"
    local req_cloud; req_cloud="$(api_submit "${jwt}" "${pkg}" "${tags}" | api_request_id)"
    [ -n "${req_cloud}" ] || { echo "[ERROR] No request_id (Cloud)"; exit 1; }
    api_wait "${jwt}" "${req_cloud}"
    api_save_reports "${jwt}" "${req_cloud}" "appinspect_${base}_cloud"
    print_grouped_findings "${SCRIPT_DIR}/appinspect_${base}_cloud.json" "CLOUD"
    check_failures "${SCRIPT_DIR}/appinspect_${base}_cloud.json" || rc_cloud=$?
  else
    echo "[INFO] Cloud vetting disabled (set RUN_CLOUD_VETTING=true to enable)."
  fi

  if [ $rc_full -ne 0 ] || [ $rc_cloud -ne 0 ]; then
    echo "[ERROR] AppInspect reported failures/errors. See grouped output and saved reports."
    exit 2
  fi
  echo "[INFO] AppInspect PASSED (Full$( [ "${RUN_CLOUD_VETTING:-false}" = "true" ] && echo ' + Cloud') )"
}

# ---------- Orchestrate ----------
BUILT_PKG=""
if $DO_BUILD; then
  need_tool ucc-gen
  echo "[INFO] === BUILD FLOW ==="
  BUILT_PKG="$(run_build)"
fi

if $DO_VERIFY; then
  need_tool curl; need_tool python3
  echo "[INFO] === VERIFY FLOW ==="
  if $DO_BUILD; then
    echo "[INFO] Build+Verify: verifying the package we just built"
    verify_package "${BUILT_PKG}"
  else
    if [ -z "${VERIFY_FILE}" ]; then
      echo "[ERROR] --verify requires a package filename when --build is not provided"; exit 2
    fi
    # Resolve to absolute path for safety
    if [ -f "${VERIFY_FILE}" ]; then
      VERIFY_FILE="$(cd "$(dirname "${VERIFY_FILE}")" && pwd)/$(basename "${VERIFY_FILE}")"
    fi
    verify_package "${VERIFY_FILE}"
  fi
else
  echo "[INFO] Verification not requested."
fi

echo "[INFO] DONE."
