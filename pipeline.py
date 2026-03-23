import json
import time
import signal
import logging
import sys
import argparse
from pathlib import Path

import requests


# ══════════════════════════════════════════════════════════════════════
#  CONFIGURATION  –  edit these before running
# ══════════════════════════════════════════════════════════════════════

# ── DB / Cohort API ────────────────────────────────────────────────────
DB_API_URL    = "https://ig.gov-cloud.ai/pi-cohorts-service-dbaas/v1.0/cohorts/adhoc"  # ← no trailing comma!
AUTH_TOKEN    = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI3Ny1NUVdFRTNHZE5adGlsWU5IYmpsa2dVSkpaWUJWVmN1UmFZdHl5ejFjIn0.eyJleHAiOjE3NzI1NTYxMzEsImlhdCI6MTc3MjUyMDEzMSwianRpIjoiMjU4ZjMyMjUtODRjZS00YjFlLWE5YjMtYWMzYTFhNTFlMTkxIiwiaXNzIjoiaHR0cDovL2tleWNsb2FrLXNlcnZpY2Uua2V5Y2xvYWsuc3ZjLmNsdXN0ZXIubG9jYWw6ODA4MC9yZWFsbXMvbWFzdGVyIiwiYXVkIjpbIkJPTFRaTUFOTl9CT1RfbW9iaXVzIiwiMmNmNzZlNWYtMjZhZC00ZjJjLWJjY2MtZjRiYzFlN2JmYjY0X1RFREFBUzFfbW9iaXVzIiwiMmNmNzZlNWYtMjZhZC00ZjJjLWJjY2MtZjRiYzFlN2JmYjY0X1RlbXBsYXRlX21vYml1cyIsIkNPREVHVVJVXzY2NGFmYzNjYWYyZGZhNGU3ZWVkYTVkYiIsIlBBU0NBTF9JTlRFTExJR0VOQ0VfbW9iaXVzIiwiTU9ORVRfbW9iaXVzIiwiVklOQ0lfbW9iaXVzIiwiYWNjb3VudCJdLCJzdWIiOiIyY2Y3NmU1Zi0yNmFkLTRmMmMtYmNjYy1mNGJjMWU3YmZiNjQiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJIT0xBQ1JBQ1lfbW9iaXVzIiwic2lkIjoiZjI4MGRkMjEtNWQwZS00OTZiLWEzN2YtOTVkZmM2YzhjM2MzIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIvKiJdLCJyZXNvdXJjZV9hY2Nlc3MiOnsiSE9MQUNSQUNZX21vYml1cyI6eyJyb2xlcyI6WyJIT0xBQ1JBQ1lfQVBQUk9WRSIsIk5FR09UQVRJT05fQVBQUk9WRSIsIlBST0RVQ1RfQ1JFQVRJT05fUkVBRCIsIkFDQ09VTlRfQVBQUk9WRSIsIk9SR0FOSVpBVElPTl9SRUFEIiwiQUxMSUFOQ0VfV1JJVEUiLCJBTExJQU5DRV9FWEVDVVRFIiwiVEVOQU5UX1dSSVRFIiwiUExBVEZPUk1fV1JJVEUiLCJSQVRFX0NBUkRfQVBQUk9WRSIsIkFHRU5UU19XUklURSIsIkhPTEFDUkFDWV9VU0VSIiwiUkFURV9DQVJEX1dSSVRFIiwiQUxMSUFOQ0VfQVBQUk9WRSIsIk5FR09UQVRJT05fV1JJVEUiLCJURU5BTlRfRVhFQ1VURSIsIk9SR0FOSVpBVElPTl9BUFBST1ZFIiwiUFJPRFVDVF9MSVNUSU5HX0FQUFJPVkUiLCJQUk9EVUNUX0xJU1RJTkdfRVhFQ1VURSIsIlNVQl9BTExJQU5DRV9XUklURSIsIk9SR0FOSVpBVElPTl9FWEVDVVRFIiwiUFJPRFVDVF9DUkVBVElPTl9FWEVDVVRFIiwiUFJPRFVDVF9MSVNUSU5HX1dSSVRFIiwiTkVHT1RBVElPTl9FWEVDVVRFIiwiUFJPRFVDVF9DUkVBVElPTl9BUFBST1ZFIiwiU1VQRVJBRE1JTiIsIlBST0RVQ1RfQ1JFQVRJT05fV1JJVEUiLCJBQ0NPVU5UX1JFQUQiLCJQUk9EVUNUX0xJU1RJTkdfUkVBRCIsIk9SR0FOSVpBVElPTl9XUklURSIsIlNVQl9BTExJQU5DRV9SRUFEIiwiUkFURV9DQVJEX0VYRUNVVEUiLCJBTExJQU5DRV9SRUFEIiwiVEVOQU5UX0FQUFJPVkUiLCJBR0VOVFNfUkVBRCIsIkRBT19DUkVBVEUiLCJQTEFURk9STV9SRUFEIiwiVEVOQU5UX1JFQUQiLCJQTEFURk9STV9FWEVDVVRFIiwiQUNDT1VOVF9FWEVDVVRFIiwiU1VCX0FMTElBTkNFX0VYRUNVVEUiLCJTVUJfQUxMSUFOQ0VfQVBQUk9WRSIsIkFDQ09VTlRfV1JJVEUiLCJORUdPVEFUSU9OX1JFQUQiLCJQUk9QT1NBTF9DUkVBVEUiLCJSQVRFX0NBUkRfUkVBRCIsIlBMQVRGT1JNX0FQUFJPVkUiXX0sIkJPTFRaTUFOTl9CT1RfbW9iaXVzIjp7InJvbGVzIjpbIkJPTFRaTUFOTl9CT1RfVVNFUiIsIkJPTFRaTUFOTl9CT1RfQURNSU4iLCJXT1JLRkxPV19SRUFEIiwiUElQRUxJTkVfV1JJVEUiLCJQSVBFTElORV9SRUFEIiwiQlJJQ0tfUkVBRCIsIldPUktGTE9XX1dSSVRFIiwiQlJJQ0tfV1JJVEUiXX0sIjJjZjc2ZTVmLTI2YWQtNGYyYy1iY2NjLWY0YmMxZTdiZmI2NF9URURBQVMxX21vYml1cyI6eyJyb2xlcyI6WyJERVBMT1lfVE9LRU4iLCJUT0tFTl9UUkFOU0ZFUl9MT0dTIiwiQ1JFQVRFX1BST1BPU0FMIiwiUkVWT0tFX1BST1BPU0FMIiwiUkVBRF9EQU8iLCJBTExfVE9LRU5TIiwiVE9LRU5fQkFMQU5DRSIsIldBTExFVF9UUkFOU0FDVElPTlMiLCJERUNPREVfQUNUSU9OUyIsIlJFQURfREFPX01FTUJFUlMiLCJSRUFEX1NVQkFEQU8iLCJNT0JJVF9UT0tFTl9QUklDRSIsIlRSQU5TRkVSX1RPS0VOIiwiQ0FTVF9WT1RFIiwiRU5DT0RFX0FDVElPTlMiLCJRVUlUX0ZST01fREFPIiwiRVhFQ1VURV9QUk9QT1NBTCIsIkNSRUFURV9XQUxMRVQiLCJSRUFEX1ZPVElOR19QT1dFUiIsIkRFTEVHQVRFX1ZPVEVTIiwiVE9LRU5fSE9MREVSIiwiUkVTT0xWRV9QUk9QT1NBTCIsIkRFTEVURV9XQUxMRVQiLCJDSEFMTEVOR0VfUFJPUE9TQUwiLCJSRUFEX1BST1BPU0FMIiwiR0VUX1dBTExFVCIsIkNSRUFURV9EQU8iLCJSRUFEX0RBT19ISVNUT1JZIiwiQ0xBSU1fVk9URVMiLCJSRUFEX1BST1BPU0FMU19GUk9NX0RBTyIsIkFQUFJPVkVfVE9LRU4iXX0sIjJjZjc2ZTVmLTI2YWQtNGYyYy1iY2NjLWY0YmMxZTdiZmI2NF9UZW1wbGF0ZV9tb2JpdXMiOnsicm9sZXMiOlsiVEVNUExBVEVfV1JJVEUiLCJURU1QTEFURV9SRUFEIl19LCJDT0RFR1VSVV82NjRhZmMzY2FmMmRmYTRlN2VlZGE1ZGIiOnsicm9sZXMiOlsiRVhUUkFDVF9UWVBFU0NSSVBUIiwiRlVOQ1RJT05fUkVBRCIsIkVYVFJBQ1RfUFlUSE9OIiwiRVhUUkFDVF9KQVZBIiwiRlVOQ1RJT05fQ1JFQVRFIiwiRlVOQ1RJT05fRVhFQ1VURSIsIkdFTkVSQVRFX1BZVEhPTiIsIkdFTkVSQVRFX1RZUEVTQ1JJUFQiLCJHRU5FUkFURV9KQVZBIiwiRlVOQ1RJT05fREVMRVRFIiwiR0VORVJBVEVfS05PV0xFREdFX0dSQVBIIl19LCJQQVNDQUxfSU5URUxMSUdFTkNFX21vYml1cyI6eyJyb2xlcyI6WyJQQVNDQUxfSU5URUxMSUdFTkNFX0NPTlNVTUVSIiwiUEFTQ0FMX0lOVEVMTElHRU5DRV9VU0VSIiwiQ09IT1JUU19SRUFEIiwiQ09IT1JUU19XUklURSIsIlNDSEVNQV9XUklURSIsIlVOSVZFUlNFX1dSSVRFIiwiVU5JVkVSU0VfUkVBRCIsIlBBU0NBTF9JTlRFTExJR0VOQ0VfQURNSU4iLCJTQ0hFTUFfUkVBRCJdfSwiTU9ORVRfbW9iaXVzIjp7InJvbGVzIjpbIk1PTkVUX0FQUFJPVkUiLCJNT05FVF9VU0VSIl19LCJWSU5DSV9tb2JpdXMiOnsicm9sZXMiOlsiVklOQ0lfVVNFUiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwicmVxdWVzdGVyVHlwZSI6IlRFTkFOVCIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiTW9iaXVzIEFpZHRhYXMiLCJ0ZW5hbnRJZCI6IjJjZjc2ZTVmLTI2YWQtNGYyYy1iY2NjLWY0YmMxZTdiZmI2NCIsInBsYXRmb3JtSWQiOiJtb2JpdXMiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJwYXNzd29yZF90ZW5hbnRfYWlkdGFhc0BnYWlhbnNvbHV0aW9ucy5jb20iLCJnaXZlbl9uYW1lIjoiTW9iaXVzIiwiZmFtaWx5X25hbWUiOiJBaWR0YWFzIiwiZW1haWwiOiJwYXNzd29yZF90ZW5hbnRfYWlkdGFhc0BnYWlhbnNvbHV0aW9ucy5jb20iLCJwbGF0Zm9ybXMiOnsicm9sZXMiOlsiU0NIRU1BX1JFQUQiXX19.JwUrbD4qhW9jgjF0--we50L7rJsZlRWDDZdFL_uvyMXh_a0Ghr7KvGk8OKYFI9-VTxs5Xf-FtFjsALlrYRrXmQt6UGrmZlCARP8oXNb-kFQFNKBgu2ZhZD4pdNSURVnOj9nYB5JHgIEQ76J6acFuoacL3sH0846yz-ZiJO6IpOeUwidxZu_fpncV0PcBCOD8RiwPaTpYrZF9TcOX7t0u0DAWZzLM9WGy-rRNFPFOMfUrpnQZJwSdrs4Fhm5PGqodG_-IKUZ_jwC5ESVzLsgEXsSkaKwZ3ozXAQzwXp1m9wvaymoWy1JXYsQITHvLrWL45837kd179Ri5gn8NLhlQ"          # ← paste your token
DB_TYPE       = "TIDB"
TABLE_NAME    = "t_68c484885fafa83bbffefd22_t"    # ← your table
PAGE_SIZE     = 5000                              # rows per page

# ── LLM (Ollama) API ───────────────────────────────────────────────────
LLM_API_URL   = "http://ollama-keda.mobiusdtaas.ai"
MODEL_NAME    = "gpt-oss:20b"

# ── Output files ───────────────────────────────────────────────────────
RAW_OUTPUT      = "raw_data.json"
METRICS_OUTPUT  = "metrics.json"
ENRICHED_OUTPUT = "enriched.json"
LOG_FILE        = "pipeline.log"
CHECKPOINT      = "enriched.checkpoint.json"

# ── Misc ───────────────────────────────────────────────────────────────
REQUEST_DELAY = 0.3   # seconds between LLM calls

# ── Retry settings ─────────────────────────────────────────────────────
MAX_RETRIES   = 5     # max attempts per API call
RETRY_DELAY   = 3.0   # fixed seconds to wait between retries


# ══════════════════════════════════════════════════════════════════════
#  ISIC Rev.4 Sections
# ══════════════════════════════════════════════════════════════════════

ISIC_SECTIONS = {
    "A": "Agriculture, Forestry and Fishing",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Electricity, Gas, Steam and Air Conditioning Supply",
    "E": "Water Supply, Sewerage, Waste Management and Remediation",
    "F": "Construction",
    "G": "Wholesale and Retail Trade; Repair of Motor Vehicles",
    "H": "Transportation and Storage",
    "I": "Accommodation and Food Service Activities",
    "J": "Information and Communication",
    "K": "Financial and Insurance Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Administrative and Support Service Activities",
    "O": "Public Administration and Defence; Compulsory Social Security",
    "P": "Education",
    "Q": "Human Health and Social Work Activities",
    "R": "Arts, Entertainment and Recreation",
    "S": "Other Service Activities",
    "T": "Activities of Households as Employers",
    "U": "Activities of Extraterritorial Organisations and Bodies",
}

ISIC_LIST_FOR_PROMPT = "\n".join(
    f"  {code}: {label}" for code, label in ISIC_SECTIONS.items()
)


# ══════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════

def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ══════════════════════════════════════════════════════════════════════
#  RETRY HELPER
# ══════════════════════════════════════════════════════════════════════

def with_retry(fn, label: str, logger: logging.Logger):
    """
    Call fn() up to MAX_RETRIES times with a fixed RETRY_DELAY between
    attempts. Retries on any Exception (network errors, HTTP 5xx, timeouts).

    Returns the result of fn() on success.
    Raises the last exception if all attempts are exhausted.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"  [RETRY] {label} – attempt {attempt}/{MAX_RETRIES} failed: {exc}. "
                    f"Retrying in {RETRY_DELAY}s..."
                )
                time.sleep(RETRY_DELAY)
            else:
                logger.error(
                    f"  [RETRY] {label} – all {MAX_RETRIES} attempts failed. Last error: {exc}"
                )
    raise last_exc


# ══════════════════════════════════════════════════════════════════════
#  CHECKPOINT HELPERS
# ══════════════════════════════════════════════════════════════════════

def load_checkpoint(ckpt_path: str, logger: logging.Logger):
    p = Path(ckpt_path)
    if p.exists():
        try:
            data     = json.loads(p.read_text(encoding="utf-8"))
            enriched = data.get("enriched", [])
            next_idx = data.get("next_index", len(enriched))
            logger.info(f"Checkpoint: {len(enriched)} rows done, resuming from index {next_idx}")
            return enriched, next_idx
        except Exception as e:
            logger.warning(f"Could not read checkpoint ({e}), starting fresh")
    return [], 0


def save_checkpoint(ckpt_path: str, enriched: list, next_index: int,
                    logger: logging.Logger):
    tmp = ckpt_path + ".tmp"
    try:
        Path(tmp).write_text(
            json.dumps({"next_index": next_index, "enriched": enriched},
                       indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        Path(tmp).replace(Path(ckpt_path))
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")


def save_json(path: str, data, logger: logging.Logger, label: str = ""):
    try:
        Path(path).write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        logger.info(f"{'[' + label + '] ' if label else ''}Saved {len(data)} records → {path}")
    except Exception as e:
        logger.error(f"Failed to write {path}: {e}")


# ══════════════════════════════════════════════════════════════════════
#  SHARED DB POST
# ══════════════════════════════════════════════════════════════════════

def _post_db(session: requests.Session, query: str,
             page: int, size: int, logger: logging.Logger):
    """
    POST to the DB API with fixed-delay retry (MAX_RETRIES attempts).
    Returns the data list, None on a non-success API status, or raises
    after all retries are exhausted.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AUTH_TOKEN}",
    }

    def _call():
        resp = session.post(
            DB_API_URL,
            params={"size": size, "page": page},
            headers=headers,
            json={"type": DB_TYPE, "definition": query},
            timeout=60,
        )
        resp.raise_for_status()   # raises on 4xx/5xx → triggers retry
        return resp.json()

    body = with_retry(_call, label=f"DB API page={page}", logger=logger)

    if body.get("status") != "success":
        logger.error(f"API error: {body.get('msg')}")
        return None   # signal caller to stop (not a transient error)

    return body.get("data", [])


# ══════════════════════════════════════════════════════════════════════
#  STAGE 1 – RAW DATA FETCH
# ══════════════════════════════════════════════════════════════════════

def build_raw_query(table: str, limit: int, offset: int) -> str:
    return (
        f"SELECT indicatorId, indicatorDescription, yearCode, "
        f"countryName, value, countryId "
        f"FROM {table} LIMIT {limit} OFFSET {offset};"
    )


def stage1_fetch_raw(logger: logging.Logger) -> list:
    logger.info("=" * 60)
    logger.info("STAGE 1 – Fetching raw data")
    logger.info("=" * 60)

    all_records = []
    offset, page = 0, 1
    session = requests.Session()

    while True:
        query = build_raw_query(TABLE_NAME, PAGE_SIZE, offset)
        logger.info(f"  Page {page} | offset={offset}")

        try:
            records = _post_db(session, query, page, PAGE_SIZE, logger)
        except Exception as exc:
            logger.error(f"All retries exhausted on page {page}: {exc} – stopping.")
            break

        if records is None:
            break
        if not records:
            logger.info("  No more records.")
            break

        all_records.extend(records)
        logger.info(f"  +{len(records)} rows | total={len(all_records)}")

        if len(records) < PAGE_SIZE:
            logger.info("  Last page reached.")
            break

        offset += PAGE_SIZE
        page   += 1

    save_json(RAW_OUTPUT, all_records, logger, "Stage 1")
    return all_records


# ══════════════════════════════════════════════════════════════════════
#  STAGE 2 – METRICS FETCH
# ══════════════════════════════════════════════════════════════════════

def build_metrics_query(table: str, limit: int, offset: int) -> str:
    """
    Compute per-(yearCode, countryName, series) statistics.
    indicatorDescription is aliased as 'series' in both the subquery SELECT
    and outer SELECT. GROUP BY and ORDER BY use the original column name
    for compatibility. This alias flows through to Stage 3's LLM prompt.
    ORDER BY ensures stable pagination across pages.
    """
    return f"""
SELECT
    yearCode,
    countryName,
    indicatorDescription                             AS series,
    COUNT(value)                                     AS Count,
    MIN(value)                                       AS Min,
    MAX(CASE WHEN pct <= 0.05 THEN value END)        AS P5,
    MAX(CASE WHEN pct <= 0.10 THEN value END)        AS P10,
    MAX(CASE WHEN pct <= 0.25 THEN value END)        AS P25,
    AVG(value)                                       AS Mean,
    MAX(CASE WHEN pct <= 0.50 THEN value END)        AS Median,
    MAX(CASE WHEN pct <= 0.75 THEN value END)        AS P75,
    MAX(CASE WHEN pct <= 0.90 THEN value END)        AS P90,
    MAX(CASE WHEN pct <= 0.95 THEN value END)        AS P95,
    MAX(value)                                       AS Max,
    STDDEV(value)                                    AS Std
FROM (
    SELECT
        yearCode,
        countryName,
        indicatorDescription,
        value,
        PERCENT_RANK() OVER (
            PARTITION BY yearCode, countryName
            ORDER BY value
        ) AS pct
    FROM {table}
) ranked
GROUP BY yearCode, countryName, indicatorDescription
ORDER BY yearCode, countryName, indicatorDescription
LIMIT {limit} OFFSET {offset};
""".strip()


def stage2_fetch_metrics(logger: logging.Logger) -> list:
    logger.info("=" * 60)
    logger.info("STAGE 2 – Fetching metrics (statistics per yearCode x countryName x series)")
    logger.info("=" * 60)

    all_metrics = []
    offset, page = 0, 1
    session = requests.Session()

    while True:
        query = build_metrics_query(TABLE_NAME, PAGE_SIZE, offset)
        logger.info(f"  Page {page} | offset={offset}")

        try:
            records = _post_db(session, query, page, PAGE_SIZE, logger)
        except Exception as exc:
            logger.error(f"All retries exhausted on page {page}: {exc} – stopping.")
            break

        if records is None:
            break
        if not records:
            logger.info("  No more metric rows.")
            break

        all_metrics.extend(records)
        logger.info(f"  +{len(records)} rows | total={len(all_metrics)}")

        if len(records) < PAGE_SIZE:
            logger.info("  Last page reached.")
            break

        offset += PAGE_SIZE
        page   += 1

    save_json(METRICS_OUTPUT, all_metrics, logger, "Stage 2")
    return all_metrics


# ══════════════════════════════════════════════════════════════════════
#  STAGE 3 – LLM ENRICHMENT
# ══════════════════════════════════════════════════════════════════════

# ── Model helpers ──────────────────────────────────────────────────────

def _check_model(model: str) -> bool:
    try:
        resp = requests.get(f"{LLM_API_URL}/api/tags", timeout=30)
        resp.raise_for_status()
        return any(m.get("name") == model for m in resp.json().get("models", []))
    except Exception:
        return False


def _pull_model(model: str, logger: logging.Logger) -> bool:
    def _call():
        resp = requests.post(
            f"{LLM_API_URL}/api/pull",
            json={"name": model, "stream": False},
            timeout=600,
        )
        resp.raise_for_status()
        return True

    try:
        logger.info(f"Pulling model '{model}'...")
        with_retry(_call, label=f"model pull {model}", logger=logger)
        logger.info("Model pulled successfully.")
        return True
    except Exception as e:
        logger.error(f"Pull failed after all retries: {e}")
        return False


def ensure_model(model: str, logger: logging.Logger) -> bool:
    if _check_model(model):
        logger.info(f"Model '{model}' is available.")
        return True
    logger.warning(f"Model '{model}' not found, attempting pull...")
    return _pull_model(model, logger)


# ── LLM call ───────────────────────────────────────────────────────────

def fetch_llm_enrichment(row: dict, logger: logging.Logger) -> dict:
    """
    Send a metric row to the LLM and get back domain (ISIC code) + context.

    indicatorDescription (aliased as 'series' in metrics.json) is included
    at the top of the prompt so the model classifies based on the actual
    indicator name, not just numbers.

    Retries up to MAX_RETRIES times with a fixed RETRY_DELAY on any error.
    Falls back to domain="S" only after all retries are exhausted.
    """
    series = row.get("series", "N/A")

    prompt = f"""You are a data analyst specialising in global economic and social indicators.

A dataset row contains an indicator name and its statistical summaries for a country-year combination.
Your job is to:
  1. Infer the correct ISIC Rev.4 section (A-U) this indicator belongs to.
  2. Write a concise context string describing what the indicator measures and how it is used.

ISIC Rev.4 sections:
{ISIC_LIST_FOR_PROMPT}

Rules:
- "domain" must be ONLY the single ISIC letter (A-U) that best fits the indicator.
- "context" must follow this exact format:
    "<2-3 word sub-domain> - <2 to 3 sentence plain-English explanation of what the indicator measures and how it is used>"
  Example: "Fiscal Policy - Measures gross government debt as a percentage of GDP. Used to assess fiscal sustainability and sovereign credit risk across countries."

Respond with ONLY a valid JSON object. No markdown, no extra text.

Data row:
  Series      : {series}
  Year        : {row.get('yearCode')}
  Country     : {row.get('countryName')}
  Count       : {row.get('Count')}
  Min         : {row.get('Min')}
  P5          : {row.get('P5')}
  P10         : {row.get('P10')}
  P25         : {row.get('P25')}
  Mean        : {row.get('Mean')}
  Median      : {row.get('Median')}
  P75         : {row.get('P75')}
  P90         : {row.get('P90')}
  P95         : {row.get('P95')}
  Max         : {row.get('Max')}
  Std         : {row.get('Std')}

JSON response:"""

    def _call():
        resp = requests.post(
            f"{LLM_API_URL}/api/generate",
            headers={"Content-Type": "application/json"},
            json={"model": MODEL_NAME, "prompt": prompt, "stream": False},
            timeout=300,
        )
        resp.raise_for_status()   # raises on 5xx → triggers retry

        raw = resp.json().get("response", "").strip()

        # Strip markdown fences if model adds them
        if "```json" in raw:
            raw = raw.split("```json")[1].split("```")[0]
        elif "```" in raw:
            raw = raw.split("```")[1].split("```")[0]

        result  = json.loads(raw.strip())   # raises on bad JSON → triggers retry
        domain  = result.get("domain", "S").strip().upper()
        context = result.get("context", "").strip()

        if domain not in ISIC_SECTIONS:
            logger.warning(f"  [LLM] Invalid ISIC '{domain}', defaulting to 'S'")
            domain = "S"

        return {"domain": domain, "context": context}

    try:
        result = with_retry(
            _call,
            label=f"LLM year={row.get('yearCode')} country={row.get('countryName')} series={series[:40]}",
            logger=logger,
        )
        logger.debug(f"  [LLM OK] ISIC={result['domain']}")
        return result
    except Exception as exc:
        logger.error(f"  [LLM] All retries exhausted: {exc}. Using fallback domain='S'.")
        return {"domain": "S", "context": ""}


# ── Main enrichment loop ───────────────────────────────────────────────

def stage3_enrich(metrics: list, start_index: int, logger: logging.Logger) -> list:
    logger.info("=" * 60)
    logger.info("STAGE 3 - LLM enrichment (domain + context per metric row)")
    logger.info("=" * 60)

    total = len(metrics)
    enriched, next_idx = load_checkpoint(CHECKPOINT, logger)

    # If caller specified an explicit start index, honour it
    if start_index > 0:
        enriched = enriched[:start_index]
        next_idx = start_index
        logger.info(f"Resuming from explicit index {start_index}")

    if next_idx >= total:
        logger.info("All rows already enriched.")
        save_json(ENRICHED_OUTPUT, enriched, logger, "Stage 3")
        return enriched

    logger.info(f"Total: {total} | Start: {next_idx} | Remaining: {total - next_idx}")

    if not ensure_model(MODEL_NAME, logger):
        logger.error("Model not available. Saving partial results.")
        save_json(ENRICHED_OUTPUT, enriched, logger, "Stage 3")
        sys.exit(1)

    current_idx = next_idx

    def _handle_signal(signum, frame):
        logger.warning(f"Signal {signum} - saving and exiting...")
        save_json(ENRICHED_OUTPUT, enriched, logger, "Stage 3 (partial)")
        save_checkpoint(CHECKPOINT, enriched, current_idx, logger)
        logger.info(f"Resume with --start-index {current_idx}")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    try:
        for i in range(next_idx, total):
            current_idx = i
            row         = metrics[i]
            year        = row.get("yearCode", "?")
            country     = row.get("countryName", "?")
            series      = row.get("series", "?")

            logger.info(f"[{i}/{total - 1}] year={year} | country={country} | series={series[:50]}")

            meta       = fetch_llm_enrichment(row, logger)
            isic_code  = meta["domain"]
            isic_label = ISIC_SECTIONS.get(isic_code, "")

            logger.info(f"  -> ISIC {isic_code}: {isic_label}")

            enriched.append({
                # all original metric columns (series, yearCode, countryName, stats…)
                **row,
                # enrichment added by LLM
                "domain":  f"{isic_label}-{isic_code}",
                "context": meta["context"],
            })

            save_checkpoint(CHECKPOINT, enriched, i + 1, logger)
            time.sleep(REQUEST_DELAY)

    except Exception as exc:
        logger.error(f"Unexpected error at index {current_idx}: {exc}", exc_info=True)
        save_json(ENRICHED_OUTPUT, enriched, logger, "Stage 3 (partial)")
        save_checkpoint(CHECKPOINT, enriched, current_idx, logger)
        sys.exit(1)

    save_json(ENRICHED_OUTPUT, enriched, logger, "Stage 3")

    try:
        Path(CHECKPOINT).unlink(missing_ok=True)
        logger.info("Checkpoint removed (run complete).")
    except Exception:
        pass

    return enriched


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline: fetch raw DB data -> compute metrics -> LLM enrichment"
    )
    parser.add_argument(
        "--skip-stage", type=int, nargs="*", default=[],
        metavar="N",
        help="Stages to skip: 1 (raw fetch), 2 (metrics fetch), 3 (LLM). "
             "E.g. --skip-stage 1 2  to run only Stage 3."
    )
    parser.add_argument(
        "--start-index", type=int, default=0,
        help="Resume Stage 3 LLM enrichment from this row index."
    )
    parser.add_argument("--model",    default=MODEL_NAME,  help="LLM model name")
    parser.add_argument("--api-url",  default=LLM_API_URL, help="Ollama API base URL")
    parser.add_argument("--log-file", default=LOG_FILE,    help="Log file path")
    args = parser.parse_args()

    logger = setup_logger(args.log_file)
    skip   = set(args.skip_stage or [])

    logger.info("=" * 60)
    logger.info("Pipeline starting")
    logger.info(f"  Skip stages  : {sorted(skip) or 'none'}")
    logger.info(f"  Start index  : {args.start_index or 'auto (checkpoint)'}")
    logger.info(f"  LLM model    : {MODEL_NAME}")
    logger.info(f"  LLM API      : {LLM_API_URL}")
    logger.info("=" * 60)

    # Stage 1
    if 1 not in skip:
        stage1_fetch_raw(logger)
    else:
        logger.info("Stage 1 skipped - using existing raw_data.json")

    # Stage 2
    if 2 not in skip:
        stage2_fetch_metrics(logger)
    else:
        logger.info("Stage 2 skipped - using existing metrics.json")

    # Stage 3
    if 3 not in skip:
        metrics_path = Path(METRICS_OUTPUT)
        if not metrics_path.exists():
            logger.error("metrics.json not found. Run Stage 2 first.")
            sys.exit(1)
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        stage3_enrich(metrics, args.start_index, logger)
    else:
        logger.info("Stage 3 skipped.")

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()