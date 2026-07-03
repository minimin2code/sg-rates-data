"""
Daily collector for Singapore rate data (SORA, SGS 2Y/10Y bond yields).

WHY THIS EXISTS
----------------
MAS's data endpoints (eservices.mas.gov.sg) return ConnectionError when
called from certain cloud hosting environments (e.g. Hugging Face Spaces).
This is very likely because MAS deprecated its old public CKAN-style API
in October 2023 in favour of a new subscription-based "API Catalog"
(https://eservices.mas.gov.sg/apimg-portal/), and/or blocks non-Singapore
datacenter IP ranges outright.

This script is designed to run on a DIFFERENT network (GitHub Actions,
scheduled daily via cron) and publish the results as a JSON file committed
back to this repo. The Gradio app then reads that JSON via its raw
GitHub URL — a request GitHub's CDN serves reliably from anywhere,
sidestepping whatever is blocking direct MAS access from the app's host.

WHAT TO CHECK/UPDATE
---------------------
1. SORA: attempts the legacy CKAN endpoint (resource_id below). This may
   or may not still work — deprecated does not always mean shut off
   immediately. Check the Actions log after the first run.
2. SGS 2Y / 10Y bond yields: no confirmed free keyless JSON endpoint was
   found for these at the time this script was written. Until a real
   endpoint is wired in, this script carries forward a manually-curated
   reference value (see MANUAL_OVERRIDES below) so the app has *something*
   to show, clearly tagged "source": "manual" so it's never confused with
   live data. If you register for MAS's new API Catalog
   (https://eservices.mas.gov.sg/apimg-portal/api-catalog) and find the
   correct resource ID / endpoint for SGS Benchmark Yields, plug it into
   fetch_sgs_yields_from_mas_api() below and this will switch over
   automatically (manual values are only used as a fallback when the live
   fetch returns nothing).
3. Update MANUAL_OVERRIDES periodically (e.g. monthly) by checking a
   source like https://tradingeconomics.com/singapore/2-year-bond-yield
   or MAS's own published SGS pages directly in a browser.
"""

import json
import os
import sys
import time
from datetime import date, datetime, timezone

import requests

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sg_rates.json")

SORA_RESOURCE_ID = "9a0bf149-308c-4bd2-832d-76c8e6cb47ed"  # legacy CKAN, may be deprecated
MAS_API_KEY = os.environ.get("MAS_API_KEY")  # set as a GitHub Actions secret once you have one

# Manually-curated fallback values, used only when live fetches return
# nothing. Update these every so often — they are NOT auto-refreshed.
# Values as of the dates shown, sourced from TradingEconomics.
MANUAL_OVERRIDES = {
    "sgs_2y": {"date": "2026-06-24", "value": 1.59},
    "sgs_10y": {"date": "2026-06-29", "value": 2.04},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def load_existing():
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r") as f:
            return json.load(f)
    return {"updated_at": None, "series": {"sora": [], "sgs_2y": [], "sgs_10y": []}}


def merge_records(existing_list, new_records):
    """Merges new {date, value, source} records into an existing list,
    de-duplicating by date (new records win) and keeping ascending order."""
    by_date = {r["date"]: r for r in existing_list}
    for r in new_records:
        by_date[r["date"]] = r
    return sorted(by_date.values(), key=lambda r: r["date"])


def fetch_sora_from_mas_legacy(years_back=10):
    """Attempts the legacy CKAN datastore endpoint for SORA. Chunked by
    ~2-year windows (MAS caps records per request).

    Requests are deliberately spaced out (see time.sleep below): a prior
    run showed inconsistent behavior across 5 near-simultaneous requests
    to the same host (some fast-empty-200, one ReadTimeout) — a pattern
    consistent with rate-limiting/anti-bot throttling reacting to a burst
    of requests, rather than a permanently dead endpoint. Spacing them out
    tests that theory directly."""
    today = date.today()
    chunk_years = 2
    n_chunks = max(1, -(-years_back // chunk_years))
    records = []

    for i in range(n_chunks):
        if i > 0:
            time.sleep(5)  # be polite; avoid looking like a burst scraper

        end = today.replace(year=today.year - i * chunk_years)
        start = today.replace(year=max(today.year - (i + 1) * chunk_years, today.year - years_back))
        url = (
            "https://eservices.mas.gov.sg/api/action/datastore/search.json"
            f"?resource_id={SORA_RESOURCE_ID}&limit=1000"
            f"&between[end_of_day]={start},{end}"
        )
        try:
            res = requests.get(url, headers=HEADERS, timeout=30)  # bumped for one diagnostic test — see notes
            if res.status_code != 200:
                log(f"SORA chunk {start}..{end}: HTTP {res.status_code}")
                continue
            chunk_records = res.json().get("result", {}).get("records", [])
            log(f"SORA chunk {start}..{end}: {len(chunk_records)} records")
            for rec in chunk_records:
                rate_col = next((c for c in ("sora", "SORA", "rate_sora") if c in rec), None)
                if rate_col and rec.get("end_of_day"):
                    records.append({
                        "date": rec["end_of_day"],
                        "value": float(rec[rate_col]),
                        "source": "mas_api",
                    })
        except requests.exceptions.RequestException as e:
            log(f"SORA chunk {start}..{end}: {type(e).__name__}: {e}")
        except (ValueError, KeyError) as e:
            # Got a 200 but the body wasn't valid JSON. Log everything
            # needed to diagnose why: were we redirected somewhere else
            # (e.g. to the new apimg-portal)? What content-type came back?
            # What does the body actually look like?
            redirect_chain = " -> ".join(r.url for r in res.history) if res.history else "(no redirect)"
            content_type = res.headers.get("Content-Type", "(none)")
            body_snippet = res.text[:300].replace("\n", " ")
            log(
                f"SORA chunk {start}..{end}: {type(e).__name__}: {e} | "
                f"final_url={res.url} | redirects={redirect_chain} | "
                f"content-type={content_type} | body_len={len(res.text)} | "
                f"body_snippet={body_snippet!r}"
            )

    return records


def fetch_sgs_yields_from_mas_api():
    """PLACEHOLDER for MAS's new API Catalog (apimg-portal). No confirmed
    endpoint was available when this script was written. Once you have
    registered and found the correct resource/endpoint for SGS Benchmark
    Yields, implement the real request here. Until then this always
    returns empty lists, and MANUAL_OVERRIDES is used instead."""
    if not MAS_API_KEY:
        log("SGS yields: MAS_API_KEY not set, skipping live fetch (using manual override)")
        return [], []

    # --- TODO: replace with the real endpoint once you have it ---
    # Example shape once you know the endpoint:
    # res = requests.get(
    #     "https://eservices.mas.gov.sg/apimg-portal/api/<real-path>",
    #     headers={**HEADERS, "Authorization": f"Bearer {MAS_API_KEY}"},
    #     timeout=15,
    # )
    log("SGS yields: MAS API endpoint not yet configured (see TODO in script)")
    return [], []


def apply_manual_override(key, live_records):
    """If live fetch produced nothing, fall back to the single manually
    curated data point so the app has something to show (clearly tagged)."""
    if live_records:
        return live_records
    override = MANUAL_OVERRIDES.get(key)
    if not override:
        return []
    return [{"date": override["date"], "value": override["value"], "source": "manual"}]


def main():
    data = load_existing()

    log("Fetching SORA...")
    sora_records = fetch_sora_from_mas_legacy()
    if sora_records:
        data["series"]["sora"] = merge_records(data["series"]["sora"], sora_records)
    else:
        log("SORA: no live records obtained this run (keeping existing history, if any)")

    log("Fetching SGS 2Y / 10Y yields...")
    sgs_2y_records, sgs_10y_records = fetch_sgs_yields_from_mas_api()
    sgs_2y_records = apply_manual_override("sgs_2y", sgs_2y_records)
    sgs_10y_records = apply_manual_override("sgs_10y", sgs_10y_records)
    data["series"]["sgs_2y"] = merge_records(data["series"]["sgs_2y"], sgs_2y_records)
    data["series"]["sgs_10y"] = merge_records(data["series"]["sgs_10y"], sgs_10y_records)

    data["updated_at"] = datetime.now(timezone.utc).isoformat()

    with open(OUTPUT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    log(f"Wrote {OUTPUT_PATH}: "
        f"sora={len(data['series']['sora'])} pts, "
        f"sgs_2y={len(data['series']['sgs_2y'])} pts, "
        f"sgs_10y={len(data['series']['sgs_10y'])} pts")

    # Fail the Action loudly if EVERYTHING came back empty (helps catch a
    # total outage rather than silently committing an empty file forever)
    if not any(data["series"].values()):
        log("ERROR: all series are empty — failing so this is visible in the Actions tab")
        sys.exit(1)


if __name__ == "__main__":
    main()

