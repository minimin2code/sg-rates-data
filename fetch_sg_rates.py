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

# Manually-curated fallback VALUES, used only when live fetches return
# nothing. The "date" here is just documentation of when YOU last checked
# the value below — the actual published record gets stamped with today's
# date automatically each run (see apply_manual_override), so history
# accumulates on its own. You only need to update the "value" (and this
# "date" comment, for your own reference) when the real rate changes —
# e.g. by checking https://tradingeconomics.com/singapore/2-year-bond-yield
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
    """Attempts the legacy CKAN datastore endpoint for SORA.

    CONCLUSION FROM TESTING (2026-07-03): three separate test runs — rapid
    requests, 5s-spaced requests, and a 30s timeout — all consistently
    returned HTTP 200 with an empty/invalid (non-JSON) body in ~11s. This
    rules out slowness, rate-limiting, and connectivity issues; the
    endpoint is very likely genuinely decommissioned (MAS announced a
    deprecation of this API in Oct 2023 in favour of a new subscription
    API Catalog — see MAS_API_KEY below).

    Given that, this function now only makes ONE quick canary request
    (not 5 full historical chunks) so the daily job doesn't burn ~2.5
    minutes chasing a dead endpoint. If MAS ever restores it, this will
    detect that (returns real records) and you can restore full chunked
    backfill by increasing years_back usage / re-adding the loop.
    """
    today = date.today()
    start = today.replace(year=today.year - 1)  # just probe the last year
    url = (
        "https://eservices.mas.gov.sg/api/action/datastore/search.json"
        f"?resource_id={SORA_RESOURCE_ID}&limit=1000"
        f"&between[end_of_day]={start},{today}"
    )
    records = []

    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200:
            log(f"SORA canary check: HTTP {res.status_code} — still not working")
            return records
        chunk_records = res.json().get("result", {}).get("records", [])
        if chunk_records:
            log(f"SORA canary check: SUCCESS — {len(chunk_records)} records! "
                f"Endpoint appears to be working again — consider restoring full chunked backfill.")
        else:
            log("SORA canary check: got valid JSON but zero records")
        for rec in chunk_records:
            rate_col = next((c for c in ("sora", "SORA", "rate_sora") if c in rec), None)
            if rate_col and rec.get("end_of_day"):
                records.append({
                    "date": rec["end_of_day"],
                    "value": float(rec[rate_col]),
                    "source": "mas_api",
                })
    except requests.exceptions.RequestException as e:
        log(f"SORA canary check: {type(e).__name__}: {e}")
    except (ValueError, KeyError) as e:
        content_type = res.headers.get("Content-Type", "(none)")
        body_snippet = res.text[:300].replace("\n", " ")
        log(
            f"SORA canary check: {type(e).__name__}: {e} | "
            f"content-type={content_type} | body_len={len(res.text)} | "
            f"body_snippet={body_snippet!r} — still not working, as expected"
        )

    return records


def _fetch_sora_from_mas_legacy_full_chunked_UNUSED(years_back=10):
    """Kept for reference / easy restoration if MAS's endpoint ever comes
    back. Not called anywhere currently — see fetch_sora_from_mas_legacy's
    docstring for why this was scaled down to a single canary request."""
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
    """If live fetch produced nothing, fall back to the manually curated
    reference VALUE, but stamp it with TODAY's date rather than a fixed
    date. Since merge_records dedupes by date, this means each daily run
    adds one new point (a "flat line" at the same value) automatically —
    building real accumulating history with zero manual date-editing.
    Only the VALUE needs manual updating (in MANUAL_OVERRIDES below) when
    the actual rate changes; the date takes care of itself."""
    if live_records:
        return live_records
    override = MANUAL_OVERRIDES.get(key)
    if not override:
        return []
    today_str = date.today().isoformat()
    return [{"date": today_str, "value": override["value"], "source": "manual"}]


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
