"""
Daily collector for Singapore rate data (SORA, SGS 2Y/10Y bond yields).

WHY THIS EXISTS
----------------
MAS's old public CKAN-style API (eservices.mas.gov.sg/api/action/datastore/
search.json) is decommissioned — confirmed via repeated live testing (see
git history of this file for the earlier canary-check function and its
findings). This script now uses two DIFFERENT live sources instead:

1. SORA: MAS's new subscription API Catalog ("API for Domestic Interest
   Rates - Daily"). Requires a free MAS_API_KEY (see README / repo secrets
   for how this is set as a GitHub Actions secret). Auth is via a `KeyId`
   header — NOT `Authorization: Bearer` — confirmed working via Swagger's
   "Try it out" against real MAS data.

2. SGS 2Y/10Y Benchmark Yields: no equivalent API Catalog endpoint exists
   for these (manually searched, not found) — but MAS's own
   SgsBenchmarkIssuePrices.aspx page renders its "Closing Levels" table as
   plain server-side HTML on a bare GET request (confirmed by inspection:
   no JS/AJAX needed), and conveniently defaults to showing the trailing
   ~1 week of business days with no parameters required. This script
   scrapes that table directly.

This script is designed to run on a DIFFERENT network from the main app
(GitHub Actions, scheduled daily via cron) and publish the results as a
JSON file committed back to this repo. The Gradio app then reads that
JSON via its raw GitHub URL.

WHAT TO CHECK / MAINTAIN
--------------------------
- SORA: if MAS_API_KEY expires, is revoked, or the API Catalog changes
  its auth scheme, fetch_sora_from_mas_api_catalog() will start logging
  HTTP errors — check Action logs.
- SGS yields: this is a page scrape, not an API — the MOST likely future
  break is MAS redesigning SgsBenchmarkIssuePrices.aspx (new column
  order, renamed heading, JS-rendered table, etc). fetch_sgs_yields_from_
  mas_page() logs specifically when the page structure doesn't match
  what's expected, rather than failing silently, to make that visible
  fast. If it does break, MANUAL_OVERRIDES below is used as a fallback
  so the app always has *something* to show — those fallback points are
  tagged "source": "manual" (as opposed to "mas_page_scrape") so
  downstream consumers can tell real scraped data from the placeholder.
- Update MANUAL_OVERRIDES periodically (e.g. monthly) by checking
  https://tradingeconomics.com/singapore/2-year-bond-yield or MAS's own
  page directly in a browser, in case the scraper is ever down for a
  stretch.
"""

import json
import os
import re
import sys
from datetime import date, datetime, timezone

import requests
from bs4 import BeautifulSoup

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sg_rates.json")

MAS_API_KEY = os.environ.get("MAS_API_KEY")  # set as a GitHub Actions secret

MAS_SORA_API_URL = (
    "https://eservices.mas.gov.sg/apimg-gw/server/monthly_statistical_bulletin_non610mssql/"
    "domestic_interest_rates_daily/views/domestic_interest_rates_daily"
)
MAS_SGS_PRICES_URL = "https://eservices.mas.gov.sg/statistics/fdanet/SgsBenchmarkIssuePrices.aspx"

_DATE_ROW_RE = re.compile(r"^\d{2} \w{3} \d{4}$")

# Manually-curated fallback VALUES, used only when the SGS page scrape
# returns nothing (page down, redesigned, etc). The "date" here is just
# documentation of when YOU last checked the value below — the actual
# published record gets stamped with today's date automatically each run
# (see apply_manual_override), so history accumulates on its own. You only
# need to update the "value" (and this "date" comment, for reference) when
# checking a source like https://tradingeconomics.com/singapore/2-year-bond-yield
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


def fetch_sora_from_mas_api_catalog(years_back=10):
    """Fetches SORA history from MAS's API Catalog ("API for Domestic
    Interest Rates - Daily"). Auth confirmed via Swagger "Authorize":
    header name is `KeyId`, value is the raw API key. Response shape
    confirmed: {"name": ..., "elements": [ {...}, ... ]}. `sora` can be
    null for the current day if it hasn't been published yet — filter
    those out rather than treating them as errors.
    """
    if not MAS_API_KEY:
        log("SORA: MAS_API_KEY not set, skipping live fetch")
        return []

    today = date.today()
    start = today.replace(year=today.year - years_back)

    params = {
        "$filter": f"end_of_day>='{start.isoformat()}' AND end_of_day<='{today.isoformat()}'",
        "$orderby": "end_of_day ASC",
        "$select": "end_of_day,sora",
        "$count": 5000,
    }
    headers = {
        **HEADERS,
        "KeyId": MAS_API_KEY,
    }

    records = []
    try:
        res = requests.get(MAS_SORA_API_URL, headers=headers, params=params, timeout=20)
        if res.status_code == 204:
            log("SORA: HTTP 204 — no results for this filter")
            return []
        if res.status_code != 200:
            log(f"SORA: HTTP {res.status_code} — {res.text[:300]!r}")
            return []

        rows = res.json().get("elements", [])
        for row in rows:
            if row.get("sora") is not None and row.get("end_of_day"):
                records.append({
                    "date": row["end_of_day"][:10],
                    "value": float(row["sora"]),
                    "source": "mas_api",
                })
        log(f"SORA: fetched {len(records)} records from API Catalog "
            f"({len(rows) - len(records)} skipped as null/unpublished)")

    except requests.exceptions.RequestException as e:
        log(f"SORA: {type(e).__name__}: {e}")
    except (ValueError, KeyError, TypeError) as e:
        log(f"SORA: {type(e).__name__}: {e} | body_snippet={res.text[:300]!r}")

    return records


def fetch_sgs_yields_from_mas_page():
    """Scrapes 2Y/10Y SGS benchmark yields from MAS's
    SgsBenchmarkIssuePrices.aspx page's "Closing Levels" table.

    Column layout after the date (verified against a known-good external
    reference for 10 Sep 2026, which matched exactly):
    6M-Yield, 1Y-Yield, 2Y-Price, 2Y-Yield, 5Y-Price, 5Y-Yield,
    10Y-Price, 10Y-Yield, 15Y-Price, 15Y-Yield, 20Y-Price, 20Y-Yield,
    30Y-Price, 30Y-Yield, 50Y-Price, 50Y-Yield.

    Uses BeautifulSoup + fixed column positions rather than
    pandas.read_html(), since the table's header spans three rows with
    rowspan/colspan that read_html tends to mis-align. Instead this scans
    every <tr> and treats any row whose first cell matches a date pattern
    (e.g. "11 Sep 2026") as a data row, skipping header rows entirely
    without needing to parse them.
    """
    try:
        res = requests.get(MAS_SGS_PRICES_URL, headers=HEADERS, timeout=20)
        if res.status_code != 200:
            log(f"SGS yields (page scrape): HTTP {res.status_code}")
            return [], []

        soup = BeautifulSoup(res.text, "html.parser")

        # Anchor on the "Closing Levels" heading, then take the next
        # <table> after it — distinct from the "High / Low Levels" table
        # further down the same page.
        heading = soup.find(string=re.compile("Closing Levels", re.IGNORECASE))
        if heading is None:
            log("SGS yields (page scrape): 'Closing Levels' heading not found — page structure may have changed")
            return [], []

        table = heading.find_next("table")
        if table is None:
            log("SGS yields (page scrape): no <table> found after 'Closing Levels' heading")
            return [], []

        sgs_2y_records = []
        sgs_10y_records = []

        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if not cells or not _DATE_ROW_RE.match(cells[0]):
                continue  # header row, or a row with a different shape — skip

            values = cells[1:]
            if len(values) < 8:
                log(f"SGS yields (page scrape): row for {cells[0]} has only "
                    f"{len(values)} value columns (expected >=8) — skipping, "
                    f"page layout may have changed")
                continue

            try:
                iso_date = datetime.strptime(cells[0], "%d %b %Y").date().isoformat()
                sgs_2y_records.append({
                    "date": iso_date,
                    "value": float(values[3]),  # 2Y-Yield
                    "source": "mas_page_scrape",
                })
                sgs_10y_records.append({
                    "date": iso_date,
                    "value": float(values[7]),  # 10Y-Yield
                    "source": "mas_page_scrape",
                })
            except (ValueError, IndexError) as e:
                log(f"SGS yields (page scrape): couldn't parse row for {cells[0]}: {e}")
                continue

        log(f"SGS yields (page scrape): parsed {len(sgs_2y_records)} rows")
        return sgs_2y_records, sgs_10y_records

    except requests.exceptions.RequestException as e:
        log(f"SGS yields (page scrape): {type(e).__name__}: {e}")
        return [], []


def apply_manual_override(key, live_records, existing_records):
    """If the live fetch produced nothing, decide what (if anything) to
    write for today:
    - Live fetch succeeded -> use those records, as before.
    - Live fetch failed AND we already have real history for this series
      -> return [] (write nothing). A transient failure should never
      inject a placeholder dated "today" that could sort after, and
      therefore outrank, the last real data point when the app picks
      the most recent value. This is what happened on 2026-09-12: a
      failed run stamped a fallback with that date, and it silently
      became "current" ahead of the real 2026-09-11 figure until
      manually removed.
    - Live fetch failed AND we have NO history at all for this series
      (true cold start, e.g. first-ever run before the scraper existed)
      -> fall back to the manually curated reference VALUE, stamped
      with today's date, so the app has *something* to show rather
      than a totally empty series.
    """
    if live_records:
        return live_records
    if existing_records:
        log(f"{key}: live fetch failed, but existing history is present — "
            f"writing nothing this run rather than a placeholder")
        return []
    override = MANUAL_OVERRIDES.get(key)
    if not override:
        return []
    today_str = date.today().isoformat()
    log(f"{key}: no existing history and live fetch failed — using manual "
        f"fallback value for cold start")
    return [{"date": today_str, "value": override["value"], "source": "manual"}]


def main():
    data = load_existing()

    log("Fetching SORA...")
    sora_records = fetch_sora_from_mas_api_catalog()
    if sora_records:
        data["series"]["sora"] = merge_records(data["series"]["sora"], sora_records)
    else:
        log("SORA: no live records obtained this run (keeping existing history, if any)")

    log("Fetching SGS 2Y / 10Y yields...")
    sgs_2y_records, sgs_10y_records = fetch_sgs_yields_from_mas_page()
    sgs_2y_records = apply_manual_override("sgs_2y", sgs_2y_records, data["series"]["sgs_2y"])
    sgs_10y_records = apply_manual_override("sgs_10y", sgs_10y_records, data["series"]["sgs_10y"])
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
