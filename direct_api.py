"""
Direct API scraper. Bypasses Apollo's frontend (and its 3-page free-plan
paywall overlay) by hitting /api/v1/mixed_people/search directly with
the same cookies + CSRF token the browser uses.

If the 3-page cap is a frontend-only check, this gets us all ~895 rows
in one script run. If the backend also caps free accounts, we'll see
that clearly in the response and fall back to manual pagination.

Usage:
    python direct_api.py              # scrape all pages
    python direct_api.py --max-pages 5
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

from apollo_scraper import COOKIES_FILE, _load_cookies

HERE = Path(__file__).parent
CAPTURED_REQ = HERE / "captured_request.json"
RAW_OUT = HERE / "apollo_data.raw.json"
CSV_OUT = HERE / "apollo_data.csv"

BASE = "https://app.apollo.io"
ENDPOINT = "/api/v1/mixed_people/search"


def _build_session() -> tuple[requests.Session, str]:
    """Build a requests.Session with Apollo cookies + CSRF token."""
    if not COOKIES_FILE.exists():
        print(f"ERROR: {COOKIES_FILE} missing.")
        sys.exit(1)

    cookies_raw = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
    session = requests.Session()
    csrf = None
    for c in cookies_raw:
        name = c.get("name")
        value = c.get("value")
        domain = (c.get("domain") or "").lstrip(".")
        if not name or value is None:
            continue
        session.cookies.set(name, value, domain=domain or None,
                            path=c.get("path") or "/")
        if name == "X-CSRF-TOKEN":
            csrf = value

    if not csrf:
        print("WARNING: X-CSRF-TOKEN cookie missing; requests may be rejected.")
    return session, csrf or ""


def _build_payload(page: int, per_page: int = 25) -> dict:
    """Build the POST body matching what the frontend sends."""
    # If we have a captured body, use it as a template so schema matches
    template: dict = {}
    if CAPTURED_REQ.exists():
        try:
            cap = json.loads(CAPTURED_REQ.read_text(encoding="utf-8"))
            body = cap.get("post_data_json")
            if isinstance(body, dict):
                template = dict(body)
        except Exception:
            pass

    payload = dict(template) if template else {
        "person_titles": ["project manager", "chef de projet"],
        "person_locations": ["France"],
        "q_organization_keyword_tags": ["mécanique"],
        "included_organization_keyword_fields": ["tags", "name"],
        "recommendation_config_id": "score",
        "sort_ascending": False,
        "sort_by_field": "recommendations_score",
        "context": "people-index-page",
        "display_mode": "explorer_mode",
        "finder_verson": 2,
        "show_suggestions": False,
        "use_cache": False,
    }
    payload["page"] = page
    payload["per_page"] = per_page
    # Drop metadata-mode marker if present: we want full people data
    payload.pop("display_mode", None)
    payload.pop("cacheKey", None)
    payload.pop("num_fetch_result", None)
    return payload


def _headers(csrf: str) -> dict:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": BASE,
        "Referer": BASE + "/",
        "X-CSRF-Token": csrf,
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/129.0.0.0 Safari/537.36"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=100)
    ap.add_argument("--per-page", type=int, default=25)
    ap.add_argument("--sleep", type=float, default=1.0,
                    help="seconds between requests")
    args = ap.parse_args()

    session, csrf = _build_session()
    all_people: list[dict] = []
    seen_ids: set[str] = set()

    for page in range(1, args.max_pages + 1):
        payload = _build_payload(page, args.per_page)
        headers = _headers(csrf)
        try:
            r = session.post(BASE + ENDPOINT, json=payload,
                             headers=headers, timeout=30)
        except Exception as e:
            print(f"[p{page}] network error: {e}")
            break

        if r.status_code == 401 or r.status_code == 403:
            print(f"[p{page}] {r.status_code} — auth rejected. "
                  f"Cookies likely expired or CSRF mismatch.")
            print(f"  response: {r.text[:400]}")
            break

        if r.status_code == 422 or r.status_code == 402:
            print(f"[p{page}] {r.status_code} — paywall / quota hit.")
            print(f"  response: {r.text[:400]}")
            break

        if r.status_code != 200:
            print(f"[p{page}] unexpected {r.status_code}: {r.text[:300]}")
            break

        try:
            data = r.json()
        except Exception:
            print(f"[p{page}] non-JSON response.")
            break

        people = data.get("people") or data.get("contacts") or []
        pagination = data.get("pagination") or {}

        new_count = 0
        for p in people:
            pid = p.get("id")
            if pid and pid in seen_ids:
                continue
            if pid:
                seen_ids.add(pid)
            all_people.append(p)
            new_count += 1

        total_entries = pagination.get("total_entries")
        total_pages = pagination.get("total_pages")
        print(f"[p{page}] got {len(people)} people "
              f"(+{new_count} new, total: {len(all_people)})  "
              f"server says total_entries={total_entries} "
              f"total_pages={total_pages}")

        # Save incrementally
        RAW_OUT.write_text(
            json.dumps(all_people, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        if not people:
            print("  empty page — stopping.")
            break
        if total_pages and page >= total_pages:
            print("  reached last page.")
            break
        if new_count == 0:
            print("  no new people — stopping.")
            break

        time.sleep(args.sleep)

    print(f"\nScraped {len(all_people)} unique people "
          f"across {page} page request(s).")

    # Also write the CSV same way apollo_scraper does
    try:
        from apollo_scraper import _row_from_person
        import csv
        with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["name", "title", "company", "company_website",
                        "email", "linkedin", "location"])
            for p in all_people:
                w.writerow(_row_from_person(p))
        print(f"CSV: {CSV_OUT}")
    except Exception as e:
        print(f"(CSV write skipped: {e})")
    print(f"Raw JSON: {RAW_OUT}")


if __name__ == "__main__":
    main()
