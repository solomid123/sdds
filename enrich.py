"""
Enrich apollo_data.raw.json with real emails via Hunter.io.

- Reads the raw JSON produced by apollo_scraper.py (has first_name,
  last_name, and organization.primary_domain — the fields Hunter needs).
- For each person, calls Hunter's email-finder endpoint.
- Writes apollo_data_enriched.csv with verified emails + confidence.
- Caches every lookup in enrichment_cache.json so re-runs don't burn
  your Hunter quota.

Pricing/limits (as of 2026):
- Free tier: 25 requests/month
- Paid tiers scale from there
- Hunter rate-limits aggressively above ~15 req/s; we space at 0.25s

Usage:
    python enrich.py                 # uses apollo_data.raw.json
    python enrich.py path/to/raw.json
"""

from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

HERE = Path(__file__).parent
KEY_FILE = HERE / "hunter_key.txt"
DEFAULT_INPUT = HERE / "apollo_data.raw.json"
OUTPUT_CSV = HERE / "apollo_data_enriched.csv"
CACHE_FILE = HERE / "enrichment_cache.json"

RATE_DELAY = 0.25   # seconds between requests
TIMEOUT = 20

API = "https://api.hunter.io/v2/email-finder"


def _load_key() -> str:
    if not KEY_FILE.exists():
        print(f"ERROR: {KEY_FILE} not found. Put your Hunter API key there.")
        sys.exit(1)
    key = KEY_FILE.read_text(encoding="utf-8").strip()
    if not key:
        print("ERROR: hunter_key.txt is empty.")
        sys.exit(1)
    return key


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _domain_from(person: dict) -> str | None:
    """Get a bare domain like 'allia-europe.com' from the person dict."""
    org = person.get("organization") or {}
    for field in ("primary_domain", "website_url", "domain"):
        val = org.get(field) or person.get(field)
        if not val:
            continue
        val = str(val).strip()
        if "://" not in val:
            val = "http://" + val
        host = urlparse(val).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if "." in host:
            return host
    return None


def _cache_key(first: str, last: str, domain: str) -> str:
    return f"{first.lower().strip()}|{last.lower().strip()}|{domain.lower().strip()}"


def _find_email(first: str, last: str, domain: str, key: str) -> dict:
    params = {
        "domain": domain,
        "first_name": first,
        "last_name": last,
        "api_key": key,
    }
    r = requests.get(API, params=params, timeout=TIMEOUT)
    # Handle rate-limit / quota cleanly
    if r.status_code == 429:
        return {"error": "rate_limited", "status": 429}
    if r.status_code == 401:
        return {"error": "unauthorized", "status": 401}
    if r.status_code == 402 or r.status_code == 451:
        return {"error": "quota_exceeded", "status": r.status_code}
    try:
        return r.json()
    except Exception:
        return {"error": "bad_json", "status": r.status_code, "text": r.text[:200]}


def main() -> None:
    input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INPUT
    if not input_path.exists():
        print(f"ERROR: {input_path} not found. Run apollo_scraper.py first.")
        sys.exit(1)

    key = _load_key()
    people = json.loads(input_path.read_text(encoding="utf-8"))
    cache = _load_cache()
    print(f"Loaded {len(people)} people from {input_path.name}.")
    print(f"Cache has {len(cache)} prior lookups.")

    rows = []
    stats = {"ok": 0, "skip": 0, "miss": 0, "error": 0, "cached": 0, "new_calls": 0}

    try:
        for i, p in enumerate(people, 1):
            first = (p.get("first_name") or "").strip()
            last = (p.get("last_name") or "").strip()
            name = p.get("name") or f"{first} {last}".strip()
            domain = _domain_from(p)
            org_name = (p.get("organization") or {}).get("name") or ""
            title = p.get("title") or ""
            location = ", ".join(
                x for x in (p.get("city"), p.get("state"), p.get("country")) if x
            )
            linkedin = p.get("linkedin_url") or ""

            base = {
                "name": name,
                "title": title,
                "company": org_name,
                "domain": domain or "",
                "linkedin": linkedin,
                "location": location,
                "email": "",
                "email_confidence": "",
                "email_source": "",
                "email_verification": "",
            }

            if not first or not last or not domain:
                stats["skip"] += 1
                base["email_source"] = "skipped_no_domain_or_name"
                rows.append(base)
                print(f"[{i:3d}/{len(people)}] SKIP  {name:30s} (missing domain/name)")
                continue

            ck = _cache_key(first, last, domain)
            if ck in cache:
                result = cache[ck]
                stats["cached"] += 1
                tag = "HIT " if result.get("email") else "MISS"
            else:
                time.sleep(RATE_DELAY)
                resp = _find_email(first, last, domain, key)
                stats["new_calls"] += 1
                if resp.get("error"):
                    print(f"[{i:3d}/{len(people)}] ERR  {name:30s} -> "
                          f"{resp.get('error')} ({resp.get('status')})")
                    stats["error"] += 1
                    if resp.get("error") in ("quota_exceeded", "unauthorized"):
                        print("Stopping — API says we're done.")
                        break
                    continue
                data = resp.get("data") or {}
                result = {
                    "email": data.get("email") or "",
                    "score": data.get("score"),
                    "verification": (data.get("verification") or {}).get("status"),
                    "sources": len(data.get("sources") or []),
                }
                cache[ck] = result
                _save_cache(cache)
                tag = "HIT " if result.get("email") else "MISS"

            if result.get("email"):
                stats["ok"] += 1
            else:
                stats["miss"] += 1

            base["email"] = result.get("email") or ""
            base["email_confidence"] = str(result.get("score") or "")
            base["email_source"] = f"hunter (sources={result.get('sources', 0)})"
            base["email_verification"] = result.get("verification") or ""

            print(f"[{i:3d}/{len(people)}] {tag} {name:30s} -> "
                  f"{base['email'] or '(no match)':40s} "
                  f"conf={base['email_confidence']}")
            rows.append(base)

    except KeyboardInterrupt:
        print("\nInterrupted. Writing partial results...")

    # Write CSV (overwrite)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "name", "title", "company", "domain", "linkedin", "location",
                "email", "email_confidence", "email_verification", "email_source",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print("\n=== Enrichment complete ===")
    print(f"  Found email:       {stats['ok']}")
    print(f"  No match:          {stats['miss']}")
    print(f"  Skipped:           {stats['skip']}")
    print(f"  Errors:            {stats['error']}")
    print(f"  Cache hits:        {stats['cached']}")
    print(f"  New API calls:     {stats['new_calls']}")
    print(f"\nOutput: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
