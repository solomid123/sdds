"""
Browser-driven direct-API scraper.

Uses Playwright to open Apollo (establishing a legitimate browser session),
then calls /api/v1/mixed_people/search via window.fetch() INSIDE the page.
Because the actual network request is issued by Chrome itself, Cloudflare's
TLS fingerprinting and sec-ch-* header checks all pass automatically.

This bypasses the 3-page UI paywall if the backend doesn't enforce it.

Usage:
    python browser_api.py
    python browser_api.py --max-pages 50
    python browser_api.py --per-page 100
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path

from playwright.async_api import async_playwright

from apollo_scraper import (
    URL, COOKIES_FILE, USER_DATA_DIR, _load_cookies, _row_from_person,
)

HERE = Path(__file__).parent
RAW_OUT = HERE / "apollo_data.raw.json"
CSV_OUT = HERE / "apollo_data.csv"
CAPTURED_REQ = HERE / "captured_request.json"

ENDPOINT = "/api/v1/mixed_people/search"


def _build_payload_template() -> dict:
    """Take the captured frontend payload as a baseline."""
    if CAPTURED_REQ.exists():
        try:
            cap = json.loads(CAPTURED_REQ.read_text(encoding="utf-8"))
            body = cap.get("post_data_json")
            if isinstance(body, dict):
                t = dict(body)
                t.pop("display_mode", None)       # want full rows, not metadata
                t.pop("cacheKey", None)
                t.pop("num_fetch_result", None)
                return t
        except Exception:
            pass
    # Fallback (shouldn't normally be hit)
    return {
        "person_titles": ["project manager", "chef de projet"],
        "person_locations": ["France"],
        "q_organization_keyword_tags": ["mécanique"],
        "included_organization_keyword_fields": ["tags", "name"],
        "recommendation_config_id": "score",
        "sort_ascending": False,
        "sort_by_field": "recommendations_score",
        "context": "people-index-page",
        "finder_verson": 2,
    }


JS_FETCH = """
async (args) => {
  const { endpoint, payload, csrf } = args;
  const r = await fetch(endpoint, {
    method: 'POST',
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json, text/plain, */*',
      'X-CSRF-Token': csrf,
      'X-Requested-With': 'XMLHttpRequest',
    },
    body: JSON.stringify(payload),
  });
  let text;
  try { text = await r.text(); } catch(e) { text = ''; }
  let parsed = null;
  try { parsed = JSON.parse(text); } catch(e) {}
  return { status: r.status, body: parsed, text_snippet: text.slice(0, 400) };
}
"""


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=100)
    ap.add_argument("--per-page", type=int, default=25)
    ap.add_argument("--sleep", type=float, default=1.5)
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    template = _build_payload_template()

    USER_DATA_DIR.mkdir(exist_ok=True)

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=args.headless,
            viewport={"width": 1440, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        cookies = _load_cookies(COOKIES_FILE)
        if cookies:
            await ctx.add_cookies(cookies)

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # Open the app first so the SPA initializes and sets up any
        # internal auth state. Also loads the CSRF meta tag into DOM.
        print("Opening Apollo to establish browser session...")
        await page.goto(URL, wait_until="domcontentloaded")
        await asyncio.sleep(5)

        # Grab CSRF token from page/meta or cookies
        csrf = ""
        try:
            csrf = await page.evaluate(
                "() => document.querySelector('meta[name=\"csrf-token\"]')?.content || ''"
            )
        except Exception:
            pass
        if not csrf:
            for c in (cookies or []):
                if c.get("name") == "X-CSRF-TOKEN":
                    csrf = c.get("value")
                    break
        if not csrf:
            for c in await ctx.cookies():
                if c.get("name") == "X-CSRF-TOKEN":
                    csrf = c.get("value")
                    break
        print(f"CSRF token: {csrf[:20]}...{csrf[-6:]}  (len={len(csrf)})")

        all_people: list[dict] = []
        seen_ids: set = set()

        for page_num in range(1, args.max_pages + 1):
            payload = dict(template)
            payload["page"] = page_num
            payload["per_page"] = args.per_page

            try:
                result = await page.evaluate(
                    JS_FETCH,
                    {"endpoint": ENDPOINT, "payload": payload, "csrf": csrf},
                )
            except Exception as e:
                print(f"[p{page_num}] evaluate error: {e}")
                break

            status = result.get("status")
            body = result.get("body")

            if status != 200:
                print(f"[p{page_num}] status={status}")
                print(f"  body: {result.get('text_snippet')}")
                # Distinguish paywall from other errors
                if status == 422:
                    print("  (looks like a paywall / bot-check error)")
                break

            if not isinstance(body, dict):
                print(f"[p{page_num}] non-dict body; stopping.")
                break

            people = body.get("people") or body.get("contacts") or []
            pagination = body.get("pagination") or {}
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
            print(f"[p{page_num}] +{new_count} new  "
                  f"(total kept: {len(all_people)})  "
                  f"server: total_entries={total_entries} pages={total_pages}")

            # Persist incrementally
            RAW_OUT.write_text(
                json.dumps(all_people, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            if not people:
                print("  empty page — stopping.")
                break
            if total_pages and page_num >= total_pages:
                print("  reached last page.")
                break
            if new_count == 0:
                print("  no new people — stopping.")
                break

            await asyncio.sleep(args.sleep)

        await ctx.close()

    # Write CSV
    with CSV_OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "title", "company", "company_website",
                    "email", "linkedin", "location"])
        for p in all_people:
            w.writerow(_row_from_person(p))

    print(f"\nDone. {len(all_people)} unique people.")
    print(f"CSV: {CSV_OUT}")
    print(f"Raw: {RAW_OUT}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
