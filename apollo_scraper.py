"""
Apollo.io people scraper (Playwright, Windows).

How it works:
  1. Launches Chromium with a persistent user-data-dir, so you log into
     Apollo once and the session is reused on subsequent runs.
  2. Navigates to the search URL you provide.
  3. Hooks into network responses and captures the JSON payloads Apollo
     itself fetches from /api/v1/mixed_people/search (same data the UI uses).
  4. Writes rows to apollo_data.csv.

Notes:
  - First run: a browser window opens. If you see the login page, log in
     manually, then come back to the terminal and press Enter.
  - Paginate by setting PAGES below (it clicks the next-page button).
  - This violates Apollo's ToS; your account is at your own risk.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Response, TimeoutError as PWTimeout

# ---------------- config ----------------
URL = (
    "https://app.apollo.io/#/people"
    "?page=1"
    "&personTitles[]=project%20manager"
    "&personTitles[]=chef%20de%20projet"
    "&personLocations[]=France"
    "&qOrganizationKeywordTags[]=m%C3%A9canique"
    "&includedOrganizationKeywordFields[]=tags"
    "&includedOrganizationKeywordFields[]=name"
    "&recommendationConfigId=score"
    "&sortAscending=false"
    "&sortByField=recommendations_score"
)

PAGES = 5                      # short test run after cooldown; bump to 36 once confirmed
HEADLESS = False               # keep False the first time, so you can log in
OUTPUT_CSV = Path(__file__).parent / "apollo_data.csv"
USER_DATA_DIR = Path(__file__).parent / ".apollo_userdata"
COOKIES_FILE = Path(__file__).parent / "cookies.json"


def _load_cookies(path: Path) -> list[dict]:
    """Convert a cookie-export JSON (EditThisCookie / Cookie-Editor format)
    into Playwright's add_cookies() schema."""
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    samesite_map = {
        "no_restriction": "None",
        "unspecified": "Lax",
        "lax": "Lax",
        "strict": "Strict",
        "none": "None",
    }
    out: list[dict] = []
    for c in raw:
        name = c.get("name")
        value = c.get("value")
        if not name or value is None:
            continue
        domain = c.get("domain") or ""
        # Playwright wants bare domain when hostOnly; a leading "." means
        # it covers subdomains (which Playwright also accepts as-is).
        if c.get("hostOnly") and domain.startswith("."):
            domain = domain.lstrip(".")
        ck: dict = {
            "name": name,
            "value": str(value),
            "domain": domain,
            "path": c.get("path") or "/",
            "secure": bool(c.get("secure", False)),
            "httpOnly": bool(c.get("httpOnly", False)),
        }
        ss = c.get("sameSite")
        ck["sameSite"] = samesite_map.get(
            (ss or "").lower() if isinstance(ss, str) else "", "Lax"
        )
        exp = c.get("expirationDate")
        if exp and not c.get("session"):
            ck["expires"] = float(exp)
        out.append(ck)
    return out

# Apollo's internal search endpoint. The path has evolved over time; we
# match loosely so we catch whatever variant is in use.
SEARCH_PATH_MARKERS = ("mixed_people/search", "/people/search")
# ----------------------------------------


def _row_from_person(p: dict) -> list[str]:
    name = p.get("name") or (
        f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
    )
    title = p.get("title") or ""
    org = p.get("organization") or {}
    company = org.get("name") or p.get("organization_name") or ""
    company_website = org.get("website_url") or ""
    # Apollo hides emails behind reveal credits; we capture whatever is there.
    email = p.get("email") or "N/A"
    if email == "email_not_unlocked@domain.com":
        email = "locked"
    linkedin = p.get("linkedin_url") or ""
    location = ", ".join(
        x for x in (p.get("city"), p.get("state"), p.get("country")) if x
    )
    return [name, title, company, company_website, email, linkedin, location]


async def main() -> None:
    captured: list[dict] = []

    async def on_response(resp: Response) -> None:
        try:
            url = resp.url
            if not any(m in url for m in SEARCH_PATH_MARKERS):
                return
            ctype = resp.headers.get("content-type", "")
            if "application/json" not in ctype:
                return
            data = await resp.json()
        except Exception:
            return

        people = data.get("people") if isinstance(data, dict) else None
        if not people:
            return
        print(f"  -> intercepted {len(people)} people from {urlparse(url).path}")
        captured.extend(people)

    USER_DATA_DIR.mkdir(exist_ok=True)

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=HEADLESS,
            viewport={"width": 1440, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        cookies = _load_cookies(COOKIES_FILE)
        if cookies:
            await ctx.add_cookies(cookies)
            print(f"Loaded {len(cookies)} cookies from {COOKIES_FILE.name}")

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.on("response", on_response)

        print(f"Opening: {URL}")
        await page.goto(URL, wait_until="domcontentloaded")

        # If not logged in, Apollo redirects to /login. Wait for user.
        try:
            await page.wait_for_url("**/#/people**", timeout=15000)
        except PWTimeout:
            print(
                "\nNot logged in yet. A browser window is open — please sign "
                "in to Apollo, land on the search page, then press Enter here."
            )
            input("Press Enter once you see the people search results... ")
            await page.goto(URL, wait_until="domcontentloaded")

        # Give the SPA time to fire its first search XHR.
        print("Waiting for search results to load...")
        for _ in range(30):
            await asyncio.sleep(1)
            if captured:
                break
        if not captured:
            print("No search response captured yet — scrolling to trigger load.")
            await page.mouse.wheel(0, 1200)
            await asyncio.sleep(4)

        # Paginate: Apollo is a hash-routed SPA. Navigating to a new hash
        # doesn't always re-fire the search XHR, so we combine three tactics:
        #   1. goto the new URL
        #   2. if no XHR after 20s, try a page.reload()
        #   3. if still nothing, try clicking the UI's "next page" button
        # Also add a small inter-page delay to avoid rate-limiting.
        import re as _re
        import random as _random
        # Gentle pacing — Apollo's anti-bot flags accounts that hit pagination
        # too fast. 15-25s between pages keeps us off the radar.
        INTER_PAGE_SLEEP_MIN = 15.0
        INTER_PAGE_SLEEP_MAX = 25.0
        PER_ATTEMPT_TIMEOUT = 30

        async def _wait_for_new(before: int, timeout: int) -> bool:
            for _ in range(timeout):
                await asyncio.sleep(1)
                if len(captured) > before:
                    return True
            return False

        async def _try_click_next() -> bool:
            for sel in [
                'button[aria-label="next page"]',
                'button[aria-label="Next page"]',
                'button:has-text("Next")',
                '[data-testid="pagination-next"]',
                'button:has(svg[data-icon="chevron-right"])',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.count() and await btn.is_enabled():
                        await btn.click()
                        return True
                except Exception:
                    continue
            return False

        consecutive_failures = 0
        for i in range(2, PAGES + 1):
            before = len(captured)
            next_url = _re.sub(r"([?&])page=\d+", rf"\1page={i}", URL)
            sleep = _random.uniform(INTER_PAGE_SLEEP_MIN, INTER_PAGE_SLEEP_MAX)
            print(f"Page {i}/{PAGES}  (captured so far: {before}, "
                  f"waiting {sleep:.1f}s before fetch)")
            await asyncio.sleep(sleep)

            got_new = False
            # Attempt 1: goto
            try:
                await page.goto(next_url, wait_until="domcontentloaded")
            except Exception as e:
                print(f"  goto error: {e}")
            got_new = await _wait_for_new(before, PER_ATTEMPT_TIMEOUT)

            # Attempt 2: reload
            if not got_new:
                print(f"  no XHR after goto, trying reload...")
                try:
                    await page.reload(wait_until="domcontentloaded")
                except Exception as e:
                    print(f"  reload error: {e}")
                got_new = await _wait_for_new(before, PER_ATTEMPT_TIMEOUT)

            # Attempt 3: click next button in UI
            if not got_new:
                print(f"  no XHR after reload, trying UI next-button...")
                clicked = await _try_click_next()
                if clicked:
                    got_new = await _wait_for_new(before, PER_ATTEMPT_TIMEOUT)

            if not got_new:
                consecutive_failures += 1
                print(f"  page {i} failed (failures in a row: {consecutive_failures})")
                if consecutive_failures >= 2:
                    print("  two consecutive failures, stopping pagination.")
                    break
            else:
                consecutive_failures = 0

        await ctx.close()

    # Dedupe by id when available
    seen = set()
    unique: list[dict] = []
    for p in captured:
        key = p.get("id") or (p.get("name"), p.get("linkedin_url"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(p)

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            ["name", "title", "company", "company_website",
             "email", "linkedin", "location"]
        )
        for p in unique:
            w.writerow(_row_from_person(p))

    print(f"\nDone. {len(unique)} unique people written to {OUTPUT_CSV}")
    # Also dump raw JSON for debugging
    raw = OUTPUT_CSV.with_suffix(".raw.json")
    raw.write_text(json.dumps(unique, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Raw JSON dump: {raw}")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
