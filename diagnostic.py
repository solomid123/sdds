"""
Apollo diagnostic: does ANY endpoint leak real emails/phones
without us clicking 'Access email'?

Flow:
  1. Load search URL with your cookies (authenticated, no credit spend).
  2. Dump every JSON response to disk.
  3. Click the first contact row to open the profile panel.
  4. Dump those responses too.
  5. Also intercept the "Export" / "Save to list" / bulk-enrich endpoints
     if the UI triggers them.
  6. Scan ALL captured responses for:
       - real email-shaped strings (excluding the placeholder)
       - phone-number-shaped strings (7+ digits with optional +)
  7. Report every hit with the endpoint it came from.

This does NOT click any 'Access email' or 'reveal' buttons, so your
credits are safe.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Response

# Reuse config + cookie loader from the main scraper
from apollo_scraper import URL, COOKIES_FILE, USER_DATA_DIR, _load_cookies  # noqa: E402

OUT_DIR = Path(__file__).parent / "diagnostic_dump"
OUT_DIR.mkdir(exist_ok=True)

EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
)
PHONE_RE = re.compile(r"\+?\d[\d\s\-().]{7,}\d")

# Placeholders Apollo uses for locked data — ignore these when searching
PLACEHOLDER_EMAILS = {
    "email_not_unlocked@domain.com",
    "email_not_unlocked@example.com",
    "email_not_unlocked@apollo.io",
    "domain@domain.com",
}


def _safe_filename(url: str, idx: int) -> str:
    p = urlparse(url)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", p.path)[:80].strip("_")
    return f"{idx:04d}_{slug}.json"


async def main() -> None:
    captured_files: list[tuple[str, Path]] = []   # (url, saved path)
    idx = [0]

    async def on_response(resp: Response) -> None:
        try:
            url = resp.url
            # Skip irrelevant hosts / assets
            if "apollo.io" not in url:
                return
            ctype = resp.headers.get("content-type", "")
            if "application/json" not in ctype and "text/json" not in ctype:
                return
            body = await resp.body()
        except Exception:
            return
        try:
            data = json.loads(body.decode("utf-8", errors="replace"))
        except Exception:
            return

        idx[0] += 1
        fname = _safe_filename(url, idx[0])
        out = OUT_DIR / fname
        try:
            out.write_text(
                json.dumps(
                    {"url": url, "status": resp.status, "body": data},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            captured_files.append((url, out))
        except Exception:
            return

    cookies = _load_cookies(COOKIES_FILE)

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=False,
            viewport={"width": 1440, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        if cookies:
            await ctx.add_cookies(cookies)
            print(f"Loaded {len(cookies)} cookies.")

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.on("response", on_response)

        print(f"Opening search URL...")
        await page.goto(URL, wait_until="domcontentloaded")

        print("Waiting 15s for search results + initial XHRs...")
        await asyncio.sleep(15)

        # Try to click the first contact row to open the side panel.
        # Apollo's DOM changes; try several plausible selectors.
        print("Attempting to open the first contact's profile panel...")
        clicked = False
        for sel in [
            'a[href*="/#/people/"]',                        # profile link
            'div[class*="zp_"] a[href*="people"]',          # namespaced div
            'tr[class*="zp_"]:has(a[href*="people"]) a',    # table row
            '[data-cy="person-name-link"]',
            '[data-cy="contact-row"] a',
            'button:has-text("View")',
        ]:
            try:
                loc = page.locator(sel).first
                if await loc.count():
                    await loc.click(timeout=3000)
                    clicked = True
                    print(f"  clicked: {sel}")
                    break
            except Exception:
                continue
        if not clicked:
            print("  could not find a clickable profile link — will still "
                  "analyze search-level responses.")

        print("Waiting 10s for profile-panel XHRs...")
        await asyncio.sleep(10)

        # Also scroll inside the panel to trigger lazy loads
        try:
            await page.mouse.wheel(0, 600)
            await asyncio.sleep(3)
        except Exception:
            pass

        await ctx.close()

    # ----- analysis -----
    print(f"\nCaptured {len(captured_files)} JSON responses. Scanning...")
    real_email_hits: list[tuple[str, list[str]]] = []
    phone_hits: list[tuple[str, list[str]]] = []

    for url, path in captured_files:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        emails = set(EMAIL_RE.findall(text)) - PLACEHOLDER_EMAILS
        # Strip emails that are obviously apollo's own (@apollo.io, etc.)
        emails = {
            e for e in emails
            if not e.lower().endswith(
                ("@apollo.io", "@domain.com", "@example.com",
                 "@sentry.io", "@intercom.io", "@segment.io",
                 "@2x.png", "@3x.png")
            )
        }
        if emails:
            real_email_hits.append((url, sorted(emails)[:5]))

        # Phone: look for digits long enough to be real, avoid timestamps
        # (timestamps are usually 10-13 digits with no + or spaces).
        # Prefer + and () indicators.
        phones = set()
        for m in PHONE_RE.findall(text):
            cleaned = re.sub(r"\D", "", m)
            if 8 <= len(cleaned) <= 15 and ("+" in m or "(" in m or " " in m or "-" in m):
                phones.add(m.strip())
        if phones:
            phone_hits.append((url, sorted(phones)[:5]))

    # Report
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n=== DIAGNOSTIC REPORT @ {ts} ===\n")

    if real_email_hits:
        print(f"[!] REAL emails found in {len(real_email_hits)} endpoint(s):\n")
        for url, emails in real_email_hits:
            print(f"  URL: {url}")
            for e in emails:
                print(f"    -> {e}")
            print()
    else:
        print("[ok] No real emails leaked in any JSON response.\n"
              "     (Apollo is serving only placeholder emails, as expected.)\n")

    if phone_hits:
        print(f"[!] Phone-like strings found in {len(phone_hits)} endpoint(s):\n")
        for url, phones in phone_hits:
            print(f"  URL: {url}")
            for p in phones:
                print(f"    -> {p}")
            print()
    else:
        print("[ok] No phone-like strings found in any JSON response.\n")

    print(f"All raw responses saved to: {OUT_DIR}")
    print("You can grep them yourself, e.g.:")
    print(f'  Select-String -Path "{OUT_DIR}\\*.json" -Pattern "@"')


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
