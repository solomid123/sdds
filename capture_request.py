"""
One-shot diagnostic: capture the full POST request body + headers Apollo's
frontend sends to /api/v1/mixed_people/search. We need this to replay the
request directly via `requests`, bypassing the UI's 3-page paywall.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from playwright.async_api import async_playwright, Request

from apollo_scraper import URL, COOKIES_FILE, USER_DATA_DIR, _load_cookies

OUT = Path(__file__).parent / "captured_request.json"


async def main() -> None:
    captured: dict | None = None

    async def on_request(req: Request) -> None:
        nonlocal captured
        if "mixed_people/search" not in req.url:
            return
        if req.method != "POST":
            return
        if captured is not None:
            return
        body = req.post_data
        try:
            body_json = json.loads(body) if body else None
        except Exception:
            body_json = body
        captured = {
            "url": req.url,
            "method": req.method,
            "headers": dict(req.headers),
            "post_data_raw": body,
            "post_data_json": body_json,
        }
        print("Captured POST request — saving.")

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            headless=False,
            viewport={"width": 1440, "height": 900},
        )
        cookies = _load_cookies(COOKIES_FILE)
        if cookies:
            await ctx.add_cookies(cookies)

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.on("request", on_request)

        print("Opening search URL to trigger a search POST...")
        await page.goto(URL, wait_until="domcontentloaded")

        # Wait up to 20s for the POST to happen
        for _ in range(20):
            await asyncio.sleep(1)
            if captured:
                break

        await ctx.close()

    if captured:
        OUT.write_text(
            json.dumps(captured, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Saved: {OUT}")
    else:
        print("No POST captured — Apollo may have used GET or a different path.")


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
