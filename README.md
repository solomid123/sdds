# Apollo.io Scraper (Playwright)

Scrapes people results from an Apollo.io search URL by intercepting the
internal JSON API calls the app itself makes. No selector guessing, no
brittle DOM parsing.

> Heads up: this violates Apollo's Terms of Service and may get your
> account suspended. Use at your own risk.

## One-time setup

Open **PowerShell** in this folder and run:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
```

## Run

```powershell
.\.venv\Scripts\Activate.ps1
python apollo_scraper.py
```

- A Chromium window opens.
- **First run only:** you'll land on Apollo's login page. Sign in
  normally. Once you see the people search results, go back to the
  terminal and press **Enter**. Your session is saved in
  `.apollo_userdata/` so subsequent runs are automatic.
- Output goes to `apollo_data.csv` (appended) and `apollo_data.raw.json`
  (overwritten, for debugging).

## Tweaks

In `apollo_scraper.py`:

- `URL` — change the search URL.
- `PAGES` — how many result pages to click through.
- `HEADLESS` — set `True` once login is stored and you trust it.

## Troubleshooting

- **"No search response captured yet"**: Apollo changed the endpoint name.
  Open DevTools → Network, filter by `search`, and update
  `SEARCH_PATH_MARKERS` in the script.
- **Email shows `locked`**: Apollo requires reveal credits to expose
  emails; the API returns a placeholder until you spend them.
- **Cloudflare challenge**: slow down, reduce `PAGES`, don't run in
  headless mode.
