# Running enrichment in GitHub Codespaces

Your local Windows machine has **port 25 blocked** by your ISP, which disables
SMTP verification (the free-Tier-1 step of the enrichment pipeline).

GitHub Codespaces runs on Microsoft Azure, where port 25 is usually open for
outbound SMTP verification requests. Running `enrich_hybrid.py` from a
Codespace upgrades many **Tier 3 (pattern-only)** results to **Tier 1
(SMTP-verified)** — at zero cost.

---

## One-time setup (5 minutes)

### 1. Open this repo in a Codespace

1. Push this project to GitHub (see `PUBLISH.md` if needed).
2. Go to the repo on GitHub.
3. Click the green **Code** button → **Codespaces** tab → **Create codespace
   on main**.
4. Wait ~60 seconds for the devcontainer to build. The `postCreateCommand`
   installs Python deps automatically.

### 2. Provide your Hunter API key

Two options — pick one:

**Option A — Codespaces secret (recommended)**
- Repo → Settings → Secrets and variables → Codespaces → **New repository
  secret**.
- Name: `HUNTER_API_KEY`
- Value: your key
- Rebuild/restart the Codespace.

**Option B — File inside the Codespace**
```bash
echo "YOUR_HUNTER_KEY_HERE" > hunter_key.txt
```

### 3. Upload your data files

Drag-and-drop into the file explorer on the left:
- `apollo_data.raw.json` — your scraped contacts
- `enrichment_cache.json` (optional) — preserves cached domain patterns so
  no Hunter credits are wasted re-discovering them

---

## Running it

```bash
python enrich_hybrid.py
```

You should see lines like:

```
[1/4] MX classification...
  google: 3  microsoft: 26  other: 24  none: 3
[3/4] Candidate generation + SMTP verify...
  probing catch-all status on 24 domains...
  [  25/74] last: Raphael Resse    tier=1  email=raphael.resse@vermande.fr
```

If port 25 is open (which it is in Codespaces), you won't see the
⚠ "Port 25 appears BLOCKED" warning this time.

### Expected improvement

On your Windows machine:
- **Tier 1 (SMTP-verified):** 0
- **Tier 3 (pattern unverified):** 54

In Codespaces (after fix):
- **Tier 1 (SMTP-verified):** ~15-20 (the "other" MX domains only; O365/Gmail
  are still catch-all-filtered into Tier 3)
- **Tier 3:** ~35

---

## Download the result

The enriched CSV lives at `apollo_data_enriched.csv` inside the Codespace.
Right-click it in the file explorer → **Download** to pull it back to your
local machine.

---

## Costs

- Codespaces: **free** for up to 60 core-hours/month on personal accounts.
  This job uses ~10 minutes of a 2-core machine = ~0.3 core-hours per run.
- Hunter: this re-run uses **0–1 credits** because your existing
  `enrichment_cache.json` already has all the domain patterns.
