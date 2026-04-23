#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing Python deps ==="
pip install --upgrade pip
pip install -r requirements.txt

echo "=== Installing Playwright Chromium (optional, only needed if scraping) ==="
# Skip if you only plan to run enrichment (no scraping in Codespaces)
python -m playwright install --with-deps chromium || true

echo ""
echo "================================================================"
echo " Setup complete."
echo ""
echo " NEXT STEPS INSIDE THIS CODESPACE:"
echo ""
echo "  1. Create your Hunter API key file:"
echo "       echo 'YOUR_HUNTER_KEY_HERE' > hunter_key.txt"
echo ""
echo "     OR set it as a Codespaces secret named HUNTER_API_KEY"
echo "     (Repo Settings > Secrets and variables > Codespaces)."
echo ""
echo "  2. Upload your scraped raw data:"
echo "       - Drag-and-drop apollo_data.raw.json into the file"
echo "         explorer on the left."
echo ""
echo "  3. Upload your enrichment cache (optional but saves Hunter credits):"
echo "       - Drag-and-drop enrichment_cache.json."
echo ""
echo "  4. Run the enricher (port 25 works here):"
echo "       python enrich_hybrid.py"
echo ""
echo "================================================================"
