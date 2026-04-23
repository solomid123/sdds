"""
Hybrid email enricher: Hunter domain-search + SMTP verify + fallback.

Pipeline
--------
1. Read apollo_data.raw.json produced by apollo_scraper.py.
2. Group contacts by company domain.
3. For each unique domain:
     a. DNS MX lookup  -> classify provider (google/microsoft/other/none)
     b. Hunter /v2/domain-search (1 credit/domain) -> get email pattern
        and up to 10 already-verified emails.
4. For each contact, apply the domain pattern to generate a candidate email.
5. SMTP-verify candidates on non-catch-all, non-O365/Gmail servers (free).
6. Fall back to Hunter /v2/email-finder for contacts that failed steps 3-5
   (uses 1 credit each, but only where strictly needed).

Output tiers:
   Tier 1 = SMTP-verified email          (highest confidence, free)
   Tier 2 = Hunter domain-search result  (already verified by Hunter)
   Tier 3 = Pattern-generated only       (no verification, flag before use)
   Tier 4 = Hunter email-finder result   (scored 0-100)
   Tier 5 = No email found

Everything is cached in enrichment_cache.json so re-runs don't re-spend.

Usage:
   python enrich_hybrid.py                      # default: apollo_data.raw.json
   python enrich_hybrid.py path/to/raw.json
   python enrich_hybrid.py --no-smtp            # skip SMTP entirely
   python enrich_hybrid.py --no-hunter          # skip Hunter (fully free)
   python enrich_hybrid.py --hunter-budget 50   # max Hunter credits to spend
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import smtplib
import socket
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import dns.resolver
import requests

HERE = Path(__file__).parent
KEY_FILE = HERE / "hunter_key.txt"
DEFAULT_INPUT = HERE / "apollo_data.raw.json"
OUTPUT_CSV = HERE / "apollo_data_enriched.csv"
CACHE_FILE = HERE / "enrichment_cache.json"

HUNTER_RATE_DELAY = 0.3
SMTP_TIMEOUT = 10
DNS_TIMEOUT = 5
SMTP_CONCURRENCY = 15
# SMTP probe identity — use plausible-looking sender + HELO hostname so
# mail servers don't classify our probe as obvious spam-scanner and return
# blanket 550 rejections for every address.  Override via env vars if you
# own a domain with SPF pointing to your IP (best case).
PROBE_FROM = os.environ.get("SMTP_PROBE_FROM", "postmaster@gmail.com")
PROBE_HELO = os.environ.get("SMTP_PROBE_HELO", "mail.gmail.com")
FAKE_LOCAL = "zz_nonexistent_user_9f8e7d6c5b4a3"

# ---------------- helpers ----------------

def _load_key() -> str | None:
    """Load Hunter API key from env var or hunter_key.txt (in that order)."""
    env_key = os.environ.get("HUNTER_API_KEY", "").strip()
    if env_key:
        return env_key
    if KEY_FILE.exists():
        k = KEY_FILE.read_text(encoding="utf-8").strip()
        return k or None
    return None


def _load_cache() -> dict:
    default = {"domains": {}, "smtp": {}, "finder": {}}
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return default
            # Ensure required keys exist (migrates older caches)
            for k, v in default.items():
                data.setdefault(k, v)
            return data
        except Exception:
            pass
    return default


def _save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _domain_from_person(p: dict) -> str | None:
    org = p.get("organization") or {}
    for field in ("primary_domain", "website_url", "domain"):
        val = org.get(field) or p.get(field)
        if not val:
            continue
        val = str(val).strip()
        if "://" not in val:
            val = "http://" + val
        host = urlparse(val).netloc.lower()
        host = host.removeprefix("www.")
        if "." in host:
            return host
    return None


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s or "")
        if unicodedata.category(c) != "Mn"
    )


def _normalize_name_part(s: str) -> str:
    s = _strip_accents(s or "").lower()
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def _apply_pattern(pattern: str, first: str, last: str, domain: str) -> str | None:
    """Convert Hunter's pattern template (e.g. '{first}.{last}') into an email."""
    if not pattern or not first or not last or not domain:
        return None
    f = _normalize_name_part(first)
    l = _normalize_name_part(last)
    if not f or not l:
        return None
    local = pattern
    replacements = {
        "{first}": f,
        "{last}": l,
        "{f}": f[:1],
        "{l}": l[:1],
        "{initial}": f[:1],
        "{first_initial}": f[:1],
        "{last_initial}": l[:1],
    }
    for k, v in replacements.items():
        local = local.replace(k, v)
    if "{" in local:  # unknown placeholder
        return None
    local = re.sub(r"[^a-z0-9._+-]", "", local)
    if not local:
        return None
    return f"{local}@{domain}"


# ---------------- DNS / MX classification ----------------

_DNS_RESOLVER = None


def _get_resolver() -> dns.resolver.Resolver:
    """Build a resolver that works on Windows where the default config
    sometimes fails to autodiscover nameservers. Uses public DNS."""
    global _DNS_RESOLVER
    if _DNS_RESOLVER is not None:
        return _DNS_RESOLVER
    r = dns.resolver.Resolver(configure=True)
    # If system didn't give us any nameservers, or gave only IPv6/local ones
    # that dnspython can't use, fall back to public DNS.
    if not r.nameservers or all(
        ns.startswith(("::1", "fe80:", "127.")) for ns in r.nameservers
    ):
        r.nameservers = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
    r.timeout = DNS_TIMEOUT
    r.lifetime = DNS_TIMEOUT
    _DNS_RESOLVER = r
    return r


def _mx_records(domain: str) -> list[str]:
    try:
        ans = _get_resolver().resolve(domain, "MX")
        return sorted([str(r.exchange).rstrip(".").lower() for r in ans])
    except dns.resolver.NoAnswer:
        return []
    except dns.resolver.NXDOMAIN:
        return []
    except Exception as e:
        # Log once at debug level; don't spam
        if not getattr(_mx_records, "_warned", False):
            print(f"  DNS lookup failed for {domain}: {type(e).__name__}: {e}")
            print("  (will fall back to public DNS if not already)")
            _mx_records._warned = True  # type: ignore[attr-defined]
        # Retry once with explicit public DNS
        try:
            r = dns.resolver.Resolver(configure=False)
            r.nameservers = ["1.1.1.1", "8.8.8.8"]
            r.timeout = DNS_TIMEOUT
            r.lifetime = DNS_TIMEOUT
            ans = r.resolve(domain, "MX")
            return sorted([str(x.exchange).rstrip(".").lower() for x in ans])
        except Exception:
            return []


def _classify_mx(mxs: list[str]) -> str:
    """Return one of: 'google', 'microsoft', 'other', 'none'."""
    if not mxs:
        return "none"
    joined = " ".join(mxs)
    if "google.com" in joined or "googlemail.com" in joined:
        return "google"
    if ("protection.outlook.com" in joined
            or "mail.protection.outlook.com" in joined
            or "outlook.com" in joined):
        return "microsoft"
    return "other"


# ---------------- Hunter ----------------

class HunterClient:
    def __init__(self, key: str | None, budget: int | None):
        self.key = key
        self.budget = budget            # None = unlimited (uses Hunter's quota)
        self.calls = 0
        self.disabled = key is None

    def _can_call(self) -> bool:
        if self.disabled:
            return False
        if self.budget is not None and self.calls >= self.budget:
            return False
        return True

    def _call(self, endpoint: str, params: dict) -> dict:
        params = {**params, "api_key": self.key}
        time.sleep(HUNTER_RATE_DELAY)
        try:
            r = requests.get(f"https://api.hunter.io/v2/{endpoint}",
                             params=params, timeout=20)
        except Exception as e:
            return {"error": "network", "detail": str(e)}
        self.calls += 1
        if r.status_code == 401:
            self.disabled = True
            return {"error": "unauthorized"}
        if r.status_code in (402, 451, 429):
            self.disabled = True
            return {"error": "quota"}
        try:
            return r.json()
        except Exception:
            return {"error": "bad_json", "status": r.status_code}

    def domain_search(self, domain: str) -> dict:
        if not self._can_call():
            return {"error": "skipped"}
        return self._call("domain-search", {"domain": domain, "limit": 10})

    def email_finder(self, first: str, last: str, domain: str) -> dict:
        if not self._can_call():
            return {"error": "skipped"}
        return self._call("email-finder",
                          {"domain": domain, "first_name": first,
                           "last_name": last})


# ---------------- SMTP verification ----------------

class SMTPVerifier:
    """Checks (email, mx_host) via RCPT TO. Detects catch-all per domain."""

    def __init__(self):
        self.catchall_cache: dict[str, bool] = {}
        self.port25_ok = None          # None=unknown, True=usable, False=blocked
        self.port25_failures = 0

    def _smtp_probe(self, mx_host: str, email: str) -> tuple[int, str]:
        try:
            with smtplib.SMTP(mx_host, 25, timeout=SMTP_TIMEOUT,
                              local_hostname=PROBE_HELO) as s:
                # Force EHLO (modern) then fall back; pass our HELO name so
                # servers doing reverse DNS / HELO checks don't blanket-reject.
                code, _ = s.ehlo(PROBE_HELO)
                if code >= 500:
                    s.helo(PROBE_HELO)
                s.mail(PROBE_FROM)
                code, msg = s.rcpt(email)
                return code, (msg.decode() if isinstance(msg, bytes)
                              else str(msg))
        except (smtplib.SMTPServerDisconnected,
                smtplib.SMTPConnectError,
                socket.timeout,
                OSError) as e:
            return -1, str(e)
        except Exception as e:
            return -2, str(e)

    def is_catchall(self, domain: str, mx_host: str) -> bool | None:
        """Returns True if server accepts a clearly-fake address."""
        if domain in self.catchall_cache:
            return self.catchall_cache[domain]
        code, _ = self._smtp_probe(mx_host, f"{FAKE_LOCAL}@{domain}")
        if code == -1:
            self.port25_failures += 1
            if self.port25_failures >= 3 and self.port25_ok is None:
                self.port25_ok = False
            return None
        self.port25_ok = True
        val = 250 <= code < 260     # accepted => catch-all
        self.catchall_cache[domain] = val
        return val

    def verify(self, email: str, mx_host: str) -> tuple[str, str]:
        """Returns (status, detail). Status is one of:
           'verified' | 'rejected' | 'catchall' | 'greylisted' | 'unreachable'
        Detail contains the raw SMTP response for transparency."""
        if self.port25_ok is False:
            return "unreachable", "port25_blocked"
        domain = email.split("@", 1)[1]
        catch = self.is_catchall(domain, mx_host)
        if catch is None:
            return "unreachable", "catchall_probe_failed"
        if catch:
            return "catchall", "server_accepts_all"
        code, msg = self._smtp_probe(mx_host, email)
        detail = f"{code} {msg[:80]}" if code >= 0 else f"conn_err:{msg[:60]}"
        if code == -1:
            return "unreachable", detail
        if 250 <= code < 260:
            return "verified", detail
        if code in (450, 451, 452):
            return "greylisted", detail        # retry later; don't trust
        if 500 <= code < 600:
            return "rejected", detail
        return "unreachable", detail


# ---------------- main pipeline ----------------

def _process(args, people: list[dict], cache: dict,
             hunter: HunterClient, smtp: SMTPVerifier) -> list[dict]:
    # Group contacts by domain
    domain_to_people: dict[str, list[dict]] = {}
    no_domain = []
    for p in people:
        d = _domain_from_person(p)
        if d:
            domain_to_people.setdefault(d, []).append(p)
        else:
            no_domain.append(p)

    print(f"Grouped {len(people)} contacts into {len(domain_to_people)} "
          f"unique domains ({len(no_domain)} without a usable domain).")

    # ---- Phase 1: classify domains via MX ----
    print("\n[1/4] MX classification...")
    for d in list(domain_to_people.keys()):
        dc = cache["domains"].setdefault(d, {})
        if "mx_class" not in dc:
            mxs = _mx_records(d)
            dc["mx"] = mxs
            dc["mx_class"] = _classify_mx(mxs)
    _save_cache(cache)
    classes = [cache["domains"][d]["mx_class"] for d in domain_to_people]
    print(f"  google: {classes.count('google')}  "
          f"microsoft: {classes.count('microsoft')}  "
          f"other: {classes.count('other')}  "
          f"none: {classes.count('none')}")

    # ---- Phase 2: Hunter domain-search for pattern ----
    print("\n[2/4] Hunter domain-search (pattern discovery)...")
    if args.no_hunter:
        print("  skipped (--no-hunter)")
    else:
        todo = [d for d, dc in cache["domains"].items()
                if d in domain_to_people and "hunter_pattern" not in dc]
        for i, d in enumerate(todo, 1):
            dc = cache["domains"][d]
            res = hunter.domain_search(d)
            if res.get("error"):
                dc["hunter_error"] = res["error"]
                if res["error"] == "quota":
                    print(f"  Hunter quota hit after {hunter.calls} calls.")
                    break
                continue
            data = res.get("data") or {}
            dc["hunter_pattern"] = data.get("pattern") or ""
            dc["hunter_emails"] = [
                {"value": e.get("value"),
                 "first_name": e.get("first_name"),
                 "last_name": e.get("last_name"),
                 "confidence": e.get("confidence")}
                for e in (data.get("emails") or [])
            ]
            _save_cache(cache)
            print(f"  [{i:3d}/{len(todo)}] {d}  pattern={dc['hunter_pattern']!r}"
                  f"  emails={len(dc['hunter_emails'])}")

    # ---- Phase 3: Build candidate per contact + SMTP verify ----
    print("\n[3/4] Candidate generation + SMTP verify...")
    results: list[dict] = []

    def work(person: dict, domain: str) -> dict:
        dc = cache["domains"].get(domain, {})
        first = (person.get("first_name") or "").strip()
        last = (person.get("last_name") or "").strip()
        name = person.get("name") or f"{first} {last}".strip()

        row = {
            "name": name,
            "title": person.get("title") or "",
            "company": (person.get("organization") or {}).get("name") or "",
            "domain": domain,
            "linkedin": person.get("linkedin_url") or "",
            "location": ", ".join(
                x for x in (person.get("city"), person.get("state"),
                            person.get("country")) if x),
            "email": "",
            "tier": 5,
            "confidence": "",
            "source": "",
        }

        # (a) Direct hit in Hunter's domain-search results
        hunter_hits = dc.get("hunter_emails") or []
        fnorm = _normalize_name_part(first)
        lnorm = _normalize_name_part(last)
        for e in hunter_hits:
            if not e.get("value"):
                continue
            ef = _normalize_name_part(e.get("first_name") or "")
            el = _normalize_name_part(e.get("last_name") or "")
            if ef == fnorm and el == lnorm:
                row.update(email=e["value"], tier=2,
                           confidence=str(e.get("confidence") or ""),
                           source="hunter_domain_search_match")
                return row

        # (b) Pattern-apply
        pattern = dc.get("hunter_pattern")
        candidate = _apply_pattern(pattern, first, last, domain) if pattern else None
        if candidate:
            mx_class = dc.get("mx_class")
            mx_list = dc.get("mx") or []
            if (not args.no_smtp
                    and mx_class == "other"
                    and mx_list
                    and smtp.port25_ok is not False):
                mx_host = mx_list[0]
                ck = f"{candidate}|{mx_host}"
                cached = cache["smtp"].get(ck)
                if isinstance(cached, str):
                    # Legacy cache: old runs stored only the status string.
                    status, detail = cached, "cached"
                elif isinstance(cached, list) and len(cached) == 2:
                    status, detail = cached
                else:
                    status, detail = smtp.verify(candidate, mx_host)
                    cache["smtp"][ck] = [status, detail]
                row["source"] = f"smtp:{status}|{detail}"
                if status == "verified":
                    row.update(email=candidate, tier=1, confidence="95")
                    return row
                if status == "catchall":
                    row.update(email=candidate, tier=3, confidence="60")
                    return row
                if status == "greylisted":
                    # Server deferred; treat like catch-all (unverified but plausible)
                    row.update(email=candidate, tier=3, confidence="50")
                    return row
                if status == "rejected":
                    # Server said "no such user" — but could be false positive
                    # if the server is rejecting ALL probes. Mark email but
                    # drop confidence low and call it out.
                    row.update(email=candidate, tier=4, confidence="25")
                    return row
                # unreachable -> fall through to Hunter fallback
            else:
                # Skipped SMTP (google/microsoft/none) -> pattern only
                row.update(email=candidate, tier=3, confidence="55",
                           source=f"pattern_only:{mx_class}")
                return row

        # (c) No pattern -> fall back to Hunter email-finder
        if not args.no_hunter and first and last:
            ck = f"{fnorm}|{lnorm}|{domain}"
            cached = cache["finder"].get(ck)
            if cached is None:
                resp = hunter.email_finder(first, last, domain)
                if resp.get("error"):
                    cached = {"error": resp["error"]}
                else:
                    d = resp.get("data") or {}
                    cached = {
                        "email": d.get("email") or "",
                        "score": d.get("score"),
                        "verification":
                            (d.get("verification") or {}).get("status"),
                    }
                cache["finder"][ck] = cached
            if cached.get("email"):
                row.update(email=cached["email"], tier=4,
                           confidence=str(cached.get("score") or ""),
                           source="hunter_email_finder")
                return row

        return row

    # Sequential for simplicity; SMTP concurrency happens inside verifier
    # in a batch pass below. First, do SMTP catch-all probes concurrently
    # for all 'other' domains so subsequent verifies are fast.
    other_domains = [
        d for d, dc in cache["domains"].items()
        if d in domain_to_people and dc.get("mx_class") == "other"
        and dc.get("mx") and d not in smtp.catchall_cache
    ]
    if other_domains and not args.no_smtp:
        print(f"  probing catch-all status on {len(other_domains)} domains...")
        with ThreadPoolExecutor(max_workers=SMTP_CONCURRENCY) as ex:
            futures = {
                ex.submit(smtp.is_catchall, d,
                          cache["domains"][d]["mx"][0]): d
                for d in other_domains
            }
            for i, fut in enumerate(as_completed(futures), 1):
                d = futures[fut]
                try:
                    fut.result()
                except Exception:
                    pass
                if i % 25 == 0:
                    print(f"    catch-all probed {i}/{len(other_domains)}")
        if smtp.port25_ok is False:
            print("  ⚠ Port 25 appears BLOCKED by your ISP/firewall. "
                  "SMTP verification disabled; all pattern results will be Tier 3. "
                  "Re-run from a VPS or GitHub Codespaces to enable SMTP.")

    # Now enrich every contact
    total = sum(len(v) for v in domain_to_people.values())
    done = 0
    for d, plist in domain_to_people.items():
        for p in plist:
            row = work(p, d)
            results.append(row)
            done += 1
            if done % 25 == 0 or done == total:
                print(f"  [{done:4d}/{total}] last: {row['name'][:25]:25s} "
                      f"tier={row['tier']}  email={row['email'] or '(none)'}")
        # persist cache periodically
        if done % 50 == 0:
            _save_cache(cache)
    _save_cache(cache)

    # People with no domain: can't enrich
    for p in no_domain:
        first = (p.get("first_name") or "").strip()
        last = (p.get("last_name") or "").strip()
        results.append({
            "name": p.get("name") or f"{first} {last}",
            "title": p.get("title") or "",
            "company": (p.get("organization") or {}).get("name") or "",
            "domain": "",
            "linkedin": p.get("linkedin_url") or "",
            "location": "",
            "email": "",
            "tier": 5,
            "confidence": "",
            "source": "no_domain",
        })

    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", nargs="?", default=str(DEFAULT_INPUT))
    ap.add_argument("--no-smtp", action="store_true",
                    help="Disable SMTP verification entirely.")
    ap.add_argument("--no-hunter", action="store_true",
                    help="Disable Hunter calls (fully free, pattern-guess only).")
    ap.add_argument("--hunter-budget", type=int, default=None,
                    help="Max number of Hunter API calls this run.")
    ap.add_argument("--retry-smtp", action="store_true",
                    help="Invalidate cached SMTP results and re-probe.")
    args = ap.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: {input_path} not found. Run apollo_scraper.py first.")
        sys.exit(1)

    people = json.loads(input_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(people)} contacts from {input_path.name}.")

    key = _load_key()
    if key:
        print(f"Hunter API key loaded (budget={args.hunter_budget or 'unlimited'}).")
    else:
        print("No Hunter key; running free-only mode.")

    cache = _load_cache()
    if args.retry_smtp:
        n = len(cache.get("smtp", {}))
        cache["smtp"] = {}
        print(f"Invalidated {n} cached SMTP results (will re-probe).")
    hunter = HunterClient(None if args.no_hunter else key, args.hunter_budget)
    smtp = SMTPVerifier()

    rows = _process(args, people, cache, hunter, smtp)

    # Write CSV
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "tier", "name", "title", "company", "domain",
            "email", "confidence", "source", "linkedin", "location",
        ])
        w.writeheader()
        # sort by tier asc so best results first
        rows.sort(key=lambda r: (r["tier"], r["company"]))
        for r in rows:
            w.writerow(r)

    # Stats
    from collections import Counter
    tiers = Counter(r["tier"] for r in rows)
    print("\n=== Enrichment complete ===")
    labels = {
        1: "SMTP-verified       (~95%, safe to send)",
        2: "Hunter domain match (~90%, safe to send)",
        3: "Pattern unverified  (~55-70%, risky)",
        4: "Low confidence      (~25%, probably wrong)",
        5: "No email found",
    }
    for t in sorted(labels):
        print(f"  Tier {t} ({labels[t]:44s}): {tiers.get(t, 0):4d}")
    print(f"\n  Hunter credits used this run: {hunter.calls}")
    print(f"  Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
