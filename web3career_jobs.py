#!/usr/bin/env python3
"""
web3career_jobs.py - Web3.career job-posting discovery collector for the Snag
GTM signal pipeline. Classifies role + JD against the trigger framework, scores
each posting (provisional floor; company-fit + convergence added at enrichment),
dedupes via a seen-cache, and writes the same JSON shape as the Galxe/Zealy
collectors. Python 3.9 compatible.

When R2 credentials are present in the environment, the output file is uploaded
to Cloudflare R2 (so the snag-scoring job can read it). On a machine without
those variables (e.g. a manual Mac run) the R2 step is a silent no-op and
behavior is unchanged.

Environment:
    WEB3CAREER_TOKEN - web3.career API token (required)
    REDIS_URL - Redis/Valkey connection string for the persistent seen-cache
                (optional; falls back to a local file if absent, e.g. a Mac run)
    R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET - optional (enables R2 upload)
"""

import argparse, html, json, os, re, sys, time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

try:
    import requests
except ImportError:
    print("Missing dependency. Run: pip3 install requests", file=sys.stderr)
    sys.exit(1)

API_BASE = "https://web3.career/api/v1"

# --- Tags pulled from the API (one tag per request; we fan out) -----------
TAGS = ["community-manager", "developer-relations", "marketing", "sales",
        "product-manager", "non-tech"]

# --- Role rules (ordered, first match wins). Tuple: (category, tier, needs_jd) ---
# needs_jd=True means the title is too generic to fire alone (e.g. "Product
# Manager", "Community Manager") - only fires if the JD carries real intent.
ROLE_RULES = [
    # Tier 1
    ("devrel", 1, False, ["developer relations", "devrel", "dev rel", "developer advocate",
                          "developer community", "developer experience", "developer marketing"]),
    ("grants", 1, False, ["grants lead", "grants manager", "grant manager", "grant lead", "head of grants"]),
    ("ecosystem", 1, False, ["head of ecosystem", "ecosystem lead", "ecosystem growth",
                             "ecosystem manager", "ecosystem partner"]),
    ("product", 1, False, ["head of product", "director of product", "vp product", "vp of product",
                           "group product manager", "principal product manager", "lead product manager",
                           "product lead", "growth product manager", "ecosystem product manager",
                           "developer platform product manager", "developer experience product manager",
                           "platform product manager", "web3 product manager", "token product manager",
                           "protocol product manager", "consumer product manager"]),
    ("growth", 1, False, ["head of growth", "growth lead", "vp growth", "vp of growth", "director of growth"]),
    # Tier 2  (marketing rule before generic product to catch "Product Marketing Manager")
    ("marketing", 2, False, ["product marketing"]),
    ("product", 2, True, ["senior product manager", "product manager"]),
    ("community", 2, True, ["head of community", "community lead"]),
    ("growth", 2, False, ["growth marketing", "user growth", "lifecycle marketing", "crm marketing"]),
    ("partnerships", 2, False, ["strategic partnership", "business development", "partner manager",
                                "partnerships", "head of partnerships"]),
    # Tier 3
    ("creator", 3, False, ["ambassador", "creator marketing", "kol", "influencer marketing"]),
    ("ops", 3, False, ["marketing operations", "marketing ops", "growth ops", "growth operations",
                       "revops", "rev ops"]),
    ("community", 3, True, ["community manager", "community"]),
]

SENIOR_TERMS = ["head of", "director", "vp ", "vp of", "vice president", "principal",
                "group product", "lead"]

# --- Keyword groups -------------------------------------------------------
CORE_KEYWORDS = [  # +3
    "quest", "mission", "campaign", "points program", "points", "loyalty", "rewards",
    "referral", "incentive", "airdrop", "token claim", "claims", "minting", "onboarding",
    "activation", "retention", "engagement", "user journey", "product-led growth", "plg",
    "growth loop", "developer portal", "ecosystem portal", "user segmentation",
]
PROGRAM_KEYWORDS = [  # +2
    "hackathon", "grants", "ambassador", "developer adoption", "developer growth",
    "dev ecosystem", "builder program", "validator growth", "app ecosystem", "liquidity incentive",
]
BUILD_VS_BUY = [  # +2  (explicit buy-over-build signal)
    "without pulling engineering", "reduce engineering", "engineering dependency",
    "internal tool", "no-code", "no code", "experimentation", "launch experiments", "ship faster",
]

# --- Routing maps ---------------------------------------------------------
PERSONA = {"product": "Product", "ecosystem": "Ecosystem", "growth": "GTM", "devrel": "DevRel",
           "community": "Community", "creator": "Community", "partnerships": "GTM",
           "grants": "DevRel", "marketing": "GTM", "ops": "GTM"}
MOTION = {"product": "developer portal / ecosystem hub / loyalty layer",
          "ecosystem": "ecosystem hub / quest-campaign system",
          "growth": "loyalty layer / CRM-segmentation layer",
          "devrel": "developer portal / developer incentives",
          "community": "quest-campaign system / referral program",
          "creator": "referral program / quest-campaign system",
          "partnerships": "App Hub co-marketing campaigns",
          "grants": "developer portal / token claim",
          "marketing": "quest-campaign system", "ops": "CRM/segmentation layer"}
CATEGORY_PRIORITY = {"product": 0, "ecosystem": 0, "growth": 1, "devrel": 1, "grants": 2,
                     "partnerships": 3, "community": 3, "creator": 4, "marketing": 4, "ops": 5}

TITLE_POINTS = {1: 3, 2: 2, 3: 1}
PER_TAG_LIMIT = 100
MAX_RETRIES = 3


# --- Cloudflare R2 upload (S3-compatible) ---------------------------------
def _r2_client():
    """Return (client, bucket) for Cloudflare R2, or (None, None) if not configured.
    Mirrors the helper in frontrun_signals.py so behavior matches exactly. Every
    R2 step becomes a silent no-op when unconfigured, so a manual Mac run is
    unaffected."""
    endpoint   = os.environ.get("R2_ENDPOINT")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    bucket     = os.environ.get("R2_BUCKET")
    if not all([endpoint, access_key, secret_key, bucket]):
        return None, None
    try:
        import boto3
    except ImportError:
        print("  [warn] R2 vars set but boto3 not installed. Run: pip3 install boto3", file=sys.stderr)
        return None, None
    client = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=access_key, aws_secret_access_key=secret_key,
        region_name="auto",
    )
    return client, bucket


def upload_to_r2(local_path: str) -> None:
    """Upload the output JSON to R2 (no-op if R2 isn't configured)."""
    client, bucket = _r2_client()
    if client is None:
        print("  R2 not configured - skipping upload (local file written only).")
        return
    key = os.path.basename(local_path)
    client.upload_file(local_path, bucket, key)
    print("  Uploaded to R2: %s/%s" % (bucket, key))


# --- HTTP -----------------------------------------------------------------
def fetch_jobs_for_tag(token: str, tag: str) -> List[dict]:
    params = {"token": token, "tag": tag, "limit": PER_TAG_LIMIT, "show_description": "true"}
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(API_BASE, params=params, timeout=30)
            if resp.status_code == 200:
                return extract_jobs(resp.json())
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == MAX_RETRIES:
                    print("  [warn] tag=%s -> HTTP %s after retries" % (tag, resp.status_code), file=sys.stderr)
                    return []
                time.sleep(min(2 ** attempt, 10)); continue
            print("  [warn] tag=%s -> HTTP %s (not retrying)" % (tag, resp.status_code), file=sys.stderr)
            return []
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                print("  [warn] tag=%s network error: %s" % (tag, e), file=sys.stderr)
                return []
            time.sleep(min(2 ** attempt, 10))
    return []

def extract_jobs(data) -> List[dict]:
    """Response is mixed-type: [str, str, [job,...]]. Find the nested list."""
    if not isinstance(data, list):
        return []
    nested = next((item for item in data if isinstance(item, list)), None)
    if nested is not None:
        return [j for j in nested if isinstance(j, dict)]
    if data and isinstance(data[0], dict):
        return [j for j in data if isinstance(j, dict)]
    return []

# --- Classification + scoring --------------------------------------------
def strip_html(text: Optional[str]) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()

def classify_role(title: Optional[str]) -> Optional[Tuple[str, int, bool]]:
    t = (title or "").lower()
    for category, tier, needs_jd, needles in ROLE_RULES:
        for n in needles:
            if n in t:
                return (category, tier, needs_jd)
    return None

def matches(hay: str, words: List[str]) -> List[str]:
    # word-boundary match, tolerant of a trailing plural 's' (avoids
    # "quest" firing inside "questions", "mission" inside "permission", etc.)
    found = []
    for w in words:
        if re.search(r"\b" + re.escape(w) + r"s?\b", hay):
            found.append(w)
    return found

def is_senior(title: str) -> bool:
    t = (title or "").lower()
    return any(term in t for term in SENIOR_TERMS)

def band(score: int) -> str:
    if score >= 11:
        return "A"
    if score >= 6:
        return "B"
    return "C"

# --- Job lifecycle state (Redis hash, with local-file fallback) -----------
# We track each matching role's lifecycle so it stays a scoreable signal while
# the role is OPEN and for a retention window after it's taken down. State per
# job_id: {first_seen, last_seen, signal}. "Open" = appeared in today's API
# pull; "taken down" = stopped appearing (last_seen stops advancing). A role is
# dropped once it hasn't been seen for RETENTION_DAYS. State persists in Redis
# (the snag-seen Valkey instance) across Render's ephemeral runs; falls back to
# a local JSON file when REDIS_URL is absent (e.g. a manual Mac run).
STATE_KEY = "web3career:jobs_state"
RETENTION_DAYS = 60  # keep scoring a role ~2 months after it's taken down

def _redis_client():
    url = os.environ.get("REDIS_URL")
    if not url:
        return None
    try:
        import redis
    except ImportError:
        print("  [warn] REDIS_URL set but redis not installed. Run: pip3 install redis", file=sys.stderr)
        return None
    try:
        client = redis.from_url(url, decode_responses=True)
        client.ping()
        return client
    except Exception as e:
        print("  [warn] could not connect to Redis (%s) - using local file this run" % e, file=sys.stderr)
        return None

def load_state(client, local_path: str) -> dict:
    """Return {job_id: {first_seen, last_seen, signal}}."""
    if client is not None:
        try:
            raw = client.hgetall(STATE_KEY)
            return {k: json.loads(v) for k, v in raw.items()}
        except Exception as e:
            print("  [warn] Redis read failed (%s) - using local file this run" % e, file=sys.stderr)
    if os.path.exists(local_path):
        try:
            with open(local_path) as f:
                return json.load(f)
        except (ValueError, IOError):
            return {}
    return {}

def save_state(client, local_path: str, state: dict) -> None:
    """Persist state, mirroring deletions (expiry) so the store matches exactly."""
    if client is not None:
        try:
            existing = set(client.hkeys(STATE_KEY))
            to_del = existing - set(state.keys())
            pipe = client.pipeline()
            if to_del:
                pipe.hdel(STATE_KEY, *to_del)
            for jid, entry in state.items():
                pipe.hset(STATE_KEY, jid, json.dumps(entry))
            pipe.execute()
            return
        except Exception as e:
            print("  [warn] Redis write failed (%s) - writing local file instead" % e, file=sys.stderr)
    with open(local_path, "w") as f:
        json.dump(state, f, indent=2)

def _days_since(date_str: Optional[str], today) -> int:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return (today - d).days
    except (ValueError, TypeError):
        return 0

# --- Main -----------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)  # parity only; dedup is id-based
    ap.add_argument("--output-dir", default=".")
    args = ap.parse_args()

    token = os.environ.get("WEB3CAREER_TOKEN")
    if not token:
        print("ERROR: set WEB3CAREER_TOKEN env var (export WEB3CAREER_TOKEN=...)", file=sys.stderr)
        return 1

    out_dir = args.output_dir
    state_path = os.path.join(out_dir, "web3career_jobs_state.json")
    rclient = _redis_client()
    state = load_state(rclient, state_path)
    print("Job state: %s (%d tracked roles loaded)"
          % ("Redis" if rclient is not None else "local file", len(state)))

    # 1. Fetch the FULL current matching population (not a delta).
    raw_by_id = {}
    for tag in TAGS:
        for job in fetch_jobs_for_tag(token, tag):
            jid = str(job.get("id") or job.get("url") or "")
            if jid and jid not in raw_by_id:
                raw_by_id[jid] = job

    # 2. Classify + score every matching role (no seen-skip; we want the population).
    partials = []
    seen_role_keys = set()
    for jid, job in raw_by_id.items():
        # clean HTML entities in title/company (e.g. "Verification &amp; Activation")
        job["title"] = html.unescape((job.get("title") or "").strip())
        job["company"] = html.unescape((job.get("company") or "").strip())
        title = job["title"]
        hit = classify_role(title)
        if hit is None:
            continue
        category, tier, needs_jd = hit
        desc = strip_html(job.get("description", ""))
        hay = (title + " " + desc).lower()
        core = matches(hay, CORE_KEYWORDS)
        program = matches(hay, PROGRAM_KEYWORDS)
        bvb = matches(hay, BUILD_VS_BUY)
        if needs_jd and not (core or program or bvb):
            continue
        # collapse near-duplicate postings: same company + title, different id
        role_key = (job["company"].lower(), title.lower())
        if role_key in seen_role_keys:
            continue
        seen_role_keys.add(role_key)
        score = TITLE_POINTS[tier]
        if is_senior(title):
            score += 2
        if core:
            score += 3
        if program:
            score += 2
        if bvb:
            score += 2
        partials.append({"jid": jid, "job": job, "category": category, "tier": tier,
                         "core": core, "program": program, "bvb": bvb, "score": score})

    norm = lambda s: (s or "").strip().lower()
    counts = {}
    for p in partials:
        counts[norm(p["job"].get("company"))] = counts.get(norm(p["job"].get("company")), 0) + 1
    for p in partials:
        p["cluster"] = counts.get(norm(p["job"].get("company")), 0) >= 2
        if p["cluster"]:
            p["score"] += 2

    today = datetime.now(timezone.utc).date()
    today_str = today.strftime("%Y-%m-%d")

    # 3. Upsert today's open roles into the lifecycle state.
    open_ids = set()
    new_count = 0
    for p in partials:
        jid = p["jid"]
        sig = build_signal(p)
        open_ids.add(jid)
        if jid in state:
            state[jid]["last_seen"] = today_str
            state[jid]["signal"] = sig          # refresh in case details changed
        else:
            state[jid] = {"first_seen": today_str, "last_seen": today_str, "signal": sig}
            new_count += 1

    # 4. Expire roles unseen for longer than the retention window.
    expired = [jid for jid, e in state.items()
               if _days_since(e.get("last_seen", today_str), today) > RETENTION_DAYS]
    for jid in expired:
        del state[jid]

    # 5. Emit the FULL retained population (open + taken-down-but-within-retention).
    signals = []
    for jid, e in state.items():
        sig = dict(e["signal"])
        is_open = jid in open_ids
        job_blk = dict(sig.get("job") or {})
        # recency anchor: real posted_at if the API gave one, else first_seen.
        if not job_blk.get("posted_at"):
            job_blk["posted_at"] = e.get("first_seen")
        job_blk["first_seen"] = e.get("first_seen")
        job_blk["last_seen"] = e.get("last_seen")
        job_blk["is_open"] = is_open
        job_blk["days_since_last_seen"] = _days_since(e.get("last_seen", today_str), today)
        sig["job"] = job_blk
        sig["is_open"] = is_open
        sig["is_new"] = (e.get("first_seen") == today_str)
        signals.append(sig)

    signals.sort(key=lambda s: (-s["lead_score"],
                                CATEGORY_PRIORITY.get(s["job"]["role_category"], 9),
                                s["job"]["trigger_tier"]))

    band_counts, role_counts = {}, {}
    open_n = 0
    for s in signals:
        band_counts[s["priority_band"]] = band_counts.get(s["priority_band"], 0) + 1
        rc = s["job"]["role_category"]; role_counts[rc] = role_counts.get(rc, 0) + 1
        if s["is_open"]:
            open_n += 1

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_hours": args.hours,
        "total_signals": len(signals),
        "open_roles": open_n,
        "new_today": new_count,
        "expired_dropped": len(expired),
        "retention_days": RETENTION_DAYS,
        "signal_counts": {"job_posting": len(signals)},
        "band_counts": band_counts,
        "role_counts": role_counts,
        "signals": signals,
    }
    # Stable snapshot filename (not date-stamped): it always holds the CURRENT
    # retained population, so the scorer reads one current file and same-day
    # re-runs can't clobber it with a smaller delta.
    out_path = os.path.join(out_dir, "web3career_jobs_latest.json")
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
    save_state(rclient, state_path, state)

    print("Tracked roles: %d (%d open, %d new today, %d expired/dropped)"
          % (len(signals), open_n, new_count, len(expired)))
    print("Bands: %s | Roles: %s" % (band_counts if band_counts else "none", role_counts if role_counts else "none"))

    # Mirror the snapshot to R2 so the snag-scoring job can read it (no-op if R2 unset).
    upload_to_r2(out_path)
    return 0


def build_signal(p: dict) -> dict:
    job, category, tier = p["job"], p["category"], p["tier"]
    company = (job.get("company") or "").strip()
    title = (job.get("title") or "").strip()
    link = job.get("apply_url") or job.get("url") or ""
    matched = p["core"] + p["program"] + p["bvb"]
    score = p["score"]
    prio = band(score)
    reason = "%s role (T%d); JD: %s" % (category, tier, ", ".join(matched) if matched else "no keyword hits")
    if p["cluster"]:
        reason += "; company hiring 2+ roles"
    notes = "[%s %d/20 prov.] %s - %s" % (prio, score, title, company)
    if matched:
        notes += " | kw: %s" % ", ".join(matched)
    if link:
        notes += " | %s" % link
    return {
        "signal_type": "job_posting",
        "source": "web3career",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "space": None,
        "campaign": None,
        "job": {
            "id": job.get("id"), "title": title, "company": company,
            "role_category": category, "trigger_tier": tier,
            "matched_keywords": matched, "cluster": p["cluster"],
            "location": job.get("location"), "remote": bool(job.get("remote")),
            "apply_url": job.get("apply_url"), "url": job.get("url"),
            "posted_at": job.get("postedAt"),
        },
        "scoring": {
            "lead_score": score, "score_basis": "provisional floor (role+JD+cluster only)",
            "pending_enrichment": ["company_type (+3)", "convergence_join (+3)"],
        },
        "lead_score": score,
        "priority_band": prio,
        "trigger_reason": reason,
        "likely_buyer_persona": PERSONA.get(category),
        "suggested_snag_motion": MOTION.get(category),
        "engineering_dependency_risk": "high" if p["bvb"] else "low",
        "hubspot_ready": {
            "company_name": company,
            "website": None,
            "twitter_handle": None,
            "lead_source": "Web3.career Jobs",
            "signal": "job_posting",
            "notes": notes,
        },
    }


if __name__ == "__main__":
    sys.exit(main())
