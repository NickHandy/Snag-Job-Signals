#!/usr/bin/env python3
"""
slack_brief.py - reads web3career_jobs_latest.json (the collector's rolling
snapshot) and posts a daily brief to Slack via an incoming webhook. Only roles
first seen today (is_new) are listed in full; on a day with no new roles it
still posts a one-line heartbeat, so silence in the channel means the job broke.
Run right after the collector.

Setup: create a Slack incoming webhook for the target channel, then
  export SLACK_JOBS_WEBHOOK="https://hooks.slack.com/services/XXX/YYY/ZZZ"

Usage:
  python3 slack_brief.py --output-dir /path            # posts today's brief
  python3 slack_brief.py --output-dir /path --print     # preview only, no post
  python3 slack_brief.py --file path/to/file.json       # post a specific file

Python 3.9 compatible.
"""
import argparse, json, os, sys
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    print("Missing dependency. Run: pip3 install requests", file=sys.stderr)
    sys.exit(1)

A_DETAIL_CAP = 25      # max A-tier leads shown in full
B_LINE_CAP = 12        # max B-tier one-liners shown
SECTION_CHAR_CAP = 2800  # Slack section text limit is 3000

def lead_line(s: dict) -> str:
    j = s["job"]
    score = s.get("lead_score", 0)
    title = j.get("title", "?")
    company = j.get("company", "?")
    persona = s.get("likely_buyer_persona") or j.get("role_category", "")
    kws = ", ".join(j.get("matched_keywords", [])[:5])
    link = j.get("apply_url") or j.get("url")
    cluster = " · :fire: 2+ roles" if j.get("cluster") else ""
    line = "*%d/20* — %s @ *%s*  _%s_%s" % (score, title, company, persona, cluster)
    extra = []
    if kws:
        extra.append(kws)
    if link:
        extra.append("<%s|view>" % link)
    if extra:
        line += "\n   " + "  ·  ".join(extra)
    return line

def chunk_sections(lines):
    """Group lines into section blocks under the char cap."""
    blocks, buf = [], ""
    for ln in lines:
        if len(buf) + len(ln) + 2 > SECTION_CHAR_CAP and buf:
            blocks.append({"type": "section", "mrkdwn": True, "text": buf})
            buf = ""
        buf += (("\n\n" if buf else "") + ln)
    if buf:
        blocks.append({"type": "section", "mrkdwn": True, "text": buf})
    # convert to proper block format
    return [{"type": "section", "text": {"type": "mrkdwn", "text": b["text"]}} for b in blocks]

def build(payload: dict, date_str: str):
    all_sigs = payload.get("signals", [])
    # Collector emits the full retained population; only brief what's new today.
    sigs = [s for s in all_sigs if s.get("is_new")]
    open_n = payload.get("open_roles", sum(1 for s in all_sigs if s.get("is_open", True)))
    a = [s for s in sigs if s.get("priority_band") == "A"]
    b = [s for s in sigs if s.get("priority_band") == "B"]
    c = [s for s in sigs if s.get("priority_band") == "C"]
    ob = payload.get("band_counts", {})

    header = "Job-Signal Brief - %s" % date_str
    tracked = "%d open roles tracked (A %d / B %d / C %d)" % (
        open_n, ob.get("A", 0), ob.get("B", 0), ob.get("C", 0))

    if not sigs:
        text = "*%s*: no new roles today · %s" % (header, tracked)
        return {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}}],
                "text": "%s: 0 new" % header}

    summary = "*%d new roles today*  ·  :red_circle: A %d   :large_blue_circle: B %d   :white_circle: C %d\n%s\n_Scores are provisional (role + JD only). Company-fit is your call._" % (
        len(sigs), len(a), len(b), len(c), tracked)

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": header[:150]}},
        {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
        {"type": "divider"},
    ]

    if a:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*:red_circle: New A-tier - work today*"}})
        blocks += chunk_sections([lead_line(s) for s in a[:A_DETAIL_CAP]])
        if len(a) > A_DETAIL_CAP:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "+%d more A-tier in the file" % (len(a) - A_DETAIL_CAP)}]})

    if b:
        blocks.append({"type": "divider"})
        b_lines = ["• *%d* %s @ *%s* _%s_" % (s.get("lead_score", 0), s["job"].get("title", "?"),
                   s["job"].get("company", "?"), s.get("likely_buyer_persona") or "")
                   for s in b[:B_LINE_CAP]]
        tail = "" if len(b) <= B_LINE_CAP else "\n_+%d more B-tier_" % (len(b) - B_LINE_CAP)
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
                       "text": "*:large_blue_circle: New B-tier - qualified, review*\n" + "\n".join(b_lines) + tail}})

    if c:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": ":white_circle: %d C-tier held (low priority)" % len(c)}]})

    fallback = "%s: %d new (A %d / B %d / C %d)" % (header, len(sigs), len(a), len(b), len(c))
    return {"blocks": blocks, "text": fallback}

def render_text(payload, date_str):
    """Plain-text version for --print preview."""
    sigs = [s for s in payload.get("signals", []) if s.get("is_new")]
    a = [s for s in sigs if s.get("priority_band") == "A"]
    out = ["=== Job-Signal Brief - %s ===" % date_str,
           "%d new | A %d  B %d  C %d" % (len(sigs),
               len(a), sum(1 for s in sigs if s.get("priority_band") == "B"),
               sum(1 for s in sigs if s.get("priority_band") == "C")), ""]
    out.append("-- A-tier --")
    for s in a:
        j = s["job"]
        out.append("%d/20  %s @ %s  [%s]%s" % (s.get("lead_score", 0), j.get("title"),
                   j.get("company"), s.get("likely_buyer_persona") or "",
                   "  (2+ roles)" if j.get("cluster") else ""))
        if j.get("matched_keywords"):
            out.append("       kw: %s" % ", ".join(j["matched_keywords"][:6]))
    return "\n".join(out)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=".")
    ap.add_argument("--file", default=None)
    ap.add_argument("--print", dest="preview", action="store_true", help="preview only, do not post")
    args = ap.parse_args()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = args.file
    if not path:
        latest = os.path.join(args.output_dir, "web3career_jobs_latest.json")
        dated = os.path.join(args.output_dir, "web3career_jobs_%s.json" % today)
        path = latest if os.path.exists(latest) else dated
    if not os.path.exists(path):
        print("No brief file found at %s" % path, file=sys.stderr)
        return 1
    with open(path) as f:
        payload = json.load(f)

    if args.preview:
        print(render_text(payload, today))
        return 0

    webhook = os.environ.get("SLACK_JOBS_WEBHOOK")
    if not webhook:
        print("ERROR: set SLACK_JOBS_WEBHOOK env var to your Slack incoming webhook URL", file=sys.stderr)
        return 1
    msg = build(payload, today)
    resp = requests.post(webhook, json=msg, timeout=20)
    if resp.status_code == 200:
        print("Posted brief to Slack (%d new today, %d open)." % (
            payload.get("new_today", 0), payload.get("open_roles", 0)))
        return 0
    print("Slack post failed: HTTP %s %s" % (resp.status_code, resp.text[:200]), file=sys.stderr)
    return 1

if __name__ == "__main__":
    sys.exit(main())
