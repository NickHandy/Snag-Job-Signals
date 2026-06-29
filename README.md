# Snag Job-Signal Pipeline (Render)

Autonomous daily job-posting signal pipeline. Runs on Render with no laptop
needed. Pulls Web3.career postings, scores them against the Snag trigger
framework, and posts a triaged brief to Slack.

## What runs
1. `web3career_jobs.py` — pulls + classifies + scores postings, writes today's JSON.
2. `slack_brief.py` — reads that JSON and posts the A/B/C brief to Slack.

The two run back-to-back in one scheduled job.

## State
The "seen" cache (which stops the same posting firing twice) lives in **Redis**
when `REDIS_URL` is set, and in a local file otherwise. On Render it uses Redis;
on your Mac it uses the file — same code, no changes.

## Environment variables
| Var | What | Where to get it |
|-----|------|-----------------|
| `WEB3CAREER_TOKEN` | Web3.career API token | web3.career/web3-jobs-api |
| `SLACK_JOBS_WEBHOOK` | Slack incoming webhook URL | api.slack.com/apps -> your app -> Incoming Webhooks |
| `REDIS_URL` | Render Key Value connection string | Render dashboard -> your Key Value instance |

## Render setup (dashboard)
1. **Key Value:** New + -> Key Value -> name it `snag-seen` -> Create. Copy its
   Internal Connection URL (starts `redis://`).
2. **Cron Job:** New + -> Cron Job -> connect this GitHub repo.
   - Build command: `pip install -r requirements.txt`
   - Start command: `python web3career_jobs.py --output-dir . && python slack_brief.py --output-dir .`
   - Schedule: `30 13 * * *`  (UTC = 9:30am ET in summer; see note below)
   - Add the three env vars above. Paste the Key Value URL into `REDIS_URL`.
3. Click **Trigger Run** to test immediately, then check the logs and your Slack channel.

## Schedule note
Render cron is **UTC**. `30 13 * * *` is 9:30am US Eastern during daylight time
(summer) and 8:30am during standard time (winter). For a daily brief the 1-hour
winter drift is harmless; adjust to `30 14 * * *` in winter if you want it exact.

## Run locally (your Mac, unchanged)
    export WEB3CAREER_TOKEN=...
    export SLACK_JOBS_WEBHOOK=...
    python3 web3career_jobs.py --output-dir .
    python3 slack_brief.py --output-dir .          # add --print to preview without posting
