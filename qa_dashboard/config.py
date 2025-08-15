# config.py — central app settings

import os
from pathlib import Path

# ── Storage ──────────────────────────────────────────────────────────────
# SQLite DB lives alongside the code in ./data/qa.db
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str(BASE_DIR / "data" / "qa.db")

# ── Zendesk creds ────────────────────────────────────────────────────────
# Do NOT hardcode secrets. Set these in Streamlit Secrets or environment.
#   ZD_SUBDOMAIN = "candivore"
#   ZD_EMAIL     = "nitzan@candivore.io/token"
#   ZD_API_TOKEN = "<your real token>"

# ── Bots (tickets assigned to these should be skipped) ───────────────────
# Your Apps Script only had ilya, but your earlier note said both Ilya + Maor
# are automation. Keep both here; remove if not needed.
BOT_EMAILS = {
    "ilya@candivore.io",
    "maor@candivore.io",
}

# ── Default excluded tags (from your Apps Script) ────────────────────────
DEFAULT_EXCLUDED_TAGS = [
    "connection", "connection_issue", "closed_by_merge", "swat", "swat_mass_reply",
    "backlog_mode", "game_crash", "opp_out_of_time", "kb_003", "kb_005", "kb_006", "kb_010",
    "duplicate_stickers", "friend_invite_prize", "friend_fiesta_prizes",
    "repeated_complaints", "repeated_technical", "kb_002", "game_lag", "lags_issue",
]

# ── Custom field IDs (from your Apps Script) ─────────────────────────────
# Map your Zendesk ticket field IDs to names the app expects.
CUSTOM_FIELDS = {
    "topic":       360019266879,
    "sub_topic":   5066696830106,
    "version":     1260819767490,
    "language":    5428339880602,
    "payer_tier":  6645722066458,   # GAS 'segment_payer'
    # (Other fields from GAS you’re not using in the table right now)
    # "user_id":   360007137153,
    # "vlog":      360017297540,
    # "assignee_name": 9433810178970,
}
