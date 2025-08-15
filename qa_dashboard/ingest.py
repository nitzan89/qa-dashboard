# ingest.py — pull recent solved tickets from Zendesk into SQLite

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Callable

import requests

# ───────── Credentials (env first, then Streamlit Secrets) ───────── #
SUBDOMAIN = os.getenv("ZD_SUBDOMAIN")
EMAIL = os.getenv("ZD_EMAIL")
TOKEN = os.getenv("ZD_API_TOKEN")
try:
    # Streamlit Cloud provides secrets via st.secrets
    import streamlit as st  # type: ignore
    SUBDOMAIN = SUBDOMAIN or st.secrets.get("ZD_SUBDOMAIN")
    EMAIL = EMAIL or st.secrets.get("ZD_EMAIL")
    TOKEN = TOKEN or st.secrets.get("ZD_API_TOKEN")
except Exception:
    pass

# ───────── Pull optional BOT_EMAILS & CUSTOM_FIELDS from config ───────── #
try:
    from config import BOT_EMAILS as CONFIG_BOT_EMAILS  # type: ignore
except Exception:
    CONFIG_BOT_EMAILS = None

try:
    from config import CUSTOM_FIELDS as CONFIG_CUSTOM_FIELDS  # type: ignore
except Exception:
    CONFIG_CUSTOM_FIELDS = None

# == From your Apps Script (merged with config if present) ==
BOT_EMAILS_DEFAULT = {"ilya@candivore.io"}
CUSTOM_FIELDS_DEFAULT = {
    "topic":       360019266879,
    "sub_topic":   5066696830106,
    "version":     1260819767490,
    "language":    5428339880602,
    "payer_tier":  6645722066458,  # GAS 'segment_payer'
}

# Final effective settings
BOT_EMAILS = set(CONFIG_BOT_EMAILS) if CONFIG_BOT_EMAILS else set(BOT_EMAILS_DEFAULT)
CUSTOM_FIELDS: Dict[str, int] = dict(CUSTOM_FIELDS_DEFAULT)
if isinstance(CONFIG_CUSTOM_FIELDS, dict):
    # allow overrides from config.py
    CUSTOM_FIELDS.update({k: v for k, v in CONFIG_CUSTOM_FIELDS.items() if v})

from db import (
    get_conn,
    init_db,
    upsert_ticket,
    upsert_comment,
    upsert_audit,
    rebuild_fts,
)

# ───────────────────────── HTTP helpers ───────────────────────── #

def _base() -> str:
    if not SUBDOMAIN or not EMAIL or not TOKEN:
        raise SystemExit("Missing Zendesk credentials (set ZD_SUBDOMAIN, ZD_EMAIL, ZD_API_TOKEN).")
    return f"https://{SUBDOMAIN}.zendesk.com/api/v2"


def get_json(url: str, params: Optional[Dict] = None, *, who: str = "") -> Dict:
    """
    GET with retry/backoff.
    - Handles 429 with Retry-After.
    - Bounded retries for other 4xx/5xx; raises fast for 400/401/403/404/422.
    - 30s request timeout so we don't hang the app.
    """
    backoff = 2
    tries = 0
    while tries < 5:
        tries += 1
        r = requests.get(url, params=params, auth=(EMAIL, TOKEN), timeout=30)
        if r.status_code == 429:
            sleep_for = int(r.headers.get("Retry-After", backoff))
            time.sleep(min(sleep_for, 20))
            backoff = min(backoff * 2, 40)
            continue
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # For obvious bad inputs, bubble up immediately
            if r.status_code in (400, 401, 403, 404, 422):
                raise
            # brief backoff for transient 5xx
            time.sleep(min(backoff, 10))
            backoff = min(backoff * 2, 40)
            continue
        return r.json() or {}
    raise RuntimeError(f"GET failed after retries: {who or url}")

# ───────────────────────── Caches ───────────────────────── #

_user_cache: Dict[int, Dict] = {}
_group_cache: Dict[int, Dict] = {}

def get_user(user_id: Optional[int]) -> Dict:
    """
    Safe user fetch:
      - Return a stub for system/unknown authors (user_id <= 0 or None)
      - Swallow 400/404 and return a stub
      - Cache to reduce API calls
    """
    if not user_id or (isinstance(user_id, int) and user_id <= 0):
        return {"id": user_id, "email": "", "name": "Unknown"}

    if user_id in _user_cache:
        return _user_cache[user_id]

    try:
        data = get_json(f"{_base()}/users/{user_id}.json", who=f"user {user_id}")
        user = data.get("user", {}) or {"id": user_id, "email": "", "name": "Unknown"}
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (400, 404):
            user = {"id": user_id, "email": "", "name": "Unknown"}
        else:
            raise
    _user_cache[user_id] = user
    return user


def get_group(group_id: int) -> Dict:
    if group_id in _group_cache:
        return _group_cache[group_id]
    data = get_json(f"{_base()}/groups/{group_id}.json", who=f"group {group_id}")
    grp = data.get("group", {}) or {}
    _group_cache[group_id] = grp
    return grp


def get_user_group_names(user_id: int) -> List[str]:
    """
    GET /group_memberships.json?user_id=...
    Return list of group names the user belongs to.
    """
    names: List[str] = []
    data = get_json(f"{_base()}/group_memberships.json", params={"user_id": user_id}, who=f"group_memberships user {user_id}")
    for gm in data.get("group_memberships", []):
        gid = gm.get("group_id")
        if not gid:
            continue
        try:
            g = get_group(int(gid))
            name = g.get("name") or ""
            if name:
                names.append(name)
        except Exception:
            pass
    return names


def infer_bpo_from_groups(group_names: List[str]) -> Optional[str]:
    """
    Map assignee's groups to BPO label.
    Adjust rules to match your Zendesk group naming.
    """
    joined = " ".join(g.lower() for g in group_names)
    if "icx" in joined:
        return "ICX"
    if "tg" in joined or "telus" in joined:
        return "TG"
    if "cnx" in joined or "concentrix" in joined:
        return "CNX"
    return None

# ───────────────────────── Ticket endpoints ───────────────────────── #

def get_ticket(ticket_id: int) -> Dict:
    return get_json(f"{_base()}/tickets/{ticket_id}.json", who=f"ticket {ticket_id}").get("ticket", {}) or {}

def get_comments(ticket_id: int) -> List[Dict]:
    return get_json(f"{_base()}/tickets/{ticket_id}/comments.json", who=f"comments {ticket_id}").get("comments", []) or []

def get_audits(ticket_id: int) -> List[Dict]:
    return get_json(f"{_base()}/tickets/{ticket_id}/audits.json", who=f"audits {ticket_id}").get("audits", []) or []

def extract_custom_field(ticket: Dict, name: str):
    """
    Pull a custom field by id from our effective CUSTOM_FIELDS mapping.
    """
    fid = CUSTOM_FIELDS.get(name)
    if not fid:
        return None
    for f in ticket.get("custom_fields", []):
        if f.get("id") == fid:
            return f.get("value")
    return None


def search_ticket_ids(start_dt: datetime, end_dt: datetime):
    """
    Use Search API to find solved tickets updated in [start_dt, end_dt) window.
    Zendesk Search requires timestamps without microseconds and with 'Z' (UTC).
    """
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    q = f'status:solved updated>="{start_str}" updated<"{end_str}"'

    url = f"{_base()}/search.json"
    params = {"query": q, "page": 1}

    while True:
        data = get_json(url, params=params, who=f"search {start_str}→{end_str}")
        for r in data.get("results", []):
            if r.get("result_type") == "ticket" and "id" in r:
                yield r["id"]

        next_page = data.get("next_page")
        if not next_page:
            break
        url, params = next_page, None

# ───────────────────────── Ingest main ───────────────────────── #

def ingest(days: int = 5, progress_cb: Optional[Callable[[int, int, str], None]] = None) -> str:
    """
    Pull tickets updated in the last {days}, store in SQLite.
      - Only status: solved (includes reopened→resolved because we filter by updated_at).
      - Skip unassigned tickets.
      - Skip tickets assigned to bot emails.
      - Require at least one human public reply (author != requester and not a bot).
      - Derive BPO from assignee's group memberships.
      - Populate custom fields using your field IDs.
      - Report progress via progress_cb(step, total_steps, message) if provided.
    """
    init_db()

    # Extra safety: ensure writer waits if readers are active
    with get_conn() as _conn:
        _conn.execute("PRAGMA busy_timeout=5000;")

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    # Iterate in 6-hour slices to avoid big queries/rate limits
    slice_size = timedelta(hours=6)
    total_slices = max(1, int(((now - start).total_seconds() // 3600) / 6) + 1)
    slice_idx = 0
    if progress_cb:
        progress_cb(slice_idx, total_slices, "Starting…")

    with get_conn() as conn:
        seen = set()

        cursor = start
        while cursor < now:
            window_end = min(cursor + slice_size, now)
            slice_idx += 1
            if progress_cb:
                progress_cb(slice_idx, total_slices, f"Window {cursor.strftime('%m-%d %H:%M')} → {window_end.strftime('%m-%d %H:%M')}")

            for tid in search_ticket_ids(cursor, window_end):
                if tid in seen:
                    continue
                seen.add(tid)

                # Ticket core
                t = get_ticket(int(tid))
                assignee_id = t.get("assignee_id")
                if not assignee_id:
                    # Unassigned → skip
                    continue

                # Users
                requester = get_user(t.get("requester_id"))
                requester_email = (requester.get("email") or "").lower()

                assignee = get_user(assignee_id)
                assignee_email = (assignee.get("email") or "").lower()
                assignee_name = assignee.get("name") or "Unknown"

                # Skip bot-assigned
                if assignee_email in BOT_EMAILS:
                    continue

                # Comments (to verify human reply and to store the thread)
                comments = get_comments(int(tid))
                has_human_reply = False
                public_comments = []

                for idx, c in enumerate(comments):
                    author_id = c.get("author_id")
                    au = get_user(author_id)  # safe fetch (handles -1/None/404/400)
                    a_email = (au.get("email") or "").lower()
                    a_name = au.get("name") or "Unknown"
                    body = c.get("html_body") or c.get("body") or ""
                    public = bool(c.get("public", False))

                    public_comments.append(
                        (
                            int(tid),            # ticket_id
                            idx,                 # comment index
                            c.get("created_at"),
                            1 if public else 0,
                            author_id,
                            a_email,
                            a_name,
                            body,
                        )
                    )

                    # Human agent public reply (not requester, not bot, has email)
                    if public and a_email and (a_email not in BOT_EMAILS) and (a_email != requester_email):
                        has_human_reply = True

                # If no human reply, skip (bot-only)
                if not has_human_reply:
                    continue

                # Audits → capture macro titles if applied (latest)
                macro_titles: List[str] = []
                for a in get_audits(int(tid)):
                    for e in a.get("events", []):
                        if e.get("type") == "ApplyMacro":
                            title = e.get("value") or e.get("macro_title")
                            if title:
                                macro_titles.append(title)

                # Derive BPO from groups
                group_names = get_user_group_names(int(assignee_id))
                bpo = infer_bpo_from_groups(group_names)

                # Flatten tags
                tags = ",".join(t.get("tags", []))

                # Custom fields via your IDs
                payer_tier = extract_custom_field(t, "payer_tier")
                language = extract_custom_field(t, "language")
                topic = extract_custom_field(t, "topic")
                sub_topic = extract_custom_field(t, "sub_topic")
                version = extract_custom_field(t, "version")

                # Build ticket row in the exact schema order expected by db.py
                ticket_row = (
                    t.get("id"),
                    t.get("status"),
                    t.get("subject"),
                    t.get("created_at"),
                    t.get("updated_at"),
                    t.get("updated_at"),  # treat updated_at as solved_at proxy for our window
                    (t.get("satisfaction_rating") or {}).get("score"),
                    1 if t.get("satisfaction_rating") else 0,
                    t.get("requester_id"),
                    requester_email,
                    assignee_id,
                    assignee_email,
                    assignee_name,
                    bpo,          # derived from groups
                    payer_tier,   # from your field IDs
                    language,
                    topic,
                    sub_topic,
                    version,
                    tags,
                )
                upsert_ticket(conn, ticket_row)

                # Store public comments
                for ctuple in public_comments:
                    upsert_comment(conn, ctuple)

                # Store audits (one row per audit timestamp)
                if macro_titles:
                    upsert_audit(conn, (int(tid), t.get("updated_at"), "|".join(macro_titles)))

                # Heartbeat every ~50 tickets
                if progress_cb and (len(seen) % 50 == 0):
                    progress_cb(slice_idx, total_slices, f"Processed ~{len(seen)} tickets so far…")

            cursor = window_end

    rebuild_fts()
    if progress_cb:
        progress_cb(total_slices, total_slices, "Done")
    return f"Ingest complete: last {days} days"
