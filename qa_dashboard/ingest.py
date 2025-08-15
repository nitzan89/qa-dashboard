import os
import time
from datetime import datetime, timedelta, timezone
import requests

# ─────────── Read credentials (env first, then Streamlit Secrets) ─────────── #
SUBDOMAIN = os.getenv("ZD_SUBDOMAIN")
EMAIL = os.getenv("ZD_EMAIL")
TOKEN = os.getenv("ZD_API_TOKEN")
try:
    import streamlit as st  # optional; only used to read secrets if present
    SUBDOMAIN = SUBDOMAIN or st.secrets.get("ZD_SUBDOMAIN")
    EMAIL = EMAIL or st.secrets.get("ZD_EMAIL")
    TOKEN = TOKEN or st.secrets.get("ZD_API_TOKEN")
except Exception:
    pass

from config import BOT_EMAILS, CUSTOM_FIELDS
from db import (
    get_conn,
    init_db,
    upsert_ticket,
    upsert_comment,
    upsert_audit,
    rebuild_fts,
)

# ───────────────────────────────── Helpers ───────────────────────────────── #

def _base():
    if not SUBDOMAIN or not EMAIL or not TOKEN:
        raise SystemExit("Missing Zendesk credentials (set in Streamlit Secrets or env).")
    return f"https://{SUBDOMAIN}.zendesk.com/api/v2"


def get_json(url, params=None):
    """
    GET with retry/backoff. Handles 429 rate limits.
    """
    backoff = 2
    for _ in range(7):
        r = requests.get(url, params=params, auth=(EMAIL, TOKEN), timeout=40)
        if r.status_code == 429:
            sleep_for = int(r.headers.get("Retry-After", backoff))
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 60)
            continue
        # Raise for any other 4xx/5xx
        r.raise_for_status()
        return r.json()
    raise RuntimeError("Too many retries (Zendesk API rate limit).")


# Simple caches to cut API calls
_user_cache = {}

def get_user(user_id: int):
    """
    Safe user fetch:
    - Return a stub for system/unknown authors (user_id <= 0 or None)
    - Swallow 400/404 and return a stub
    - Cache to reduce API calls
    """
    # Stub for system/unknown/deleted
    if not user_id or (isinstance(user_id, int) and user_id <= 0):
        return {"id": user_id, "email": "", "name": "Unknown"}

    if user_id in _user_cache:
        return _user_cache[user_id]

    try:
        data = get_json(f"{_base()}/users/{user_id}.json")
        user = data.get("user", {}) or {"id": user_id, "email": "", "name": "Unknown"}
    except requests.HTTPError as e:
        # Gracefully handle not-found / bad-id edge cases
        status = getattr(e.response, "status_code", None)
        if status in (400, 404):
            user = {"id": user_id, "email": "", "name": "Unknown"}
        else:
            raise
    _user_cache[user_id] = user
    return user



def get_group(group_id: int):
    """
    GET /groups/{id}.json
    """
    if group_id in _group_cache:
        return _group_cache[group_id]
    data = get_json(f"{_base()}/groups/{group_id}.json")
    grp = data.get("group", {})
    _group_cache[group_id] = grp
    return grp


def get_user_group_names(user_id: int):
    """
    GET /group_memberships.json?user_id=...
    Return list of group names the user belongs to.
    """
    names = []
    data = get_json(f"{_base()}/group_memberships.json", params={"user_id": user_id})
    for gm in data.get("group_memberships", []):
        gid = gm.get("group_id")
        if not gid:
            continue
        try:
            g = get_group(gid)
            name = g.get("name") or ""
            if name:
                names.append(name)
        except Exception:
            # ignore group fetch errors per item; keep going
            pass
    return names


def infer_bpo_from_groups(group_names):
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


def get_ticket(ticket_id: int):
    """
    GET /tickets/{id}.json
    """
    return get_json(f"{_base()}/tickets/{ticket_id}.json").get("ticket", {})


def get_comments(ticket_id: int):
    """
    GET /tickets/{id}/comments.json
    """
    return get_json(f"{_base()}/tickets/{ticket_id}/comments.json").get("comments", [])


def get_audits(ticket_id: int):
    """
    GET /tickets/{id}/audits.json
    """
    return get_json(f"{_base()}/tickets/{ticket_id}/audits.json").get("audits", [])


def extract_custom_field(ticket: dict, name: str):
    """
    Pull a custom field by id from config.CUSTOM_FIELDS mapping.
    """
    fid = CUSTOM_FIELDS.get(name)
    if not fid:
        return None
    for f in ticket.get("custom_fields", []):
        if f.get("id") == fid:
            return f.get("value")
    return None


def search_ticket_ids(start_dt, end_dt):
    """
    Use Search API to find solved tickets updated in [start_dt, end_dt) window.
    IMPORTANT: Zendesk Search requires timestamps without microseconds and with 'Z' (UTC).
    """
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    q = f'status:solved updated>="{start_str}" updated<"{end_str}"'

    url = f"{_base()}/search.json"
    params = {"query": q, "page": 1}

    while True:
        data = get_json(url, params=params)
        for r in data.get("results", []):
            if r.get("result_type") == "ticket" and "id" in r:
                yield r["id"]

        next_page = data.get("next_page")
        if not next_page:
            break
        # next_page is a full URL
        url, params = next_page, None


# ───────────────────────────────── Ingest ───────────────────────────────── #

def ingest(days: int = 5) -> str:
    """
    Pull tickets updated in the last {days}, store in SQLite.
    Rules:
      - Only status: solved (includes reopened→resolved because we filter by updated_at).
      - Skip unassigned tickets.
      - Skip tickets assigned to bot emails (from config.BOT_EMAILS).
      - Require at least one human public reply (author != requester and not a bot).
      - Derive BPO from assignee's group memberships.
    """
    init_db()

        # NEW: writer-side timeout hint (extra safety; db.py already sets pragmas)
    with get_conn() as _conn:
        _conn.execute("PRAGMA busy_timeout=5000;")

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    # Iterate in 6-hour slices to avoid big queries/rate limits
    slice_size = timedelta(hours=6)
    cursor = start

    with get_conn() as conn:
        seen = set()

        while cursor < now:
            window_end = min(cursor + slice_size, now)

            for tid in search_ticket_ids(cursor, window_end):
                if tid in seen:
                    continue
                seen.add(tid)

                # Ticket core
                t = get_ticket(tid)
                assignee_id = t.get("assignee_id")
                if not assignee_id:
                    # Unassigned → skip
                    continue

                # Users
                requester = get_user(t.get("requester_id"))
                requester_email = (requester.get("email") or "").lower()

                assignee = get_user(assignee_id)
                assignee_email = (assignee.get("email") or "").lower()
                assignee_name = assignee.get("name") or ""

                # Skip bot-assigned
                if assignee_email in BOT_EMAILS:
                    continue

                # Comments (to verify human reply and to store the thread)
comments = get_comments(tid)
has_human_reply = False
public_comments = []

for idx, c in enumerate(comments):
    author_id = c.get("author_id")
    au = get_user(author_id)  # returns stub for -1/None/404/400
    a_email = (au.get("email") or "").lower()
    a_name = au.get("name") or "Unknown"
    body = c.get("html_body") or c.get("body") or ""
    public = bool(c.get("public", False))

    public_comments.append(
        (
            tid,              # ticket_id
            idx,              # idx
            c.get("created_at"),
            1 if public else 0,
            author_id,
            a_email,
            a_name,
            body,
        )
    )

    # Human agent public reply (not requester, not bot, and has an email)
    if public and a_email and (a_email not in BOT_EMAILS) and (a_email != requester_email):
        has_human_reply = True

# If no human reply, skip (bot-only)
if not has_human_reply:
    continue


                    # Human agent public reply (not the requester, not a bot)
                    if public and a_email not in BOT_EMAILS and a_email != requester_email:
                        has_human_reply = True

                if not has_human_reply:
                    # Bot-only or no agent reply → skip
                    continue

                # Audits → capture macro titles if applied
                macro_titles = []
                for a in get_audits(tid):
                    for e in a.get("events", []):
                        if e.get("type") == "ApplyMacro":
                            title = e.get("value") or e.get("macro_title")
                            if title:
                                macro_titles.append(title)

                # Derive BPO from groups
                group_names = get_user_group_names(assignee_id)
                bpo = infer_bpo_from_groups(group_names)

                # Flatten tags
                tags = ",".join(t.get("tags", []))

                # Build ticket row in the exact schema order
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
                    bpo,  # <— derived from groups
                    extract_custom_field(t, "payer_tier"),
                    extract_custom_field(t, "language"),
                    extract_custom_field(t, "topic"),
                    extract_custom_field(t, "sub_topic"),
                    extract_custom_field(t, "version"),
                    tags,
                )
                upsert_ticket(conn, ticket_row)

                # Store public comments
                for ctuple in public_comments:
                    upsert_comment(conn, ctuple)

                # Store audits (one row per audit timestamp)
                if macro_titles:
                    upsert_audit(conn, (tid, t.get("updated_at"), "|".join(macro_titles)))

            cursor = window_end

    rebuild_fts()
    return f"Ingest complete: last {days} days"
