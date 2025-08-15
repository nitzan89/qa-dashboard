import re
import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta

from db import get_conn, init_db, rebuild_fts
from utils import match_keywords
from config import DEFAULT_EXCLUDED_TAGS
import ingest as ingest_mod

# ───────────────────────────── Setup ───────────────────────────── #
st.set_page_config(page_title="QA Ticket Finder", layout="wide")
st.title("QA Ticket Finder")

# Avoid “database is locked” during ingest
if "ingesting" not in st.session_state:
    st.session_state.ingesting = False

# HTML stripper for keyword search & subject fallback
TAG_RE = re.compile(r"<[^>]+>")
def strip_html(x: str) -> str:
    if not x:
        return ""
    return TAG_RE.sub(" ", str(x))


# ───────────────────────── Top toolbar ───────────────────────── #
today = date.today()
default_start = today - timedelta(days=5)

c1, c2, c3, c4 = st.columns([2, 2, 2, 3])

with c1:
    preset = st.radio(
        "Preset",
        options=["Last 24h", "Last 3 days", "Last 5 days", "Custom range"],
        index=2,
        horizontal=True,
    )

with c2:
    if preset == "Custom range":
        dr = st.date_input("Date range", value=(default_start, today))
        if isinstance(dr, tuple):
            start_date, end_date = dr
        else:
            start_date, end_date = default_start, today
    else:
        start_map = {
            "Last 24h": today - timedelta(days=1),
            "Last 3 days": today - timedelta(days=3),
            "Last 5 days": today - timedelta(days=5),
        }
        start_date, end_date = start_map[preset], today
    if start_date > end_date:
        start_date, end_date = end_date, start_date

with c3:
    include_kw = st.text_input("Include keywords", "")
    exclude_kw = st.text_input("Exclude keywords", "")

with c4:
    load_clicked = st.button("🔄 Load tickets", disabled=st.session_state.ingesting)
    if load_clicked:
        st.session_state.ingesting = True
        try:
            # Keep ingest simple & reliable: always load last 5 days.
            with st.spinner("Fetching last 5 day(s) from Zendesk…"):
                msg = ingest_mod.ingest(days=5)
            st.toast(msg, icon="✅")
        finally:
            st.session_state.ingesting = False

with st.expander("Advanced filters (optional)", expanded=False):
    include_tags = st.text_input("Include tags (comma-separated)", "")
    exclude_tags_input = st.text_input(
        "Exclude tags (comma-separated)",
        ",".join(DEFAULT_EXCLUDED_TAGS),
        help="Starts with your default blacklist; you can edit."
    )
    apply_default_exclusions = st.checkbox(
        "Apply default excluded tags", value=True,
        help="Uses your standard blacklist (connection, closed_by_merge, swat, etc.)"
    )
    kw_mode = st.selectbox("Keyword match mode", ["any", "all", "phrase", "regex"], index=0)
    if st.button("Rebuild Text Index (FTS)"):
        rebuild_fts()
        st.success("FTS rebuilt.")

# If a write is running, stop early to avoid locks
if st.session_state.ingesting:
    st.info("Fetching data… please wait a moment.")
    st.stop()


# ───────────────────── Load & base filtering from DB ───────────────────── #
init_db()

with get_conn() as conn:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status, subject, created_at, updated_at, solved_at, csat, csat_offered,
               requester_email, assignee_email, assignee_name, bpo, payer_tier,
               language, topic, sub_topic, version, tags
        FROM tickets
        ORDER BY updated_at DESC
        """
    )
    rows = cur.fetchall()

cols = [
    "id","status","subject","created_at","updated_at","solved_at","csat","csat_offered",
    "requester_email","assignee_email","assignee_name","bpo","payer_tier",
    "language","topic","sub_topic","version","tags",
]
df = pd.DataFrame(rows, columns=cols)

# Normalize types
df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
df["csat"] = pd.to_numeric(df["csat"], errors="coerce")
df["updated_at_dt"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True).dt.tz_convert(None)

# Date window filter (calendar day window for presets)
# If you prefer rolling 24h, we can switch this to now - 24h.
start_dt = datetime.combine(start_date, datetime.min.time())
end_dt = datetime.combine(end_date, datetime.max.time())
df = df[(df["updated_at_dt"] >= start_dt) & (df["updated_at_dt"] <= end_dt)]

if df.empty:
    st.warning(
        "No tickets in the selected window. Click **Load tickets** to fetch, "
        "or widen the date range / clear filters."
    )
    st.stop()


# ───────────── Build comments map (for keywords & preview) ───────────── #
ticket_ids = df["id"].dropna().astype(int).tolist()
comments_map = {}
with get_conn() as conn:
    cur = conn.cursor()
    if ticket_ids:
        q_marks = ",".join("?" for _ in ticket_ids)
        cur.execute(
            f"""
            SELECT ticket_id, idx, created_at, public, author_email, author_name, body
            FROM comments
            WHERE ticket_id IN ({q_marks})
            ORDER BY ticket_id, idx ASC
            """,
            ticket_ids,
        )
        for row in cur.fetchall():
            tid = row[0]
            comments_map.setdefault(tid, []).append(
                {
                    "idx": row[1],
                    "created_at": row[2],
                    "public": bool(row[3]),
                    "author_email": row[4] or "",
                    "author_name": row[5] or "",
                    "body": row[6] or "",
                }
            )

# Subject fallback: use first public comment if subject missing/“None”
def subject_for_ticket(tid: int) -> str:
    subj = (df.loc[df["id"] == tid, "subject"].values[0] or "").strip()
    if subj and subj.lower() != "none":
        return subj
    for c in comments_map.get(int(tid), []):
        if c["public"]:
            snippet = strip_html(c.get("body") or "").strip()
            if snippet:
                return (snippet[:110] + "…") if len(snippet) > 110 else snippet
    return "[no subject]"

df["subject_clean"] = df["id"].apply(lambda x: subject_for_ticket(int(x)))


# ───────────── Strong filters (tags + HTML-aware keywords) ───────────── #
def split_tags(s: str):
    if not s:
        return []
    raw = [p.strip() for p in s.split(",")]
    out = []
    for p in raw:
        if " " in p:
            out.extend([x for x in p.split() if x])
        elif p:
            out.append(p)
    return [x.lower() for x in out if x]

inc_tags = set(split_tags(include_tags))
user_exc_tags = set(split_tags(exclude_tags_input))
default_exc = set(t.lower() for t in DEFAULT_EXCLUDED_TAGS)

def tags_ok(tag_string: str) -> bool:
    tags = [t.strip().lower() for t in (tag_string or "").split(",") if t.strip()]
    tagset = set(tags)

    if apply_default_exclusions and (tagset & default_exc):
        return False
    if user_exc_tags and (tagset & user_exc_tags):
        return False
    if inc_tags and not (tagset & inc_tags):
        return False
    return True

df = df[df["tags"].apply(tags_ok)]

def parse_kw_list(s: str):
    if not s.strip():
        return []
    parts = [p.strip() for chunk in s.split(",") for p in chunk.split()]
    return [p for p in parts if p]

inc_kw_list = parse_kw_list(include_kw)
exc_kw_list = parse_kw_list(exclude_kw)

def text_for_search(tid):
    sub = df.loc[df["id"] == tid, "subject_clean"].values[0] or ""
    bodies = [c["body"] for c in comments_map.get(int(tid), []) if c["public"]]
    text = " \n ".join([sub] + bodies)
    return strip_html(text)

def passes_keyword_filters(tid):
    text = text_for_search(tid)
    if inc_kw_list and not match_keywords(text, inc_kw_list, kw_mode):
        return False
    if exc_kw_list and match_keywords(text, exc_kw_list, "any"):
        return False
    return True

if inc_kw_list or exc_kw_list:
    df = df[df["id"].apply(passes_keyword_filters)]

if df.empty:
    st.info("No tickets match your filters. Clear filters or change the range.")
    st.stop()


# ───────────── Optional: latest macros used (from audits) ───────────── #
macros_map = {}
with get_conn() as conn:
    cur = conn.cursor()
    if ticket_ids:
        q_marks = ",".join("?" for _ in ticket_ids)
        cur.execute(
            f"""
            SELECT a.ticket_id, a.macro_titles
            FROM audits a
            JOIN (
                SELECT ticket_id, MAX(created_at) AS max_created
                FROM audits
                WHERE ticket_id IN ({q_marks})
                GROUP BY ticket_id
            ) m
            ON a.ticket_id = m.ticket_id AND a.created_at = m.max_created
            """,
            ticket_ids,
        )
        for tid, macros in cur.fetchall():
            macros_map[int(tid)] = macros or ""
df["macros_used"] = df["id"].apply(lambda x: macros_map.get(int(x), ""))


# ───────────── Pretty table (no tags/dates; short link) ───────────── #
df["id_str"] = df["id"].astype(str)
df["ticket_url"] = df["id"].apply(
    lambda x: f"https://candivore.zendesk.com/agent/tickets/{int(x)}" if pd.notna(x) else ""
)

display_cols = [
    "id_str", "subject_clean", "assignee_name", "bpo", "csat", "payer_tier",
    "topic", "sub_topic", "macros_used", "ticket_url"
]
df_view = df[display_cols].rename(columns={
    "id_str": "ZD #",
    "subject_clean": "Subject",
    "ticket_url": "Open",
    "macros_used": "Macros",
})

# Render tidy dashes instead of None/NaN/empty strings
df_view = df_view.fillna("—").replace({"": "—", "None": "—", None: "—"})

st.subheader("Tickets")
st.caption("Filtered list. Click **Open** to view in Zendesk. Pick a subject to preview the thread below.")

st.dataframe(
    df_view,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Open": st.column_config.LinkColumn("Open", help="Open in Zendesk"),
        "Subject": st.column_config.TextColumn("Subject"),
        "assignee_name": st.column_config.TextColumn("Assignee"),
        "bpo": st.column_config.TextColumn("BPO"),
        "csat": st.column_config.NumberColumn("CSAT"),
        "payer_tier": st.column_config.TextColumn("Payer tier"),
        "topic": st.column_config.TextColumn("Topic"),
        "sub_topic": st.column_config.TextColumn("Sub-topic"),
        "Macros": st.column_config.TextColumn("Macros"),
    },
)

# Subject picker → preview
subject_options = (
    df.assign(label=lambda d: d["subject_clean"].fillna("[no subject]") + "  ·  ZD #" + d["id_str"])
      .loc[:, ["id", "label"]]
      .dropna()
      .values.tolist()
)

st.subheader("Preview")
selected_label = st.selectbox(
    "Pick a ticket by subject to preview",
    options=[lbl for _, lbl in subject_options],
    index=0 if subject_options else None,
)

selected_id = None
if subject_options:
    for tid, lbl in subject_options:
        if lbl == selected_label:
            selected_id = int(tid)
            break

if selected_id is None:
    st.info("No ticket selected.")
    st.stop()

trow = df[df["id"] == selected_id].iloc[0]
zd_link = f"https://candivore.zendesk.com/agent/tickets/{selected_id}"
st.markdown(f"### {trow['subject_clean']}  ·  ZD #{selected_id}  ·  [Open]({zd_link})")
st.caption(
    f"Assignee: {trow['assignee_name']}  |  BPO: {trow['bpo']}  |  CSAT: {trow['csat']}  |  "
    f"Payer tier: {trow['payer_tier']}"
)

thread = comments_map.get(selected_id, [])
if not thread:
    st.info("No public comments on this ticket.")
else:
    for c in thread:
        is_requester = c["author_email"] == trow["requester_email"]
        role = "user" if is_requester else "assistant"
        name = c["author_name"] or ("Requester" if is_requester else "Agent")
        ts = c["created_at"] or ""
        with st.chat_message(role):
            st.markdown(f"**{name}** · {ts}")
            st.markdown(c.get("body") or "", unsafe_allow_html=True)

# Export
if st.button("Export CSV"):
    out = df_view.to_csv(index=False)
    st.download_button("Download filtered.csv", data=out, file_name="filtered.csv", mime="text/csv")
