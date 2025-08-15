import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta

from db import get_conn, init_db, rebuild_fts
from utils import match_keywords, highlight
import ingest as ingest_mod


st.set_page_config(page_title="QA Ticket Finder", layout="wide")
st.title("QA Ticket Finder")

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
            # fallback for single date selection
            start_date, end_date = default_start, today
    else:
        start_map = {
            "Last 24h": today - timedelta(days=1),
            "Last 3 days": today - timedelta(days=3),
            "Last 5 days": today - timedelta(days=5),
        }
        start_date, end_date = start_map[preset], today
    # ensure ordering
    if start_date > end_date:
        start_date, end_date = end_date, start_date

with c3:
    include_kw = st.text_input("Include keywords", "")
    exclude_kw = st.text_input("Exclude keywords", "")

with c4:
    if st.button("🔄 Load tickets"):
        # Pull from Zendesk for the window [start_date .. today]
        days = max(1, (today - start_date).days + 1)
        with st.spinner(f"Fetching last {days} day(s) from Zendesk…"):
            msg = ingest_mod.ingest(days=days)
        st.toast(msg, icon="✅")
        st.session_state.last_loaded_days = days
        st.session_state.window_start = str(start_date)
        st.session_state.window_end = str(end_date)

with st.expander("Advanced filters (optional)", expanded=False):
    include_tags = st.text_input("Include tags (comma-separated)", "")
    exclude_tags = st.text_input(
        "Exclude tags (comma-separated)",
        "connection,connection_issue,lag,crash,game_crash,network,timeout,opp_out_of_time",
    )
    kw_mode = st.selectbox("Keyword match mode", ["any", "all", "phrase", "regex"], index=0)
    if st.button("Rebuild Text Index (FTS)"):
        rebuild_fts()
        st.success("FTS rebuilt.")

# ───────────────────── Load & filter from DB ───────────────────── #
init_db()

# Load the raw tickets from DB; we’ll filter by the chosen window next.
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

# Types
df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
df["csat"] = pd.to_numeric(df["csat"], errors="coerce")
df["updated_at_dt"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True).dt.tz_convert(None)

# Window filter (use the chosen date range)
start_dt = datetime.combine(start_date, datetime.min.time())
end_dt = datetime.combine(end_date, datetime.max.time())

df = df[(df["updated_at_dt"] >= start_dt) & (df["updated_at_dt"] <= end_dt)]

# Early prompts for common confusion
if df.empty:
    st.warning(
        "No tickets in the selected window. Click **Load tickets** to fetch, "
        "or widen the date range / clear filters."
    )
    st.stop()

# Build comments map for preview + keyword search
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

# ───────────────────── Filters (tags + keywords) ───────────────────── #
inc_tags = [t.strip() for t in include_tags.split(",") if t.strip()]
exc_tags = [t.strip() for t in exclude_tags.split(",") if t.strip()]

def tags_ok(tag_string: str) -> bool:
    tags = [t for t in (tag_string or "").split(",") if t]
    if inc_tags and not any(t in tags for t in inc_tags):
        return False
    if exc_tags and any(t in tags for t in exc_tags):
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
    sub = df.loc[df["id"] == tid, "subject"].values[0] or ""
    texts = [sub] + [c["body"] for c in comments_map.get(int(tid), []) if c["public"]]
    return "\n".join(texts)

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

# ───────────────────── Pretty table (no tags/dates) ───────────────────── #
df["id_str"] = df["id"].astype(str)
df["ticket_url"] = df["id"].apply(
    lambda x: f"https://candivore.zendesk.com/agent/tickets/{int(x)}" if pd.notna(x) else ""
)

display_cols = [
    "id_str",          # ZD #
    "subject",
    "assignee_name",
    "bpo",
    "csat",
    "payer_tier",
    "topic",
    "sub_topic",
    "ticket_url",      # Open link
]
df_view = df[display_cols].rename(columns={"id_str": "ZD #", "ticket_url": "Open"})

st.subheader("Tickets")
st.caption("Filtered list. Click **Open** to view in Zendesk. Pick a subject to preview the thread below.")

st.dataframe(
    df_view,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Open": st.column_config.LinkColumn("Open", help="Open in Zendesk"),
        "subject": st.column_config.TextColumn("Subject"),
        "assignee_name": st.column_config.TextColumn("Assignee"),
        "bpo": st.column_config.TextColumn("BPO"),
        "csat": st.column_config.NumberColumn("CSAT"),
        "payer_tier": st.column_config.TextColumn("Payer tier"),
        "topic": st.column_config.TextColumn("Topic"),
        "sub_topic": st.column_config.TextColumn("Sub-topic"),
    },
)

# Subject picker (acts like “click subject to preview”)
subject_options = (
    df.assign(label=lambda d: d["subject"].fillna("").replace("", "[no subject]") + "  ·  ZD #" + d["id_str"])
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
    # reverse-lookup id by label
    for tid, lbl in subject_options:
        if lbl == selected_label:
            selected_id = int(tid)
            break

if selected_id is None:
    st.info("No ticket selected.")
    st.stop()

# Render preview like a conversation
trow = df[df["id"] == selected_id].iloc[0]
zd_link = f"https://candivore.zendesk.com/agent/tickets/{selected_id}"
st.markdown(f"### {trow['subject'] or '[no subject]'}  ·  ZD #{selected_id}  ·  [Open]({zd_link})")
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
