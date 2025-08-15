import pandas as pd
import streamlit as st

from db import get_conn, init_db, rebuild_fts
from utils import match_keywords, highlight
import ingest as ingest_mod

st.set_page_config(page_title="QA-Worthy Ticket Dashboard", layout="wide")
st.title("QA Ticket Finder")

# ─────────────── Toolbar (no sidebar) ─────────────── #
col1, col2, col3, col4 = st.columns([1, 1, 1, 2])

with col1:
    # Start gate – nothing loads until you click Start
    if "started" not in st.session_state:
        st.session_state.started = False
    if not st.session_state.started:
        if st.button("▶️ Start"):
            st.session_state.started = True
    else:
        st.success("Started")

with col2:
    if st.button("🔄 Refresh data (last 5 days)"):
        with st.spinner("Pulling recent solved tickets from Zendesk..."):
            msg = ingest_mod.ingest(days=5)  # returns a status string
        st.toast(msg, icon="✅")

with col3:
    # Simple, opinionated time range (no slider)
    range_label = st.radio(
        "Time range",
        options=["Last 24h", "Last 3 days", "Last 5 days"],
        index=2,
        horizontal=True,
    )
    days_map = {"Last 24h": 1, "Last 3 days": 3, "Last 5 days": 5}
    days = days_map[range_label]

with col4:
    # Quick search inputs
    include_kw = st.text_input("Include keywords (one line, comma or space separated)", "")
    exclude_kw = st.text_input("Exclude keywords", "")

# Advanced filters in an expander
with st.expander("Advanced filters (optional)", expanded=False):
    include_tags = st.text_input("Include tags (comma-separated)", "")
    exclude_tags = st.text_input(
        "Exclude tags (comma-separated)",
        "connection,connection_issue,lag,crash,game_crash,network,timeout,opp_out_of_time",
    )
    kw_mode = st.selectbox("Keyword match mode", ["any", "all", "phrase", "regex"], index=0)
    if st.button("Rebuild Text Index (FTS)"):
        rebuild_fts()
        st.success("FTS rebuilt")

# If not started yet, stop right here.
if not st.session_state.started:
    st.info("Click **Start** to load tickets.")
    st.stop()

# ─────────────── Load from DB ─────────────── #
init_db()

with get_conn() as conn:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status, subject, created_at, updated_at, solved_at, csat, csat_offered,
               requester_email, assignee_email, assignee_name, bpo, payer_tier,
               language, topic, sub_topic, version, tags
        FROM tickets
        WHERE datetime(updated_at) >= datetime('now', ?)
        ORDER BY updated_at DESC
        """,
        (f"-{days} days",),
    )
    rows = cur.fetchall()

cols = [
    "id","status","subject","created_at","updated_at","solved_at","csat","csat_offered",
    "requester_email","assignee_email","assignee_name","bpo","payer_tier",
    "language","topic","sub_topic","version","tags",
]
df = pd.DataFrame(rows, columns=cols)

# Normalize types we care about
df["csat"] = pd.to_numeric(df["csat"], errors="coerce")  # NaN if not numeric
df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

if df.empty:
    st.warning("No tickets in this window yet. Try **Refresh data** or widen the range.")
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

# ─────────────── Filters (tags + keywords) ─────────────── #
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

def text_for_search(tid):
    sub = df.loc[df["id"] == tid, "subject"].values[0] or ""
    texts = [sub] + [c["body"] for c in comments_map.get(int(tid), []) if c["public"]]
    return "\n".join(texts)

def parse_kw_list(s: str):
    if not s.strip():
        return []
    # support comma or space separated
    parts = [p.strip() for chunk in s.split(",") for p in chunk.split()]
    return [p for p in parts if p]

inc_kw_list = parse_kw_list(include_kw)
exc_kw_list = parse_kw_list(exclude_kw)

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

# ─────────────── Linkable ID + tidy columns ─────────────── #
df["id_str"] = df["id"].astype(str)
df["ticket_url"] = df["id"].apply(
    lambda x: f"https://candivore.zendesk.com/agent/tickets/{int(x)}" if pd.notna(x) else ""
)

st.subheader("Tickets")
st.caption("Filtered list. Click a row’s link to open in Zendesk.")

df_view = df.copy()  # keep 'id_str' as-is
st.dataframe(
    df_view[
        [
            "id_str", "assignee_name", "bpo", "csat", "payer_tier",
            "topic", "sub_topic", "tags", "updated_at", "subject", "ticket_url",
        ]
    ],
    use_container_width=True,
    hide_index=True,
    column_config={"ticket_url": st.column_config.LinkColumn("Open in Zendesk")},
)

# ─────────────── Preview (chat style) ─────────────── #
st.subheader("Preview")
selectable_ids = df["id"].dropna().astype(int).tolist()
sel = st.multiselect("Select ticket IDs to preview", selectable_ids[:50])

for tid in sel:
    trow = df[df["id"] == tid].iloc[0]
    zd_link = f"https://candivore.zendesk.com/agent/tickets/{int(tid)}"

    st.markdown(f"### Ticket #{tid} · [Open in Zendesk]({zd_link})")
    st.caption(
        f"Assignee: {trow['assignee_name']}  |  BPO: {trow['bpo']}  |  CSAT: {trow['csat']}  |  "
        f"Payer tier: {trow['payer_tier']}  |  Tags: {trow['tags']}"
    )

    thread = comments_map.get(int(tid), [])
    if not thread:
        st.info("No public comments on this ticket.")
        continue

    for c in thread:
        is_requester = c["author_email"] == trow["requester_email"]
        role = "user" if is_requester else "assistant"
        name = c["author_name"] or ("Requester" if is_requester else "Agent")
        ts = c["created_at"] or ""

        with st.chat_message(role):
            st.markdown(f"**{name}** · {ts}")
            st.markdown(c.get("body") or "", unsafe_allow_html=True)

# ─────────────── Export ─────────────── #
if st.button("Export CSV"):
    out = df_view[
        ["id_str","assignee_name","bpo","csat","payer_tier",
         "topic","sub_topic","tags","updated_at","subject","ticket_url"]
    ].to_csv(index=False)
    st.download_button("Download filtered.csv", data=out, file_name="filtered.csv", mime="text/csv")
