import pandas as pd, streamlit as st
from config import DEFAULT_EXCLUDED_TAGS, SENSITIVE_KEYWORDS, DEFAULT_WEIGHTS
from db import get_conn, rebuild_fts, init_db
from utils import match_keywords, highlight
from scoring import score_ticket, empathy_markers_in_reply, personalization_overlap, is_complaint
import ingest as ingest_mod

st.set_page_config(page_title="QA-Worthy Dashboard", layout="wide")
st.title("QA-Worthy Ticket Dashboard")

with st.sidebar:
    st.header("Data")
    if st.button("Refresh data (pull last 5 days)"):
        with st.spinner("Pulling from Zendesk (may take a minute)..."):
            msg = ingest_mod.ingest(days=5)
        st.success(msg)
        st.info("Reload the page after refresh if the table still looks stale.")

st.sidebar.header("Filters")
days = st.sidebar.slider("Solved in last N days", 1, 5, 5)
include_tags = st.sidebar.text_input("Include tags (comma-separated)", "")
exclude_tags = st.sidebar.text_input("Exclude tags (comma-separated)", ",".join(DEFAULT_EXCLUDED_TAGS))

st.sidebar.subheader("Keyword filters")
kw_mode = st.sidebar.selectbox("Include mode", ["any","all","phrase","regex"], index=0)
include_kw = st.sidebar.text_area("Include keywords (one per line)", "")
exclude_kw = st.sidebar.text_area("Exclude keywords (one per line)", "")

st.sidebar.subheader("Sensitive keyword pack")
sensitive_pack = st.sidebar.text_input("Sensitive keywords (comma-separated)", ",".join(SENSITIVE_KEYWORDS))

st.sidebar.subheader("Weights")
weights = {}
for k,v in DEFAULT_WEIGHTS.items():
    weights[k] = st.sidebar.number_input(k, value=float(v))

if st.sidebar.button("Rebuild Text Index (FTS)"):
    rebuild_fts(); st.sidebar.success("FTS rebuilt.")

init_db()

with get_conn() as conn:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, status, subject, created_at, updated_at, solved_at, csat, csat_offered,
               requester_email, assignee_email, assignee_name, bpo, payer_tier, language, topic, sub_topic, version, tags
        FROM tickets
        WHERE datetime(updated_at) >= datetime('now', ?)
        ORDER BY updated_at DESC
    """, (f'-{days} days',))
    rows = cur.fetchall()

cols = ["id","status","subject","created_at","updated_at","solved_at","csat","csat_offered",
        "requester_email","assignee_email","assignee_name","bpo","payer_tier","language","topic","sub_topic","version","tags"]
df = pd.DataFrame(rows, columns=cols)

inc_tags = [t.strip() for t in include_tags.split(",") if t.strip()]
exc_tags = [t.strip() for t in exclude_tags.split(",") if t.strip()]

def tags_ok(tag_string: str) -> bool:
    tags = (tag_string or "").split(",")
    if inc_tags and not any(t in tags for t in inc_tags): return False
    if exc_tags and any(t in tags for t in exc_tags): return False
    return True

df = df[df["tags"].apply(tags_ok)]

ticket_ids = df["id"].tolist()
comments_map = {}
with get_conn() as conn:
    cur = conn.cursor()
    if ticket_ids:
        q = ",".join("?" for _ in ticket_ids)
        cur.execute(f"SELECT ticket_id, idx, created_at, public, author_email, author_name, body FROM comments WHERE ticket_id IN ({q}) ORDER BY ticket_id, idx ASC", ticket_ids)
        for row in cur.fetchall():
            tid = row[0]
            comments_map.setdefault(tid, []).append({"idx": row[1], "created_at": row[2], "public": bool(row[3]), "author_email": row[4] or "", "author_name": row[5] or "", "body": row[6] or ""})

inc_kw_list = [k.strip() for k in include_kw.splitlines() if k.strip()]
exc_kw_list = [k.strip() for k in exclude_kw.splitlines() if k.strip()]
sensitive_list = [k.strip() for k in sensitive_pack.split(",") if k.strip()]

def text_for_search(tid):
    sub = df.loc[df["id"] == tid, "subject"].values[0] or ""
    texts = [sub] + [c["body"] for c in comments_map.get(tid, []) if c["public"]]
    return "\n".join(texts)

def passes_keyword_filters(tid):
    text = text_for_search(tid)
    from utils import match_keywords
    if inc_kw_list and not match_keywords(text, inc_kw_list, kw_mode): return False
    if exc_kw_list and match_keywords(text, exc_kw_list, "any"): return False
    return True

if inc_kw_list or exc_kw_list:
    df = df[df["id"].apply(passes_keyword_filters)]

from scoring import score_ticket, empathy_markers_in_reply, personalization_overlap, is_complaint
scores = []
for _, t in df.iterrows():
    tid = int(t["id"]); comments = comments_map.get(tid, [])
    user_text = " ".join([c["body"] for c in comments if c["public"] and c["author_email"] == t["requester_email"]])
    agent_replies = [c for c in comments if c["public"] and c["author_email"] != t["requester_email"]]
    first_agent_reply = agent_replies[0]["body"] if agent_replies else ""
    cfg = dict(
        sensitive_hit = any(match_keywords(text_for_search(tid), [s], "any") for s in sensitive_list),
        is_complaint = is_complaint(user_text),
        reopened_recently = True,
        macro_mismatch = False,
        multi_topic = len((t["tags"] or "").split(",")) > 4,
        personalization = personalization_overlap(user_text, first_agent_reply),
        empathy = empathy_markers_in_reply(first_agent_reply),
        easy_only = set((t["tags"] or "").split(",")).issubset({"connection","connection_issue","lag","crash","game_crash","network","timeout","opp_out_of_time"})
    )
    score, reasons = score_ticket(t.to_dict(), comments, DEFAULT_WEIGHTS if not weights else weights, cfg)
    scores.append((tid, score, ", ".join(reasons)))
score_df = pd.DataFrame(scores, columns=["id","score","reasons"])
df = df.merge(score_df, on="id", how="left").sort_values("score", ascending=False)

st.subheader("Candidates")
st.caption("Ranked by QA-worthiness score. Adjust filters/weights on the left.")
st.dataframe(df[["id","score","reasons","assignee_name","bpo","csat","payer_tier","topic","sub_topic","tags","updated_at","subject"]], use_container_width=True)

st.subheader("Preview")
sel = st.multiselect("Select ticket IDs to preview", df["id"].tolist()[:20])
for tid in sel:
    st.markdown(f"### Ticket #{tid}")
    trow = df[df["id"] == tid].iloc[0]
    st.write(f"Assignee: {trow['assignee_name']} | CSAT: {trow['csat']} | Payer tier: {trow['payer_tier']} | Tags: {trow['tags']}")
    body = text_for_search(tid)
    st.markdown(highlight(body, inc_kw_list + sensitive_list))

if st.button("Export CSV"):
    out = df.to_csv(index=False)
    st.download_button("Download filtered.csv", data=out, file_name="filtered.csv", mime="text/csv")
