import os, time
from datetime import datetime, timedelta, timezone
import requests
SUBDOMAIN=os.getenv("ZD_SUBDOMAIN"); EMAIL=os.getenv("ZD_EMAIL"); TOKEN=os.getenv("ZD_API_TOKEN")
try:
    import streamlit as st
    SUBDOMAIN=SUBDOMAIN or st.secrets.get("ZD_SUBDOMAIN")
    EMAIL=EMAIL or st.secrets.get("ZD_EMAIL")
    TOKEN=TOKEN or st.secrets.get("ZD_API_TOKEN")
except Exception: pass
from config import BOT_EMAILS, CUSTOM_FIELDS
from db import get_conn, init_db, upsert_ticket, upsert_comment, upsert_audit, rebuild_fts
BASE=lambda: f"https://{SUBDOMAIN}.zendesk.com/api/v2"
def get_json(url, params=None):
    for _ in range(6):
        r=requests.get(url, params=params, auth=(EMAIL,TOKEN), timeout=30)
        if r.status_code==429:
            time.sleep(int(r.headers.get("Retry-After","2"))); continue
        r.raise_for_status(); return r.json()
    raise RuntimeError("Too many retries / rate-limited")
def search_ticket_ids(start,end):
    q=f'status:solved updated>="{start.isoformat()}" updated<"{end.isoformat()}"'
    url=f"{BASE()}/search.json"; params={"query":q,"page":1}
    while True:
        data=get_json(url, params=params)
        for r in data.get("results",[]):
            if r.get("result_type")=="ticket": yield r["id"]
        nextp=data.get("next_page"); 
        if not nextp: break
        url,params=nextp,None
_user_cache={}
def get_user(uid):
    if uid in _user_cache: return _user_cache[uid]
    data=get_json(f"{BASE()}/users/{uid}.json"); user=data["user"]; _user_cache[uid]=user; return user
def get_ticket(tid): return get_json(f"{BASE()}/tickets/{tid}.json")["ticket"]
def get_comments(tid): return get_json(f"{BASE()}/tickets/{tid}/comments.json")["comments"]
def get_audits(tid): return get_json(f"{BASE()}/tickets/{tid}/audits.json")["audits"]
def extract_custom_field(ticket,name):
    fid=CUSTOM_FIELDS.get(name); 
    if not fid: return None
    for f in ticket.get("custom_fields",[]):
        if f.get("id")==fid: return f.get("value")
    return None
def ingest(days=5):
    if not SUBDOMAIN or not EMAIL or not TOKEN: raise SystemExit("Missing Zendesk credentials (set in Streamlit Secrets or env).")
    init_db()
    now=datetime.now(timezone.utc); start=now - timedelta(days=days); cursor=start; six=timedelta(hours=6)
    with get_conn() as conn:
        seen=set()
        while cursor<now:
            nxt=min(cursor+six, now)
            for tid in search_ticket_ids(cursor, nxt):
                if tid in seen: continue
                seen.add(tid)
                t=get_ticket(tid)
                assignee_id=t.get("assignee_id"); 
                if not assignee_id: continue
                requester=get_user(t["requester_id"]); requester_email=(requester.get("email") or "").lower()
                assignee=get_user(assignee_id); assignee_email=(assignee.get("email") or "").lower(); assignee_name=assignee.get("name") or ""
                if assignee_email in BOT_EMAILS: continue
                comments=get_comments(tid); has_human=False; pub=[]
                for idx,c in enumerate(comments):
                    au=get_user(c["author_id"]); a_email=(au.get("email") or "").lower(); a_name=au.get("name") or ""
                    body=c.get("html_body") or c.get("body") or ""; public=c.get("public",False)
                    pub.append(dict(ticket_id=tid, idx=idx, created_at=c["created_at"], public=1 if public else 0, author_id=c["author_id"], author_email=a_email, author_name=a_name, body=body))
                    if public and a_email not in BOT_EMAILS and a_email!=requester_email: has_human=True
                if not has_human: continue
                audits=get_audits(tid); macros=[]
                for a in audits:
                    for e in a.get("events",[]):
                        if e.get("type")=="ApplyMacro":
                            title=e.get("value") or e.get("macro_title")
                            if title: macros.append(title)
                tags=",".join(t.get("tags",[]))
                row=(t["id"], t.get("status"), t.get("subject"), t.get("created_at"), t.get("updated_at"), t.get("updated_at"),
                     t.get("satisfaction_rating",{}).get("score") if t.get("satisfaction_rating") else None,
                     1 if t.get("satisfaction_rating") else 0, t.get("requester_id"), requester_email,
                     assignee_id, assignee_email, assignee_name, None,
                     extract_custom_field(t,"payer_tier"), extract_custom_field(t,"language"),
                     extract_custom_field(t,"topic"), extract_custom_field(t,"sub_topic"),
                     extract_custom_field(t,"version"), tags)
                upsert_ticket(conn, row)
                for c in pub:
                    upsert_comment(conn, (c["ticket_id"], c["idx"], c["created_at"], c["public"], c["author_id"], c["author_email"], c["author_name"], c["body"]))
                if macros:
                    upsert_audit(conn, (tid, t.get("updated_at"), "|".join(macros)))
            cursor=nxt
    rebuild_fts(); return f"Ingest complete: last {days} days"
