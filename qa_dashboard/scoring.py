from typing import Dict, List, Tuple
from utils import top_terms, jaccard_similarity
from config import EMPATHY_MARKERS
def is_complaint(text: str) -> bool:
    if not text: return False
    bad = ["angry","furious","disappointed","unfair","scam","cheat","cheating","rigged","refund"]
    t=text.lower(); return any(w in t for w in bad)
def long_thread(comments: List[dict]) -> bool:
    return sum(1 for c in comments if c.get("public")) > 4
def multiple_humans_in_thread(comments: List[dict]) -> bool:
    emails=set()
    for c in comments:
        if c.get("public") and c.get("author_email"): emails.add(c["author_email"])
    return len(emails)>2
def empathy_markers_in_reply(agent_reply: str) -> bool:
    if not agent_reply: return False
    low=agent_reply.lower(); return any(e in low for e in EMPATHY_MARKERS)
def personalization_overlap(user_text: str, agent_reply: str, threshold: float = 0.1) -> bool:
    a=top_terms(user_text,12); b=top_terms(agent_reply,12)
    return jaccard_similarity(a,b)>=threshold
def score_ticket(t: Dict, comments: List[dict], weights: Dict, cfg: Dict) -> Tuple[int, List[str]]:
    s,reasons=0,[]
    csat=t.get("csat")
    if csat is not None and csat<=2: s+=weights["low_csat"]; reasons.append("Low CSAT")
    if cfg.get("sensitive_hit",False): s+=weights["sensitive"]; reasons.append("Sensitive keyword")
    if multiple_humans_in_thread(comments): s+=weights["multi_agents"]; reasons.append("Multiple authors")
    if t.get("payer_tier") in ("VIP","Whale") and cfg.get("is_complaint",False): s+=weights["vip_complaint"]; reasons.append("VIP complaint")
    if t.get("reopened_recently"): s+=weights["reopened"]; reasons.append("Reopened")
    if cfg.get("macro_mismatch",False): s+=weights["macro_mismatch"]; reasons.append("Macro–topic mismatch")
    if long_thread(comments): s+=weights["long_thread"]; reasons.append("Long thread")
    if cfg.get("multi_topic",False): s+=weights["multi_topic"]; reasons.append("Multi-topic")
    if (csat is not None and csat>=5) and cfg.get("personalization",False): s+=weights["excellent_personalization"]; reasons.append("Personalized & positive")
    if cfg.get("empathy",False): s+=weights["empathy"]; reasons.append("Empathy")
    if cfg.get("easy_only",False) and not cfg.get("sensitive_hit",False): s+=weights["easy_issue_penalty"]; reasons.append("Easy tech-only")
    return s,reasons
