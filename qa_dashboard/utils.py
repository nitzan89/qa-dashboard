import re
from typing import List
def normalize(text: str) -> str:
    if not text: return ""
    return re.sub(r"\s+"," ",text).strip().lower()
def match_keywords(text: str, include: List[str], mode: str = "any") -> bool:
    t = normalize(text)
    if not include: return True
    if mode=="regex": return any(re.search(p, t, flags=re.I) for p in include)
    if mode=="phrase": return any(p.lower() in t for p in include)
    if mode=="any": return any(w.lower() in t for w in include)
    if mode=="all": return all(w.lower() in t for w in include)
    return True
def highlight(text: str, terms: List[str]) -> str:
    if not text or not terms: return text or ""
    out=text
    for term in terms:
        if not term: continue
        out=re.compile(re.escape(term),flags=re.I).sub(lambda m: f"**{m.group(0)}**", out)
    return out
def jaccard_similarity(a_words: List[str], b_words: List[str]) -> float:
    a,b=set(a_words),set(b_words)
    if not a or not b: return 0.0
    return len(a & b)/len(a | b)
def top_terms(text: str, limit: int = 10) -> List[str]:
    tokens=re.findall(r"[a-zA-Z]{3,}", text.lower())
    freq={}
    for tok in tokens: freq[tok]=freq.get(tok,0)+1
    return [t for t,_ in sorted(freq.items(), key=lambda x: x[1], reverse=True)[:limit]]
