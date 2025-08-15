import os
DEFAULT_EXCLUDED_TAGS = ["connection","connection_issue","lag","crash","game_crash","network","timeout","opp_out_of_time"]
BOT_EMAILS = {"ilya@candivore.io","maor@candivore.io"}
CUSTOM_FIELDS = {"topic": None, "sub_topic": None, "version": None, "language": None, "payer_tier": None}
SENSITIVE_KEYWORDS = ["gdpr","privacy","personal data","delete account","lawsuit","legal","harassment","bully","abuse"]
EMPATHY_MARKERS = ["i understand","i can imagine","sorry to hear","i'm sorry","i am sorry"]
DEFAULT_WEIGHTS = dict(low_csat=15,sensitive=20,multi_agents=10,vip_complaint=15,reopened=10,macro_mismatch=10,long_thread=8,multi_topic=8,excellent_personalization=20,empathy=5,easy_issue_penalty=-20)
DB_PATH = os.path.join("data","qa.db"); os.makedirs("data", exist_ok=True)
