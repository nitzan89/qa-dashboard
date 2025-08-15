-- tables
CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY, status TEXT, subject TEXT, created_at TEXT, updated_at TEXT, solved_at TEXT, csat INTEGER, csat_offered INTEGER, requester_id INTEGER, requester_email TEXT, assignee_id INTEGER, assignee_email TEXT, assignee_name TEXT, bpo TEXT, payer_tier TEXT, language TEXT, topic TEXT, sub_topic TEXT, version TEXT, tags TEXT);
CREATE TABLE IF NOT EXISTS comments (ticket_id INTEGER, idx INTEGER, created_at TEXT, public INTEGER, author_id INTEGER, author_email TEXT, author_name TEXT, body TEXT, PRIMARY KEY(ticket_id, idx));
CREATE TABLE IF NOT EXISTS audits (ticket_id INTEGER, created_at TEXT, macro_titles TEXT, PRIMARY KEY(ticket_id, created_at));
CREATE TABLE IF NOT EXISTS reviews (ticket_id INTEGER PRIMARY KEY, status TEXT, reviewer_email TEXT, notes TEXT, updated_at TEXT);
CREATE VIRTUAL TABLE IF NOT EXISTS comments_fts USING fts5(body, content='comments', content_rowid='rowid');
