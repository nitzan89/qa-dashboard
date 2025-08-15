import os
import sqlite3
from contextlib import contextmanager

from config import DB_PATH


@contextmanager
def get_conn():
    """
    SQLite tuned for Streamlit:
      - WAL so readers don't block writers
      - longer timeouts
      - allow cross-thread use
    """
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    # Fewer lock conflicts
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")      # ms
    conn.execute("PRAGMA read_uncommitted=1;")
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()


def init_db():
    from pathlib import Path
    Path(os.path.dirname(DB_PATH)).mkdir(parents=True, exist_ok=True)

    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with get_conn() as conn, open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())


def upsert_ticket(conn, t):
    conn.execute(
        """
        INSERT INTO tickets (
          id, status, subject, created_at, updated_at, solved_at, csat, csat_offered,
          requester_id, requester_email, assignee_id, assignee_email, assignee_name, bpo,
          payer_tier, language, topic, sub_topic, version, tags
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET
          status=excluded.status,
          subject=excluded.subject,
          created_at=excluded.created_at,
          updated_at=excluded.updated_at,
          solved_at=excluded.solved_at,
          csat=excluded.csat,
          csat_offered=excluded.csat_offered,
          requester_id=excluded.requester_id,
          requester_email=excluded.requester_email,
          assignee_id=excluded.assignee_id,
          assignee_email=excluded.assignee_email,
          assignee_name=excluded.assignee_name,
          bpo=excluded.bpo,
          payer_tier=excluded.payer_tier,
          language=excluded.language,
          topic=excluded.topic,
          sub_topic=excluded.sub_topic,
          version=excluded.version,
          tags=excluded.tags
        """,
        t,
    )


def upsert_comment(conn, c):
    conn.execute(
        """
        INSERT INTO comments (
          ticket_id, idx, created_at, public, author_id, author_email, author_name, body
        )
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(ticket_id, idx) DO UPDATE SET
          created_at=excluded.created_at,
          public=excluded.public,
          author_id=excluded.author_id,
          author_email=excluded.author_email,
          author_name=excluded.author_name,
          body=excluded.body
        """,
        c,
    )


def upsert_audit(conn, a):
    conn.execute(
        """
        INSERT INTO audits (ticket_id, created_at, macro_titles)
        VALUES (?,?,?)
        ON CONFLICT(ticket_id, created_at) DO UPDATE SET
          macro_titles=excluded.macro_titles
        """,
        a,
    )


def rebuild_fts():
    with get_conn() as conn:
        try:
            conn.execute("DELETE FROM comments_fts;")
            conn.execute(
                "INSERT INTO comments_fts(rowid, body) "
                "SELECT rowid, body FROM comments;"
            )
        except sqlite3.DatabaseError:
            # FTS is optional; ignore if not available
            pass
