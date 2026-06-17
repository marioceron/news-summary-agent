# modules/state.py
"""
State DB helpers (PostgreSQL-ready).

- Uses SQLAlchemy with DSN from STATE_DB_URL.
- Falls back to SQLite (/state/state.db) if env var is missing.
- Provides:
    ensure_ready()
    content_digest(url, title, text)
    have_seen(source, digest)
    mark_seen(source, url, digest)
    only_new(items, *, source_key="source", url_key="url",
             title_key="title_en", text_key="text_en")
    deduplicate_and_only_new(items)  # alias of only_new
    clear_all()

Expected item shape in DAG steps: dicts with at least "source" and "url".
If don’t have title_en/text_en, it tries title/text/summary.
"""

import os
import logging
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result

# Default to Postgres inside docker-compose network
DEFAULT_DSN = os.getenv("STATE_DB_URL", "postgresql+psycopg2://admin:admin@db:5432/airflow")

class Store:
    """
    Simple Postgres-backed state store for deduplication and run metadata.
    Designed to be imported at runtime from Airflow tasks (not at DAG parse time).
    """

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or os.getenv("STATE_DB_URL") or DEFAULT_DSN
        self._engine: Engine = create_engine(self.dsn, pool_pre_ping=True, future=True)
        logging.info("[STATE] connecting using DSN: %s", self.safe_dsn())

    def safe_dsn(self) -> str:
        return self.dsn.replace("admin:admin@", "****:****@")

    # ---------- schema ----------
    def ensure_schema(self) -> None:
        ddl_article_state = """
        CREATE TABLE IF NOT EXISTS article_state (
            key TEXT PRIMARY KEY,
            first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            payload JSONB
        );
        """
        ddl_meta = """
        CREATE TABLE IF NOT EXISTS state_meta (
            key TEXT PRIMARY KEY,
            value JSONB,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self._engine.begin() as conn:
            conn.exec_driver_sql(ddl_article_state)
            conn.exec_driver_sql(ddl_meta)

    # ---------- dedupe ----------
    def has_seen(self, key: str) -> bool:
        qry = text("SELECT 1 FROM article_state WHERE key = :k LIMIT 1")
        with self._engine.begin() as conn:
            res: Result = conn.execute(qry, {"k": key})
            return res.scalar_one_or_none() is not None

    def mark_seen(self, key: str, payload: Dict[str, Any] | None = None) -> None:
        upsert = text("""
            INSERT INTO article_state(key, payload)
            VALUES (:k, CAST(:p AS JSONB))
            ON CONFLICT (key) DO UPDATE SET payload = EXCLUDED.payload
        """)
        with self._engine.begin() as conn:
            conn.execute(upsert, {"k": key, "p": None if payload is None else json_dumps(payload)})

    def insert_article(self, article: Dict[str, Any], key_fn: Callable[[Dict[str, Any]], str]) -> str:
        k = key_fn(article)
        self.mark_seen(k, payload=article)
        return k

    def filter_only_new(
        self,
        articles: Sequence[Dict[str, Any]],
        key_fn: Callable[[Dict[str, Any]], str],
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Returns the subset of `articles` that have NOT been seen before.
        Also marks any returned article keys as seen.
        """
        if not articles:
            return [], 0

        keys = [key_fn(a) for a in articles]

        # Find existing keys using a VALUES table (no giant IN list)
        select_existing = text("""
            WITH candidates(key) AS (SELECT * FROM (VALUES """ +
            ",".join(f"(:k{i})" for i in range(len(keys))) +
            """) AS v(key))
            SELECT a.key
            FROM candidates c
            JOIN article_state a ON a.key = c.key
        """)

        bind = {f"k{i}": k for i, k in enumerate(keys)}
        with self._engine.begin() as conn:
            res: Result = conn.execute(select_existing, bind)
            existing = {row[0] for row in res}

        new_pairs = [(a, k) for a, k in zip(articles, keys) if k not in existing]
        new_articles = [a for a, _ in new_pairs]

        if not new_pairs:
            return [], 0

        # Bulk insert new rows (ignore if raced)
        insert_many = text("""
            INSERT INTO article_state (key, payload)
            VALUES """ + ",".join(f"(:k{i}, CAST(:p{i} AS JSONB))" for i in range(len(new_pairs))) + """
            ON CONFLICT (key) DO NOTHING
        """)
        bind2 = {f"k{i}": k for i, k in enumerate([k for _, k in new_pairs])}
        for i, (a, _) in enumerate(new_pairs):
            bind2[f"p{i}"] = json_dumps(a)

        with self._engine.begin() as conn:
            conn.execute(insert_many, bind2)

        return new_articles, len(new_articles)

    # ---------- meta ----------
    def set_meta(self, key: str, value: Any) -> None:
        upsert = text("""
            INSERT INTO state_meta(key, value)
            VALUES (:k, CAST(:v AS JSONB))
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW()
        """)
        with self._engine.begin() as conn:
            conn.execute(upsert, {"k": key, "v": json_dumps(value)})

    def get_meta(self, key: str, default: Any = None) -> Any:
        qry = text("SELECT value FROM state_meta WHERE key = :k")
        with self._engine.begin() as conn:
            res: Result = conn.execute(qry, {"k": key})
            row = res.first()
            if row is None or row[0] is None:
                return default
            return row[0]


# ---- helper: lightweight json serializer ----
import json as _json
def json_dumps(obj: Any) -> str:
    return _json.dumps(obj, separators=(",", ":"), default=str)


# ---- module-level singleton + compatibility helpers ----
_singleton: Store | None = None

def get_store(dsn: str | None = None) -> Store:
    global _singleton
    if _singleton is None or (dsn and _singleton.dsn != dsn):
        _singleton = Store(dsn=dsn)
    return _singleton

def ensure_schema() -> None:
    get_store().ensure_schema()

def has_seen(key: str) -> bool:
    return get_store().has_seen(key)

def mark_seen(key: str, payload: Dict[str, Any] | None = None) -> None:
    get_store().mark_seen(key, payload)

def insert_article(article: Dict[str, Any], key_fn):
    return get_store().insert_article(article, key_fn)

def filter_only_new(articles: Sequence[Dict[str, Any]], key_fn):
    # Implemented here to avoid recursion with modules.dedupe
    return get_store().filter_only_new(articles, key_fn)

def set_meta(key: str, value: Any) -> None:
    get_store().set_meta(key, value)

def get_meta(key: str, default: Any = None) -> Any:
    return get_store().get_meta(key, default)

def init_state() -> bool:
    s = get_store()
    s.ensure_schema()
    return True
