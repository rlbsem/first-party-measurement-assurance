"""Atomic finite query admission; estimates and provider charges stay distinct."""

import sqlite3
from pathlib import Path


def initialize(path, limit=300, bytes_per_query=10_000_000_000):
    p = Path(path)
    if p.exists():
        raise ValueError("fresh_query_budget_required")
    with sqlite3.connect(p) as c:
        c.execute(
            "CREATE TABLE budget(singleton INTEGER PRIMARY KEY CHECK(singleton=1),used INTEGER NOT NULL,limit_count INTEGER NOT NULL,max_bytes INTEGER NOT NULL)"
        )
        c.execute("INSERT INTO budget VALUES(1,0,?,?)", (limit, bytes_per_query))


def reserve(path, maximum_bytes):
    with sqlite3.connect(path, timeout=10) as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            "SELECT used,limit_count,max_bytes FROM budget WHERE singleton=1"
        ).fetchone()
        if not row or row[0] >= row[1] or not 0 < maximum_bytes <= row[2]:
            raise ValueError("native_query_budget_exhausted")
        c.execute("UPDATE budget SET used=used+1 WHERE singleton=1")
        return row[0] + 1
