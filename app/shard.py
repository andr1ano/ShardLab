import asyncio
import json
import logging
import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("shard")
logging.getLogger("httpx").setLevel(logging.WARNING)

class RecordIn(BaseModel):
    table: str
    pk: str
    sk: str = ""
    value: Any

class Store:
    def __init__(self, path: str):
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS records (
                   tbl TEXT NOT NULL, pk TEXT NOT NULL, sk TEXT NOT NULL DEFAULT '',
                   value TEXT NOT NULL,
                   PRIMARY KEY (tbl, pk, sk)) WITHOUT ROWID"""
        )

    @staticmethod
    def _dump(v: Any) -> str:
        return json.dumps(v, ensure_ascii=False)

    def create(self, r: RecordIn) -> bool:
        with self.lock:
            try:
                with self.conn:
                    self.conn.execute(
                        "INSERT INTO records VALUES (?,?,?,?)",
                        (r.table, r.pk, r.sk, self._dump(r.value)),
                    )
                return True
            except sqlite3.IntegrityError:
                return False

    def update(self, r: RecordIn) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "UPDATE records SET value=? WHERE tbl=? AND pk=? AND sk=?",
                (self._dump(r.value), r.table, r.pk, r.sk),
            )
            return cur.rowcount > 0

    def upsert(self, r: RecordIn) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """INSERT INTO records VALUES (?,?,?,?)
                   ON CONFLICT(tbl, pk, sk) DO UPDATE SET value=excluded.value""",
                (r.table, r.pk, r.sk, self._dump(r.value)),
            )

    def get(self, table: str, pk: str, sk: str):
        with self.lock:
            row = self.conn.execute(
                "SELECT value FROM records WHERE tbl=? AND pk=? AND sk=?",
                (table, pk, sk),
            ).fetchone()
        return None if row is None else (True, json.loads(row[0]))

    def delete(self, table: str, pk: str, sk: str) -> bool:
        with self.lock, self.conn:
            cur = self.conn.execute(
                "DELETE FROM records WHERE tbl=? AND pk=? AND sk=?", (table, pk, sk)
            )
            return cur.rowcount > 0

    def partition(self, table: str, pk: str, sk_prefix: str, limit: int):
        with self.lock:
            rows = self.conn.execute(
                """SELECT sk, value FROM records
                   WHERE tbl=? AND pk=? AND substr(sk,1,?)=?
                   ORDER BY sk LIMIT ?""",
                (table, pk, len(sk_prefix), sk_prefix, limit),
            ).fetchall()
        return [{"sk": sk, "value": json.loads(v)} for sk, v in rows]

    def scan(self, after, limit: int):
        sql = "SELECT tbl, pk, sk, value FROM records"
        args: list = []
        if after:
            sql += " WHERE (tbl, pk, sk) > (?,?,?)"
            args += list(after)
        sql += " ORDER BY tbl, pk, sk LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.conn.execute(sql, args).fetchall()
        return [
            {"table": t, "pk": p, "sk": s, "value": json.loads(v)}
            for t, p, s, v in rows
        ]

    def counts(self) -> dict[str, int]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT tbl, COUNT(*) FROM records GROUP BY tbl"
            ).fetchall()
        return dict(rows)

def create_app(shard_id=None, db_path=None, coordinator_url=None, public_url=None):
    shard_id = shard_id or os.environ["SHARD_ID"]
    db_path = db_path or os.getenv("DB_PATH", f"data/{shard_id}.db")
    coordinator_url = coordinator_url or os.getenv("COORDINATOR_URL")
    public_url = public_url or os.getenv("PUBLIC_URL")
    store = Store(db_path)

    async def register_loop():
        registered = False
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                try:
                    r = await client.post(
                        f"{coordinator_url}/shards",
                        json={"id": shard_id, "url": public_url},
                    )
                    r.raise_for_status()
                    if not registered:
                        log.info("registered at %s: %s", coordinator_url, r.json())
                    registered = True
                except Exception as e:
                    registered = False
                    log.warning("registration failed (%s), retrying", e)
                await asyncio.sleep(15 if registered else 1)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = None
        if coordinator_url and public_url:
            task = asyncio.create_task(register_loop())
        yield
        if task:
            task.cancel()

    app = FastAPI(title=f"KV shard {shard_id} (internal)", lifespan=lifespan)

    @app.get("/health")
    def health():
        return {"status": "ok", "shard_id": shard_id}

    @app.get("/stats")
    def stats():
        by_table = store.counts()
        return {"shard_id": shard_id, "records": sum(by_table.values()), "tables": by_table}

    @app.post("/records", status_code=201)
    def create(r: RecordIn):
        if not store.create(r):
            raise HTTPException(409, "record already exists")
        return {"ok": True}

    @app.put("/records")
    def put(r: RecordIn, upsert: bool = False):
        if upsert:
            store.upsert(r)
        elif not store.update(r):
            raise HTTPException(404, "record not found")
        return {"ok": True}

    @app.get("/records")
    def read(table: str, pk: str, sk: str = ""):
        found = store.get(table, pk, sk)
        if found is None:
            raise HTTPException(404, "record not found")
        return {"value": found[1]}

    @app.get("/records/exists")
    def exists(table: str, pk: str, sk: str = ""):
        return {"exists": store.get(table, pk, sk) is not None}

    @app.delete("/records", status_code=204)
    def delete(table: str, pk: str, sk: str = ""):
        if not store.delete(table, pk, sk):
            raise HTTPException(404, "record not found")
        return Response(status_code=204)

    @app.get("/partitions")
    def partition(table: str, pk: str, sk_prefix: str = "", limit: int = Query(100, ge=1, le=1000)):
        return {"items": store.partition(table, pk, sk_prefix, limit)}

    @app.get("/scan")
    def scan(
        limit: int = Query(500, ge=1, le=5000),
        after_table: str | None = None,
        after_pk: str | None = None,
        after_sk: str | None = None,
    ):
        after = None
        if after_table is not None and after_pk is not None and after_sk is not None:
            after = (after_table, after_pk, after_sk)
        return {"items": store.scan(after, limit)}

    return app
