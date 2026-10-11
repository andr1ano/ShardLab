import asyncio
import json
import logging
import os
import tempfile
from collections import Counter
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from .ring import HashRing, hash64, route_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("coordinator")
logging.getLogger("httpx").setLevel(logging.WARNING)

NAME = r"^[A-Za-z][A-Za-z0-9_]{0,63}$"

class TableDef(BaseModel):
    name: str = Field(pattern=NAME)
    partition_key: str = Field(pattern=NAME)
    sort_key: str | None = Field(default=None, pattern=NAME)

    @model_validator(mode="after")
    def _distinct(self):
        if self.sort_key == self.partition_key:
            raise ValueError("sort_key must differ from partition_key")
        return self


class RecordIn(BaseModel):
    pk: str = Field(min_length=1, max_length=512)
    sk: str | None = Field(default=None, min_length=1, max_length=512)
    value: Any


class ShardIn(BaseModel):
    id: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    url: str = Field(pattern=r"^https?://\S+$")

class State:
    def __init__(self, catalog_path: str, vnodes: int):
        self.catalog_path = catalog_path
        self.tables: dict[str, TableDef] = {}
        self.shards: dict[str, str] = {}
        self.ring = HashRing(vnodes)
        self.ready = asyncio.Event()
        self.ready.set()
        self.admin_lock = asyncio.Lock()
        self.http: httpx.AsyncClient | None = None

    def load(self) -> None:
        if not os.path.exists(self.catalog_path):
            return
        with open(self.catalog_path) as f:
            data = json.load(f)
        self.tables = {n: TableDef(**t) for n, t in data.get("tables", {}).items()}
        self.shards = data.get("shards", {})
        for sid in self.shards:
            self.ring.add_shard(sid)
        log.info("catalog loaded: %d tables, %d shards", len(self.tables), len(self.shards))

    def save(self) -> None:
        d = os.path.dirname(self.catalog_path) or "."
        os.makedirs(d, exist_ok=True)
        data = {
            "tables": {n: t.model_dump() for n, t in self.tables.items()},
            "shards": self.shards,
        }
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, self.catalog_path)

async def call(st: State, method: str, url: str, **kw) -> httpx.Response:
    try:
        return await st.http.request(method, url, **kw)
    except httpx.HTTPError as e:
        raise HTTPException(503, f"shard unavailable: {url}") from e

def expect(r: httpx.Response, *codes: int) -> None:
    if r.status_code not in codes:
        raise HTTPException(502, f"unexpected shard response: {r.status_code}")

def pick_shard(st: State, table: str, pk: str) -> tuple[str, str]:
    try:
        sid = st.ring.get_shard(route_key(table, pk))
    except LookupError:
        raise HTTPException(503, "no shards registered")
    return sid, st.shards[sid]

async def rebalance(st: State) -> dict:
    scanned = 0
    moves: Counter = Counter()
    arrived: set = set()
    for sid, url in list(st.shards.items()):
        after: dict = {}
        while True:
            r = await call(st, "GET", f"{url}/scan", params={"limit": 500, **after})
            expect(r, 200)
            items = r.json()["items"]
            if not items:
                break
            for it in items:
                ident = (it["table"], it["pk"], it["sk"])
                if ident in arrived:
                    continue
                scanned += 1
                owner = st.ring.get_shard(route_key(it["table"], it["pk"]))
                if owner == sid:
                    continue
                w = await call(
                    st, "PUT", f"{st.shards[owner]}/records",
                    params={"upsert": "true"}, json=it,
                )
                expect(w, 200)
                d = await call(
                    st, "DELETE", f"{url}/records",
                    params={"table": it["table"], "pk": it["pk"], "sk": it["sk"]},
                )
                expect(d, 204, 404)
                arrived.add(ident)
                moves[f"{sid}->{owner}"] += 1
            last = items[-1]
            after = {"after_table": last["table"], "after_pk": last["pk"], "after_sk": last["sk"]}
    moved = sum(moves.values())
    return {
        "scanned": scanned,
        "moved": moved,
        "moved_fraction": round(moved / scanned, 4) if scanned else 0.0,
        "moves": dict(moves),
    }

def create_app(catalog_path: str | None = None, vnodes: int | None = None) -> FastAPI:
    st = State(
        catalog_path or os.getenv("CATALOG_PATH", "data/catalog.json"),
        vnodes or int(os.getenv("VNODES", "128")),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        st.load()
        st.http = httpx.AsyncClient(timeout=30)
        yield
        await st.http.aclose()

    app = FastAPI(title="Sharded KV store — coordinator", version="1.0.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        msg = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
        return JSONResponse(status_code=400, content={"detail": msg})

    async def gate():
        await st.ready.wait()

    def table_or_404(name: str) -> TableDef:
        t = st.tables.get(name)
        if t is None:
            raise HTTPException(404, f"table '{name}' not found")
        return t

    def check_key(t: TableDef, sk: str | None) -> None:
        if t.sort_key is None and sk is not None:
            raise HTTPException(400, f"table '{t.name}' has no sort key")
        if t.sort_key is not None and sk is None:
            raise HTTPException(400, f"table '{t.name}' requires sk ({t.sort_key})")

    PK = Query(..., min_length=1, max_length=512)
    SK = Query(None, min_length=1, max_length=512)

    @app.get("/health")
    async def health():
        return {"status": "ok", "shards": len(st.shards), "tables": len(st.tables)}

    @app.post("/tables", status_code=201)
    async def create_table(t: TableDef):
        async with st.admin_lock:
            if t.name in st.tables:
                raise HTTPException(409, f"table '{t.name}' already exists")
            st.tables[t.name] = t
            st.save()
        return t

    @app.get("/tables")
    async def list_tables():
        return list(st.tables.values())

    @app.get("/tables/{table}")
    async def get_table(table: str):
        return table_or_404(table)

    # ── записи ──
    @app.get("/tables/{table}/records/exists", dependencies=[Depends(gate)])
    async def exists(table: str, pk: str = PK, sk: str | None = SK):
        t = table_or_404(table)
        check_key(t, sk)
        _, url = pick_shard(st, table, pk)
        r = await call(st, "GET", f"{url}/records/exists",
                       params={"table": table, "pk": pk, "sk": sk or ""})
        expect(r, 200)
        return r.json()

    @app.post("/tables/{table}/records", status_code=201, dependencies=[Depends(gate)])
    async def create_record(table: str, rec: RecordIn):
        t = table_or_404(table)
        check_key(t, rec.sk)
        _, url = pick_shard(st, table, rec.pk)
        r = await call(st, "POST", f"{url}/records",
                       json={"table": table, "pk": rec.pk, "sk": rec.sk or "", "value": rec.value})
        if r.status_code == 409:
            raise HTTPException(409, "record already exists")
        expect(r, 201)
        return {"table": table, "pk": rec.pk, "sk": rec.sk, "value": rec.value}

    @app.put("/tables/{table}/records", dependencies=[Depends(gate)])
    async def update_record(table: str, rec: RecordIn):
        t = table_or_404(table)
        check_key(t, rec.sk)
        _, url = pick_shard(st, table, rec.pk)
        r = await call(st, "PUT", f"{url}/records",
                       json={"table": table, "pk": rec.pk, "sk": rec.sk or "", "value": rec.value})
        if r.status_code == 404:
            raise HTTPException(404, "record not found")
        expect(r, 200)
        return {"table": table, "pk": rec.pk, "sk": rec.sk, "value": rec.value}

    @app.get("/tables/{table}/records", dependencies=[Depends(gate)])
    async def read_record(table: str, pk: str = PK, sk: str | None = SK):
        t = table_or_404(table)
        check_key(t, sk)
        _, url = pick_shard(st, table, pk)
        r = await call(st, "GET", f"{url}/records",
                       params={"table": table, "pk": pk, "sk": sk or ""})
        if r.status_code == 404:
            raise HTTPException(404, "record not found")
        expect(r, 200)
        return {"table": table, "pk": pk, "sk": sk, "value": r.json()["value"]}

    @app.delete("/tables/{table}/records", status_code=204, dependencies=[Depends(gate)])
    async def delete_record(table: str, pk: str = PK, sk: str | None = SK):
        t = table_or_404(table)
        check_key(t, sk)
        _, url = pick_shard(st, table, pk)
        r = await call(st, "DELETE", f"{url}/records",
                       params={"table": table, "pk": pk, "sk": sk or ""})
        if r.status_code == 404:
            raise HTTPException(404, "record not found")
        expect(r, 204)
        return Response(status_code=204)

    @app.get("/tables/{table}/query", dependencies=[Depends(gate)])
    async def query_partition(
        table: str,
        pk: str = PK,
        sk_prefix: str = Query("", max_length=512),
        limit: int = Query(100, ge=1, le=1000),
    ):
        t = table_or_404(table)
        if t.sort_key is None:
            raise HTTPException(400, f"table '{table}' has no sort key; use read")
        _, url = pick_shard(st, table, pk)
        r = await call(st, "GET", f"{url}/partitions",
                       params={"table": table, "pk": pk, "sk_prefix": sk_prefix, "limit": limit})
        expect(r, 200)
        return {"table": table, "pk": pk, "items": r.json()["items"]}

    @app.post("/shards")
    async def register_shard(body: ShardIn, response: Response):
        url = body.url.rstrip("/")
        h = await call(st, "GET", f"{url}/health")
        expect(h, 200)
        async with st.admin_lock:
            current = st.shards.get(body.id)
            shard = {"id": body.id, "url": url}
            if current == url:
                return {"shard": shard, "created": False, "rebalance": None}
            st.shards[body.id] = url
            if current is not None:
                st.save()
                return {"shard": shard, "created": False, "rebalance": None}
            st.ready.clear()
            try:
                st.ring.add_shard(body.id)
                st.save()
                report = await rebalance(st)
            except HTTPException as e:
                raise HTTPException(
                    500, f"shard registered, but rebalance failed ({e.detail}); retry POST /rebalance"
                )
            finally:
                st.ready.set()
            log.info("shard %s joined: %s", body.id, report)
            response.status_code = 201
            return {"shard": shard, "created": True, "rebalance": report}

    @app.get("/shards")
    async def list_shards():
        return [{"id": i, "url": u} for i, u in sorted(st.shards.items())]

    @app.post("/rebalance")
    async def run_rebalance():
        async with st.admin_lock:
            st.ready.clear()
            try:
                return await rebalance(st)
            finally:
                st.ready.set()

    @app.get("/stats", dependencies=[Depends(gate)])
    async def stats():
        out: dict = {}
        total = 0
        for sid, url in sorted(st.shards.items()):
            try:
                r = await st.http.get(f"{url}/stats")
                n = r.json()["records"]
                total += n
            except Exception:
                n = None
            out[sid] = {"url": url, "records": n}
        return {"total": total, "shards": out}

    @app.get("/route")
    async def route(table: str, pk: str = PK):
        sid, url = pick_shard(st, table, pk)
        return {"shard_id": sid, "url": url, "hash": hash64(route_key(table, pk))}

    return app
