"""Frontend for the claim-form sandbox.

Reads conversation hashes (``claim-form:<conversation_id>``) from Redis and
serves a single-page UI plus a small JSON API:

* ``GET /``                       — HTML shell
* ``GET /api/conversations``      — list, newest first
* ``GET /api/conversations/{id}`` — full hash for one conversation
"""

import os

import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
KEYSPACE = os.environ.get("KEYSPACE", "claim-form")

FACT_FIELDS = {
    "claimant_name",
    "date_of_birth",
    "contact_phone",
    "address",
    "policy_number",
    "incident_type",
    "date_of_incident",
    "incident_description",
    "claim_amount",
}

app = FastAPI(title="claim-form frontend")
templates = Jinja2Templates(directory="templates")
r = redis.from_url(REDIS_URL, decode_responses=True)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "keyspace": KEYSPACE,
            "fact_total": len(FACT_FIELDS),
        },
    )


@app.get("/api/conversations")
async def list_conversations() -> list[dict]:
    items: list[dict] = []
    for key in r.scan_iter(match=f"{KEYSPACE}:*", count=200):
        h = r.hgetall(key)
        if not h:
            continue
        items.append(
            {
                "id": key.split(":", 1)[1],
                "facts_filled": sum(1 for k in h if k in FACT_FIELDS),
                "claimant_name": h.get("claimant_name"),
                "incident_type": h.get("incident_type"),
                "captured_at": h.get("captured_at"),
            }
        )
    items.sort(key=lambda x: x.get("captured_at") or "", reverse=True)
    return items


@app.get("/api/conversations/{conv_id}")
async def get_conversation(conv_id: str) -> dict:
    h = r.hgetall(f"{KEYSPACE}:{conv_id}")
    if not h:
        raise HTTPException(status_code=404, detail="conversation not found")
    return h
