"""Frontend for the claim-form sandbox.

Reads conversation hashes (``claim-form:<conversation_id>``) from Redis and
serves a single-page UI plus a small JSON API:

* ``GET /``                       — HTML shell
* ``GET /api/conversations``      — list, newest first; supports optional
  ``claimant``, ``policy_number``, ``date_of_incident``, ``description``
  filters served by a RediSearch index over the claim-form hashes
* ``GET /api/conversations/{id}`` — full hash for one conversation
"""

import os
import re

import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from redis.commands.search.field import TagField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.exceptions import ResponseError

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
KEYSPACE = os.environ.get("KEYSPACE", "claim-form")
INDEX_NAME = os.environ.get("INDEX_NAME", "idx:claim-form")
SEARCH_LIMIT = int(os.environ.get("SEARCH_LIMIT", "500"))

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

# Characters that must be backslash-escaped inside a RediSearch TAG value.
_TAG_SPECIALS = re.compile(r"([,.<>{}\[\]\"':;!@#$%^&*()\-+=~/\\\s])")
# Characters that must be backslash-escaped inside a RediSearch TEXT phrase.
_TEXT_SPECIALS = re.compile(r"([\\\"])")

app = FastAPI(title="claim-form frontend")
templates = Jinja2Templates(directory="templates")
r = redis.from_url(REDIS_URL, decode_responses=True)


def ensure_index() -> None:
    """Create the RediSearch index over claim-form hashes if missing."""
    try:
        r.ft(INDEX_NAME).create_index(
            (
                TextField("claimant_name", sortable=True),
                TagField("policy_number"),
                TagField("date_of_incident", sortable=True),
                TextField("incident_description"),
            ),
            definition=IndexDefinition(
                prefix=[f"{KEYSPACE}:"], index_type=IndexType.HASH
            ),
        )
    except ResponseError as exc:
        if "Index already exists" not in str(exc):
            raise


@app.on_event("startup")
async def _startup() -> None:
    ensure_index()


def _escape_tag(value: str) -> str:
    return _TAG_SPECIALS.sub(r"\\\1", value)


def _escape_text(value: str) -> str:
    return _TEXT_SPECIALS.sub(r"\\\1", value)


def _build_query(
    claimant: str | None,
    policy_number: str | None,
    date_of_incident: str | None,
    description: str | None,
) -> str:
    parts: list[str] = []
    if claimant:
        parts.append(f'@claimant_name:"{_escape_text(claimant)}"')
    if policy_number:
        parts.append(f"@policy_number:{{{_escape_tag(policy_number)}}}")
    if date_of_incident:
        parts.append(f"@date_of_incident:{{{_escape_tag(date_of_incident)}}}")
    if description:
        parts.append(f'@incident_description:"{_escape_text(description)}"')
    return " ".join(parts) if parts else "*"


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
async def list_conversations(
    claimant: str | None = None,
    policy_number: str | None = None,
    date_of_incident: str | None = None,
    description: str | None = None,
) -> list[dict]:
    query_str = _build_query(claimant, policy_number, date_of_incident, description)
    query = Query(query_str).paging(0, SEARCH_LIMIT).no_content()
    try:
        result = r.ft(INDEX_NAME).search(query)
    except ResponseError as exc:
        raise HTTPException(status_code=400, detail=f"search error: {exc}") from exc

    if not result.docs:
        return []

    pipe = r.pipeline()
    for doc in result.docs:
        pipe.hgetall(doc.id)
    hashes = pipe.execute()

    items: list[dict] = []
    for doc, h in zip(result.docs, hashes):
        if not h:
            continue
        items.append(
            {
                "id": doc.id.split(":", 1)[1],
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
