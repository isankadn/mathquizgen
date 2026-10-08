"""Independent, bearer-authenticated PRIME Streamable HTTP service.

Importing this module never connects to Mongo or imports the Gradio application.
"""
from __future__ import annotations

import hmac
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from lti13 import LTI13ConfigurationError
from mcp.server.lowlevel import Server
from mcp.server.transport_security import TransportSecuritySettings
from mcp_types import CallToolResult, ListToolsResult, TextContent, Tool, ToolAnnotations
from pydantic import ValidationError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from starlette.responses import JSONResponse
import prime_repository as repository

class BearerMiddleware:
    def __init__(self, app, service_state):
        self.app = app
        self.state = service_state

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        token = self.state.get("token")
        headers = [value for key, value in scope.get("headers", []) if key.lower() == b"authorization"]
        valid = False
        if token is not None and len(headers) == 1:
            scheme, separator, supplied = headers[0].partition(b" ")
            valid = scheme.lower() == b"bearer" and bool(separator) and hmac.compare_digest(supplied, token)
        if not valid:
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1008})
                return
            response = JSONResponse({"detail": "Unauthorized"}, status_code=401,
                                    headers={"WWW-Authenticate": "Bearer"})
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)


def create_app(*, database=None, token=None):
    state = {}

    async def list_tools(ctx, params):
        return ListToolsResult(tools=[Tool(
            name=name, description=repository.TOOL_DESCRIPTIONS[name], inputSchema=model.model_json_schema(),
            annotations=ToolAnnotations(readOnlyHint=name != "configure_prime_platform",
                                        destructiveHint=False, openWorldHint=False),
        ) for name, model in repository.TOOL_MODELS.items()])

    async def call_tool(ctx, params):
        try:
            result = await run_in_threadpool(state["store"].call, params.name, params.arguments or {})
        except ValidationError:
            return CallToolResult(isError=True, content=[TextContent(type="text", text="Invalid tool arguments.")])
        except (repository.AccessDenied, LTI13ConfigurationError) as exc:
            return CallToolResult(isError=True, content=[TextContent(type="text", text=str(exc))])
        except PyMongoError:
            # Never expose or log database connection credentials or exception text.
            return CallToolResult(isError=True, content=[TextContent(type="text", text="PRIME data is unavailable.")])
        return CallToolResult(content=[TextContent(type="text", text=json.dumps(result, ensure_ascii=False))],
                              structuredContent=result)

    server = Server("prime-mathquizgen", version="1.0", on_list_tools=list_tools, on_call_tool=call_tool)
    allowed_hosts = [host.strip() for host in os.getenv(
        "PRIME_MCP_ALLOWED_HOSTS", "127.0.0.1:*,localhost:*"
    ).split(",") if host.strip()]
    transport = server.streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True, json_response=True,
        max_request_body_size=32768,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True, allowed_hosts=allowed_hosts, allowed_origins=[],
        ),
    )

    @asynccontextmanager
    async def lifespan(app):
        secret = token if token is not None else os.getenv("PRIME_MCP_TOKEN", "")
        if not isinstance(secret, str) or len(secret) < 32 or not secret.isascii() or any(c.isspace() for c in secret):
            raise RuntimeError("PRIME_MCP_TOKEN must contain at least 32 non-whitespace ASCII characters.")
        client = None
        try:
            db = database
            if db is None:
                mongo_url = os.getenv("MONGO_URL", "")
                if not mongo_url:
                    raise RuntimeError("MONGO_URL is required.")
                try:
                    client = MongoClient(mongo_url, connect=False, serverSelectionTimeoutMS=repository.QUERY_MS,
                                         connectTimeoutMS=repository.QUERY_MS, socketTimeoutMS=repository.QUERY_MS,
                                         timeoutMS=repository.QUERY_MS)
                    db = client["prime"]
                except (PyMongoError, ValueError):
                    raise RuntimeError("PRIME Mongo connection configuration is invalid.") from None
            store = repository.PrimeStore(db)
            try:
                await run_in_threadpool(store.ensure_indexes)
            except PyMongoError:
                raise RuntimeError("PRIME Mongo initialization failed; review connectivity and unique platform records.") from None
            state.update(token=secret.encode("ascii"), store=store)
            async with server.session_manager.run():
                yield
        finally:
            state.clear()
            if client is not None:
                client.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(BearerMiddleware, service_state=state)
    app.mount("/", transport)
    return app


app = create_app()
