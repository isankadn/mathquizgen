"""Current signed LTI authority for Gradio 5.23 session state and result queues."""

import hashlib
import json
import re

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


NATIVE_IDENTITY_PREFIX = "prime-lti13:"
_SCOPE_FIELDS = (
    "lti_version", "lti_issuer", "lti_client_id", "lti_deployment_id",
    "user_id", "context_id",
)
_SESSION_HASH = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


class NativeIdentity(str):
    """Server-side marker, not a reserved prefix in a legacy LMS subject."""


def native_identity(session):
    user = session.get("user")
    if not isinstance(user, dict) or user.get("lti_version") != "1.3":
        return None
    values = [user.get(field) for field in _SCOPE_FIELDS]
    values.append(session.get("lti13_launch_id"))
    if any(not isinstance(value, str) or not value for value in values):
        return None
    encoded = json.dumps(values, separators=(",", ":")).encode()
    return NativeIdentity(NATIVE_IDENTITY_PREFIX + hashlib.sha256(encoded).hexdigest())


def _caller_identity(session):
    user = session.get("user")
    if not isinstance(user, dict) or not user.get("user_id"):
        return None
    if user.get("lti_version") == "1.3":
        return native_identity(session)
    # Preserve legacy callback usernames while binding their state to scope too.
    values = [str(user.get(field, "")) for field in (
        "user_id", "school_id", "oauth_consumer_key", "context_id",
    )]
    encoded = json.dumps(values, separators=(",", ":")).encode()
    return "prime-lti11:" + hashlib.sha256(encoded).hexdigest()


class NativeGradioScopeMiddleware(BaseHTTPMiddleware):
    """Bind state to a signed launch, never to a browser-chosen hash alone.

    Ownership lives in Gradio's bounded state cache. If that cache evicts an
    owner while a queue still retains results, deny rather than rebind the hash.
    Gradio's API event records retain the scoped authenticated username, which
    also guards its stateless call/stream result paths.
    """

    def __init__(self, app, *, blocks, path):
        super().__init__(app)
        self.blocks = blocks
        self.path = path.rstrip("/") + "/"
        # Only active HTTP admissions live here; always released in finally.
        self.admissions = {}

    async def __call__(self, scope, receive, send):
        if scope["type"] == "websocket" and scope["path"].startswith(self.path):
            # This UI uses SSE, not Gradio's unauthenticated event-mutation WS.
            await send({"type": "websocket.close", "code": 1008})
            return
        await super().__call__(scope, receive, send)

    @staticmethod
    def denied():
        return JSONResponse(
            {"detail": "Your PRIME session changed. Reload this page before continuing."},
            status_code=403,
        )

    def bind(self, session_hash, identity):
        if not isinstance(session_hash, str) or not _SESSION_HASH.fullmatch(session_hash):
            return False
        admission = self.admissions.get(session_hash)
        if admission is not None and admission[0] != identity:
            return False
        holder = self.blocks.state_holder
        state = holder.session_data.get(session_hash)
        owner = getattr(state, "_prime_identity", None)
        queue = self.blocks._queue
        for event_id in queue.pending_event_ids_session.get(session_hash, ()):
            pending = queue.event_ids_to_events.get(event_id)
            if pending is not None and isinstance(pending.username, NativeIdentity):
                if pending.username != identity:
                    return False
        if owner is not None:
            if owner != identity:
                return False
            holder[session_hash]  # Refresh the same owner's existing cache entry.
            return True
        event = queue.event_ids_to_events.get(session_hash)
        if event is not None and isinstance(event.username, NativeIdentity):
            if event.username != identity:
                return False
        elif (state is not None or session_hash in queue.pending_messages_per_session
              or session_hash in queue.pending_event_ids_session
              or session_hash in self.blocks.pending_streams):
            # Existing unowned state/retained results are not safe to adopt.
            return False
        if identity is not None:
            holder[session_hash]._prime_identity = identity
        return True

    async def dispatch(self, request, call_next):
        if not request.url.path.startswith(self.path):
            return await call_next(request)
        relative = request.url.path[len(self.path):].removeprefix("gradio_api/")
        parts = relative.strip("/").split("/")
        if parts[0] == "deep_link" or "deep_link" in request.query_params:
            return self.denied()
        if parts[0] not in {"api", "run", "call", "queue", "cancel", "reset", "stream", "heartbeat"}:
            return await call_next(request)
        session = request.scope.get("session", {})
        identity = _caller_identity(session)
        user = session.get("user", {})
        if isinstance(user, dict) and user.get("lti_version") == "1.3" and identity is None:
            return self.denied()
        if not isinstance(user, dict) or not user.get("user_id"):
            return JSONResponse({"detail": "Not authenticated."}, status_code=401)
        session_hash = request.query_params.get("session_hash")
        event_id = None
        if request.method == "POST" and not (parts[0] == "stream" and parts[-1] == "close"):
            try:
                body = await request.json()
            except (ValueError, UnicodeError):
                return JSONResponse({"detail": "Invalid request."}, status_code=400)
            if not isinstance(body, dict):
                return JSONResponse({"detail": "Invalid request."}, status_code=400)
            session_hash = body.get("session_hash", session_hash)
            event_id = body.get("event_id")
        if parts[0] == "heartbeat" and len(parts) == 2:
            session_hash = parts[1]
        elif parts[0] == "stream" and request.method == "GET" and len(parts) >= 4:
            session_hash = parts[1]
        elif parts[0] == "stream" and len(parts) >= 2:
            event_id = parts[1]
        elif parts[0] == "call" and request.method == "GET" and len(parts) == 3:
            event_id = parts[2]
        if event_id is not None:
            if not isinstance(event_id, str):
                return self.denied()
            event = self.blocks._queue.event_ids_to_events.get(event_id)
            if event is not None and isinstance(event.username, NativeIdentity):
                if event.username != identity:
                    return self.denied()
        if session_hash is not None and not self.bind(session_hash, identity):
            return self.denied()
        if session_hash is None:
            return await call_next(request)
        admission = self.admissions.setdefault(session_hash, [identity, 0])
        admission[1] += 1
        try:
            return await call_next(request)
        finally:
            admission[1] -= 1
            if admission[1] == 0:
                del self.admissions[session_hash]
