from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import os
import socket
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import jwt
import requests
from bson import ObjectId
from bson.errors import InvalidId
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, RedirectResponse, Response
from jwcrypto.jwk import JWK
from pylti1p3.cookie import CookieService
from pylti1p3.exception import LtiException, OIDCException
from pylti1p3.launch_data_storage.base import LaunchDataStorage
from pylti1p3.message_launch import MessageLaunch
from pylti1p3.oidc_login import OIDCLogin
from pylti1p3.redirect import Redirect
from pylti1p3.request import Request as LtiRequest
from pylti1p3.session import SessionService
from pylti1p3.tool_config import ToolConfDict
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, PyMongoError

LTI_CLAIM = "https://purl.imsglobal.org/spec/lti/claim/"
RESOURCE_LINK_MESSAGE = "LtiResourceLinkRequest"
MAX_FORM_BYTES = 128 * 1024
MAX_JWKS_BYTES = 1024 * 1024


class LTI13ConfigurationError(ValueError):
    pass


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)


def consume_lti11_nonce(database: Database, params: Mapping[str, Any]) -> bool:
    consumer_key = str(params.get("oauth_consumer_key", "")).strip()
    nonce = str(params.get("oauth_nonce", "")).strip()
    timestamp_value = str(params.get("oauth_timestamp", "")).strip()
    if not consumer_key or not nonce or len(nonce) > 256:
        return False
    try:
        timestamp = int(timestamp_value)
    except ValueError:
        return False
    now = int(time.time())
    if abs(now - timestamp) > 300:
        return False
    collection = database["lti11_nonces"]
    collection.create_index(
        [("expires_at", ASCENDING)], expireAfterSeconds=0, name="lti11_nonce_ttl"
    )
    nonce_id = hashlib.sha256(f"{consumer_key}\x00{nonce}".encode()).hexdigest()
    try:
        collection.insert_one(
            {
                "_id": nonce_id,
                "expires_at": datetime.now(timezone.utc) + timedelta(minutes=10),
            }
        )
    except DuplicateKeyError:
        return False
    return True


def normalize_base_path(path_prefix: str) -> str:
    return "/" + path_prefix.strip("/")


def validate_url(value: str, field_name: str, *, allow_http: bool = False) -> str:
    value = value.strip()
    parsed = urlparse(value)
    allowed_schemes = {"https"}
    if allow_http:
        allowed_schemes.add("http")
    if parsed.scheme not in allowed_schemes or not parsed.hostname:
        expected = "HTTPS URL" if not allow_http else "HTTP or HTTPS URL"
        raise LTI13ConfigurationError(f"{field_name} must be an absolute {expected}.")
    if parsed.username or parsed.password or parsed.fragment:
        raise LTI13ConfigurationError(
            f"{field_name} must not contain credentials or a URL fragment."
        )
    return value


def assert_safe_remote_url(url: str) -> None:
    allow_private = env_flag("LTI13_ALLOW_PRIVATE_NETWORKS", False)
    parsed = urlparse(url)
    if not parsed.hostname:
        raise LTI13ConfigurationError("Remote URL has no hostname.")
    try:
        addresses = {
            ipaddress.ip_address(item[4][0])
            for item in socket.getaddrinfo(
                parsed.hostname,
                parsed.port or (443 if parsed.scheme == "https" else 80),
                type=socket.SOCK_STREAM,
            )
        }
    except (OSError, ValueError) as exc:
        raise LTI13ConfigurationError("Remote hostname could not be resolved.") from exc
    if not addresses:
        raise LTI13ConfigurationError("Remote hostname resolved to no addresses.")
    if not allow_private and any(not address.is_global for address in addresses):
        raise LTI13ConfigurationError(
            "Remote URL resolves to a private, loopback, link-local, or reserved address."
        )


@dataclass(frozen=True)
class PlatformRegistration:
    id: str | None
    name: str
    issuer: str
    client_id: str
    deployment_ids: tuple[str, ...]
    auth_login_url: str
    auth_token_url: str
    auth_audience: str | None
    key_set_url: str
    enabled: bool = True

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> PlatformRegistration:
        deployments_value = data.get("deployment_ids", ())
        if isinstance(deployments_value, str):
            deployments = tuple(
                item.strip()
                for item in deployments_value.replace(",", "\n").splitlines()
                if item.strip()
            )
        else:
            deployments = tuple(
                str(item).strip() for item in deployments_value if str(item).strip()
            )
        object_id = data.get("_id") or data.get("id")
        audience_value = data.get("auth_audience")
        auth_audience = (
            str(audience_value).strip() if audience_value is not None else ""
        ) or None
        enabled_value = data.get("enabled", False)
        enabled = (
            enabled_value
            if isinstance(enabled_value, bool)
            else str(enabled_value).strip().lower() in {"1", "true", "yes", "on"}
        )
        return cls(
            id=str(object_id) if object_id else None,
            name=str(data.get("name", "")).strip(),
            issuer=str(data.get("issuer", "")).strip(),
            client_id=str(data.get("client_id", "")).strip(),
            deployment_ids=deployments,
            auth_login_url=str(data.get("auth_login_url", "")).strip(),
            auth_token_url=str(data.get("auth_token_url", "")).strip(),
            auth_audience=auth_audience,
            key_set_url=str(data.get("key_set_url", "")).strip(),
            enabled=enabled,
        )

    def validate(self) -> PlatformRegistration:
        allow_http = env_flag("LTI13_ALLOW_INSECURE_HTTP", False)
        if not self.name:
            raise LTI13ConfigurationError("Platform name is required.")
        if not self.client_id:
            raise LTI13ConfigurationError("Client ID is required.")
        if not self.deployment_ids:
            raise LTI13ConfigurationError("At least one deployment ID is required.")
        if len(set(self.deployment_ids)) != len(self.deployment_ids):
            raise LTI13ConfigurationError("Deployment IDs must be unique.")
        validate_url(self.issuer, "Issuer", allow_http=allow_http)
        validate_url(
            self.auth_login_url, "OIDC authentication endpoint", allow_http=allow_http
        )
        validate_url(
            self.auth_token_url, "OAuth 2 token endpoint", allow_http=allow_http
        )
        validate_url(self.key_set_url, "Platform JWKS URL", allow_http=allow_http)
        if self.auth_audience:
            validate_url(self.auth_audience, "OAuth 2 audience", allow_http=allow_http)
        return self

    def as_document(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "issuer": self.issuer,
            "client_id": self.client_id,
            "deployment_ids": list(self.deployment_ids),
            "auth_login_url": self.auth_login_url,
            "auth_token_url": self.auth_token_url,
            "auth_audience": self.auth_audience,
            "key_set_url": self.key_set_url,
            "enabled": self.enabled,
        }


class PlatformRepository:
    def __init__(self, database: Database):
        self.database = database
        self.collection: Collection = database["lti13_platforms"]
        self.state_collection: Collection = database["lti13_state"]
        self._indexes_ready = False

    def ensure_indexes(self) -> None:
        if self._indexes_ready:
            return
        self.collection.create_index(
            [("issuer", ASCENDING), ("client_id", ASCENDING)],
            unique=True,
            name="lti13_issuer_client_unique",
        )
        self.state_collection.create_index(
            [("expires_at", ASCENDING)], expireAfterSeconds=0, name="lti13_state_ttl"
        )
        self._indexes_ready = True

    def list_all(self) -> list[PlatformRegistration]:
        self.ensure_indexes()
        return [
            PlatformRegistration.from_mapping(item)
            for item in self.collection.find().sort("name", ASCENDING)
        ]

    def list_enabled(self) -> list[PlatformRegistration]:
        self.ensure_indexes()
        return [
            PlatformRegistration.from_mapping(item)
            for item in self.collection.find({"enabled": True}).sort("name", ASCENDING)
        ]

    def get(self, registration_id: str) -> PlatformRegistration | None:
        self.ensure_indexes()
        try:
            object_id = ObjectId(registration_id)
        except InvalidId:
            return None
        item = self.collection.find_one({"_id": object_id})
        return PlatformRegistration.from_mapping(item) if item else None

    def save(self, registration: PlatformRegistration) -> PlatformRegistration:
        self.ensure_indexes()
        registration.validate()
        document = registration.as_document()
        document["updated_at"] = datetime.now(timezone.utc)
        if registration.id:
            try:
                object_id = ObjectId(registration.id)
            except InvalidId as exc:
                raise LTI13ConfigurationError(
                    "Invalid registration identifier."
                ) from exc
            result = self.collection.update_one({"_id": object_id}, {"$set": document})
            if result.matched_count != 1:
                raise LTI13ConfigurationError("Registration was not found.")
        else:
            document["created_at"] = document["updated_at"]
            object_id = self.collection.insert_one(document).inserted_id
        saved = self.get(str(object_id))
        if not saved:
            raise LTI13ConfigurationError("Registration could not be reloaded.")
        return saved

    def delete(self, registration_id: str) -> bool:
        self.ensure_indexes()
        try:
            object_id = ObjectId(registration_id)
        except InvalidId:
            return False
        return self.collection.delete_one({"_id": object_id}).deleted_count == 1


class MongoLaunchDataStorage(LaunchDataStorage[Any]):
    def __init__(self, collection: Collection):
        super().__init__()
        self.collection = collection

    def get_session_cookie_name(self) -> str | None:
        if self._request and not self._request.is_secure():
            return None
        return super().get_session_cookie_name()

    def can_set_keys_expiration(self) -> bool:
        return True

    def get_value(self, key: str) -> Any:
        item = self.collection.find_one(
            {
                "_id": self._prepare_key(key),
                "expires_at": {"$gt": datetime.now(timezone.utc)},
            }
        )
        return item.get("value") if item else None

    def set_value(self, key: str, value: Any, exp: int | None = None) -> None:
        lifetime = exp if exp is not None else 3600
        self.collection.replace_one(
            {"_id": self._prepare_key(key)},
            {
                "_id": self._prepare_key(key),
                "value": value,
                "expires_at": datetime.now(timezone.utc) + timedelta(seconds=lifetime),
            },
            upsert=True,
        )

    def check_value(self, key: str) -> bool:
        item = self.collection.find_one_and_delete(
            {
                "_id": self._prepare_key(key),
                "expires_at": {"$gt": datetime.now(timezone.utc)},
            }
        )
        return item is not None


@dataclass(frozen=True)
class ToolKeyMaterial:
    private_pem: str
    public_pem: str
    kid: str
    jwks: dict[str, Any]


def load_tool_key_material() -> ToolKeyMaterial:
    private_value = os.getenv("LTI13_PRIVATE_KEY", "").replace("\\n", "\n").strip()
    key_path_value = os.getenv(
        "LTI13_PRIVATE_KEY_FILE", "/run/secrets/lti13_private.pem"
    )
    if not private_value and key_path_value:
        key_path = Path(key_path_value)
        try:
            if key_path.is_file():
                if key_path.stat().st_size > 64 * 1024:
                    raise LTI13ConfigurationError(
                        "LTI 1.3 private key file is unexpectedly large."
                    )
                private_value = key_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise LTI13ConfigurationError(
                "LTI 1.3 private key file cannot be read."
            ) from exc
    if not private_value:
        raise LTI13ConfigurationError(
            "LTI 1.3 private key is not configured. Set LTI13_PRIVATE_KEY_FILE."
        )
    try:
        private_key = serialization.load_pem_private_key(
            private_value.encode("utf-8"), password=None
        )
    except (TypeError, ValueError) as exc:
        raise LTI13ConfigurationError("LTI 1.3 private key is not valid PEM.") from exc
    if not isinstance(private_key, rsa.RSAPrivateKey) or private_key.key_size < 2048:
        raise LTI13ConfigurationError(
            "LTI 1.3 requires an RSA private key of at least 2048 bits."
        )
    public_bytes = private_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_pem = public_bytes.decode("utf-8")
    public_jwk = json.loads(JWK.from_pem(public_bytes).export_public())
    configured_kid = os.getenv("LTI13_KEY_ID", "").strip()
    if configured_kid:
        kid = configured_kid
    else:
        digest = hashlib.sha256(
            private_key.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        ).digest()
        kid = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    public_jwk.update({"kid": kid, "alg": "RS256", "use": "sig"})
    return ToolKeyMaterial(
        private_pem=private_value + "\n",
        public_pem=public_pem,
        kid=kid,
        jwks={"keys": [public_jwk]},
    )


def build_tool_configuration(
    repository: PlatformRepository, key_material: ToolKeyMaterial
) -> ToolConfDict:
    settings: dict[str, list[dict[str, Any]]] = {}
    for registration in repository.list_enabled():
        registration.validate()
        settings.setdefault(registration.issuer, []).append(
            {
                "default": False,
                "client_id": registration.client_id,
                "auth_login_url": registration.auth_login_url,
                "auth_token_url": registration.auth_token_url,
                "auth_audience": registration.auth_audience,
                "key_set_url": registration.key_set_url,
                "key_set": None,
                "deployment_ids": list(registration.deployment_ids),
            }
        )
    for registrations in settings.values():
        if len(registrations) == 1:
            registrations[0]["default"] = True
    tool_configuration = ToolConfDict(settings)
    for issuer, registrations in settings.items():
        for registration in registrations:
            client_id = registration["client_id"]
            tool_configuration.set_private_key(
                issuer, key_material.private_pem, client_id=client_id
            )
            tool_configuration.set_public_key(
                issuer, key_material.public_pem, client_id=client_id
            )
    return tool_configuration


class FastAPIRequestAdapter(LtiRequest):
    def __init__(self, request: Request, data: Mapping[str, str]):
        self.request = request
        self.data = data

    @property
    def session(self) -> dict[str, Any]:
        return self.request.session

    def is_secure(self) -> bool:
        return self.request.url.scheme == "https"

    def get_param(self, key: str) -> str | None:
        return self.data.get(key)

    def get_cookie(self, key: str) -> str | None:
        return self.request.cookies.get(key)


class FastAPICookieService(CookieService):
    def __init__(self, request: FastAPIRequestAdapter):
        self.request = request
        self.cookies_to_set: dict[str, dict[str, Any]] = {}

    def _get_key(self, key: str) -> str:
        return f"{self._cookie_prefix}-{key}"

    def get_cookie(self, name: str) -> str | None:
        return self.request.get_cookie(self._get_key(name))

    def set_cookie(self, name: str, value: str, exp: int | None = 3600) -> None:
        self.cookies_to_set[self._get_key(name)] = {"value": value, "exp": exp}

    def update_response(self, response: Response) -> Response:
        secure = self.request.is_secure()
        for key, cookie_data in self.cookies_to_set.items():
            response.set_cookie(
                key=key,
                value=cookie_data["value"],
                max_age=cookie_data["exp"],
                secure=secure,
                path="/",
                httponly=True,
                samesite="none" if secure else "lax",
            )
        return response


class FastAPIRedirect(Redirect[Response]):
    def __init__(self, location: str, cookie_service: FastAPICookieService):
        super().__init__()
        self.location = location
        self.cookie_service = cookie_service

    def do_redirect(self) -> Response:
        return self.cookie_service.update_response(
            RedirectResponse(self.location, status_code=302)
        )

    def do_js_redirect(self) -> Response:
        raise LTI13ConfigurationError("JavaScript redirects are not enabled.")

    def set_redirect_url(self, location: str) -> None:
        self.location = location

    def get_redirect_url(self) -> str:
        return self.location


class FastAPISessionService(SessionService):
    pass


class FastAPIOIDCLogin(OIDCLogin):
    def __init__(
        self,
        request: FastAPIRequestAdapter,
        tool_configuration: ToolConfDict,
        storage: MongoLaunchDataStorage,
    ):
        self.cookie_service = FastAPICookieService(request)
        super().__init__(
            request=request,
            tool_config=tool_configuration,
            session_service=FastAPISessionService(request),
            cookie_service=self.cookie_service,
            launch_data_storage=storage,
        )

    def get_redirect(self, url: str) -> FastAPIRedirect:
        return FastAPIRedirect(url, self.cookie_service)


class FastAPIMessageLaunch(MessageLaunch):
    def __init__(
        self,
        request: FastAPIRequestAdapter,
        tool_configuration: ToolConfDict,
        storage: MongoLaunchDataStorage,
        requests_session: requests.Session,
    ):
        self.cookie_service = FastAPICookieService(request)
        super().__init__(
            request=request,
            tool_config=tool_configuration,
            session_service=FastAPISessionService(request),
            cookie_service=self.cookie_service,
            launch_data_storage=storage,
            requests_session=requests_session,
        )

    def _get_request_param(self, key: str) -> str | None:
        return self._request.get_param(key)


class SafeRequestsSession(requests.Session):
    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        assert_safe_remote_url(url)
        kwargs.setdefault("timeout", (3.05, 8.0))
        kwargs["allow_redirects"] = False
        kwargs["stream"] = True
        response = super().request(method, url, **kwargs)
        completed = False
        try:
            if response.is_redirect or response.is_permanent_redirect:
                raise requests.RequestException(
                    "Redirects from platform endpoints are not allowed."
                )
            response.raise_for_status()
            try:
                content_length = int(response.headers.get("content-length", "0") or "0")
            except ValueError as exc:
                raise requests.RequestException(
                    "Platform response has an invalid Content-Length header."
                ) from exc
            if content_length > MAX_JWKS_BYTES:
                raise requests.RequestException("Platform response is too large.")
            body = bytearray()
            for chunk in response.iter_content(chunk_size=64 * 1024):
                body.extend(chunk)
                if len(body) > MAX_JWKS_BYTES:
                    raise requests.RequestException("Platform response is too large.")
            response._content = bytes(body)
            response._content_consumed = True
            completed = True
            return response
        finally:
            if not completed:
                response.close()


async def read_request_data(request: Request) -> dict[str, str]:
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_FORM_BYTES:
                raise HTTPException(status_code=413, detail="LTI request is too large.")
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="Invalid Content-Length header."
            ) from exc
    if request.method == "GET":
        return {key: value for key, value in request.query_params.items()}
    content_type = request.headers.get("content-type", "").lower()
    if (
        "application/x-www-form-urlencoded" not in content_type
        and "multipart/form-data" not in content_type
    ):
        raise HTTPException(
            status_code=415, detail="LTI requests must use form encoding."
        )
    form = await request.form()
    return {str(key): str(value) for key, value in form.items()}


def require_rs256_header(id_token: str) -> None:
    try:
        header = jwt.get_unverified_header(id_token)
    except jwt.InvalidTokenError as exc:
        raise LTI13ConfigurationError("The LTI id_token header is invalid.") from exc
    if header.get("alg") != "RS256" or not header.get("kid"):
        raise LTI13ConfigurationError(
            "The LTI id_token must use RS256 and include a key ID."
        )


def validate_launch_claims(
    launch_data: Mapping[str, Any],
    client_id: str,
    expected_target_link_uri: str,
) -> None:
    now = int(time.time())
    try:
        issued_at = int(launch_data["iat"])
        expires_at = int(launch_data["exp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LTI13ConfigurationError(
            "The LTI id_token requires integer iat and exp claims."
        ) from exc
    max_age = bounded_env_int(
        "LTI13_MAX_TOKEN_AGE_SECONDS", default=300, minimum=60, maximum=600
    )
    clock_skew = bounded_env_int(
        "LTI13_CLOCK_SKEW_SECONDS", default=60, minimum=0, maximum=120
    )
    if issued_at > now + clock_skew or issued_at < now - max_age:
        raise LTI13ConfigurationError(
            "The LTI id_token was issued outside the allowed time window."
        )
    if expires_at <= now - clock_skew or expires_at > issued_at + max_age + clock_skew:
        raise LTI13ConfigurationError("The LTI id_token expiration is invalid.")
    audience = launch_data.get("aud")
    if (
        isinstance(audience, list)
        and len(audience) > 1
        and launch_data.get("azp") != client_id
    ):
        raise LTI13ConfigurationError(
            "The LTI id_token azp claim must identify this client when aud has multiple values."
        )
    target_link_uri = str(launch_data.get(f"{LTI_CLAIM}target_link_uri", ""))
    if not hmac.compare_digest(target_link_uri, expected_target_link_uri):
        raise LTI13ConfigurationError(
            "The LTI target_link_uri does not match the login request."
        )
    if launch_data.get(f"{LTI_CLAIM}message_type") != RESOURCE_LINK_MESSAGE:
        raise LTI13ConfigurationError("Only LTI Resource Link launches are supported.")
    if not str(launch_data.get("sub", "")).strip():
        raise LTI13ConfigurationError("The LTI subject claim is required.")


def user_info_from_launch(launch_data: Mapping[str, Any]) -> dict[str, Any]:
    issuer = str(launch_data["iss"])
    audience = launch_data["aud"]
    client_id = str(audience[0] if isinstance(audience, list) else audience)
    deployment_id = str(launch_data[f"{LTI_CLAIM}deployment_id"])
    context = launch_data.get(f"{LTI_CLAIM}context") or {}
    resource_link = launch_data.get(f"{LTI_CLAIM}resource_link") or {}
    roles_value = launch_data.get(f"{LTI_CLAIM}roles") or []
    roles = (
        [str(role) for role in roles_value]
        if isinstance(roles_value, list)
        else [str(roles_value)]
    )
    return {
        "user_id": str(launch_data["sub"]),
        "roles": ",".join(roles),
        "browser_language": "en",
        "oauth_consumer_key": client_id,
        "context_id": str(context.get("id", "unknown")),
        "context_title": str(context.get("title", context.get("label", "unknown"))),
        "school_id": f"{issuer}::{deployment_id}",
        "lti_version": "1.3",
        "lti_issuer": issuer,
        "lti_client_id": client_id,
        "lti_deployment_id": deployment_id,
        "lti_resource_link_id": str(resource_link.get("id", "")),
    }


class LTI13Service:
    def __init__(self, database: Database, path_prefix: str):
        self.repository = PlatformRepository(database)
        self.base_path = normalize_base_path(path_prefix)

    def _public_base_url(self, request: Request) -> str:
        configured = os.getenv("LTI13_PUBLIC_BASE_URL", "").strip().rstrip("/")
        if configured:
            return validate_url(
                configured,
                "LTI13_PUBLIC_BASE_URL",
                allow_http=env_flag("LTI13_ALLOW_INSECURE_HTTP", False),
            )
        return f"{str(request.base_url).rstrip('/')}{self.base_path}"

    def endpoint_urls(self, request: Request) -> dict[str, str]:
        base_url = self._public_base_url(request)
        return {
            "login": f"{base_url}/lti/1p3/login",
            "launch": f"{base_url}/lti/1p3/launch",
            "jwks": f"{base_url}/lti/1p3/jwks",
            "target": f"{base_url}/ui/",
        }

    def _validate_target_link_uri(self, request: Request, target_link_uri: str) -> str:
        allow_http = env_flag("LTI13_ALLOW_INSECURE_HTTP", False)
        target_link_uri = validate_url(
            target_link_uri, "target_link_uri", allow_http=allow_http
        )
        public_base = urlparse(self._public_base_url(request))
        target = urlparse(target_link_uri)
        if target.scheme != public_base.scheme or target.netloc != public_base.netloc:
            raise LTI13ConfigurationError(
                "target_link_uri must use this tool's public origin."
            )
        base_path = public_base.path.rstrip("/")
        if target.path != base_path and not target.path.startswith(base_path + "/"):
            raise LTI13ConfigurationError(
                "target_link_uri must stay within this tool's base path."
            )
        return target_link_uri

    def _tool_configuration(self) -> ToolConfDict:
        return build_tool_configuration(self.repository, load_tool_key_material())

    def _storage(self) -> MongoLaunchDataStorage:
        self.repository.ensure_indexes()
        return MongoLaunchDataStorage(self.repository.state_collection)

    async def oidc_login(self, request: Request) -> Response:
        try:
            data = await read_request_data(request)
            target_link_uri = self._validate_target_link_uri(
                request, data.get("target_link_uri", "")
            )
            adapter = FastAPIRequestAdapter(request, data)
            login = FastAPIOIDCLogin(
                adapter, self._tool_configuration(), self._storage()
            ).pass_params_to_launch({"target_link_uri": target_link_uri})
            response = login.redirect(self.endpoint_urls(request)["launch"])
            response.headers["Cache-Control"] = "no-store"
            return response
        except (
            LTI13ConfigurationError,
            LtiException,
            OIDCException,
            PyMongoError,
        ) as exc:
            print(f"[LTI 1.3] OIDC login rejected: {type(exc).__name__}")
            raise HTTPException(
                status_code=400, detail="LTI 1.3 login request was rejected."
            ) from exc

    async def launch(self, request: Request) -> Response:
        try:
            data = await read_request_data(request)
            require_rs256_header(data.get("id_token", ""))
            adapter = FastAPIRequestAdapter(request, data)
            storage = self._storage()
            message_launch = FastAPIMessageLaunch(
                adapter,
                self._tool_configuration(),
                storage,
                SafeRequestsSession(),
            )
            message_launch.set_public_key_caching(storage, cache_lifetime=3600)
            await run_in_threadpool(message_launch.validate)
            launch_data = message_launch.get_launch_data()
            login_params = message_launch.get_params_from_login() or {}
            expected_target = str(login_params.get("target_link_uri", ""))
            client_id = message_launch.get_client_id()
            validate_launch_claims(launch_data, client_id, expected_target)
            request.session.clear()
            request.session["user"] = user_info_from_launch(launch_data)
            request.session["lti13_launch_id"] = message_launch.get_launch_id()
            response = RedirectResponse(
                url=f"{self.base_path}/ui/", status_code=status.HTTP_303_SEE_OTHER
            )
            message_launch.cookie_service.update_response(response)
            response.headers["Cache-Control"] = "no-store"
            return response
        except (
            KeyError,
            TypeError,
            LTI13ConfigurationError,
            LtiException,
            PyMongoError,
            requests.RequestException,
        ) as exc:
            print(f"[LTI 1.3] Launch rejected: {type(exc).__name__}")
            raise HTTPException(
                status_code=401, detail="LTI 1.3 launch was rejected."
            ) from exc

    def jwks_response(self) -> JSONResponse:
        try:
            response = JSONResponse(load_tool_key_material().jwks)
        except LTI13ConfigurationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        response.headers["Cache-Control"] = "public, max-age=3600"
        return response

    def status_response(self) -> JSONResponse:
        try:
            configured = bool(self.repository.list_enabled())
            database_ready = True
        except PyMongoError:
            configured = False
            database_ready = False
        try:
            load_tool_key_material()
            key_ready = True
        except LTI13ConfigurationError:
            key_ready = False
        return JSONResponse(
            {
                "lti_1_1_supported": True,
                "lti_1_3_supported": True,
                "lti_1_3_platform_configured": configured,
                "lti_1_3_key_ready": key_ready,
                "database_ready": database_ready,
            },
            status_code=200 if database_ready and key_ready else 503,
        )

    async def health_check(
        self, registration: PlatformRegistration
    ) -> list[dict[str, str]]:
        checks: list[dict[str, str]] = []
        try:
            registration.validate()
            checks.append(
                {
                    "name": "Configuration",
                    "status": "pass",
                    "detail": "Required fields and URLs are valid.",
                }
            )
        except LTI13ConfigurationError as exc:
            return [{"name": "Configuration", "status": "fail", "detail": str(exc)}]
        try:
            key = load_tool_key_material()
            checks.append(
                {
                    "name": "Tool signing key",
                    "status": "pass",
                    "detail": f"RSA key is ready (kid {key.kid}).",
                }
            )
        except LTI13ConfigurationError as exc:
            checks.append(
                {"name": "Tool signing key", "status": "fail", "detail": str(exc)}
            )
        try:
            await run_in_threadpool(
                self.repository.database.client.admin.command, "ping"
            )
            checks.append(
                {
                    "name": "Shared database",
                    "status": "pass",
                    "detail": "MongoDB is reachable.",
                }
            )
        except PyMongoError:
            checks.append(
                {
                    "name": "Shared database",
                    "status": "fail",
                    "detail": "MongoDB is unavailable.",
                }
            )
        try:
            response = await run_in_threadpool(
                SafeRequestsSession().get, registration.key_set_url
            )
            payload = response.json()
            keys = payload.get("keys") if isinstance(payload, dict) else None
            signing_keys = [
                item
                for item in keys or []
                if isinstance(item, dict)
                and item.get("kty") == "RSA"
                and item.get("kid")
                and item.get("use", "sig") == "sig"
            ]
            if not signing_keys:
                raise LTI13ConfigurationError(
                    "JWKS contains no identified RSA signing key."
                )
            checks.append(
                {
                    "name": "Platform JWKS",
                    "status": "pass",
                    "detail": f"Fetched {len(signing_keys)} RSA signing key(s).",
                }
            )
        except (requests.RequestException, ValueError, LTI13ConfigurationError) as exc:
            checks.append(
                {"name": "Platform JWKS", "status": "fail", "detail": str(exc)}
            )
        return checks


def create_lti13_service(mongo_url: str, path_prefix: str) -> LTI13Service:
    client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
    return LTI13Service(client["prime"], path_prefix)
