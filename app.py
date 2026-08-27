#lti
from fastapi import FastAPI, Request, HTTPException, Depends, status, APIRouter
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware # for autocomplete
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.status import HTTP_404_NOT_FOUND
import uvicorn
import httpx
from pylti.common import verify_request_common, LTIException
from dotenv import load_dotenv
from lti13 import (
    bounded_env_int,
    consume_lti11_nonce,
    create_lti13_service,
    env_flag,
    read_request_data,
)


# gradio_app.pyのASGIアプリをインポート！
from gradio import mount_gradio_app
from gradio_app import demo as demo1  # ← gradio_app.pyで定義したBlocks
from gradio_evaluation_app import demo as demo2

#general
import os
from dotenv import load_dotenv
import gradio as gr

from fastapi.templating import Jinja2Templates

load_dotenv()

app = FastAPI()
router = APIRouter()

allowed_origins = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
]
if allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

allowed_hosts = [
    host.strip()
    for host in os.getenv("ALLOWED_HOSTS", "127.0.0.1,localhost").split(",")
    if host.strip()
]
app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)

path_prefix = os.getenv("APP_PATH_PREFIX", "mathquizgen").strip("/")
session_secret = os.getenv("FASTAPI_SECRET_KEY", "")
if len(session_secret) < 32:
    raise RuntimeError("FASTAPI_SECRET_KEY must contain at least 32 characters.")
session_https_only = env_flag("SESSION_HTTPS_ONLY", False)
app.add_middleware(
    SessionMiddleware,
    secret_key=session_secret,
    https_only=session_https_only,
    same_site="none" if session_https_only else "lax",
    max_age=bounded_env_int(
        "SESSION_MAX_AGE_SECONDS", default=28800, minimum=300, maximum=86400
    ),
)

@app.on_event("startup")
def startup_event():
    print("[46] Server startup: Initializing database")
    app.mount(f"/{path_prefix}/static", StaticFiles(directory="static"), name="static")

def get_browser_language(request: Request):
    try:
        accept_language = request.headers.get("accept-language", "en")
        languages = accept_language.split(',')
        for lang in languages:
            if lang.strip().lower().startswith('ja'):
                print(f"[55] Browser language: {lang}")
                return 'ja'
            elif lang.strip().lower().startswith('en'):
                print(f"[57] Browser language: {lang}")
                return 'en'
        # If no match found, default to the first language in the list
        primary_language = languages[0].split(';')[0].split('-')[0].lower()
        print(f"[62] Browser language: {primary_language}")
    except Exception as e:
        primary_language = 'en'  # default to English
        print(f"[65] Error getting browser language: {str(e)}")
    return primary_language

# Set up Jinja2 templates
templates = Jinja2Templates(directory="templates")
mongo_url = os.getenv("MONGO_URL", "")
if not mongo_url:
    raise RuntimeError("MONGO_URL is required.")
lti13_service = create_lti13_service(mongo_url, path_prefix)

LTI_URL = os.getenv(
    "LTI_URL", f"https://lab.let.media.kyoto-u.ac.jp/{path_prefix}/lti/login"
)

css = """
h1 {
    text-align: center;
    display: block;
}
"""
###########################

def load_lti_credentials():
    consumers = {}
    try:
        maximum = min(max(int(os.getenv("MAX_GENERATION", "100")), 1), 100)
    except ValueError:
        maximum = 100
    for index in range(1, maximum + 1):
        key = os.getenv(f"LTI_CONSUMER_KEY_{index}")
        secret = os.getenv(f"LTI_SHARED_SECRET_{index}")
        if key and secret:
            consumers[key] = {"secret": secret}
    return consumers


LTI_CONSUMERS = load_lti_credentials()

# LTI Request validation
async def validate_lti_request(request: Request):
    form_data = await read_request_data(request)
    if form_data.get("oauth_signature_method") != "HMAC-SHA1":
        print("[LTI 1.1] Unsupported signature method rejected")
        return False

    # dictionary of consumers
    consumers = LTI_CONSUMERS
    print("[LTI 1.1] Validating signed launch request")
    common_request_verification = False

    try: 
        # Call verify_request_common with all the necessary parameters
        common_request_verification = verify_request_common(
            consumers=consumers,
            url=LTI_URL,#str(request.url),
            method=request.method,
            headers=dict(request.headers),
            params=dict(form_data)  # Ensure this is a dict if not already
        )
        if common_request_verification and not consume_lti11_nonce(
            lti13_service.repository.database, form_data
        ):
            print("[LTI 1.1] Launch replay or invalid timestamp rejected")
            return False
        print("[LTI 1.1] Launch signature and nonce validated")

    except LTIException:
        print("[LTI 1.1] Launch signature validation failed")

    return common_request_verification


async def get_current_user(request: Request): # Dependsと関わってくる
    user = request.session.get('user', {})

    if not user:
        print("[Auth] Request has no authenticated session")
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

def gradio_auth_dependency(request: Request):
    user = request.session.get("user", {})
    user_id = user.get("user_id") if isinstance(user, dict) else None
    return str(user_id) if user_id else None


####################################### apps in separate dockers

@app.exception_handler(Exception)
async def universal_exception_handler(request: Request, exc: Exception):
    print(
        f"[Error] Unhandled {type(exc).__name__} on "
        f"{request.method} {request.url.path}"
    )
    status_code = exc.status_code if isinstance(exc, HTTPException) else 500
    return JSONResponse(
        status_code=status_code,
        content={"detail": "サーバー内部でエラーが発生しました"},
    )

# Create a single client for each service to be reused
async def get_httpx_client(base_url: str):
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
        http2=True
    )

# @app.on_event("startup")
# async def startup_event():
#     print("[162] Server startup: Initializing HTML clients")
# 
#     # Fetch URLs from environment variables
#     mathgen_url = os.getenv("MATHGEN_URL", "http://fastapi-app:7860")
# 
#     app.state.mathgen_client = await get_httpx_client(mathgen_url)
# 
# 
# @app.on_event("shutdown")
# async def shutdown_event():
#     print("[172] Server shutdown: Closing HTTP clients")
#     await app.state.mathgen_saikyo_client.aclose()
#     await app.state.mathgen_dcat_client.aclose()


# Log incoming requests
@app.middleware("http") 
async def log_request(request: Request, call_next):
    response = await call_next(request)
    print(f"[HTTP] {request.method} {request.url.path} -> {response.status_code}")
    return response

async def stream_response(response):
    async for chunk in response.aiter_raw():
        yield chunk

async def proxy_request(client, request: Request, path: str):
    url = httpx.URL(path=path, query=request.url.query.encode("utf-8"))
    
    headers = dict(request.headers)
    headers.pop("host", None)

    max_retries = 3
    for attempt in range(max_retries):
        try:
            rp_req = client.build_request(
                request.method, 
                url,
                headers=headers,
                content=await request.body()
            )
            rp_resp = await client.send(rp_req, stream=True)
            print(f"[204] Proxy request to {url} completed with status {rp_resp.status_code} (attempt {attempt + 1})")
            return StreamingResponse(
                stream_response(rp_resp),
                status_code=rp_resp.status_code,
                headers=dict(rp_resp.headers)
            )
        except (httpx.RequestError, TimeoutError) as e:
            print(f"[211] Attempt {attempt + 1} failed for proxying request to {url}: {e}")      

    # This line should never be reached due to the exception in the loop
    print(f"[214] Max retries reached for proxying request to {url}")
    raise HTTPException(status_code=502, detail="Max retries reached")

# @app.api_route("/" + path_prefix + "/{path:path}") # legacy path
# async def proxy_to_mathgen(request: Request, path: str, user: dict = Depends(get_current_user)):
#     return await proxy_request(app.state.mathgen_client, request, path)

####### LTI #############################################################################

@app.api_route(f"/{path_prefix}/lti/1p3/login", methods=["GET", "POST"])
async def lti13_login(request: Request):
    return await lti13_service.oidc_login(request)


@app.post(f"/{path_prefix}/lti/1p3/launch")
async def lti13_launch(request: Request):
    return await lti13_service.launch(request)


@app.get(f"/{path_prefix}/lti/1p3/jwks")
async def lti13_jwks():
    return lti13_service.jwks_response()


@app.get(f"/{path_prefix}/lti/status")
async def lti_status():
    return lti13_service.status_response()


@app.post(f"/{path_prefix}/lti/login")
async def lti_launch(request: Request):
    valid = await validate_lti_request(request)
    if not valid:
        raise HTTPException(status_code=401, detail="Invalid LTI 1.1 request")

    form_data = await request.form()
    user_id = str(form_data.get("user_id", "")).strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="LTI 1.1 user_id is required")

    roles = str(form_data.get("roles", "unknown"))
    oauth_consumer_key = str(form_data.get("oauth_consumer_key", "unknown"))
    context_id = str(form_data.get("context_id", "unknown"))
    context_title = str(form_data.get("context_title", "unknown"))
    request.session.clear()
    request.session["user"] = {
        "user_id": user_id,
        "roles": roles,
        "browser_language": get_browser_language(request),
        "oauth_consumer_key": oauth_consumer_key,
        "context_id": context_id,
        "context_title": context_title,
        "school_id": oauth_consumer_key,
        "lti_version": "1.1",
    }
    print("[LTI 1.1] Authenticated launch accepted")
    return RedirectResponse(
        url=f"/{path_prefix}/ui/", status_code=status.HTTP_303_SEE_OTHER
    )

@router.get("/go-to-gradio")
async def go_to_gradio():
    return RedirectResponse(url=f"/{path_prefix}/ui/")

app.include_router(router, prefix=f"/{path_prefix}")

# Gradio Mount（/ui以下はGradio専用にする！）
mount_gradio_app(
    app,
    demo1,
    path=f"/{path_prefix}/ui",
    root_path=f"/{path_prefix}/ui",
    auth_dependency=gradio_auth_dependency,
)

if __name__ == '__main__':
    uvicorn.run(app, root_path=f"/{path_prefix}")