#!/usr/bin/env python3
"""Operator-invoked local PRIME runtime; never reads the repository .env."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / ".dev-runtime"
VENV = RUNTIME / "venv"
SOURCE = Path("/tmp/meta-prime-mcp-venv")
CONTAINER = "prime-mathquizgen-dev-mongo"
VOLUME = "prime-mathquizgen-dev-mongo-data"
LABEL = "org.prime.local-development"
IMAGE = "mongo:7"


def protected(path: Path, directory: bool = False) -> None:
    info = path.lstat()
    expected = 0o700 if directory else 0o600
    if (path.is_symlink() or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != expected
            or (directory and not stat.S_ISDIR(info.st_mode))
            or (not directory and not stat.S_ISREG(info.st_mode))):
        raise RuntimeError(f"Expected owner-only {'directory' if directory else 'file'}: {path}")


def save_once(path: Path, content: str) -> None:
    if not path.exists():
        with path.open("x", encoding="utf-8") as stream:
            stream.write(content)
    protected(path)


def clean_environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(RUNTIME / "home"),
        "LANG": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
        "PYTHON_DOTENV_DISABLED": "1",
        "GRADIO_ANALYTICS_ENABLED": "False",
        "HF_HUB_OFFLINE": "1",
        "GRADIO_TEMP_DIR": str(RUNTIME / "gradio"),
        "TMPDIR": str(RUNTIME / "tmp"),
    }


def docker(*args: str) -> str:
    result = subprocess.run(
        ["docker", *args], env={
            "PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(RUNTIME / "home"),
            "DOCKER_CONFIG": str(RUNTIME / "home/docker"),
            "DOCKER_HOST": "unix:///var/run/docker.sock",
        },
        check=False, capture_output=True, text=True,
    )
    if result.returncode:
        # Docker diagnostics may contain environment/configuration; do not echo them.
        raise RuntimeError(f"Docker {args[0]} failed; inspect infrastructure locally.")
    return result.stdout.strip()


def inspect_existing(kind: str, name: str) -> dict | None:
    names = docker(*(["ps", "-a", "--format", "{{.Names}}"] if kind == "container"
                     else ["volume", "ls", "--format", "{{.Name}}"]))
    if name not in names.splitlines():
        return None
    command = ["inspect", name] if kind == "container" else ["volume", "inspect", name]
    item = json.loads(docker(*command))[0]
    labels = item["Config"].get("Labels", {}) if kind == "container" else item.get("Labels", {})
    if labels.get(LABEL) != str(ROOT):
        raise RuntimeError(f"Refusing to reuse unrelated {kind}: {name}")
    return item


def free_port(port: int, host: str = "0.0.0.0") -> None:
    with socket.socket() as listener:
        try:
            listener.bind((host, port))
        except OSError:
            raise RuntimeError(f"Port {port} is unavailable; select another in .dev-runtime/config.json.") from None


def prepare() -> None:
    for path in (RUNTIME, RUNTIME / "home", RUNTIME / "tmp", RUNTIME / "gradio"):
        path.mkdir(mode=0o700, exist_ok=True)
        protected(path, directory=True)
    with (RUNTIME / "bootstrap.lock").open("a") as lock:
        protected(RUNTIME / "bootstrap.lock")
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not VENV.exists():
            interpreter = (SOURCE / "bin/python").resolve(strict=True)
            if str(interpreter).startswith("/tmp/"):
                raise RuntimeError("The source interpreter must survive /tmp cleanup.")
            sites = list((SOURCE / "lib").glob("python3.11/site-packages"))
            if len(sites) != 1:
                raise RuntimeError("Installed Python 3.11 source runtime is missing.")
            # Build only the venv shell; copy installed dependencies without pip/network.
            subprocess.run([str(interpreter), "-m", "venv", "--without-pip", str(VENV)],
                           env=clean_environment(), check=True)
            target = VENV / "lib/python3.11/site-packages"
            shutil.copytree(sites[0], target, dirs_exist_ok=True, symlinks=False)
            save_once(RUNTIME / "venv-ready", "Python 3.11 dependencies copied locally.\n")
        if not (RUNTIME / "venv-ready").is_file():
            raise RuntimeError("Incomplete runtime exists; review it manually, no automatic deletion/reinstallation.")
        protected(RUNTIME / "venv-ready")
        save_once(RUNTIME / "config.json", json.dumps({
            "ui_port": 7863, "mcp_port": 7864, "mongo_port": 27090,
            "allowed_hosts": ["127.0.0.1", "localhost", "10.236.173.129"],
            "public_base_url": "",
        }, indent=2) + "\n")
        save_once(RUNTIME / "mongo-password", secrets.token_urlsafe(48))
        mongo_env = ("MONGO_INITDB_ROOT_USERNAME=prime_dev\n"
                     "MONGO_INITDB_ROOT_PASSWORD=" + (RUNTIME / "mongo-password").read_text().strip() + "\n")
        save_once(RUNTIME / "mongo.env", mongo_env)
        if (RUNTIME / "mongo.env").read_text() != mongo_env:
            raise RuntimeError("Local Mongo bootstrap credentials differ; review protected files without deleting storage.")
        if not (RUNTIME / "lti13-private.pem").exists():
            # Use the copied runtime, not a dependency installation or application import.
            code = ("from cryptography.hazmat.primitives.asymmetric import rsa; "
                    "from cryptography.hazmat.primitives import serialization; "
                    "import pathlib,sys; "
                    "key=rsa.generate_private_key(public_exponent=65537,key_size=3072); "
                    "data=key.private_bytes(serialization.Encoding.PEM,"
                    "serialization.PrivateFormat.PKCS8,serialization.NoEncryption()); "
                    "pathlib.Path(sys.argv[1]).open('xb').write(data)")
            subprocess.run([str(VENV / "bin/python"), "-c", code, str(RUNTIME / "lti13-private.pem")],
                           env=clean_environment(), check=True)
        protected(RUNTIME / "lti13-private.pem")
        save_once(RUNTIME / "infrastructure.json", json.dumps({
            "FASTAPI_SECRET_KEY": secrets.token_urlsafe(48),
            "PRIME_MCP_TOKEN": secrets.token_urlsafe(48),
        }, indent=2) + "\n")


def configuration() -> dict:
    protected(RUNTIME / "config.json")
    config = json.loads((RUNTIME / "config.json").read_text())
    ports = [config[key] for key in ("ui_port", "mcp_port", "mongo_port")]
    if any(type(port) is not int or not 1024 <= port <= 65535 for port in ports):
        raise RuntimeError("Development ports must be integers from 1024 through 65535.")
    if len(set(ports)) != 3 or 27089 in ports:
        raise RuntimeError("Use distinct ports; 27089 is reserved for ephemeral tests.")
    hosts = config["allowed_hosts"]
    if not isinstance(hosts, list) or not hosts or any(
        not isinstance(host, str) or not host or any(c in host for c in "*,/ \r\n")
        for host in hosts
    ):
        raise RuntimeError("Configure exact UI/MCP hosts, never wildcard hosts.")
    public_url = config["public_base_url"]
    if public_url and (not isinstance(public_url, str) or not public_url.startswith("https://")):
        raise RuntimeError("The canonical LTI public base URL must use HTTPS.")
    return config


def mongo(config: dict) -> None:
    # No pulls, builds, existing-stack lifecycle, volume removals, or record seeding.
    docker("image", "inspect", IMAGE)
    volume = inspect_existing("volume", VOLUME)
    container = inspect_existing("container", CONTAINER)
    if container is not None:
        binding = container["HostConfig"]["PortBindings"].get("27017/tcp")
        mounts = container.get("Mounts", [])
        if (binding != [{"HostIp": "127.0.0.1", "HostPort": str(config["mongo_port"])}]
                or container["Config"]["Image"] != IMAGE or volume is None
                or not any(m.get("Name") == VOLUME and m.get("Destination") == "/data/db" for m in mounts)
                or "MONGO_INITDB_ROOT_USERNAME=prime_dev" not in container["Config"]["Env"]
                or ("MONGO_INITDB_ROOT_PASSWORD=" + (RUNTIME / "mongo-password").read_text().strip())
                not in container["Config"]["Env"]):
            raise RuntimeError("Existing development Mongo configuration differs; review without deleting data.")
        if not container["State"]["Running"]:
            free_port(config["mongo_port"], "127.0.0.1")
            docker("start", CONTAINER)
    else:
        # An orphaned volume may contain data with different credentials; never adopt it.
        if volume is not None:
            raise RuntimeError("Development volume exists without its container; review credentials/data manually.")
        free_port(config["mongo_port"], "127.0.0.1")
        docker("volume", "create", "--label", f"{LABEL}={ROOT}", VOLUME)
        docker("run", "--detach", "--pull=never", "--name", CONTAINER,
               "--label", f"{LABEL}={ROOT}", "--restart", "unless-stopped",
               "--publish", f"127.0.0.1:{config['mongo_port']}:27017",
               "--mount", f"type=volume,src={VOLUME},dst=/data/db",
               "--env-file", str(RUNTIME / "mongo.env"), IMAGE)
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
    url = application_environment(config, None)["MONGO_URL"]
    with MongoClient(url, serverSelectionTimeoutMS=1000, connectTimeoutMS=1000,
                     socketTimeoutMS=1000) as client:
        for _ in range(60):
            try:
                client["prime"].command("ping")
                return
            except PyMongoError:
                time.sleep(1)
    raise RuntimeError("Dedicated Mongo was not ready; preserved container and volume for review.")


def application_environment(
    config: dict, provider_env: Path | None, *, allow_local_http: bool = False,
) -> dict[str, str]:
    protected(RUNTIME / "infrastructure.json")
    protected(RUNTIME / "mongo-password")
    environment = clean_environment()
    infrastructure = json.loads((RUNTIME / "infrastructure.json").read_text())
    for key in ("FASTAPI_SECRET_KEY", "PRIME_MCP_TOKEN"):
        value = infrastructure[key]
        if not isinstance(value, str) or len(value) < 32 or not value.isascii() or any(c.isspace() for c in value):
            raise RuntimeError("Invalid infrastructure secret; review protected file locally.")
        environment[key] = value
    if provider_env is not None:
        protected(provider_env)
        from dotenv import dotenv_values
        # Explicit selective opt-in; cannot override Mongo, LTI, host or session settings.
        values = dotenv_values(provider_env, interpolate=False)
        for key in ("OPENAI_API_KEY", "GEMINI_API_KEY", "LLM_JP_API_KEY"):
            if values.get(key):
                environment[key] = values[key]
    password = (RUNTIME / "mongo-password").read_text().strip()
    if not password or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in password):
        raise RuntimeError("Invalid local Mongo credential; review protected file locally.")
    environment.update({
        "MONGO_URL": f"mongodb://prime_dev:{password}@127.0.0.1:{config['mongo_port']}/?authSource=admin&directConnection=true",
        "LTI13_PRIVATE_KEY_FILE": str(RUNTIME / "lti13-private.pem"),
        "APP_PATH_PREFIX": "mathquizgen",
        "ALLOWED_HOSTS": ",".join(config["allowed_hosts"]),
        "PRIME_MCP_ALLOWED_HOSTS": ",".join(f"{host}:{config['mcp_port']}" for host in config["allowed_hosts"]),
        "SESSION_HTTPS_ONLY": "true",
        "LTI13_ALLOW_INSECURE_HTTP": "false",
        "LTI13_ALLOW_PRIVATE_NETWORKS": "false",
    })
    public_base_url = config["public_base_url"]
    if allow_local_http:
        if "10.236.173.129" not in config["allowed_hosts"]:
            raise RuntimeError("Local HTTP requires the exact development server host in allowed_hosts.")
        public_base_url = f"http://10.236.173.129:{config['ui_port']}/mathquizgen"
        environment.update({
            "SESSION_HTTPS_ONLY": "false",
            "LTI13_ALLOW_INSECURE_HTTP": "true",
            "LTI13_ALLOW_PRIVATE_NETWORKS": "true",
        })
    if public_base_url:
        environment["LTI13_PUBLIC_BASE_URL"] = public_base_url.rstrip("/")
        environment["LTI_URL"] = environment["LTI13_PUBLIC_BASE_URL"] + "/lti/login"
    return environment


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "mongo", "ui", "mcp", "start"))
    parser.add_argument("--provider-env", type=Path, help="Optional owner-only file; only provider keys are selected.")
    parser.add_argument(
        "--allow-local-http", action="store_true",
        help="Opt into HTTP only for this local PRIME development runtime; default HTTPS policy remains unchanged.",
    )
    args = parser.parse_args()
    os.umask(0o077)
    prepare()
    if args.action == "prepare":
        print(f"Prepared local runtime: {RUNTIME}; no services started.")
        return
    if sys.prefix != str(VENV):
        os.execve(str(VENV / "bin/python"), [str(VENV / "bin/python"), str(Path(__file__).resolve()), *sys.argv[1:]], clean_environment())
    config = configuration()
    if args.action == "mongo":
        mongo(config)
        print(f"Dedicated development Mongo: 127.0.0.1:{config['mongo_port']}; database prime.")
        return
    services = ("ui", "mcp") if args.action == "start" else (args.action,)
    for service in services:
        free_port(config[f"{service}_port"])
    environment = application_environment(
        config, args.provider_env.resolve() if args.provider_env else None,
        allow_local_http=args.allow_local_http,
    )
    mongo(config)
    processes = []
    stopping = False

    def stop(signum: int, frame: object) -> None:
        nonlocal stopping
        stopping = True
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        for service in services:
            if stopping:
                break
            command = [str(VENV / "bin/python"), "-m", "uvicorn",
                       "app:app" if service == "ui" else "prime_mcp:create_app",
                       "--host", "0.0.0.0", "--port", str(config[f"{service}_port"]),
                       "--reload", "--reload-dir", str(ROOT), "--reload-include", "*.py",
                       "--reload-include", "templates/**", "--reload-include", "static/**",
                       "--reload-exclude", ".dev-runtime/**", "--reload-exclude", ".git/**",
                       "--reload-exclude", ".env*", "--no-access-log", "--no-proxy-headers"]
            if service == "mcp":
                command.append("--factory")
            processes.append(subprocess.Popen(command, cwd=ROOT, env=environment, start_new_session=True))
        while not stopping and all(process.poll() is None for process in processes):
            time.sleep(0.5)
        failed = any(process.poll() not in (None, 0) for process in processes)
    finally:
        stop(signal.SIGINT, None)
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    if failed:
        raise RuntimeError("A development service exited unsuccessfully; Mongo and secrets were preserved.")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        # Only deliberately sanitized operator messages use RuntimeError above.
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except Exception:
        # Config/subprocess exceptions can embed secrets. Never expose their repr.
        print("Development runtime failed; review prerequisites, ports, protected configuration and local service output.", file=sys.stderr)
        sys.exit(1)
