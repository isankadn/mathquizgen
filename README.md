# mathquizgen

## Independent PRIME MCP service

`prime_mcp.py` serves authenticated Streamable HTTP at `/mcp`, independently of
Gradio. Importing it does not connect to Mongo, seed data, or import the Gradio
application. This is a read-only learner interface plus one administrator
platform-provisioning tool; generation remains in PRIME's authenticated UI.
The shared `prime_repository.py` contains `Scope` and `PrimeStore`, independent
of the MCP SDK, for reuse by the native LTI UI without extra dependencies.
`PrimeStore(database).list_prime_materials(scope)` performs the platform check
and returns the bounded materials result. For native question queries, call
`store.authorize(scope)` first, then use `store.question_scope(scope)` as the
visibility/ownership filter (`originals=True` for originals only). Do not
replace it with unrestricted legacy course/user filters.

### Standalone factory launch

Use a separate Python **3.11** virtual environment for a fresh installation.
For this server's already installed runtime, use the local-development launcher
below instead of reinstalling. The service pins the actual `mcp==2.1.1` SDK,
not FastMCP. SSE is pinned to 3.0.3 to avoid later transport dependency upgrades.

```sh
python3.11 -m venv .venv-mcp
.venv-mcp/bin/python -m pip install -r requirements-mcp.txt
export MONGO_URL='mongodb://<service-credentials>@<mongo-host>/'
export PRIME_MCP_TOKEN='<random-secret-at-least-32-characters>'
export PRIME_MCP_ALLOWED_HOSTS='prime.example.edu,127.0.0.1:*,localhost:*'
.venv-mcp/bin/python -m uvicorn prime_mcp:create_app --factory --host 127.0.0.1 --port 8013 --no-access-log
```

Run from the repository directory. `MONGO_URL` is the existing PRIME connection
variable; the database is always `prime`, never supplied by a tool caller.
The token must contain at least 32 non-whitespace ASCII characters; use a
cryptographically random secret. Missing/invalid configuration fails startup.
This entrypoint does not load a `.env` file: inject environment variables or
secrets through the process manager. Never put a token in a URL.

Expose the loopback listener through an HTTPS reverse proxy, preserve the
`Authorization` and protocol headers and `/mcp` path, allow only intended
machine clients, and configure the exact externally presented Host in
`PRIME_MCP_ALLOWED_HOSTS` (comma-separated SDK Host patterns). Do not use a
wildcard public Host allowlist. Browser Origin requests are not enabled.
Do not log authorization headers or request bodies in the proxy. No anonymous
health/docs/OpenAPI endpoint exists: authentication wraps every path, including
initialization, tool discovery, calls, and unsupported paths.

Mongo credentials need read access to `question_bank`, `history`, and
`lti13_platforms`, update/upsert access to `lti13_platforms`, and permission to
create service indexes. Startup creates unique issuer/client and issuer/deployment
indexes plus scoped question/history indexes, not records. Each
`issuer::deployment` namespace belongs to exactly one client, including disabled
registrations; duplicate ownership fails closed and requires explicit review.
Delimiter ambiguities are rejected (`::` inside either component, a trailing
issuer colon or leading deployment colon). Records are never merged or deleted
to resolve conflicts. Queries have two-second server/client time bounds and
bounded cursor windows. Verify indexes against synthetic data before any new deployment.

### Persistent local development with source reload

The authorized local development deployment is already running under the
`prime-mathquizgen-dev` user service, with native UI and MCP source reload.
Production/UAT stacks are unchanged. Its launcher command, also usable for an
authorized foreground restart when the service is stopped, is:

```sh
.dev-runtime/venv/bin/python dev_runtime.py start --allow-local-http --provider-env .dev-runtime/providers.env
```

Do not launch a duplicate on these ports or print the protected configuration.
The launcher never starts implicitly on import. For Meta's separate live-reload
lifecycle, use the [root runner guide](../../meta-agent-2/meta-agent/README.md#live-reload-on-the-development-server)
(workspace-local link), not PRIME's launcher.

The existing Python 3.11 dependencies are copied once into
`.dev-runtime/venv`; its interpreter is the durable interpreter backing the
source environment, not a link to the temporary environment. No `pip`, image
pull, build, test, or formatter is run. The source environment is untouched.
An interrupted/incomplete copy is refused rather than silently reinstalled or
deleted. All commands can also use the durable interpreter after `prepare`.
`mongo`, `ui`, and `mcp` actions support infrastructure-only or separate-terminal
starts; each application action starts/reuses only the dedicated local Mongo.

Current local listeners:

| Service | Listener | Entry/path |
| --- | --- | --- |
| Authenticated native UI | `0.0.0.0:7863` | `app:app`, `/mathquizgen/ui/` |
| Bearer-protected MCP | `0.0.0.0:7864` | `prime_mcp:create_app --factory`, `/mcp` |
| Dedicated persistent Mongo | `127.0.0.1:27090` | Shared database `prime` |

Native UI: `http://10.236.173.129:7863/mathquizgen/ui/` (development ingress open).
MCP: `http://10.236.173.129:7864/mcp` (no remote firewall ingress; same-host Meta
can reach it). Direct UI access still requires a signed LTI session.

Both Uvicorn processes reload Python, template, and static source changes;
runtime/secrets, `.git`, and `.env` changes are excluded. No access log or
untrusted proxy-header processing is enabled. Ctrl-C/termination stops only
the launcher's application process groups; Mongo, volume, and secrets remain.
Environment/configuration changes require a launcher restart, not source reload.

Mongo uses the already cached `mongo:7` image, dedicated container
`prime-mathquizgen-dev-mongo`, and named volume
`prime-mathquizgen-dev-mongo-data`, with database authentication and a
loopback-only published port. Labels, credentials and binding/mount configuration are
checked before reuse; unrelated containers/volumes are never adopted or stopped.
Orphaned storage or mismatched configuration requires manual owner review,
never destructive cleanup. Port `27089` remains exclusively for ephemeral
tests; existing `prime-lti` storage and all other stacks remain untouched.
The launcher creates no learner/platform records. The native app's existing
stock-material initialization still runs against this dedicated development DB.

All runtime directories are owner-only `0700`; infrastructure files are `0600`
and ignored by Git:

- `.dev-runtime/config.json`: ports, exact host allowlist, optional HTTPS
  `public_base_url` including `/mathquizgen`.
- `.dev-runtime/infrastructure.json`: stable session secret and MCP bearer
  (`PRIME_MCP_TOKEN` field; administrator reads it privately, never displays it).
- `.dev-runtime/mongo-password`: dedicated local Mongo bootstrap credential.
- `.dev-runtime/mongo.env`: selective Mongo-only Docker bootstrap environment;
  no provider, session, LTI or repository `.env` settings are copied to Mongo.
- `.dev-runtime/lti13-private.pem`: stable generated 3072-bit RSA LTI key.
- `.dev-runtime/providers.env`: protected provider configuration selected by
  the current authorized launcher; never expose keys or source it wholesale.

Do not display these files, export secrets into shell history, put bearer
credentials in URLs, or copy a full `.env` into the served directory.
Configuration/bearer updates in the administrator UI must read secrets
privately from these files. The launcher discards inherited application
environment, disables dotenv and Gradio telemetry, and uses local Mongo
exclusively. Default starts have no provider keys, BookRoll/ClickHouse settings
or LTI 1.1 credentials; startup does not contact providers or BookRoll.

The current service explicitly selects `.dev-runtime/providers.env`, using the
existing development OpenAI credential. Configuration is not evidence of a
provider call: paid generation has not been exercised. `--provider-env` requires
an owner-only `0600` file; only `OPENAI_API_KEY`, `GEMINI_API_KEY` and
`LLM_JP_API_KEY` are selected, without interpolation or printing. Mongo,
infrastructure and BookRoll settings are ignored. Never source the repository's
full `.env`, which may contain remote services.

Prerequisites for a new local runtime: Docker daemon access, cached `mongo:7`,
the installed source environment for preparation, its durable interpreter
thereafter, and available ports. The native Gradio mount explicitly blocks the
resolved repository root and configured private key, including runtime, `.git`,
`.env` and source files. Gradio 5.23.1 does **not** honor `GRADIO_BLOCKED_PATHS`
by itself. Real signed-session browser smoke checks denied source/runtime/private
files and traversal aliases; permissions alone do not prevent file-serving leaks.

Without an opt-in, session cookies remain HTTPS-only and HTTP direct access
allows protected endpoint checks, not a valid cross-site LTI launch. Provision
a genuine sanctioned development platform and use an HTTPS reverse proxy with
a matching canonical `public_base_url`. Native LTI authentication remains
required; no fake platform/learner is seeded and unregistered reads are denied.

The explicitly user-authorized `--allow-local-http` option derives the canonical
UI base `http://10.236.173.129:<ui_port>/mathquizgen`, overriding the configured
HTTPS base for that invocation. It requires that exact server address in the
host allowlist and sets `SESSION_HTTPS_ONLY=false`,
`LTI13_ALLOW_INSECURE_HTTP=true`, and `LTI13_ALLOW_PRIVATE_NETWORKS=true` only
in this launcher's children. Omitting the flag restores the existing HTTPS
mode; no global policy or persistent `.env` is changed. Repeat the flag for
separate `ui`/`mcp` starts or a persistent systemd command using this mode.
Meta currently keeps `LTI_REQUIRE_HTTPS=true` and permits only the exact local
PRIME origin `http://10.236.173.129:7863` as the sanctioned browser-LTI exception.
HTTP does not encrypt sessions or bearer credentials: restrict access to trusted
development clients, never expose this opt-in deployment to the public Internet.
Configure exact externally used hosts; avoid wildcard host allowlists.

HTTP launch compatibility is Chromium-only development evidence. OIDC state
cookies alone omit SameSite and expire within 120 seconds for the browser's
recent-cookie top-level POST grace period. State-cookie matching and nonce/JWT
validation remain mandatory; ordinary session cookies retain explicit Lax.
Delayed/embedded launches and other browser engines are unverified. Production
HTTPS retains Secure plus SameSite=None state cookies.

Ports `7863`, `7864` and `27090` now belong to this running development stack.
For a separate runtime, use unused ports in protected configuration; never
reclaim another service's port. Existing Mongo port/mount changes are refused
to protect storage and credentials.

The persistent user service owns the application's process groups; Mongo uses
`--restart unless-stopped`. Keep secrets in protected files, not unit contents
or command-line values. Mongo remains loopback-only; never open its port.
Environment/configuration changes need an authorized launcher restart.


### Administrator binding and platform provisioning

Sign in with the existing super-admin account at
`https://dev.let.media.kyoto-u.ac.jp/meta-agent-dev/admin/login`, then open
`https://dev.let.media.kyoto-u.ac.jp/meta-agent-dev/admin/mcp` in the same browser.
The login form has rendered, but authenticated public setup/provisioning has not
been exercised. Secure, host-only admin cookies do not transfer to private-IP
HTTP. PRIME configuration, provisioning and approval mutations require recent
super-admin authentication, session-bound CSRF and the exact public Origin.
The service bearer is not an admin login credential.

Meta backend/frontend source reload is running; public browser refresh remains
manual until the upstream TLS proxy forwards the HMR WebSocket (currently `404`).
Worker/databases are unchanged. See the workspace-local
[Meta integration guide](../../meta-agent-2/meta-agent/docs/prime-integration-setup.md).

In the Meta-Agent MCP admin UI, save the endpoint, bearer read privately from
`.dev-runtime/infrastructure.json`, active downstream registration and enabled
PRIME resource. Discover tools, then use **Provision PRIME platform**.
Server availability and teacher/student approvals default off; approve only the
three learner tools after manual review. Teacher PRIME approval is forbidden.
The trusted service bearer can invoke
`configure_prime_platform(issuer, client_id, deployment_id, auth_login_url,
key_set_url)`; this tool must never be approved for teacher/student model use.
An optional explicit `auth_token_url` is supported for operator callers.

Provisioning upserts one `lti13_platforms` record per issuer/client, enables it,
and adds the supplied deployment without removing other deployments. It
synchronizes the explicitly supplied login/JWKS endpoints while preserving the
existing name, token endpoint, audience, and other configuration unless an
explicit token endpoint is supplied. For a new ResourceLink-only registration,
`auth_token_url=auth_login_url` satisfies PRIME's existing adapter validation;
this is **not** a token endpoint discovery mechanism or AGS/NRPS support.
Provisioning returns only `{"configured": true}`. Deactivating the platform
immediately denies subsequent learner reads.

The existing LTI UI needs its own signing key and valid LTI 1.3 registration.
With the default prefix, configure these exact standard URLs:

| Purpose | HTTPS example |
| --- | --- |
| Initiate login | `https://<prime-host>/mathquizgen/lti/1p3/login` |
| Form-post receiver | `https://<prime-host>/mathquizgen/lti/1p3/launch` |
| Tool JWKS | `https://<prime-host>/mathquizgen/lti/1p3/jwks` |
| Enabled resource target | `https://<prime-host>/mathquizgen/ui/` |

For the sanctioned local service, use origin `http://10.236.173.129:7863` with
the same paths. The resource target is the UI, **not** the launch receiver or
legacy `/lti/login`; exact target, role and course grants still apply.
MCP neither supplies browser deployment secrets nor performs a launch.
Meta-Agent constructs the exact registered handoff; PRIME returns no launch URL.

### Learner tools and data boundaries

All three tools require bounded string scope fields `platform_issuer`,
`client_id`, `deployment_id`, `learner_id`, and `course_id`. The trusted
Meta-Agent must hide these from the model and inject the persisted pairwise LTI
subject/course. The bearer authenticates that service, not individual learners;
never share it with browsers, students, or arbitrary model integrations.
Every read verifies an enabled issuer/client registration and deployment.

- `list_prime_materials`: at most 50 distinct original material IDs from up to
  500 authorized questions; reports truncation. Titles are stored material
  metadata or the ID, never invented curricular units.
- `get_prime_practice`: optional `material_id`, `goal` (`review`, `extension`,
  `test_preparation`), and integer `limit` 1–5 (default 3). Review/test preparation
  prioritize latest own reports “解説を見てもわからなかった” then
  “解説を見てわかった”; extension prioritizes “すべて自力で解けた”.
  Returns bounded original-question text, opaque Mongo question IDs, the
  explicit self-report or null, timestamp, and factual recommendation reason.
  Missing evidence is `no_self_report`, not inferred understanding/mastery.
- `get_prime_question`: an opaque Mongo `question_id`; authorized question
  text and at most 20 rubric main steps only. Invalid and inaccessible IDs
  have the same error. Answers, prompts, raw submitted answers, private history
  text, identifiers, peer data, and roster/class statistics are not returned.

Questions must be visible (`show=true`) under
`school_id='<issuer>::<deployment>'`. Original school/course fields can be
arrays: originals permit the exact course or shared `prime` course, and exact
learner or shared `prime` owner within the authorized namespace. Generated
questions require **both** the exact course and exact learner; shared fallbacks
never authorize them. The question index is school/user/visibility/material,
not course, because legacy stock school/course can both be arrays; authorization
queries remain unchanged. A generated self-report can suggest only an existing
authorized original referenced consistently by its own authorized generated
record and `previous_quiz`. Such evidence is linked generated practice, never
mastery of the original.
Generated-question `page` is returned as null because the existing UI stores
the learner principal in that field; it is not a material page number.

History reads require the exact tenant/course/learner. The service examines at
most 200 recent reports after separately scanning at most 200 BSON dates and
200 ISO timestamp strings, then compares normalized UTC timestamps. ISO string
scan windows use stored lexical order, so noncanonical timestamp storage is not
a claim of complete chronological coverage. Invalid timestamps are ignored;
an unknown latest understanding does not resurrect an older report. The
response states coverage and truncation. Available-question fallback also
uses a bounded 500-question window.

**Publication and identity gates:** startup's legacy stock bank is not
automatically published into the Meta pairwise tenant/course. Platform
provisioning does not migrate learner/course records or publish materials.
New identities can honestly have no questions/reports. Any publication or
legacy migration requires separately verified ownership, correct scope and
explicit authorization; never broaden queries or link by email/name to make
data appear. No ClickHouse history, class ranking/majority rule, 30-report
mastery threshold, teacher PrimeModel API, objective grading or generation API
is added.

### Native UI acceptance invariant

Every protected native callback is bound to the **current HTTP caller's
signed LTI session** (issuer, client, deployment, learner, course and launch).
Active admissions are pinned across state eviction; unrelated callers cannot
adopt retained state or queued results. Unused WebSocket mutation and deep-link
snapshot routes are disabled. Browser actor/course switches and synchronized
eviction passed; paid generation and real-school author attribution are untested.

Native windows are bounded to 500 authorized questions and 200 recent own-course,
own-learner self-reports; detail text is bounded to 12,000 characters per bounded
field and rubric detail to 20 steps. Missing evidence and truncation are disclosed,
not inferred as non-practice or mastery. No class-wide history is exposed.

### Synthetic checks (owner runs after concurrent edits finish)

Dev-only dependencies are isolated from runtime:

```sh
.venv-mcp/bin/python -m pip install -r requirements-mcp-dev.txt
.venv-mcp/bin/python -m unittest discover -s tests -p 'test_prime_mcp.py'
.venv-mcp/bin/python -m unittest discover -s tests -p 'test_lti13.py'
```

`test_prime_mcp.py` uses only mongomock fixtures and the actual SDK ASGI
transport, covering bearer initialization/discovery/calls, strict schemas,
spoofed selectors, tenant/course/owner denial, generated-reference checks,
timestamp precedence, goals/limits, honest empty evidence, answer/history
redaction, provisioning preservation, deactivation, and bounded scans.
These tests do not replace real synthetic-Mongo/index checks, current-caller
native authorization/author attribution and private-file checks, or Meta's
signed-wire/browser end-to-end verification.

Completed locally: 45 PRIME tests (including real Mongo and synchronized
admission/eviction), 29 real SDK transport assertions, four cross-site signed
HTTPS-document to HTTP native launches, scoped UI/question/KaTeX display,
actor/course queue denial, snapshot/WebSocket and private-file denials.
The HTTPS responses came from an isolated real Meta signing/session fixture,
not authenticated public catalog setup or a real school. Human security review,
stock publication/migration, paid generation, other-browser/expired-cookie
coverage and UAT/production deployment remain separate gates.