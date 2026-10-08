"""Consumer-visible authorization over real Gradio state and result queues."""

import asyncio
import base64
import json
import threading
import unittest

import gradio as gr
from fastapi import FastAPI, Request
from itsdangerous import TimestampSigner
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from prime_gradio_auth import NativeGradioScopeMiddleware, native_identity


class GradioScopeTests(unittest.TestCase):
    def setUp(self):
        self.secret = "synthetic-session-secret-for-isolated-tests"
        reports = {
            ("actor-a", "course-a"): "PRIVATE report A, course A",
            ("actor-b", "course-a"): "PRIVATE report B, course A",
            ("actor-a", "course-b"): "PRIVATE report A, course B",
            ("prime-lti13:legacy-subject", "course-a"): "PRIVATE legacy report",
        }

        def load_user(request: gr.Request):
            return request.request.session["user"]

        def read_report(user):
            return reports[(user["user_id"], user["context_id"])]

        def read_stateless_report(request: gr.Request):
            return read_report(request.request.session["user"])

        with gr.Blocks(analytics_enabled=False) as blocks:
            state = gr.State()
            output = gr.Textbox()
            blocks.load(load_user, outputs=state, api_name="load_user", queue=False)
            gr.Button().click(read_report, inputs=state, outputs=output,
                              api_name="read_report", queue=False)
            gr.Button().click(read_stateless_report, outputs=output,
                              api_name="read_stateless_report")
        blocks.queue()
        self.blocks = blocks

        def authenticated_user(request: Request):
            user = request.session.get("user", {})
            if user.get("lti_version") == "1.3":
                return native_identity(request.session)
            return user.get("user_id")

        app = FastAPI()
        self.admission_hook = None

        async def admitted(request, call_next):
            if self.admission_hook is not None:
                await self.admission_hook(request)
            return await call_next(request)

        app.add_middleware(BaseHTTPMiddleware, dispatch=admitted)
        app.add_middleware(NativeGradioScopeMiddleware, blocks=blocks, path="/ui")
        app.add_middleware(SessionMiddleware, secret_key=self.secret)
        gr.mount_gradio_app(app, blocks, path="/ui", auth_dependency=authenticated_user)
        self.client = self.enterContext(TestClient(app))

    def login(self, actor="actor-a", course="course-a", launch="launch-a", version="1.3"):
        user = {
            "user_id": actor, "context_id": course, "lti_version": version,
            "lti_issuer": "https://synthetic.example.test", "lti_client_id": "client",
            "lti_deployment_id": "deployment", "school_id": "synthetic-school",
            "oauth_consumer_key": "synthetic-legacy-consumer",
        }
        data = base64.b64encode(json.dumps({"user": user, "lti13_launch_id": launch}).encode())
        self.client.cookies.clear()
        self.client.cookies.set("session", TimestampSigner(self.secret).sign(data).decode())

    def load(self, session_hash):
        result = self.client.post("/ui/gradio_api/api/load_user", json={
            "data": [], "session_hash": session_hash,
        })
        self.assertEqual(result.status_code, 200, result.text)

    def report(self, session_hash):
        return self.client.post("/ui/gradio_api/api/read_report", json={
            "data": [None], "session_hash": session_hash,
        })

    def test_actor_course_and_launch_switches_cannot_reuse_prior_state(self):
        for index, changes in enumerate((
            {"actor": "actor-b"}, {"course": "course-b"}, {"launch": "fresh-launch"},
        )):
            with self.subTest(changes=changes):
                old_hash = f"old-{index}"
                self.login()
                self.load(old_hash)
                self.assertEqual(self.report(old_hash).json()["data"], ["PRIVATE report A, course A"])
                self.login(**changes)
                denied = self.report(old_hash)
                self.assertEqual(denied.status_code, 403)
                self.assertNotIn("PRIVATE report", denied.text)
                self.assertEqual(self.client.get("/ui/gradio_api/queue/data", params={
                    "session_hash": old_hash,
                }).status_code, 403)
                new_hash = f"fresh-{index}"
                self.load(new_hash)
                expected = "PRIVATE report " + ("B" if changes.get("actor") else "A")
                expected += ", course " + ("B" if changes.get("course") else "A")
                self.assertEqual(self.report(new_hash).json()["data"], [expected])

    def test_other_principals_cannot_cancel_or_access_retained_session_streams(self):
        self.login()
        self.load("owned-state")
        self.login(actor="actor-b")
        for path in ("heartbeat/owned-state", "stream/owned-state/0/0/playlist.m3u8"):
            self.assertEqual(self.client.get("/ui/gradio_api/" + path).status_code, 403)
        self.assertEqual(self.client.post("/ui/gradio_api/cancel", json={
            "session_hash": "owned-state", "event_id": "unused", "fn_index": 1,
        }).status_code, 403)

    def test_stateless_queued_results_are_bound_to_the_signed_launch(self):
        self.login()
        submitted = self.client.post("/ui/gradio_api/call/read_stateless_report", json={"data": []})
        self.assertEqual(submitted.status_code, 200, submitted.text)
        event_id = submitted.json()["event_id"]
        self.login(actor="actor-b")
        denied = self.client.get("/ui/gradio_api/call/read_stateless_report/" + event_id)
        self.assertEqual(denied.status_code, 403)
        self.assertNotIn("PRIVATE report", denied.text)
        self.login()
        own = self.client.get("/ui/gradio_api/call/read_stateless_report/" + event_id)
        self.assertEqual(own.status_code, 200)
        self.assertIn("PRIVATE report A, course A", own.text)

    def test_legacy_and_unauthenticated_callers_cannot_adopt_native_state(self):
        self.login()
        self.load("native-state")
        self.login(version="1.1")
        self.assertEqual(self.report("native-state").status_code, 403)
        self.load("legacy-state")
        self.assertEqual(self.report("legacy-state").json()["data"], ["PRIVATE report A, course A"])
        self.client.cookies.clear()
        self.assertEqual(self.report("native-state").status_code, 401)

    def test_unused_websocket_and_private_snapshot_aliases_are_disabled(self):
        self.login()
        self.load("private-state")
        self.assertEqual(self.report("private-state").json()["data"], ["PRIVATE report A, course A"])
        for path in (
            "/ui/gradio_api/deep_link?session_hash=private-state",
            "/ui/?deep_link=retained-private-snapshot",
            "/ui/config?deep_link=retained-private-snapshot",
            "/ui/page?deep_link=retained-private-snapshot",
        ):
            for authenticated in (True, False):
                if authenticated:
                    self.login(actor="actor-b")
                else:
                    self.client.cookies.clear()
                denied = self.client.get(path)
                self.assertEqual(denied.status_code, 403, path)
                self.assertNotIn("PRIVATE report", denied.text)
        with self.assertRaises(WebSocketDisconnect) as denied:
            with self.client.websocket_connect("/ui/gradio_api/stream/retained-event"):
                self.fail("An unused unauthenticated mutation transport was accepted.")
        self.assertEqual(denied.exception.code, 1008)

    def test_eviction_during_admission_cannot_rebind_another_principals_state(self):
        admitted = threading.Event()
        release = threading.Event()
        replies = []

        async def evict_between_admission_and_use(request):
            if request.url.path.endswith("/load_user"):
                self.admission_hook = None
                self.blocks.state_holder.session_data.pop("race-state")
                admitted.set()
                if not await asyncio.to_thread(release.wait, 5):
                    raise RuntimeError("Admission test barrier timed out.")

        self.admission_hook = evict_between_admission_and_use
        self.login()
        cookie = "session=" + self.client.cookies.get("session")
        first = threading.Thread(target=lambda: replies.append(self.client.post(
            "/ui/gradio_api/api/load_user",
            headers={"Cookie": cookie},
            json={"data": [], "session_hash": "race-state"},
        )))
        first.start()
        try:
            self.assertTrue(admitted.wait(5), "The synchronized eviction was not reached.")
            self.login(actor="actor-b")
            denied = self.client.post("/ui/gradio_api/api/load_user", json={
                "data": [], "session_hash": "race-state",
            })
            self.assertEqual(denied.status_code, 403)
        finally:
            release.set()
            first.join(5)
        self.assertFalse(first.is_alive())
        self.assertEqual([reply.status_code for reply in replies], [200])
        # The first response refreshes this shared test client's cookie jar.
        # Authenticate the second principal again before probing its reads.
        self.login(actor="actor-b")
        denied = self.report("race-state")
        self.assertEqual(denied.status_code, 403)
        self.assertNotIn("PRIVATE report A", denied.text)
        self.load("fresh-b-state")
        self.assertEqual(self.report("fresh-b-state").json()["data"], ["PRIVATE report B, course A"])

    def test_legacy_subject_prefix_does_not_impersonate_a_native_queue_owner(self):
        self.login(actor="prime-lti13:legacy-subject", version="1.1")
        submitted = self.client.post("/ui/gradio_api/call/read_stateless_report", json={"data": []})
        self.assertEqual(submitted.status_code, 200)
        event_id = submitted.json()["event_id"]
        own = self.client.get("/ui/gradio_api/call/read_stateless_report/" + event_id)
        self.assertEqual(own.status_code, 200)
        self.assertIn("PRIVATE legacy report", own.text)


if __name__ == "__main__":
    unittest.main()
