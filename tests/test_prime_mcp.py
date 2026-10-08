"""Synthetic-only tests: no live database, learner data, or network service."""
import importlib
import json
import os
import subprocess
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import mongomock
from bson import ObjectId
from fastapi.testclient import TestClient
from pydantic import ValidationError
from pymongo.errors import DuplicateKeyError, OperationFailure

import prime_mcp
from prime_mcp import create_app
from prime_repository import (
    AccessDenied, HISTORY_SCAN_LIMIT, MATERIAL_LIMIT, QUESTION_SCAN_LIMIT,
    PracticeArguments, PrimeStore, ProvisionArguments, QuestionArguments, Scope,
    TOOL_MODELS, UNDERSTANDING,
)

TOKEN = "synthetic-only-test-token-" + "x" * 32
ISSUER = "https://meta.example.test"
SCOPE = dict(platform_issuer=ISSUER, client_id="client", deployment_id="deployment",
             learner_id="pairwise-learner", course_id="pairwise-course")
SCHOOL = f"{ISSUER}::deployment"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient()["prime"]
        self.store = PrimeStore(self.db)
        self.store.ensure_indexes()
        self.scope = Scope(**SCOPE)
        self.provision()

    def provision(self, **changes):
        values = dict(issuer=ISSUER, client_id="client", deployment_id="deployment",
                      auth_login_url=ISSUER + "/auth", key_set_url=ISSUER + "/jwks")
        values.update(changes)
        return self.store.configure_prime_platform(ProvisionArguments(**values))

    def question(self, **changes):
        values = dict(school_id=SCHOOL, course_id="pairwise-course", user="prime", show=True,
                      contents_id="material", page="1", no=str(ObjectId()), quiz_title="Algebra",
                      quiz_text="Solve x + 1 = 3.", standard_answer="SECRET_ANSWER",
                      prompt_exercise="SECRET_PROMPT", rubric={"step": {"main": "Isolate x", "answer": "SECRET_RUBRIC"}})
        values.update(changes)
        values["_id"] = self.db.question_bank.insert_one(values).inserted_id
        return values

    def report(self, question, understanding=UNDERSTANDING[0], timestamp=NOW, **changes):
        values = dict(school_id=SCHOOL, course_id="pairwise-course", user="pairwise-learner",
                      contents_id=question["contents_id"], page=question["page"], no=question["no"],
                      understanding=understanding, timestamp=timestamp,
                      user_answer="PRIVATE_SUBMITTED_ANSWER", report={"report_text": "PRIVATE_IDENTIFYING_TEXT"})
        values.update(changes)
        self.db.history.insert_one(values)

    def practice(self, **changes):
        return self.store.get_prime_practice(PracticeArguments(**{**SCOPE, **changes}))

    def get(self, identifier, **changes):
        return self.store.get_prime_question(QuestionArguments(**{**SCOPE, "question_id": str(identifier), **changes}))

    def test_provision_preserves_deployments_and_unrelated_configuration(self):
        self.db.lti13_platforms.update_one({}, {"$set": {
            "name": "Operator label", "auth_token_url": ISSUER + "/real-token",
            "auth_audience": ISSUER + "/audience", "operator_only": "preserved"}})
        self.assertEqual(self.provision(deployment_id="other-deployment"), {"configured": True})
        self.provision()
        registration = self.db.lti13_platforms.find_one({})
        self.assertEqual(self.db.lti13_platforms.count_documents({}), 1)
        self.assertEqual(set(registration["deployment_ids"]), {"deployment", "other-deployment"})
        self.assertEqual(registration["auth_token_url"], ISSUER + "/real-token")
        self.assertEqual(registration["auth_audience"], ISSUER + "/audience")
        self.assertEqual(registration["name"], "Operator label")
        self.assertEqual(registration["operator_only"], "preserved")
        with self.assertRaises(DuplicateKeyError):
            self.db.lti13_platforms.insert_one({"issuer": ISSUER, "client_id": "client"})

    def test_provision_fallback_explicit_token_and_https_validation(self):
        self.assertEqual(self.db.lti13_platforms.find_one({})["auth_token_url"], ISSUER + "/auth")
        self.provision(auth_token_url=ISSUER + "/token")
        self.assertEqual(self.db.lti13_platforms.find_one({})["auth_token_url"], ISSUER + "/token")
        with patch.dict(os.environ, {"LTI13_ALLOW_INSECURE_HTTP": "false"}):
            with self.assertRaises(ValueError):
                self.provision(key_set_url="http://invalid.test/jwks")
        self.assertEqual(self.db.lti13_platforms.find_one({})["key_set_url"], ISSUER + "/jwks")

    def test_every_read_checks_platform_enabled_client_and_deployment(self):
        question = self.question()
        for changes in ({"client_id": "other"}, {"deployment_id": "other"},
                        {"platform_issuer": "https://other.test"}):
            with self.subTest(changes=changes):
                with self.assertRaises(AccessDenied):
                    self.practice(**changes)
                with self.assertRaises(AccessDenied):
                    self.store.list_prime_materials(Scope(**{**SCOPE, **changes}))
                with self.assertRaises(AccessDenied):
                    self.get(question["_id"], **changes)
        self.db.lti13_platforms.update_one({}, {"$set": {"enabled": False}})
        for callback in (lambda: self.practice(), lambda: self.get(question["_id"]),
                         lambda: self.store.list_prime_materials(self.scope)):
            with self.assertRaises(AccessDenied):
                callback()

    def test_question_cross_tenant_course_owner_visibility_and_invalid_id_denied_identically(self):
        errors = []
        for changes in ({"school_id": "other-school"}, {"course_id": "other-course"},
                        {"user": "other-learner"}, {"show": False}):
            question = self.question(**changes)
            with self.assertRaises(AccessDenied) as caught:
                self.get(question["_id"])
            errors.append(str(caught.exception))
        for identifier in ("not-an-id", ObjectId()):
            with self.assertRaises(AccessDenied) as caught:
                self.get(identifier)
            errors.append(str(caught.exception))
        self.assertEqual(set(errors), {"Question is not available."})
        self.assertEqual(self.practice()["questions"], [])
        self.assertEqual(self.store.list_prime_materials(self.scope)["materials"], [])

    def test_original_shared_fallback_but_generated_requires_exact_owner_and_course(self):
        original = self.question(course_id="prime", user="prime")
        self.assertEqual(self.get(original["_id"])["question_id"], str(original["_id"]))
        for changes in ({"course_id": "prime", "user": "pairwise-learner"},
                        {"course_id": "pairwise-course", "user": "prime"},
                        {"course_id": "pairwise-course", "user": "other-learner"}):
            generated = self.question(contents_id="ai_generated", **changes)
            with self.assertRaises(AccessDenied):
                self.get(generated["_id"])
        own = self.question(contents_id="ai_generated", user="pairwise-learner", page="pairwise-learner")
        own_result = self.get(own["_id"])
        self.assertEqual(own_result["material_id"], "ai_generated")
        self.assertIsNone(own_result["page"])
        self.assertNotIn("pairwise-learner", json.dumps(own_result))
        self.assertNotIn("ai_generated", [m["material_id"] for m in self.store.list_prime_materials(self.scope)["materials"]])
        self.assertTrue(all(q["material_id"] != "ai_generated" for q in self.practice()["questions"]))

    def test_no_answers_prompts_history_text_or_identity_returned(self):
        question = self.question()
        self.report(question)
        result = self.get(question["_id"])
        self.assertEqual(result["rubric_main_steps"], ["Isolate x"])
        serialized = json.dumps([result, self.practice(), self.store.list_prime_materials(self.scope)])
        for forbidden in ("SECRET_ANSWER", "SECRET_PROMPT", "SECRET_RUBRIC",
                          "PRIVATE_SUBMITTED_ANSWER", "PRIVATE_IDENTIFYING_TEXT",
                          "pairwise-learner", "pairwise-course", SCHOOL, "launch_url"):
            self.assertNotIn(forbidden, serialized)

    def test_latest_own_report_compares_datetime_iso_and_offsets_not_insertion(self):
        question = self.question()
        self.report(question, UNDERSTANDING[2], "2026-01-01T10:00:00+09:00")
        self.report(question, UNDERSTANDING[0], NOW)  # Inserted later, older event.
        self.report(question, UNDERSTANDING[1], "2026-01-01T02:00:00Z")
        for changes in ({"user": "other-learner"}, {"course_id": "other-course"},
                        {"school_id": "other-school"}):
            self.report(question, UNDERSTANDING[0], NOW + timedelta(days=10), **changes)
        self.report(question, UNDERSTANDING[0], "invalid-date")
        item = self.practice()["questions"][0]
        self.assertEqual(item["self_reported_understanding"], UNDERSTANDING[1])
        self.assertEqual(item["recorded_at"], "2026-01-01T02:00:00+00:00")

    def test_unknown_latest_report_does_not_resurrect_old_understanding(self):
        question = self.question()
        self.report(question)
        self.report(question, "unrecognized", NOW + timedelta(days=1))
        item = self.practice()["questions"][0]
        self.assertIsNone(item["self_reported_understanding"])
        self.assertEqual(item["recommendation_reason"], "no_self_report")

    def test_goals_and_limits_prioritize_explicit_latest_reports(self):
        red, yellow, green, empty = [self.question() for _ in range(4)]
        for question, understanding in zip((red, yellow, green), UNDERSTANDING):
            self.report(question, understanding)
        for goal in ("review", "test_preparation"):
            self.assertEqual([q["question_id"] for q in self.practice(goal=goal, limit=2)["questions"]],
                             [str(red["_id"]), str(yellow["_id"])])
        self.assertEqual(self.practice(goal="extension", limit=1)["questions"][0]["question_id"], str(green["_id"]))
        for limit in (0, 6, "2", True):
            with self.assertRaises(ValidationError):
                self.practice(limit=limit)
        with self.assertRaises(ValidationError):
            self.practice(goal="mastery")
        for key in SCOPE:
            for value in ({"$ne": ""}, "", None):
                with self.assertRaises(ValidationError):
                    self.practice(**{key: value})
        with self.assertRaises(ValidationError):
            self.practice(material_id={"$ne": ""})
        with self.assertRaises(ValidationError):
            self.store.call("get_prime_practice", {**SCOPE, "database": "other", "filter": {"user": "other"}})

    def test_no_evidence_is_honest_and_material_filter_is_literal(self):
        question = self.question(contents_id="$literal", material_title="Book title")
        item = self.practice(material_id="$literal")["questions"][0]
        self.assertIsNone(item["self_reported_understanding"])
        self.assertIsNone(item["recorded_at"])
        self.assertEqual(item["recommendation_reason"], "no_self_report")
        self.assertEqual(self.practice(material_id="$literal")["evidence"], "no_self_report")
        self.assertEqual(self.practice(material_id="missing")["questions"], [])
        self.assertEqual(self.practice(material_id="ai_generated")["questions"], [])
        self.assertEqual(self.store.list_prime_materials(self.scope)["materials"],
                         [{"material_id": "$literal", "title": "Book title"}])

    def test_generated_report_requires_owned_record_consistent_original_and_authorization(self):
        original = self.question()
        ref = {key: original[key] for key in ("school_id", "contents_id", "page", "no")}
        generated = self.question(contents_id="ai_generated", user="pairwise-learner", previous_quiz=ref)
        self.report(generated, previous_quiz=ref)
        item = self.practice()["questions"][0]
        self.assertEqual(item["question_id"], str(original["_id"]))
        self.assertEqual(item["self_report_source"], "linked_generated_practice")
        self.assertIn("not evidence of original-question mastery", item["recommendation_reason"])
        self.db.question_bank.update_one({"_id": generated["_id"]}, {"$set": {"user": "other-learner"}})
        self.assertEqual(self.practice()["questions"][0]["recommendation_reason"], "no_self_report")
        self.db.question_bank.update_one({"_id": generated["_id"]}, {"$set": {"user": "pairwise-learner"}})
        self.db.history.update_one({}, {"$set": {"previous_quiz.no": "spoofed"}})
        self.assertEqual(self.practice()["questions"][0]["recommendation_reason"], "no_self_report")
        self.db.history.update_one({}, {"$set": {"previous_quiz": ref}})
        self.db.question_bank.update_one({"_id": original["_id"]}, {"$set": {"show": False}})
        self.assertEqual(self.practice()["questions"], [])
        self.db.question_bank.delete_one({"_id": generated["_id"]})
        self.assertEqual(self.practice()["questions"], [])

    def test_bounded_scans_signal_truncation_and_bound_text(self):
        for index in range(QUESTION_SCAN_LIMIT + 1):
            self.question(contents_id=f"material-{index}", quiz_title="t" * 300, quiz_text="q" * 13000)
        materials = self.store.list_prime_materials(self.scope)
        self.assertEqual(len(materials["materials"]), MATERIAL_LIMIT)
        self.assertTrue(materials["truncated"])
        question = self.db.question_bank.find_one({})
        for index in range(HISTORY_SCAN_LIMIT + 1):
            self.report(question, timestamp=NOW + timedelta(seconds=index))
        result = self.practice(limit=5)
        self.assertLessEqual(len(result["questions"]), 5)
        self.assertTrue(result["coverage"]["history_truncated"])
        self.assertTrue(result["coverage"]["questions_truncated"])
        self.assertEqual(result["coverage"]["history_scanned"], HISTORY_SCAN_LIMIT)
        for item in result["questions"]:
            self.assertLessEqual(len(item["title"]), 256)
            self.assertLessEqual(len(item["text"]), 12000)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient()["prime"]

    def client(self, token=TOKEN):
        with patch.dict(os.environ, {"PRIME_MCP_ALLOWED_HOSTS": "testserver"}):
            return TestClient(create_app(database=self.db, token=token))

    def rpc(self, client, method, params=None, authenticated=True):
        headers = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-03-26"}
        if authenticated:
            headers["Authorization"] = "Bearer " + TOKEN
        return client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method,
                                        "params": params or {}}, headers=headers)

    def test_import_is_safe_without_credentials_or_gradio(self):
        with patch.dict(os.environ, {"MONGO_URL": "", "PRIME_MCP_TOKEN": ""}), \
             patch("pymongo.MongoClient", side_effect=AssertionError("Import must not connect")):
            importlib.reload(prime_mcp)
        self.assertNotIn("gradio_app", sys.modules)

    def test_native_repository_import_needs_no_sdk_or_connection(self):
        code = (
            "import sys; from unittest.mock import patch; "
            "sys.modules['mcp'] = None; sys.modules['mcp_types'] = None; "
            "guard = patch('pymongo.MongoClient', side_effect=AssertionError('must not connect')); "
            "guard.start(); import prime_repository; "
            "assert 'gradio_app' not in sys.modules; "
            "assert 'prime_mcp' not in sys.modules; "
            "assert prime_repository.Scope and prime_repository.PrimeStore"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                                cwd=os.path.dirname(os.path.dirname(__file__)), timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_or_short_token_and_missing_database_fail_at_startup(self):
        for token in ("", "short", "x" * 31, "x" * 32 + " "):
            with self.subTest(token_length=len(token)), self.assertRaises(RuntimeError):
                with self.client(token):
                    pass
        with patch.dict(os.environ, {"MONGO_URL": ""}), self.assertRaises(RuntimeError):
            with TestClient(create_app(token=TOKEN)):
                pass

    def test_authentication_covers_initialize_list_call_and_all_paths(self):
        with self.client() as client:
            for method in ("initialize", "tools/list", "tools/call"):
                response = self.rpc(client, method, authenticated=False)
                self.assertEqual(response.status_code, 401)
                self.assertNotIn(TOKEN, response.text)
            for path in ("/", "/docs", "/openapi.json", "/mcp", "/unknown"):
                self.assertEqual(client.get(path).status_code, 401)
            for auth in ("Bearer wrong", "Basic " + TOKEN, "Bearer", TOKEN):
                self.assertEqual(client.post("/mcp", headers={"Authorization": auth}, json={}).status_code, 401)
            response = client.post("/mcp", headers=[("Authorization", "Bearer " + TOKEN),
                                                    ("Authorization", "Bearer wrong")], json={})
            self.assertEqual(response.status_code, 401)

    def test_authenticated_sdk_initialize_list_and_call(self):
        with self.client() as client:
            response = self.rpc(client, "initialize", {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "synthetic-test", "version": "1"}})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn("serverInfo", response.json()["result"])
            listed = self.rpc(client, "tools/list")
            self.assertEqual(listed.status_code, 200, listed.text)
            tools = listed.json()["result"]["tools"]
            self.assertEqual({tool["name"] for tool in tools}, set(TOOL_MODELS))
            for tool in tools:
                schema = tool["inputSchema"]
                self.assertFalse(schema["additionalProperties"])
                if tool["name"] != "configure_prime_platform":
                    self.assertTrue(set(SCOPE).issubset(schema["required"]))
                    for field in SCOPE:
                        self.assertEqual(schema["properties"][field]["type"], "string")
                        self.assertGreaterEqual(schema["properties"][field]["minLength"], 1)
                        self.assertIn("maxLength", schema["properties"][field])
            provision = self.rpc(client, "tools/call", {"name": "configure_prime_platform", "arguments": {
                "issuer": ISSUER, "client_id": "client", "deployment_id": "deployment",
                "auth_login_url": ISSUER + "/auth", "key_set_url": ISSUER + "/jwks"}})
            self.assertEqual(provision.status_code, 200, provision.text)
            self.assertEqual(provision.json()["result"]["structuredContent"], {"configured": True})
            call = self.rpc(client, "tools/call", {"name": "get_prime_practice", "arguments": SCOPE})
            self.assertEqual(call.status_code, 200, call.text)
            self.assertEqual(call.json()["result"]["structuredContent"]["evidence"], "no_self_report")
            invalid = self.rpc(client, "tools/call", {"name": "get_prime_practice", "arguments": {**SCOPE, "user": "other", "query": {"$ne": ""}}})
            self.assertTrue(invalid.json()["result"]["isError"])
            self.assertNotIn("other", invalid.text)
            self.db.lti13_platforms.update_one({}, {"$set": {"enabled": False}})
            denied = self.rpc(client, "tools/call", {"name": "get_prime_practice", "arguments": SCOPE})
            self.assertTrue(denied.json()["result"]["isError"])

    def test_database_failure_does_not_leak_exception_credentials(self):
        with self.client() as client, patch.object(PrimeStore, "call", side_effect=OperationFailure("mongodb://SECRET_PASSWORD@host")):
            response = self.rpc(client, "tools/call", {"name": "get_prime_practice", "arguments": SCOPE})
            self.assertTrue(response.json()["result"]["isError"])
            self.assertNotIn("SECRET_PASSWORD", response.text)


if __name__ == "__main__":
    unittest.main()
