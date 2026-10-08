"""Opt-in integration checks, isolated databases on an explicitly local MongoDB."""
import os
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from urllib.parse import urlsplit

from pymongo import MongoClient, timeout
from pymongo.errors import DuplicateKeyError

from lti13 import LTI13ConfigurationError
from prime_repository import (
    AccessDenied, HISTORY_SCAN_LIMIT, QUESTION_SCAN_LIMIT,
    PrimeStore, ProvisionArguments, Scope,
)

MONGO_URL = os.getenv("PRIME_TEST_MONGO_URL", "")
ISSUER = "https://synthetic-platform.example.test"
SCOPE = dict(platform_issuer=ISSUER, client_id="synthetic-client", deployment_id="synthetic-deployment",
             learner_id="synthetic-pairwise-learner", course_id="synthetic-pairwise-course")
NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


@unittest.skipUnless(MONGO_URL, "Set PRIME_TEST_MONGO_URL to an isolated local MongoDB")
class NativeMongoTests(unittest.TestCase):
    def setUp(self):
        if urlsplit(MONGO_URL).hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Native integration tests require an explicitly local MongoDB")
        self.client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        self.database_name = "prime_native_test_" + uuid4().hex
        self.db = self.client[self.database_name]
        self.addCleanup(self.client.close)
        self.addCleanup(self.client.drop_database, self.database_name)
        self.store = PrimeStore(self.db)
        self.store.ensure_indexes()
        self.scope = Scope(**SCOPE)
        self.provision()

    def provision(self, **changes):
        values = dict(issuer=ISSUER, client_id=SCOPE["client_id"], deployment_id=SCOPE["deployment_id"],
                      auth_login_url=ISSUER + "/authorize", key_set_url=ISSUER + "/jwks")
        return self.store.configure_prime_platform(ProvisionArguments(**{**values, **changes}))

    def question(self, **changes):
        value = dict(school_id=self.scope.school_id, course_id="prime", user="prime", show=True,
                     contents_id="material", page="1", no=uuid4().hex, quiz_title="Algebra", quiz_text="Solve x + 1 = 3.")
        value.update(changes)
        value["_id"] = self.db.question_bank.insert_one(value).inserted_id
        return value

    def test_deployment_namespace_has_one_owner_even_when_disabled(self):
        with self.assertRaises(AccessDenied):
            self.provision(client_id="another-client")
        with self.assertRaises(DuplicateKeyError):
            self.db.lti13_platforms.insert_one({"issuer": ISSUER, "client_id": "another-client",
                                              "deployment_ids": [SCOPE["deployment_id"]], "enabled": True})
        self.db.lti13_platforms.update_one({}, {"$set": {"enabled": False}})
        with self.assertRaises(AccessDenied):
            self.provision(client_id="another-client")
        with self.assertRaises(AccessDenied):
            self.store.authorize(self.scope)

    def test_ambiguous_existing_registrations_fail_every_read_closed(self):
        question = self.question()
        self.db.lti13_platforms.drop_index("lti13_issuer_deployment_unique")
        self.db.lti13_platforms.insert_one({"issuer": ISSUER, "client_id": "another-client",
                                          "deployment_ids": [SCOPE["deployment_id"]], "enabled": False})
        for client_id in (SCOPE["client_id"], "another-client"):
            scope = Scope(**{**SCOPE, "client_id": client_id})
            with self.assertRaises(AccessDenied):
                self.store.authorize(scope)
            with self.assertRaises(AccessDenied):
                self.store.call("get_prime_question", {**scope.model_dump(), "question_id": str(question["_id"])})

    def test_delimiter_collisions_are_rejected_without_provisioning(self):
        for issuer, deployment in (("https://tenant.example.test/a::b", "c"),
                                   ("https://tenant.example.test/a", "b::c"),
                                   ("https://tenant.example.test/a:", "b"),
                                   ("https://tenant.example.test/a", ":b")):
            with self.assertRaises(LTI13ConfigurationError):
                self.provision(issuer=issuer, deployment_id=deployment)
            with self.assertRaises(AccessDenied):
                self.store.authorize(Scope(**{**SCOPE, "platform_issuer": issuer, "deployment_id": deployment}))
        self.assertEqual(self.db.lti13_platforms.count_documents({}), 1)

    def test_overlapping_delimiter_boundaries_fail_existing_configuration_closed(self):
        scopes = [
            Scope(**{**SCOPE, "platform_issuer": "https://tenant.example.test/a:", "deployment_id": "b"}),
            Scope(**{**SCOPE, "platform_issuer": "https://tenant.example.test/a", "deployment_id": ":b"}),
        ]
        self.assertEqual(scopes[0].school_id, scopes[1].school_id)
        for scope in scopes:
            self.db.lti13_platforms.insert_one({
                "issuer": scope.platform_issuer, "client_id": scope.client_id,
                "deployment_ids": [scope.deployment_id], "enabled": True,
            })
        question = self.question(school_id=scopes[0].school_id)
        for scope in scopes:
            with self.assertRaises(AccessDenied):
                self.store.authorize(scope)
            for name, extra in (
                ("list_prime_materials", {}),
                ("get_prime_practice", {"goal": "test_preparation"}),
                ("get_prime_question", {"question_id": str(question["_id"])}),
            ):
                with self.assertRaises(AccessDenied):
                    self.store.call(name, {**scope.model_dump(), **extra})

    def test_shared_material_accepts_legacy_school_and_course_arrays(self):
        published = self.question(
            school_id=[self.scope.school_id, "another-published-school"],
            course_id=["prime", "another-published-course"],
        )
        self.question(
            school_id=["unrelated-school", "another-unrelated-school"],
            course_id=["prime", self.scope.course_id],
        )
        self.store.authorize(self.scope)
        documents, truncated = self.store.native_question_window(self.scope, "material")
        self.assertEqual({document["_id"] for document in documents}, {published["_id"]})
        self.assertFalse(truncated)

    def test_native_selector_never_shares_generated_questions(self):
        original = self.question()
        own = self.question(contents_id="ai_generated", course_id=self.scope.course_id,
                            user=self.scope.learner_id, page=self.scope.learner_id,
                            previous_quiz={"contents_id": "material"}, quiz_title="Own generated")
        for changes in ({"user": "prime"}, {"user": "other-learner"}, {"course_id": "other-course"}):
            self.question(**{
                "contents_id": "ai_generated", "course_id": self.scope.course_id,
                "user": self.scope.learner_id, "previous_quiz": {"contents_id": "material"}, **changes,
            })
        self.question(show=False)
        self.question(school_id="other-school")
        self.store.authorize(self.scope)
        with timeout(2):
            documents, truncated = self.store.native_question_window(self.scope, "material")
        self.assertEqual({doc["_id"] for doc in documents}, {original["_id"], own["_id"]})
        self.assertFalse(truncated)

    def test_native_selector_is_bounded_and_reports_clipped_content(self):
        first = self.question(quiz_text="x" * 12001, answer_id=["invalid"], answer_page_start={"invalid": "x"})
        self.db.question_bank.insert_many([
            {**{key: value for key, value in first.items() if key != "_id"},
             "no": str(number), "quiz_text": "Bounded question"}
            for number in range(QUESTION_SCAN_LIMIT)
        ])
        self.store.authorize(self.scope)
        documents, truncated = self.store.native_question_window(self.scope, "material")
        self.assertEqual(len(documents), QUESTION_SCAN_LIMIT)
        self.assertTrue(truncated)
        self.assertEqual(documents[0]["quiz_text"], "x" * 12000)
        self.assertIsNone(documents[0]["answer_id"])
        self.assertIsNone(documents[0]["answer_page_start"])
        self.assertNotIn("previous_quiz", documents[0])

    def test_native_reports_are_recent_own_course_and_do_not_truncate_identifiers_into_aliases(self):
        base = dict(school_id=self.scope.school_id, course_id=self.scope.course_id,
                    user=self.scope.learner_id, contents_id="material", page=1, no=2,
                    understanding="すべて自力で解けた", user_answer="PRIVATE", timestamp=NOW)
        self.db.history.insert_many([{**base, "timestamp": NOW + timedelta(seconds=n)}
                                     for n in range(HISTORY_SCAN_LIMIT + 1)])
        for change in ({"user": "other-learner"}, {"course_id": "other-course"}, {"school_id": "other-school"}):
            self.db.history.insert_one({**base, **change, "timestamp": NOW + timedelta(days=10)})
        latest = self.db.history.insert_one({**base, "timestamp": (NOW + timedelta(days=1)).isoformat(),
                                            "understanding": "x" * 513,
                                            "previous_quiz": {"school_id": self.scope.school_id,
                                                              "contents_id": "material", "page": "x" * 513, "no": 2}}).inserted_id
        self.store.authorize(self.scope)
        reports, truncated = self.store.native_report_window(self.scope)
        self.assertEqual(len(reports), HISTORY_SCAN_LIMIT)
        self.assertTrue(truncated)
        self.assertEqual(reports[0][1]["_id"], latest)
        self.assertIsNone(reports[0][1]["understanding"])
        self.assertIsNone(reports[0][1]["previous_quiz"]["page"])
        self.assertEqual(reports[0][1]["no"], 2)
        self.assertTrue(all("user_answer" not in doc for _, doc in reports))
        self.assertEqual(reports[-1][1]["timestamp"], (NOW + timedelta(seconds=2)).replace(tzinfo=None))


if __name__ == "__main__":
    unittest.main()
