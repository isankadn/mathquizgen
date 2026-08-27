import os
import time
import unittest
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pymongo.errors import DuplicateKeyError

from lti13 import (
    LTI_CLAIM,
    MAX_JWKS_BYTES,
    LTI13ConfigurationError,
    PlatformRegistration,
    SafeRequestsSession,
    bounded_env_int,
    consume_lti11_nonce,
    load_tool_key_material,
    user_info_from_launch,
    validate_launch_claims,
)


class FakeNonceCollection:
    def __init__(self):
        self.ids = set()

    def create_index(self, *args, **kwargs):
        return None

    def insert_one(self, document):
        if document["_id"] in self.ids:
            raise DuplicateKeyError("duplicate nonce")
        self.ids.add(document["_id"])


class FakeDatabase:
    def __init__(self):
        self.collection = FakeNonceCollection()

    def __getitem__(self, name):
        return self.collection


class LTI13SecurityTests(unittest.TestCase):
    def registration(self, **overrides):
        values = {
            "name": "Test LMS",
            "issuer": "https://lms.example.edu",
            "client_id": "client-123",
            "deployment_ids": ["deployment-123"],
            "auth_login_url": "https://lms.example.edu/oidc/auth",
            "auth_token_url": "https://lms.example.edu/oauth/token",
            "key_set_url": "https://lms.example.edu/.well-known/jwks.json",
            "enabled": True,
        }
        values.update(overrides)
        return PlatformRegistration.from_mapping(values)

    def launch_claims(self, **overrides):
        now = int(time.time())
        values = {
            "iss": "https://lms.example.edu",
            "aud": "client-123",
            "sub": "opaque-user-123",
            "iat": now,
            "exp": now + 180,
            f"{LTI_CLAIM}message_type": "LtiResourceLinkRequest",
            f"{LTI_CLAIM}deployment_id": "deployment-123",
            f"{LTI_CLAIM}target_link_uri": "https://tool.example.edu/app/ui/",
            f"{LTI_CLAIM}roles": [
                "http://purl.imsglobal.org/vocab/lis/v2/membership#Learner"
            ],
            f"{LTI_CLAIM}context": {"id": "course-1", "title": "Algebra"},
            f"{LTI_CLAIM}resource_link": {"id": "resource-1"},
        }
        values.update(overrides)
        return values

    def test_platform_registration_requires_https(self):
        with (
            patch.dict(os.environ, {"LTI13_ALLOW_INSECURE_HTTP": "false"}),
            self.assertRaises(LTI13ConfigurationError),
        ):
            self.registration(issuer="http://lms.example.edu").validate()

    def test_platform_registration_rejects_duplicate_deployments(self):
        with self.assertRaises(LTI13ConfigurationError):
            self.registration(
                deployment_ids=["deployment-123", "deployment-123"]
            ).validate()

    def test_optional_audience_survives_database_round_trip(self):
        registration = PlatformRegistration.from_mapping(
            self.registration().as_document()
        )
        self.assertIsNone(registration.auth_audience)
        registration.validate()

    def test_signing_key_requires_rsa_2048_or_larger(self):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        with NamedTemporaryFile() as key_file:
            key_file.write(pem)
            key_file.flush()
            with patch.dict(
                os.environ,
                {"LTI13_PRIVATE_KEY_FILE": key_file.name, "LTI13_KEY_ID": "test-key"},
                clear=False,
            ):
                material = load_tool_key_material()
        self.assertEqual(material.kid, "test-key")
        self.assertEqual(material.jwks["keys"][0]["alg"], "RS256")
        self.assertNotIn("d", material.jwks["keys"][0])

    def test_launch_rejects_target_link_mixup(self):
        with self.assertRaises(LTI13ConfigurationError):
            validate_launch_claims(
                self.launch_claims(),
                "client-123",
                "https://tool.example.edu/app/other",
            )

    def test_launch_requires_authorized_party_for_multiple_audiences(self):
        claims = self.launch_claims(aud=["client-123", "other-client"])
        with self.assertRaises(LTI13ConfigurationError):
            validate_launch_claims(
                claims, "client-123", "https://tool.example.edu/app/ui/"
            )
        claims["azp"] = "client-123"
        validate_launch_claims(claims, "client-123", "https://tool.example.edu/app/ui/")

    def test_launch_rejects_old_tokens(self):
        now = int(time.time())
        claims = self.launch_claims(iat=now - 1000, exp=now + 30)
        with self.assertRaises(LTI13ConfigurationError):
            validate_launch_claims(
                claims, "client-123", "https://tool.example.edu/app/ui/"
            )

    def test_launch_maps_only_required_identity_context(self):
        user = user_info_from_launch(self.launch_claims(email="private@example.edu"))
        self.assertEqual(user["lti_version"], "1.3")
        self.assertEqual(user["user_id"], "opaque-user-123")
        self.assertEqual(user["context_id"], "course-1")
        self.assertNotIn("email", user)

    def test_lti11_nonce_is_consumed_once(self):
        database = FakeDatabase()
        params = {
            "oauth_consumer_key": "consumer-1",
            "oauth_nonce": "unique-nonce",
            "oauth_timestamp": str(int(time.time())),
        }
        self.assertTrue(consume_lti11_nonce(database, params))
        self.assertFalse(consume_lti11_nonce(database, params))

    def test_lti11_nonce_rejects_future_timestamp(self):
        params = {
            "oauth_consumer_key": "consumer-1",
            "oauth_nonce": "future-nonce",
            "oauth_timestamp": str(int(time.time()) + 1000),
        }
        self.assertFalse(consume_lti11_nonce(FakeDatabase(), params))

    def test_security_timing_environment_is_bounded(self):
        with patch.dict(
            os.environ,
            {
                "LTI13_MAX_TOKEN_AGE_SECONDS": "999999",
                "LTI13_CLOCK_SKEW_SECONDS": "-10",
            },
        ):
            self.assertEqual(
                bounded_env_int(
                    "LTI13_MAX_TOKEN_AGE_SECONDS", 300, minimum=60, maximum=600
                ),
                600,
            )
            self.assertEqual(
                bounded_env_int("LTI13_CLOCK_SKEW_SECONDS", 60, minimum=0, maximum=120),
                0,
            )

    def test_jwks_response_without_length_is_still_size_limited(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b"x" * (MAX_JWKS_BYTES + 1)
        response._content_consumed = True
        with (
            patch("lti13.assert_safe_remote_url"),
            patch.object(requests.Session, "request", return_value=response),
            self.assertRaises(requests.RequestException),
        ):
            SafeRequestsSession().get("https://lms.example.edu/jwks")


if __name__ == "__main__":
    unittest.main()
