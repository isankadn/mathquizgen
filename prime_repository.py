"""Shared PRIME Mongo authorization and bounded learner reads.

Safe to import from the native UI: no MCP SDK, Gradio, connections, or writes.
Call authorize(scope) before using question_scope() with native UI queries.
"""
from __future__ import annotations

from datetime import datetime, timezone
from itertools import islice
from typing import Annotated, Literal

from bson import ObjectId
from bson.errors import InvalidId
from lti13 import PlatformRegistration
from pydantic import BaseModel, ConfigDict, Field
from pymongo import ASCENDING, timeout

QUERY_MS = 2000
QUESTION_SCAN_LIMIT = 500
HISTORY_SCAN_LIMIT = 200
MATERIAL_LIMIT = 50
UNDERSTANDING = (
    "解説を見てもわからなかった", "解説を見てわかった", "すべて自力で解けた"
)
BoundedId = Annotated[str, Field(strict=True, min_length=1, max_length=512, pattern=r"\S")]
BoundedUrl = Annotated[str, Field(strict=True, min_length=1, max_length=2048)]


class StrictArguments(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Scope(StrictArguments):
    platform_issuer: BoundedUrl
    client_id: BoundedId
    deployment_id: BoundedId
    learner_id: BoundedId
    course_id: BoundedId

    @property
    def school_id(self):
        return f"{self.platform_issuer}::{self.deployment_id}"


class PracticeArguments(Scope):
    material_id: BoundedId | None = None
    goal: Literal["review", "extension", "test_preparation"] = "review"
    limit: Annotated[int, Field(strict=True, ge=1, le=5)] = 3


class QuestionArguments(Scope):
    question_id: Annotated[str, Field(strict=True, min_length=1, max_length=128)]


class ProvisionArguments(StrictArguments):
    issuer: BoundedUrl
    client_id: BoundedId
    deployment_id: BoundedId
    auth_login_url: BoundedUrl
    key_set_url: BoundedUrl
    auth_token_url: BoundedUrl | None = None


TOOL_MODELS = {
    "configure_prime_platform": ProvisionArguments,
    "list_prime_materials": Scope,
    "get_prime_practice": PracticeArguments,
    "get_prime_question": QuestionArguments,
}
TOOL_DESCRIPTIONS = {
    "configure_prime_platform": "Administrator only: provision an LTI 1.3 platform. Never expose to learner or teacher models.",
    "list_prime_materials": "List visible original PRIME materials in the trusted learner scope, not curricular units.",
    "get_prime_practice": "Suggest authorized original questions using recent own explicit self-reports, not inferred mastery. No answers or generation.",
    "get_prime_question": "Read an authorized question and rubric main steps, withholding answers and prompts.",
}
QUESTION_PROJECTION = {
    "contents_id": 1, "page": 1, "no": 1, "quiz_title": 1,
    "quiz_text": 1, "rubric": 1, "material_title": 1, "contents_title": 1,
    "previous_quiz.school_id": 1, "previous_quiz.contents_id": 1,
    "previous_quiz.page": 1, "previous_quiz.no": 1,
}
HISTORY_PROJECTION = {
    "contents_id": 1, "page": 1, "no": 1, "understanding": 1, "timestamp": 1,
    "previous_quiz.school_id": 1, "previous_quiz.contents_id": 1,
    "previous_quiz.page": 1, "previous_quiz.no": 1,
}


class AccessDenied(ValueError):
    pass


def bounded_text(value, maximum):
    return value[:maximum] if isinstance(value, str) else ""


def _mongo_bounded_field(name, maximum, *, clip=False):
    value = f"${name}"
    text = ({"$substrCP": [value, 0, maximum]} if clip else
            {"$cond": [{"$lte": [{"$strLenCP": value}, maximum]}, value, None]})
    return {"$switch": {"branches": [
        {"case": {"$eq": [{"$type": value}, "string"]}, "then": text},
        {"case": {"$in": [{"$type": value}, ["int", "long"]]}, "then": value},
    ], "default": None}}


def _ordered_reports(documents, truncated):
    dated = [(report_time(doc.get("timestamp")), doc) for doc in documents]
    dated = [item for item in dated if item[0] is not None]
    dated.sort(key=lambda item: (item[0], str(item[1]["_id"])), reverse=True)
    return dated[:HISTORY_SCAN_LIMIT], truncated or len(dated) > HISTORY_SCAN_LIMIT



def reference(document):
    # Do not convert objects/arrays into database selectors or match missing fields.
    fields = tuple(document.get(key) for key in ("contents_id", "page", "no"))
    if not all(isinstance(value, (str, int)) and not isinstance(value, bool) for value in fields):
        return None
    if not isinstance(fields[0], str) or not fields[0] or any(len(str(value)) > 512 for value in fields):
        return None
    return fields


def report_time(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def question_result(document):
    return {
        "question_id": str(document["_id"]),
        "material_id": bounded_text(document.get("contents_id"), 512),
        # Gradio stores the learner principal as generated-question page.
        "page": None if document.get("contents_id") == "ai_generated" else bounded_text(str(document.get("page", "")), 512),
        "no": bounded_text(str(document.get("no", "")), 512),
        "title": bounded_text(document.get("quiz_title"), 256),
        "text": bounded_text(document.get("quiz_text"), 12000),
    }


class PrimeStore:
    def __init__(self, database):
        self.platforms = database["lti13_platforms"]
        self.questions = database["question_bank"]
        self.history = database["history"]

    def ensure_indexes(self):
        self.platforms.create_index(
            [("issuer", ASCENDING), ("client_id", ASCENDING)],
            unique=True, name="lti13_issuer_client_unique", maxTimeMS=QUERY_MS,
        )
        self.platforms.create_index(
            [("issuer", ASCENDING), ("deployment_ids", ASCENDING)],
            unique=True, name="lti13_issuer_deployment_unique", maxTimeMS=QUERY_MS,
        )
        self.questions.create_index(
            [("school_id", ASCENDING), ("user", ASCENDING),
             ("show", ASCENDING), ("contents_id", ASCENDING)],
            name="prime_mcp_visible_scope", maxTimeMS=QUERY_MS,
        )
        self.history.create_index(
            [("school_id", ASCENDING), ("course_id", ASCENDING),
             ("user", ASCENDING), ("timestamp", -1)],
            name="prime_mcp_own_reports", maxTimeMS=QUERY_MS,
        )

    def authorize(self, scope):
        if ("::" in scope.platform_issuer or scope.platform_issuer.endswith(":")
                or "::" in scope.deployment_id or scope.deployment_id.startswith(":")):
            raise AccessDenied("PRIME scope is not available.")
        registrations = list(self.platforms.find(
            {"issuer": scope.platform_issuer, "deployment_ids": scope.deployment_id},
            {"client_id": 1, "enabled": 1},
        ).limit(2).max_time_ms(QUERY_MS))
        if (len(registrations) != 1 or registrations[0].get("client_id") != scope.client_id
                or registrations[0].get("enabled") is not True):
            raise AccessDenied("PRIME scope is not available.")

    def question_scope(self, scope, *, originals=False):
        base = {"school_id": scope.school_id, "show": True}
        original = {
            "contents_id": {"$ne": "ai_generated"},
            "course_id": {"$in": [scope.course_id, "prime"]},
            "user": {"$in": [scope.learner_id, "prime"]},
        }
        if originals:
            base.update(original)
        else:
            base["$or"] = [original, {
                "contents_id": "ai_generated", "course_id": scope.course_id,
                "user": scope.learner_id,
            }]
        return base

    def original_window(self, scope, material_id=None):
        query = self.question_scope(scope, originals=True)
        if material_id is not None:
            if material_id == "ai_generated":
                return [], False
            query["contents_id"] = material_id
        documents = list(self.questions.find(query, QUESTION_PROJECTION)
                         .sort("_id", ASCENDING).limit(QUESTION_SCAN_LIMIT + 1)
                         .max_time_ms(QUERY_MS))
        return documents[:QUESTION_SCAN_LIMIT], len(documents) > QUESTION_SCAN_LIMIT

    def native_question_window(self, scope, material_id):
        if not isinstance(material_id, str) or not material_id or len(material_id) > 512:
            raise AccessDenied("Material is not available.")
        query = self.question_scope(scope)
        query["$and"] = [{"$or": [
            {"contents_id": material_id}, {"previous_quiz.contents_id": material_id},
        ]}]
        projection = {name: _mongo_bounded_field(name, maximum, clip=name in {"quiz_title", "quiz_text"})
                      for name, maximum in {
                          "school_id": 3072, "contents_id": 512, "page": 512, "no": 512,
                          "quiz_title": 256, "quiz_text": 12000, "answer_id": 512,
                      }.items()}
        projection.update({"answer_page_start": _mongo_bounded_field("answer_page_start", 32),
                           "answer_page_end": _mongo_bounded_field("answer_page_end", 32),
                           "_text_truncated": {"$gt": [{"$strLenCP": {"$cond": [
                               {"$eq": [{"$type": "$quiz_text"}, "string"]}, "$quiz_text", "",
                           ]}}, 12000]}})
        documents = list(self.questions.aggregate([
            {"$match": query}, {"$sort": {"_id": 1}},
            {"$limit": QUESTION_SCAN_LIMIT + 1}, {"$project": projection},
        ], maxTimeMS=QUERY_MS))
        truncated = len(documents) > QUESTION_SCAN_LIMIT
        documents = documents[:QUESTION_SCAN_LIMIT]
        for document in documents:
            truncated |= document.pop("_text_truncated", False)
        return documents, truncated

    def native_report_window(self, scope):
        base = {"school_id": scope.school_id, "course_id": scope.course_id, "user": scope.learner_id}
        projection = {name: _mongo_bounded_field(name, 512)
                      for name in ("contents_id", "page", "no", "understanding")}
        projection["previous_quiz"] = {
            name: _mongo_bounded_field(f"previous_quiz.{name}", 3072 if name == "school_id" else 512)
            for name in ("school_id", "contents_id", "page", "no")
        }
        projection["timestamp"] = {"$cond": [
            {"$eq": [{"$type": "$timestamp"}, "string"]},
            _mongo_bounded_field("timestamp", 128), "$timestamp",
        ]}
        documents = []
        truncated = False
        for kind in ("date", "string"):
            window = list(self.history.aggregate([
                {"$match": {**base, "timestamp": {"$type": kind}}},
                {"$sort": {"timestamp": -1, "_id": -1}},
                {"$limit": HISTORY_SCAN_LIMIT + 1}, {"$project": projection},
            ], maxTimeMS=QUERY_MS))
            truncated |= len(window) > HISTORY_SCAN_LIMIT
            documents.extend(window[:HISTORY_SCAN_LIMIT])
        return _ordered_reports(documents, truncated)


    def list_prime_materials(self, scope):
        self.authorize(scope)
        questions, truncated = self.original_window(scope)
        materials = {}
        for question in questions:
            material = question.get("contents_id")
            if not isinstance(material, str) or not material or len(material) > 512:
                continue
            if material not in materials:
                title = question.get("material_title") or question.get("contents_title")
                materials[material] = {
                    "material_id": material, "title": bounded_text(title, 256) or material,
                }
        return {
            "materials": list(materials.values())[:MATERIAL_LIMIT],
            "limit": MATERIAL_LIMIT, "truncated": truncated or len(materials) > MATERIAL_LIMIT,
            "question_scan_limit": QUESTION_SCAN_LIMIT,
        }

    def get_prime_question(self, args):
        self.authorize(args)
        try:
            identifier = ObjectId(args.question_id)
        except (InvalidId, TypeError):
            raise AccessDenied("Question is not available.") from None
        query = self.question_scope(args)
        query["_id"] = identifier
        question = self.questions.find_one(query, QUESTION_PROJECTION, max_time_ms=QUERY_MS)
        if not question:
            raise AccessDenied("Question is not available.")
        result = question_result(question)
        rubric = question.get("rubric")
        steps = []
        if isinstance(rubric, dict):
            for item in islice(rubric.values(), 20):
                if isinstance(item, dict) and isinstance(item.get("main"), str):
                    steps.append(item["main"][:512])
        result["rubric_main_steps"] = steps
        return result

    def own_report_window(self, scope):
        base = {"school_id": scope.school_id, "course_id": scope.course_id,
                "user": scope.learner_id}
        documents = []
        truncated = False
        # BSON dates and historical ISO strings sort by type rather than time.
        # Bound each storage representation separately, then compare normalized times.
        for kind in ("date", "string"):
            query = {**base, "timestamp": {"$type": kind}}
            window = list(self.history.find(query, HISTORY_PROJECTION)
                          .sort([("timestamp", -1), ("_id", -1)])
                          .limit(HISTORY_SCAN_LIMIT + 1).max_time_ms(QUERY_MS))
            truncated |= len(window) > HISTORY_SCAN_LIMIT
            documents.extend(window[:HISTORY_SCAN_LIMIT])
        return _ordered_reports(documents, truncated)

    def authorized_reference(self, scope, ref, *, original):
        if ref is None or (original and ref[0] == "ai_generated"):
            return None
        query = self.question_scope(scope, originals=original)
        query.update(dict(zip(("contents_id", "page", "no"), ref)))
        return self.questions.find_one(query, QUESTION_PROJECTION, max_time_ms=QUERY_MS)

    def get_prime_practice(self, args):
        self.authorize(args)
        reports, history_truncated = self.own_report_window(args)
        latest = {}
        report_questions = {}
        for timestamp, report in reports:
            ref = reference(report)
            generated = ref is not None and ref[0] == "ai_generated"
            if generated:
                # The owned generated record AND its original reference must exist.
                owned = self.authorized_reference(args, ref, original=False)
                previous = report.get("previous_quiz")
                stored_previous = owned.get("previous_quiz") if owned else None
                if not isinstance(previous, dict) or not isinstance(stored_previous, dict):
                    continue
                if previous.get("school_id") != args.school_id or stored_previous.get("school_id") != args.school_id:
                    continue
                ref = reference(previous)
                if ref != reference(stored_previous):
                    continue
            if ref is None or ref[0] == "ai_generated":
                continue
            if args.material_id is not None and ref[0] != args.material_id:
                continue
            question = self.authorized_reference(args, ref, original=True)
            if question is None:
                continue
            key = str(question["_id"])
            if key not in latest:
                # Even an unknown latest value supersedes an older known value.
                value = report.get("understanding")
                latest[key] = (timestamp, value if value in UNDERSTANDING else None, generated)
                report_questions[key] = question
        available, question_truncated = self.original_window(args, args.material_id)
        candidates = {str(question["_id"]): question for question in available if reference(question)}
        candidates.update(report_questions)
        priorities = ({UNDERSTANDING[2]: 0} if args.goal == "extension" else
                      {UNDERSTANDING[0]: 0, UNDERSTANDING[1]: 1})
        def rank(key):
            report = latest.get(key)
            return (priorities.get(report[1], 2) if report else 3,
                    -report[0].timestamp() if report else 0, key)
        suggestions = []
        for key in sorted(candidates, key=rank)[:args.limit]:
            item = question_result(candidates[key])
            report = latest.get(key)
            item["self_reported_understanding"] = report[1] if report else None
            item["recorded_at"] = report[0].isoformat() if report else None
            item["self_report_source"] = ("linked_generated_practice" if report[2] else "original") if report else None
            if report is None or report[1] is None:
                item["recommendation_reason"] = "no_self_report"
            elif report[2]:
                item["recommendation_reason"] = "Own latest explicit self-report on generated practice linked to this authorized original; not evidence of original-question mastery."
            else:
                item["recommendation_reason"] = "Own latest explicit self-report on this original question in the bounded recent window."
            suggestions.append(item)
        return {
            "questions": suggestions, "goal": args.goal,
            "evidence": "own_recent_self_reports" if any(item["self_reported_understanding"] for item in suggestions) else "no_self_report",
            "coverage": {
                "history_limit": HISTORY_SCAN_LIMIT, "history_scanned": len(reports),
                "history_truncated": history_truncated,
                "question_scan_limit": QUESTION_SCAN_LIMIT, "questions_truncated": question_truncated,
                "note": "Bounded recent window, not complete history. Dates and ISO strings are scanned separately; string windows use stored lexical order. Self-reports are not objective correctness or mastery.",
            },
        }

    def configure_prime_platform(self, args):
        key = {"issuer": args.issuer, "client_id": args.client_id}
        existing = self.platforms.find_one(key, max_time_ms=QUERY_MS) or {}
        if self.platforms.find_one(
            {"issuer": args.issuer, "deployment_ids": args.deployment_id,
             "client_id": {"$ne": args.client_id}}, {"_id": 1}, max_time_ms=QUERY_MS,
        ):
            raise AccessDenied("PRIME deployment namespace is already registered to another client.")
        token_url = args.auth_token_url or existing.get("auth_token_url") or args.auth_login_url
        values = {**existing, **key, "name": existing.get("name") or "Meta-Agent",
                  "auth_login_url": args.auth_login_url, "auth_token_url": token_url,
                  "key_set_url": args.key_set_url, "enabled": True,
                  "deployment_ids": [args.deployment_id]}
        # Existing import-safe validation is shared with the actual LTI adapter.
        PlatformRegistration.from_mapping(values).validate()
        if "deployment_ids" in existing and not isinstance(existing["deployment_ids"], list):
            raise AccessDenied("Existing platform configuration requires operator review.")
        now = datetime.now(timezone.utc)
        updates = {"auth_login_url": args.auth_login_url, "key_set_url": args.key_set_url,
                   "enabled": True, "updated_at": now}
        insert_values = {"name": "Meta-Agent", "auth_audience": None, "created_at": now}
        if args.auth_token_url is not None:
            updates["auth_token_url"] = args.auth_token_url
        else:
            insert_values["auth_token_url"] = token_url
        self.platforms.update_one(key, {
            "$set": updates, "$addToSet": {"deployment_ids": args.deployment_id},
            "$setOnInsert": insert_values,
        }, upsert=True)
        if args.auth_token_url is None:
            # Do not overwrite a real endpoint installed concurrently by an operator.
            self.platforms.update_one(
                {**key, "auth_token_url": {"$in": [None, ""]}},
                {"$set": {"auth_token_url": token_url}},
            )
        return {"configured": True}

    def call(self, name, arguments):
        model = TOOL_MODELS.get(name)
        if model is None:
            raise AccessDenied("Tool is not available.")
        args = model.model_validate(arguments)
        with timeout(QUERY_MS / 1000):
            return getattr(self, name)(args)


