import gradio as gr
from openai import OpenAI
import time
import random
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, ReplaceOne, timeout
from pymongo.errors import PyMongoError
from pydantic import ValidationError
import uuid
import json
import base64
import clickhouse_connect
from google import genai
import re
from functools import wraps
from inspect import signature
from prime_repository import AccessDenied, PrimeStore, Scope, QUERY_MS, QUESTION_SCAN_LIMIT, HISTORY_SCAN_LIMIT

JST = timezone(timedelta(hours=9))

load_dotenv()

# Mongo 起動を待ってから接続
mongo_client = MongoClient(os.getenv("MONGO_URL"))

# データベースとコレクションを定義
quiz_generator_db = mongo_client["prime"]
exercise_col = quiz_generator_db["question_bank"]
history_col = quiz_generator_db["history"]
logs_col = quiz_generator_db["logs"]
xapi_col = quiz_generator_db["xAPI"]


def _prime_lti_scope(lti):
    return Scope(
        platform_issuer=lti["lti_issuer"], client_id=lti["lti_client_id"],
        deployment_id=lti["lti_deployment_id"], learner_id=lti["user_id"],
        course_id=lti["context_id"],
    )


def _prime_mongo_budget(callback):
    """One absolute Mongo budget, including nested reads, for native 1.3 callbacks."""
    lti_index = list(signature(callback).parameters).index("lti")

    @wraps(callback)
    def bounded(*args, **kwargs):
        lti = kwargs.get("lti") if "lti" in kwargs else args[lti_index]
        if lti and lti.get("lti_version") == "1.3":
            try:
                with timeout(QUERY_MS / 1000):
                    return callback(*args, **kwargs)
            except (PyMongoError, AccessDenied, ValidationError) as error:
                print(f"Native PRIME operation failed: {type(error).__name__}")
                raise gr.Error("PRIME is unavailable for this course. Please ask your teacher to check the connection.") from None
        return callback(*args, **kwargs)

    return bounded


def _prime_authorized_lti(lti):
    if lti.get("lti_version") != "1.3":
        return lti
    scope = _prime_lti_scope(lti)
    PrimeStore(quiz_generator_db).authorize(scope)
    return {**lti, "school_id": scope.school_id, "context_id": scope.course_id,
            "user_id": scope.learner_id}


def _native_text_projection(path, maximum):
    return {"$substrCP": [{"$cond": [
        {"$eq": [{"$type": path}, "string"]}, path, "",
    ]}, 0, maximum]}


def _native_history_notice(truncated):
    notice = f"現在のコースの自分の直近最大{HISTORY_SCAN_LIMIT}件の記録のみを表示・集計しています。全期間の履歴やクラスのデータではありません。自己申告であり、正答や習得度の判定ではありません。"
    if truncated:
        notice += " 古い記録は省略されています。"
    return notice


@_prime_mongo_budget
def _scoped_exercise_info(lti, reference):
    if lti.get("lti_version") == "1.3":
        store = PrimeStore(quiz_generator_db)
        scope = _prime_lti_scope(lti)
        store.authorize(scope)
        # Only scalar question references may become database predicates.
        if (any(not isinstance(reference.get(key), (str, int)) or isinstance(reference.get(key), bool)
                or not str(reference.get(key)) or len(str(reference.get(key))) > 512
                for key in ("contents_id", "page", "no"))
                or not isinstance(reference.get("contents_id"), str)):
            raise gr.Error("The selected question is no longer available.")
        selected = {key: reference[key] for key in ("contents_id", "page", "no")}
        if selected["contents_id"] == "ai_generated":
            selected.update(user=scope.learner_id, course_id=scope.course_id,
                            page=scope.learner_id)
            if reference["page"] != scope.learner_id:
                raise gr.Error("The selected question is no longer available.")
        projection = {
            "_id": 0, "isfigure": {"$eq": ["$isfigure", True]},
            **{key: _native_text_projection(f"${key}", 12000)
               for key in ("standard_answer", "quiz_ans", "answer", "figure_explanation")},
            "rubric": {"$arrayToObject": {"$map": {
                "input": {"$slice": [{"$objectToArray": {"$cond": [
                    {"$eq": [{"$type": "$rubric"}, "object"]}, "$rubric", {},
                ]}}, 20]},
                "as": "step",
                "in": {"k": _native_text_projection("$$step.k", 512),
                       "v": {"main": _native_text_projection("$$step.v.main", 512)}},
            }}},
        }
        projection["detail_truncated"] = {"$or": [
            *[{"$gt": [{"$strLenCP": {"$cond": [
                {"$eq": [{"$type": f"${key}"}, "string"]}, f"${key}", "",
            ]}}, 12000]} for key in ("standard_answer", "quiz_ans", "answer", "figure_explanation")],
            {"$gt": [{"$size": {"$objectToArray": {"$cond": [
                {"$eq": [{"$type": "$rubric"}, "object"]}, "$rubric", {},
            ]}}}, 20]},
            {"$anyElementTrue": [{"$map": {
                "input": {"$slice": [{"$objectToArray": {"$cond": [
                    {"$eq": [{"$type": "$rubric"}, "object"]}, "$rubric", {},
                ]}}, 20]},
                "as": "step",
                "in": {"$gt": [{"$strLenCP": {"$cond": [
                    {"$eq": [{"$type": "$$step.v.main"}, "string"]}, "$$step.v.main", "",
                ]}}, 512]},
            }}]},
        ]}
        documents = list(exercise_col.aggregate([
            {"$match": {"$and": [store.question_scope(scope), selected]}},
            {"$sort": {"_id": 1}}, {"$limit": 1}, {"$project": projection},
        ], maxTimeMS=QUERY_MS))
        document = documents[0] if documents else None
        if document:
            if document.pop("detail_truncated", False):
                gr.Info("問題の詳細は文字数・項目数上限のある表示です。長い解答・説明または解答のポイントが省略されています。")
            document["standard_answer"] = (document["standard_answer"]
                                           or document["quiz_ans"] or document["answer"])
    else:
        document = exercise_col.find_one(reference)
    if document is None:
        raise gr.Error("The selected question is no longer available.")
    return document

# database選択
CLICKHOUSE_CONFIGS = {}
_i = 1
while True:
    _key = os.getenv(f"LTI_CONSUMER_KEY_{_i}")
    if not _key:
        break
    CLICKHOUSE_CONFIGS[_key] = {
        "host": os.getenv(f"BOOKROLL_DATABASE_HOST_{_i}"),
        "username": os.getenv(f"BOOKROLL_DATABASE_USER_{_i}"),
        "password": os.getenv(f"BOOKROLL_DATABASE_PASS_{_i}"),
        "db_name": os.getenv(f"BOOKROLL_DATABASE_NAME_{_i}", "saikyo_new")  # 指定がなければ saikyo_new
    }
    _i += 1

def get_clickhouse_client_and_db(school_id):
    """school_idに対応するClickHouseクライアントとデータベース名を返す"""
    config = CLICKHOUSE_CONFIGS.get(school_id)
    if not config:
        return None, None
    try:
        client = clickhouse_connect.get_client(
            host=config["host"],
            username=config["username"],
            password=config["password"]
        )
        return client, config["db_name"]
    except Exception as e:
        print(f"ClickHouse Connection Error for {school_id}: {e}")
        return None, None
# --------------------------------------------------------

#ltiセッション情報読み込み
def load_session_info(request: gr.Request):
    '''
    lti = {
            'user_id': user_id,
            'roles': roles,
            'browser_language': browser_language,
            'oauth_consumer_key': oauth_consumer_key,
            'context_id': context_id,
            'context_title': context_title,
            'school_id': school
        }
    '''
    lti = request.session['user']
    user = lti['user_id']
    return lti, user

openai_client = None
genai_client = None
llmjp_client = None


def get_openai_client():
    global openai_client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for OpenAI generation.")
    if openai_client is None:
        openai_client = OpenAI(api_key=api_key)
    return openai_client


def get_genai_client():
    global genai_client
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is required for Gemini generation.")
    if genai_client is None:
        genai_client = genai.Client(api_key=api_key)
    return genai_client


def get_llmjp_client():
    global llmjp_client
    api_key = os.getenv("LLM_JP_API_KEY")
    if not api_key:
        raise RuntimeError("LLM_JP_API_KEY is required for LLM-JP generation.")
    if llmjp_client is None:
        llmjp_client = OpenAI(
            base_url="https://llm-jp-playground.apps.llmc.nii.ac.jp/api/v1",
            api_key=api_key,
        )
    return llmjp_client

@_prime_mongo_budget
def get_journey_html(lti):
    if not lti or "user_id" not in lti:
        return gr.update(value="")

    lti = _prime_authorized_lti(lti)
    native = lti.get("lti_version") == "1.3"
    user_id = str(lti["user_id"])
    school_id = str(lti["school_id"])
    course_id = str(lti.get("context_id", ""))
    history_truncated = False
    all_history = []

    # -----------------------------------------------------------
    # 1. ClickHouse (元の問題の履歴 ＆ マーカーのカウント)
    # -----------------------------------------------------------
    clickhouse_client, db_name = (None, None) if native else get_clickhouse_client_and_db(school_id)
    marker_pages_count = 0

    if clickhouse_client and db_name:
        # A. 元の問題の解答履歴を取得
        try:
            sql = f"SELECT contents_id, page_no, CAST(results_response AS String), timestamp FROM {db_name}.statements_target WHERE actor_name_id = {{user:String}} AND operation_name = 'ANSWER_QUIZ' ORDER BY timestamp ASC"
            res = clickhouse_client.query(sql, {"user": user_id})
            for row in res.result_rows:
                c_id = str(row[0]) if row[0] else ""
                raw_page = row[1]
                if isinstance(raw_page, bytes):
                    p_no = raw_page.decode('utf-8', errors='ignore').replace('\x00', '').strip()
                else:
                    p_no = str(raw_page).replace('\x00', '').strip() if raw_page else ""
                
                q_key = f"{c_id}_{p_no}"
                all_history.append({"type": "original", "q_key": q_key, "result": str(row[2]) if row[2] else "", "timestamp": row[3]})
        except Exception as e:
            print(f"ClickHouse journey fetch failed: {e}")

        # B. ADD_MARKERログが存在するページ数のカウント
        course_contents_ids = set()
        try:
            for doc in exercise_col.find({"course_id": course_id}, {"contents_id": 1}):
                c_id = doc.get("contents_id")
                if c_id and c_id != "ai_generated":
                    course_contents_ids.add(c_id)
        except Exception as e:
            pass

        if course_contents_ids:
            try:
                # 修正: 'ADD_MARKER' のログが存在する(contents_id, page_no)の組み合わせの数をカウントする
                sql_marker = f"""
                SELECT count()
                FROM (
                    SELECT contents_id, page_no
                    FROM {db_name}.statements_target
                    WHERE actor_name_id = {{user:String}}
                      AND contents_id IN {{contents_list:Array(String)}}
                      AND operation_name = 'ADD_MARKER'
                    GROUP BY contents_id, page_no
                )
                """
                res_marker = clickhouse_client.query(sql_marker, {"user": user_id, "contents_list": list(course_contents_ids)})
                if res_marker.result_rows:
                    marker_pages_count = int(res_marker.result_rows[0][0])
            except Exception as e:
                print(f"ClickHouse marker count fetch failed: {e}")

    # -----------------------------------------------------------
    # 2. MongoDB (元の問題・復習問題の履歴)
    # -----------------------------------------------------------
    try:
        if native:
            reports, history_truncated = PrimeStore(quiz_generator_db).native_report_window(_prime_lti_scope(lti))
            docs = [{**doc, "timestamp": stamp} for stamp, doc in reports]
        else:
            query = {"school_id": school_id, "user": user_id}
            docs = list(history_col.find(query))
        for doc in docs:
            c_id = str(doc.get("contents_id", ""))
            p_no = str(doc.get("page", ""))
            n_no = str(doc.get("no", ""))
            q_type = "review" if c_id == "ai_generated" else "original"
            q_key = f"{c_id}_{p_no}_{n_no}"
            
            all_history.append({"type": q_type, "q_key": q_key, "result": doc.get("understanding", ""), "timestamp": doc.get("timestamp")})
    except Exception as e:
        if native:
            print(f"Native journey read failed: {type(e).__name__}")
            raise gr.Error("Recent history is temporarily unavailable.") from None
        print(f"Mongo journey fetch failed: {e}")

    # タイムスタンプでソート
    def normalize_tz(dt):
        if not dt: return datetime.min.replace(tzinfo=timezone.utc)
        if isinstance(dt, str):
            try: dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            except: pass
        if getattr(dt, "tzinfo", None) is None: return dt.replace(tzinfo=timezone.utc)
        return dt
    try: all_history.sort(key=lambda x: normalize_tz(x["timestamp"]))
    except: pass

    # -----------------------------------------------------------
    # 3. 最新のステータスだけを残す (演習も類題も)
    # -----------------------------------------------------------
    latest_orig_results = {}
    latest_rev_results = {}

    for x in all_history:
        res = str(x.get("result", ""))
        q_type = x["type"]
        q_key = x.get("q_key", "")
        
        status = None
        if "まったく" in res or "わから" in res or "不正解" in res:
            status = "red"
        elif "一部" in res or "見てわかった" in res:
            status = "yellow"
        elif "自力" in res or "正解" in res:
            status = "green"

        if status:
            if q_type == "original":
                latest_orig_results[q_key] = status
            else:
                latest_rev_results[q_key] = status

    # それぞれのカウント
    orig_green = orig_yellow = orig_red = 0
    rev_green = rev_yellow = rev_red = 0

    for status in latest_orig_results.values():
        if status == "red": orig_red += 1
        elif status == "yellow": orig_yellow += 1
        elif status == "green": orig_green += 1

    for status in latest_rev_results.values():
        if status == "red": rev_red += 1
        elif status == "yellow": rev_yellow += 1
        elif status == "green": rev_green += 1

    total_work = orig_green + orig_yellow + orig_red
    total_review = rev_green + rev_yellow + rev_red
    
    # 距離の計算 (元の問題のポイント + 類題のポイント + マーカーのページ数×2)
    distance = (29 * orig_green) + (19 * orig_yellow) + (7 * orig_red) \
             + (23 * rev_green) + (17 * rev_yellow) + (5 * rev_red) \
             + (marker_pages_count * 2)

    # -----------------------------------------------------------
    # 4. 🔄 周回ロジック と UI生成
    # -----------------------------------------------------------
    lap_length = 79037
    lap_num = (distance // lap_length) + 1
    lap_distance = distance % lap_length

    # マイルストーン定義
    milestones = [
        (0, "🏫 京都"), (134, "🇯🇵 姫路城"), (359, "🇯🇵 厳島神社"), (645, "🇯🇵 軍艦島"),
        (1243, "🇰🇷 昌徳宮"), (2428, "🇨🇳 黄山"), (3914, "🇻🇳 ハロン湾"), (4819, "🇰🇭 アンコール・ワット"),
        (5189, "🇹🇭 アユタヤ"), (7924, "🇮🇳 タージ・マハル"), (9216, "🇦🇫 バーミヤン渓谷"),
        (10873, "🇦🇪 アル・アインの遺跡群"), (13414, "🇹🇷 カッパドキア"), (14575, "🇬🇷 オリンピア"),
        (15682, "🇮🇹 フィレンツェ"), (16543, "🇩🇪 ケルン大聖堂"), (17156, "🇬🇧 ストーンヘンジ"),
        (17441, "🇫🇷 モン・サン＝ミシェル"), (18395, "🇪🇸 歴史的城壁都市クエンカ"), (19467, "🇲🇦 マラケシュ"),
        (25460, "🇹🇿 ンゴロンゴロ自然保護区"), (29326, "🇿🇦 カーステンボッシュ植物園"), (36594, "🇦🇷 ペリト・モレノ氷河"),
        (44592, "🇳🇿 トンガリロ国立公園"), (48143, "🇦🇺 グレート・バリア・リーフ"), (58438, "🇨🇱 イースター島"),
        (62240, "🇵🇪 ナスカの地上絵"), (64559, "🇪🇨 ガラパゴス"), (69072, "🇺🇸 グランド・キャニオン"),
        (71297, "🇨🇦 カナディアンロッキー"), (77746, "🇯🇵 知床"), (79037, "🏫 京都")
    ]

    current_m = milestones[0][1]
    current_d_rel = milestones[0][0]
    next_m = milestones[1][1]
    next_d_rel = milestones[1][0]

    for i in range(len(milestones)):
        if lap_distance >= milestones[i][0]:
            current_m = milestones[i][1]
            current_d_rel = milestones[i][0]
            if i + 1 < len(milestones):
                next_m = milestones[i+1][1]
                next_d_rel = milestones[i+1][0]
            else:
                current_m = milestones[0][1]
                current_d_rel = milestones[0][0]
                next_m = milestones[1][1]
                next_d_rel = milestones[1][0]

    segment_length = next_d_rel - current_d_rel
    if segment_length > 0:
        progress_pct = ((lap_distance - current_d_rel) / segment_length) * 100
    else:
        progress_pct = 100
    
    progress_pct = max(0, min(100, progress_pct))

    # 軌跡（煙）の生成（直近40回のあらゆるアクション）
    recent = all_history[-40:]
    trail = "".join(["🟢" if "自力" in str(x.get("result","")) or "正解" in str(x.get("result","")) else 
                     "🟡" if "一部" in str(x.get("result","")) or "見てわかった" in str(x.get("result","")) else 
                     "🔴" if x.get("result") else "⚪" for x in recent])

    # 修正: UIの「マーカー」表示を削除
    html = f"""<div style="background: linear-gradient(135deg, #f1f5f9 0%, #d0ddfb 100%); padding: 20px; border-radius: 12px; font-family: sans-serif; box-shadow: 0 4px 12px rgba(0,0,0,0.08); border: 1px solid #cbd5e1;" translate="no"><div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; border-bottom: 1px solid rgba(0,0,0,0.1); padding-bottom: 12px;"><div style="font-size: 20px; font-weight: bold; letter-spacing: 1px; color: #1e293b; padding-top: 4px;">🌏 PRIME - WORLD HERITAGE - ({lap_num}周目)<br><br>問題を解いたり、まちがいを直したりすると、移動距離UP!</div><div style="display: flex; flex-direction: column; align-items: flex-end; gap: 6px;"><div style="font-size: 14px; background: #e2e8f0; padding: 4px 12px; border-radius: 20px; color: #334155; border: 1px solid #cbd5e1;">📚 演習: <b style="color: #0f172a;">{total_work}</b> 問 &nbsp;|&nbsp; 🔄 復習: <b style="color: #0f172a;">{total_review}</b> 問</div><div style="font-size: 13px; background: #e2e8f0; padding: 4px 12px; border-radius: 20px; color: #334155; border: 1px solid #cbd5e1;">✅: <b style="color: #0f172a;">{orig_green+rev_green}</b> &nbsp;|&nbsp; 🟨: <b style="color: #0f172a;">{orig_yellow+rev_yellow}</b> &nbsp;|&nbsp; 🟥: <b style="color: #0f172a;">{orig_red+rev_red}</b></div></div></div><div style="background: #ffffff; border-radius: 8px; padding: 16px; position: relative; overflow: hidden; border: 1px solid #e2e8f0; box-shadow: inset 0 2px 4px rgba(0,0,0,0.02);"><div style="font-size: 15px; margin-bottom: 6px; display: flex; justify-content: space-between;"><span style="color: #475569;">移動距離: <span style="font-size: 22px; font-weight: bold; color: #0f172a;">{distance:,} km</span></span><span style="font-size: 13px; align-self: flex-end; color: #64748b;">(次の目的地 <span style="color: #334155; font-weight: bold;">{next_m}</span> まであと <span style="color: #0f172a; font-weight: bold;">{next_d_rel - lap_distance:,} km</span>)</span></div><div style="padding: 0 10px;"><div style="display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 14px;"><span style="color: #0f172a; font-weight: bold;">{current_m}</span><span style="color: #475569; font-weight: bold;">{next_m}</span></div><div style="position: relative; width: 100%; height: 36px;"><div style="position: absolute; left: 0; right: 0; top: 14px; height: 6px; background: #e2e8f0; border-radius: 3px;"></div><div style="position: absolute; left: 0; top: 14px; height: 6px; background: #3b82f6; border-radius: 3px; box-shadow: 0 0 8px rgba(59, 130, 246, 0.4); width: {progress_pct}%;"></div><div style="position: absolute; left: {progress_pct}%; top: -4px; font-size: 28px; transform: translateX(-50%); filter: drop-shadow(0 2px 4px rgba(0,0,0,0.2)); z-index: 10;">✈️</div></div></div></div></div>"""
    if native:
        html = '<p translate="no">' + _native_history_notice(history_truncated) + '</p>' + html
    return gr.update(value=html, visible=True)

# genaiのapiを走らせる
def gpt_exection(model, query):
    '''
    str: model, str: query
    '''
    if model in ["gpt-3.5-turbo", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini", "o1-preview", "o1-mini", "o3-mini", "o4-mini", "gpt-5", "gpt-4.1-nano"]:
        completion = get_openai_client().chat.completions.create(
            model=model,
            messages=[
                {
                "role": "user", "content": query
                }
            ]
        )
        response = completion.choices[0].message.content
    elif model in ["llm-jp-4-32b-a3b-thinking", "llm-jp-4-8b-instruct"]:
        completion = get_llmjp_client().chat.completions.create(
                    model=model,
                    messages=[
                        {
                        "role": "user", "content": query
                        }
                    ]
                )
        response = completion.choices[0].message.content
    else:
        ans = get_genai_client().models.generate_content(
        model=model, contents = query
        )
        response = ans.text
    return response

@_prime_mongo_budget
def handle_answer(exercise, answer, save, user_id, session, contents_id, page, no, y_marker, r_marker, tags, markers, school, new_contents_id, new_page, new_no, description, user_answer, understanding, rating, difficulty, fluency, relevance, new_checkbox, lti, report_type, report_text):
    lti = _prime_authorized_lti(lti)
    if lti.get("lti_version") == "1.3":
        user_id, school = lti["user_id"], lti["school_id"]
        if contents_id == "ai_generated":
            page = user_id
        if new_contents_id == "ai_generated":
            new_page = user_id
    report = {
        "report_type": report_type,
        "report_text": report_text
    }
    if save:
        previous_quiz = {
            "school_id": school,
            "contents_id": contents_id,
            "page": page,
            "no": no,
            "tags": tags,
            "yellow_marker": y_marker,
            "red_marker": r_marker,
            "markers": markers
        }
    else:
        previous_quiz = {}
    
    history_doc = {
        "user": user_id,
        "user_role": lti["roles"],
        "contents_id": new_contents_id,
        "page": new_page,
        "no": new_no,
        "exercise_text": exercise,
        "standard_answer": answer,
        "description": description,
        "user_answer": user_answer,
        "understanding": understanding,
        "understanding_details": new_checkbox,
        "rating": rating,
        "difficulty": difficulty,
        "fluency": fluency,
        "relevance": relevance,
        "report": report,
        "previous_quiz": previous_quiz,
        "timestamp": datetime.now(JST),
        "school_id": lti["school_id"],
        "course_id": lti["context_id"],
        "session_id": session
    }
    history_col.insert_one(history_doc)
    return

def handle_exercise(prev_contentsid, prev_page, prev_no, save, no, quiz_text, standard_answer, user, exercise_creation_time, answer_creation_time, model, session, lti, prompt_exercise, prompt_answer, custom_prompt="", rubric=None, figure_explanation=""):
    if lti.get("lti_version") == "1.3":
        with timeout(QUERY_MS / 1000):
            lti = _prime_authorized_lti(lti)
        user = lti["user_id"]
        if prev_contentsid == "ai_generated":
            prev_page = user
    if save:
        title = gpt_exection("gpt-4.1-nano", "以下の問題に短いタイトルをつけてください。タイトルのみを出力してください。\n{}".format(quiz_text))
        # rubricがNoneなら空にする
        rubric = rubric or {}
        previous_quiz = {
            "school_id": lti["school_id"],
            "contents_id": prev_contentsid,
            "page": prev_page,
            "no": prev_no
        }
        # 保存対象の小問構造
        new_entry = {
            "contents_id": "ai_generated",
            "page": user,
            "no": no,
            "quiz_title": title,
            "quiz_text": quiz_text,
            "standard_answer": standard_answer,
            "figure_explanation": figure_explanation,
            "rubric": rubric,
            "exercise_creation_time": exercise_creation_time,
            "answer_creation_time": answer_creation_time,
            "creation_model": model,
            "prompt_exercise": prompt_exercise,
            "prompt_answer": prompt_answer,
            "custom_prompt": custom_prompt,
            "previous_quiz": previous_quiz,
            "school_id": lti["school_id"],
            "course_id": lti["context_id"],
            "user": user,
            "session_id": session,
            "timestamp": datetime.now(JST),
            "show": True
        }
        if lti.get("lti_version") == "1.3":
            with timeout(QUERY_MS / 1000):
                exercise_col.insert_one(new_entry)
        else:
            exercise_col.insert_one(new_entry)
    return

def generate_xapi_statement(user_id, operationname_raw, session, lti, value=None):
    """
    表の定義に基づき、xAPI形式のログフォーマットを生成する関数
    """
    if lti.get("lti_version") == "1.3":
        user_id = lti["user_id"]
    # 1. Gradioから渡される操作名を、表の operation_name にマッピングする
    op_map = {
        "StartedSession": "StartedSession",
        "Traveled": "Traveled",
        "SelectedContents": "SelectedContents",
        "SelectedExercise": "SelectedQuestion",  # アプリ内ではSelectedExercise
        "SelectedBtnFromMarker": "SelectedCondition",  # 条件選択（マーカー）
        "SelectedBtnFromRubric": "SelectedCondition",  # 条件選択（ルーブリック）
        "SelectedRubricStatus": "SelectedRubricStatus",
        "SelectedMarkerInput": "SelectedMarkerInput",
        "SelectedModel": "SelectedModel",
        "SubmittedCheck": "SubmittedCheck",
        "RevSubmittedCheck": "SubmittedCheck", # そのまま解くボタン
        "CreatedQuestion": "CreatedQuestion",
        "CreateDescription": "CreatedDescription",
        "CreatedAnswer": "CreatedAnswer",
        "InputStudentAnswer": "ChangedAnswer", # アプリ内ではInputStudentAnswer
        "AnsweredExercise": "AnsweredQuestion", # アプリ内ではAnsweredExercise
        "SelectedComprehensibility": "UpdatedReflection_Understanding",
        "SelectedUsefulness": "UpdatedReflection_Rating",
        "SelectedDifficulty": "UpdatedReflection_Difficulty",
        "SelectedFluency": "UpdatedReflection_Fluency",
        "SelectedRelevance": "UpdatedReflection_Relevance",
        "SelectedNewRubricStatus": "UpdatedReflection_Rubricstatus",
        "Reported": "Reported"
    }
    
    op_name = op_map.get(operationname_raw, operationname_raw)
    
    # 2. 表のデータに基づくverb, contextマッピング
    meta = {
        "StartedSession": {"verb": "started", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/started", "c1": "PRIME", "c2": "Others", "c3": "StartedSession"},
        "Traveled": {"verb": "progressed", "verb_id": "http://adlnet.gov/expapi/verbs/progressed", "c1": "PRIME", "c2": "Others", "c3": "Traveled"},
        "SelectedContents": {"verb": "selected", "verb_id": "https://w3id.org/xapi/adb/verbs/selected", "c1": "PRIME", "c2": "Select", "c3": "SelectedContents"},
        "SelectedQuestion": {"verb": "selected", "verb_id": "https://w3id.org/xapi/adb/verbs/selected", "c1": "PRIME", "c2": "Select", "c3": "SelectedQuestion"},
        "SelectedCondition": {"verb": "selected", "verb_id": "https://w3id.org/xapi/adb/verbs/selected", "c1": "PRIME", "c2": "Assess", "c3": "SelectedCondition"},
        "SelectedRubricStatus": {"verb": "selected", "verb_id": "https://w3id.org/xapi/adb/verbs/selected", "c1": "PRIME", "c2": "Assess", "c3": "SelectedRubricStatus"},
        "SelectedMarkerInput": {"verb": "selected", "verb_id": "https://w3id.org/xapi/adb/verbs/selected", "c1": "PRIME", "c2": "Assess", "c3": "SelectedMarkerInput"},
        "SelectedModel": {"verb": "selected", "verb_id": "https://w3id.org/xapi/adb/verbs/selected", "c1": "PRIME", "c2": "Others", "c3": "SelectedModel"},
        "SubmittedCheck": {"verb": "submitted", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/submitted", "c1": "PRIME", "c2": "Create", "c3": "SubmittedCheck"},
        "CreatedQuestion": {"verb": "viewed", "verb_id": "http://id.tincanapi.com/verb/viewed", "c1": "PRIME", "c2": "Create", "c3": "CreatedQuestion"},
        "CreatedDescription": {"verb": "enabled", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/enabled", "c1": "PRIME", "c2": "Create", "c3": "CreatedDescription"},
        "CreatedAnswer": {"verb": "enabled", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/enabled", "c1": "PRIME", "c2": "Create", "c3": "CreatedAnswer"},
        "ChangedAnswer": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Work", "c3": "ChangedAnswer"},
        "AnsweredQuestion": {"verb": "attempted", "verb_id": "http://adlnet.gov/expapi/verbs/attempted", "c1": "PRIME", "c2": "Check", "c3": "AnsweredQuestion"},
        "UpdatedReflection_Understanding": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Reflect", "c3": "UpdatedReflection_Understanding"},
        "UpdatedReflection_Rating": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Reflect", "c3": "UpdatedReflection_Rating"},
        "UpdatedReflection_Difficulty": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Reflect", "c3": "UpdatedReflection_Difficulty"},
        "UpdatedReflection_Fluency": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Reflect", "c3": "UpdatedReflection_Fluency"},
        "UpdatedReflection_Relevance": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Reflect", "c3": "UpdatedReflection_Relevance"},
        "UpdatedReflection_Rubricstatus": {"verb": "updated", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/updated", "c1": "PRIME", "c2": "Reflect", "c3": "UpdatedReflection_Rubricstatus"},
        "Reported": {"verb": "submitted", "verb_id": "https://w3id.org/xapi/dod-isd/verbs/submitted", "c1": "PRIME", "c2": "Others", "c3": "Reported"}
    }
    
    op_info = meta.get(op_name)
    if not op_info:
        # 表に記載のない処理の場合は記録しない
        return {}

    # 3. extensions の中身を構築
    ext_data = {
        'session_id': str(session)
    }
    
    # 引数 value に入ってくる値（Gradioのステート）に応じた振り分け
    if value is not None:
        if op_name == "SelectedContents":
            ext_data['contents_name'] = str(value)
        elif op_name == "SelectedQuestion":
            ext_data['quiz_title'] = str(value)
        elif op_name == "SelectedModel":
            ext_data['model'] = str(value)
        elif op_name in ["SelectedRubricStatus", "UpdatedReflection_Rubricstatus"]:
            ext_data['rubrics'] = value
        elif op_name == "SelectedMarkerInput":
            ext_data['marker'] = value
        elif op_name == "ChangedAnswer":
            ext_data['student_answer'] = str(value)
        elif "UpdatedReflection_" in op_name:
            cat = op_name.split("_")[1]
            ext_data['categories'] = cat
            ext_data['value'] = str(value)
        else:
            ext_data['raw_value'] = value

    # operationname_raw 固有の例外処理（マーカーで作る/解答のポイントで作る のボタン判別）
    if operationname_raw == "SelectedBtnFromMarker":
        ext_data['value'] = {'marker': 'True', 'rubric': 'False'}
    elif operationname_raw == "SelectedBtnFromRubric":
        ext_data['value'] = {'marker': 'False', 'rubric': 'True'}
    elif operationname_raw == "RevSubmittedCheck":
        ext_data['review_type'] = 'reviewing'
    elif operationname_raw == "SubmittedCheck":
        ext_data['review_type'] = 'creating'

    # 4. xAPIステートメントの組み立て
    xapi_statement = {
        "actor": {
            "account": {
                "homePage": "https://prime.example.com", # 固定値
                "name": str(user_id)
            }
        },
        "verb": {
            "id": op_info["verb_id"],
            "display": {
                "en-US": op_info["verb"]
            }
        },
        "object": {
            "id": f"https://w3id.org/xapi/PRIME/activities/{op_name}",
            "definition": {
                "name": {
                    "en-US": "PRIME"
                },
                "description": {
                    "en-US": "数学問題生成機能"
                },
                "extensions": {
                    "https://w3id.org/xapi/PRIME/extension": ext_data
                }
            }
        },
        "context": {
            "contextActivities": {
                "category": [
                    {"id": f"https://w3id.org/xapi/PRIME/context/{op_info['c1']}"},
                    {"id": f"https://w3id.org/xapi/PRIME/context/{op_info['c2']}"},
                    {"id": f"https://w3id.org/xapi/PRIME/context/{op_info['c3']}"}
                ]
            }
        },
        "timestamp": datetime.now(JST).isoformat()
    }
    
    return xapi_statement

@_prime_mongo_budget
def handle_logs(user_id, operationname, session, lti, value=None):
    lti = _prime_authorized_lti(lti)
    if lti.get("lti_version") == "1.3":
        user_id = lti["user_id"]
    # 1. 既存のログ保存
    logs_col.insert_one({
        "school_id": lti["school_id"],
        "course_id": lti["context_id"],
        "user": user_id,
        "user_role": lti["roles"],
        "session_id": session,
        "timestamp": datetime.now(JST),
        "operationname": operationname,
        "value": value
    })
    
    # 2. xAPI形式のログ生成と保存
    xapi_data = generate_xapi_statement(user_id, operationname, session, lti, value)
    
    # 中身が実装（定義）されたらDBに書き込む処理
    # （空の辞書 {} をそのままMongoDBに保存すると不要な空データが増えるため、キーが存在するかで判定しています）
    if xapi_data:
        xapi_col.insert_one(xapi_data)
        
    return

#学年を特定する
def find_grade(context):
    grades_list = ["中学1年", "中学2年", "中学3年", "高校1年", "高校2年", "高校3年"]
    for grade in grades_list:
        if grade in context:
            return grade
    return "中学3年"

# ✅ MongoDBから再読み込みして State と Dropdown を更新する関数
@_prime_mongo_budget
def reload_quiz_map_from_mongo(lti, target_contents_id):
    native = lti.get("lti_version") == "1.3"
    debug = (lambda *args: None) if native else print
    debug("\n" + "="*50)
    debug(" [DEBUG START] reload_quiz_map_from_mongo")
    debug(f"  - target_contents_id: {target_contents_id}")
    debug("="*50)

    # 1. MongoDB (exercise_col) から問題一覧を取得
    questions_truncated = history_truncated = False
    if native:
        scope = _prime_lti_scope(lti)
        store = PrimeStore(quiz_generator_db)
        store.authorize(scope)
        documents, questions_truncated = store.native_question_window(scope, target_contents_id)
        reports, history_truncated = store.native_report_window(scope)
        history_docs = [doc for _, doc in reversed(reports)]
    else:
        query = {
            "school_id": lti["school_id"], "show": True,
            "$and": [
                {"course_id": {"$in": [lti["context_id"], "prime"]}},
                {"user": {"$in": [lti["user_id"], "prime"]}},
                {"$or": [{"contents_id": target_contents_id},
                         {"previous_quiz.contents_id": target_contents_id}]},
            ],
        }
        documents = list(exercise_col.find(query))

    debug(f"[STEP 1] exercise_col find完了: {len(documents)}件ヒット")

    if not documents:
        debug(" [RESULT] ドキュメントが0件のため、空のデータを返します。")
        if native:
            notice = _native_history_notice(history_truncated)
            if questions_truncated:
                notice += f" 問題一覧は最大{QUESTION_SCAN_LIMIT}件の限定表示です。問題または長い本文が省略されています。"
            return {}, gr.update(choices=[], value=None), gr.update(value=notice, visible=True)
        return {}, gr.update(choices=[], value=None)

    user_id = lti["user_id"]
    school_id = lti["school_id"]

    # --- 1. ClickHouseから元の問題の解答履歴を取得 ---
    ch_status_map = {}
    debug("[STEP 2] ClickHouse取得開始")
    clickhouse_client, db_name = (None, None) if native else get_clickhouse_client_and_db(school_id)
    if clickhouse_client and db_name:
        try:
            sql = f"""
            SELECT 
                page_no, 
                argMax(CAST(results_response AS String), timestamp) AS latest_response
            FROM {db_name}.statements_target
            WHERE actor_name_id = {{user:String}}
              AND contents_id = {{contents_id:String}}
              AND operation_name = 'ANSWER_QUIZ'
            GROUP BY page_no
            """
            params = {
                "user": str(user_id),
                "contents_id": str(target_contents_id)
            }
            res = clickhouse_client.query(sql, params)
            
            for row in res.result_rows:
                raw_page = row[0]
                if isinstance(raw_page, bytes):
                    page_no = raw_page.decode('utf-8', errors='ignore').replace('\x00', '').strip()
                else:
                    page_no = str(raw_page).replace('\x00', '').strip()
                    
                response_text = str(row[1]) if row[1] else ""
                
                # 判定ロジック
                if "まったく分からなかった" in response_text or "解説を見てもわからなかった" in response_text or "不正解" in response_text:
                    status = "🟥 "
                elif "一部解説を見て解いた" in response_text or "解説を見てわかった" in response_text or "一部正解" in response_text:
                    status = "🟨 "
                elif "すべて自力で解けた" in response_text or "正解" in response_text:
                    status = "✅ "
                else:
                    status = "⬜ "
                
                ch_status_map[page_no] = status
            
            debug(f" - ClickHouse結果: {len(ch_status_map)}件のページ履歴を取得")
            debug(f" - ch_status_map: {ch_status_map}")

        except Exception as e:
            debug(f" [ERROR] ClickHouse取得中に例外発生: {e}")
    else:
        debug(" - ClickHouseスキップ: 設定なし")

    # --- 2. MongoDB(history_col)から解答履歴を取得 ---
    debug(f"[STEP 3] history_col 検索ターゲット作成")
    mongo_status_map = {}
    search_targets = []
    for doc in documents:
        c_id = doc.get("contents_id")
        p = str(doc.get("page"))
        n = str(doc.get("no"))
        if c_id and p and n:
            search_targets.append({
                "contents_id": c_id,
                "page": p,
                "no": n
            })
            
    search_targets.append({"contents_id": target_contents_id})
    debug(f" - 検索ターゲット数: {len(search_targets)}")

    if not native:
        history_query = {"school_id": school_id, "user": user_id, "$or": search_targets}
        history_docs = list(history_col.find(history_query).sort("timestamp", 1))
    
    debug(f" - history_col 取得件数: {len(history_docs)}件")

    for h in history_docs:
        h_contents_id = h.get("contents_id")
        h_page = str(h.get("page"))
        h_no = str(h.get("no"))
        understanding = str(h.get("understanding", ""))
        
        emoji = ""
        if "まったく分からなかった" in understanding or "解説を見てもわからなかった" in understanding or "不正解" in understanding:
            emoji = "🟥 "
        elif "一部解説を見て解いた" in understanding or "解説を見てわかった" in understanding:
            emoji = "🟨 "
        elif "すべて自力で解けた" in understanding or "正解" in understanding:
            emoji = "✅ "
        else:
            emoji = "⬜ "

        if h_contents_id == "ai_generated":
            mongo_status_map[h_no] = emoji
        else:
            # 形式: contents_id + page + no
            key = f"{h_contents_id}_{h_page}_{h_no}"
            mongo_status_map[key] = emoji

    debug(f" - mongo_status_mapを構築完了 (件数: {len(mongo_status_map)})")
    debug(f" - mongo_status_map: {mongo_status_map}")

    def shorten_sessionid(sessionid, n=5):
        if not sessionid: return "unknown"
        try:
            sessionid_obj = uuid.UUID(sessionid)
            s = base64.urlsafe_b64encode(sessionid_obj.bytes)
            return s.decode('ascii').rstrip('=')[:n]
        except Exception as e:
            debug(f" [DEBUG] session_idの短縮に失敗: {e}")
            return "err"
    
    # --- 3. クイステキスト辞書の構築 ---
    debug(f"[STEP 4] 選択肢タイトルの構築と絵文字判定開始")
    quiz_text_dict = {}

    for doc in documents:
        text = doc.get("quiz_text")
        contents_id = doc.get("contents_id")
        page = lti["user_id"] if native and contents_id == "ai_generated" else str(doc.get("page"))
        no = str(doc.get("no"))
        ans = doc.get("answer_id", "")
        ans_page_s = doc.get("answer_page_start", "")
        ans_page_e = doc.get("answer_page_end", "")
        title = doc.get("quiz_title") or ""
        school = scope.school_id if native else doc.get("school_id")
        
        # 履歴に基づいて絵文字を決定
        prefix = "⬜ " 
        match_source = "None"
        
        if contents_id == "ai_generated":
            if no in mongo_status_map:
                prefix = mongo_status_map[no]
                match_source = f"Mongo(AI-No:{no})"
        else:
            mongo_key = f"{contents_id}_{page}_{no}"
            if mongo_key in mongo_status_map:
                prefix = mongo_status_map[mongo_key]
                match_source = f"Mongo(Key:{mongo_key})"
            elif page in ch_status_map:
                prefix = ch_status_map[page]
                match_source = f"ClickHouse(Page:{page})"

        # 判定結果を1つずつ出力
        debug(f"  [Judge] Title: {title[:15]}... | Key: {contents_id}/{page}/{no} | Prefix: {prefix} | Source: {match_source}")

        if contents_id == "ai_generated":
            session_short = "" if native else shorten_sessionid(doc.get("session_id"))
            
            # ▼追加：数字に変換できる時だけ4桁(0001等)にし、そうでない場合はそのまま表示する安全な処理
            try:
                formatted_no = f"{len(quiz_text_dict) + 1:04d}" if native else f"{int(no):04d}"
            except (ValueError, TypeError):
                formatted_no = str(no)
                
            display_title = prefix + "類題" + f"{formatted_no}: " + title
            if not native:
                display_title += f" (問題ID:{session_short})"
        else:
            display_title = prefix + title
        
        if title and text and contents_id and page and no and school:
            quiz_text_dict[display_title] = (text, contents_id, page, no, school, ans, ans_page_s, ans_page_e)

    # --- 4. ソート ---
    debug(f"[STEP 5] ソート処理開始")
    sorted_choices = sorted(
        quiz_text_dict.keys(),
        key=lambda k: k[2:] if k.startswith(('✅', '🟨', '🟥', '⬜')) else k
    )
    if native:
        notice = _native_history_notice(history_truncated)
        notice += f" 問題一覧は最大{QUESTION_SCAN_LIMIT}件です。⬜はこの最近の記録内に解答がない問題で、未学習の判定ではありません。"
        if questions_truncated:
            notice += " 問題または長い本文が省略されています。"
        print("Native scoped question window loaded.")
        return quiz_text_dict, gr.update(choices=sorted_choices, value=None, visible=True), gr.update(value=notice, visible=True)

    # ＝＝＝ 修正：演習（もとの問題）のみをカウントするように絞り込み ＝＝＝
    # quiz_text_dict[c][1] は contents_id を指します。これが "ai_generated" でないものだけを抽出
    orig_choices = [c for c in sorted_choices if quiz_text_dict[c][1] != "ai_generated"]

    cnt_green = sum(1 for c in orig_choices if c.startswith('✅'))
    cnt_yellow = sum(1 for c in orig_choices if c.startswith('🟨'))
    cnt_red = sum(1 for c in orig_choices if c.startswith('🟥'))
    cnt_white = sum(1 for c in orig_choices if c.startswith('⬜'))
    
    total_q = cnt_green + cnt_yellow + cnt_red + cnt_white

    color_green = "#00c853"
    color_yellow = "#ffb300"
    color_red = "#f44336"
    color_white = "#e0e0e0"

    blocks_html = (
        f'<span style="color: {color_green}; font-size: 20px; margin: 0 1px;">▮</span>' * cnt_green +
        f'<span style="color: {color_yellow}; font-size: 20px; margin: 0 1px;">▮</span>' * cnt_yellow +
        f'<span style="color: {color_red}; font-size: 20px; margin: 0 1px;">▮</span>' * cnt_red +
        f'<span style="color: {color_white}; font-size: 20px; margin: 0 1px;">▮</span>' * cnt_white
    )

    summary_text = f"✅：{cnt_green} &nbsp;&nbsp; 🟨：{cnt_yellow} &nbsp;&nbsp; 🟥：{cnt_red} &nbsp;&nbsp; ⬜：{cnt_white}"

    # ＝＝＝ 修正：進捗に応じた応援メッセージ（演習の進捗ベース） ＝＝＝
    progress_msg = "【あなたのこの教材の取り組み】　💪 コツコツ進めていきましょう！"
    if total_q > 0:
        if cnt_white == total_q:
            progress_msg = "【あなたのこの教材の取り組み】 🌱 さあ、学習を始めましょう！まずはBookRollで1問解いてみてください。"
        elif cnt_white == 0:
            if cnt_red == 0:
                progress_msg = "【あなたのこの教材の取り組み】 👑 素晴らしい！この教材の問題をすべてマスターしましたね！"
            else:
                progress_msg = "【あなたのこの教材の取り組み】 🔥 すべての問題に挑戦完了！🟥や🟨の問題を復習して完璧を目指しましょう！"

    if len(sorted_choices) > 0:
        summary_html = f"""
        <div style="background-color: #fcfcfc; padding: 16px; border-radius: 8px; border: 1px solid #e0e0e0; margin-top: 4px; margin-bottom: 8px;" translate="no">
            <div style="text-align: center; font-weight: bold; font-size: 16px; color: #1976d2; margin-bottom: 14px; letter-spacing: 0.5px;">
                {progress_msg}
            </div>
            <div style="display: flex; align-items: center; justify-content: center; flex-wrap: wrap; line-height: 1; margin-bottom: 12px;">
                {blocks_html}
            </div>
            <div style="text-align: center; font-weight: bold; font-size: 15px; color: #424242;">
                {summary_text}
            </div>
        </div>
        """
        summary_update = gr.update(value=summary_html, visible=True)
    else:
        summary_update = gr.update(value="", visible=False)
    # ▲▲▲ 追加ここまで ▲▲▲
    summary_update = ""

    debug(f" - ソート済み選択肢（先頭5件）: {sorted_choices[:5]}")
    debug(f" [DEBUG END] reload_quiz_map_from_mongo 正常終了")
    debug("="*50 + "\n")

    return quiz_text_dict, gr.update(choices=sorted_choices, value=None, visible=True), summary_update

@_prime_mongo_budget
def get_contents_dict_from_clickhouse(lti):
    if lti.get("lti_version") == "1.3":
        result = PrimeStore(quiz_generator_db).list_prime_materials(_prime_lti_scope(lti))
        contents_dict = {
            f"{material['title']} (ID: {material['material_id']})": {
                "contents_id": material["material_id"], "contents_name": material["title"],
            }
            for material in result["materials"]
        }
        if result["truncated"]:
            gr.Info("教材一覧は件数上限のある限定表示です。一部の教材は省略されています。")
        return contents_dict, gr.update(choices=sorted(contents_dict), value=None)
    # 1. MongoDBから該当の lti["context_id"] が course_id に含まれるものを検索
    query = {"course_id": lti["context_id"]}
    projection = {"contents_id": 1, "_id": 0}
    
    # 重複を省くために set を使用して contents_id を収集
    contents_ids = set()
    for doc in exercise_col.find(query, projection):
        c_id = doc.get("contents_id")
        if c_id:
            contents_ids.add(c_id)
            
    contents_dict = {}
    
    # 該当のIDが1つもなければ空の辞書を返す
    if not contents_ids:
        return contents_dict

    # 2. ClickHouseに接続
    school_id = str(lti["school_id"])
    clickhouse_client, db_name = get_clickhouse_client_and_db(school_id)
    
    if not clickhouse_client or not db_name:
        return contents_dict, gr.update(choices=[], value=None)

    # 3. それぞれの contents_id に対してClickHouseから contents_name を取得
    for c_id in contents_ids:
        sql = f"""
        SELECT contents_name 
        FROM {db_name}.statements_mv 
        WHERE operation_name = 'REGISTER_CONTENTS' 
        AND contents_id = {{contents_id:String}}
        ORDER BY timestamp DESC
        LIMIT 1
        """
        params = {"contents_id": str(c_id)}
        result = clickhouse_client.query(sql, params)
        
        # 取得結果が存在するかチェック
        if result.result_rows and len(result.result_rows) > 0:
            c_name = result.result_rows[0][0]
            
            # --- 追加：特定のフォーマットを検知して並び替える ---
            # 例: "第1章(問題) 正の数と負の数[STEP中1]" -> "[STEP中1] 第1章 正の数と負の数"
            match = re.match(r"^(.*?)\(問題\)\s*(.*?)(\[.*?\])$", c_name)
            if match:
                chapter = match.group(1).strip() # 例: "第1章"
                title = match.group(2).strip()   # 例: "正の数と負の数"
                tag = match.group(3).strip()     # 例: "[STEP中1]"
                c_name = f"{tag} {chapter} {title}"
            # ----------------------------------------------------
            
        else:
            c_name = "PRIMEが生成した問題"  # 万が一レコードが存在しなかった場合のフォールバック
            
        # キーの作成: contents_name(ID: 上4桁)
        key = f"{c_name}(ID: {str(c_id)[:4]})"
        
        # 値として contents_id と contents_name を持つ辞書（またはタプル）を登録
        contents_dict[key] = {
            "contents_id": c_id,
            "contents_name": c_name
        }

    return contents_dict, gr.update(choices=sorted(contents_dict.keys()), value=None)

phrases = [
    "STEP 1年7章をどんどんやるでありMath!",
    "BookRollの解答ページにマーカーを引くと、その情報を活かしMath!"
]

# userのこれまでのresultを入手する
@_prime_mongo_budget
def get_result_from_db(school, contents_id, page, no, user, lti, answer_contents_id, answer_page_start, answer_page_end):
    if lti.get("lti_version") == "1.3":
        scope = _prime_lti_scope(lti)
        store = PrimeStore(quiz_generator_db)
        store.authorize(scope)
        if contents_id == "ai_generated" and page != scope.learner_id:
            raise gr.Error("The selected question is no longer available.")
        reports, truncated = store.native_report_window(scope)
        gr.Info(_native_history_notice(truncated))
        working = reviews = 0
        history = []
        for _, document in reversed(reports):
            direct = (document.get("contents_id") == contents_id
                      and str(document.get("page")) == str(page)
                      and str(document.get("no")) == str(no))
            previous = document.get("previous_quiz")
            related = (contents_id != "ai_generated" and isinstance(previous, dict)
                       and previous.get("school_id") == scope.school_id
                       and previous.get("contents_id") == contents_id
                       and str(previous.get("page")) == str(page)
                       and str(previous.get("no")) == str(no))
            if direct:
                working += 1
            elif related:
                reviews += 1
            else:
                continue
            history.append({"type": "review" if contents_id == "ai_generated" or related else "original",
                            "result": document.get("understanding", "")})
        latest = history[-1]["result"] if history else ""
        return working, reviews, "", "", history, {}, latest
    
    num_workingquiz = 0
    num_reviewquiz = 0
    text_highlighted_yellow = ""
    text_highlighted_red = ""
    
    user_history_raw = []
    class_latest_responses = {}

    if contents_id == "ai_generated":
        # --- 類題が選ばれた場合 ---
        class_stats = {}
        try:
            query_review = {
                "user": user,
                "contents_id": contents_id,
                "page": page,
                "no": no
            }
            review_docs = list(history_col.find(query_review))
            num_workingquiz = len(review_docs)
            
            for doc in review_docs:
                ts = doc.get("timestamp")
                if ts and isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except:
                        pass
                resp = doc.get("understanding", "")
                user_history_raw.append({"type": "review", "result": resp, "timestamp": ts})
        except Exception as e:
            print(f"MongoDB query failed for ai_generated: {e}")
            
    else:
        # --- もとの問題が選ばれた場合 ---
        class_stats = {"green": [], "yellow": [], "red": []}

        clickhouse_client, db_name = get_clickhouse_client_and_db(lti["school_id"])
        
        if clickhouse_client and db_name:
            try:
                # B. 個人の元の問題の解答履歴
                sql_user_history = f"""
                SELECT CAST(results_response AS String), timestamp
                FROM {db_name}.statements_target
                WHERE operation_name='ANSWER_QUIZ'
                  AND actor_name_id={{user:String}}
                  AND contents_id={{contents_id:String}}
                  AND page_no={{page:String}}
                ORDER BY timestamp ASC
                """
                params_user = {
                    "user": str(user), 
                    "contents_id": str(contents_id), 
                    "page": str(page)
                }
                res_user = clickhouse_client.query(sql_user_history, params_user)
                
                for row in res_user.result_rows:
                    resp = str(row[0]) if row[0] else ""
                    ts = row[1]
                    user_history_raw.append({"type": "original", "result": resp, "timestamp": ts})
                    num_workingquiz += 1

                # C. クラス全体のもとの問題の正答率
                sql_class_stats = f"""
                SELECT actor_name_id, argMax(CAST(results_response AS String), timestamp)
                FROM {db_name}.statements_target
                WHERE operation_name='ANSWER_QUIZ'
                  AND contents_id={{contents_id:String}}
                  AND page_no={{page:String}}
                  AND context_id={{course_id:String}}
                GROUP BY actor_name_id
                """
                params_class = {
                    "contents_id": str(contents_id), 
                    "page": str(page),
                    "course_id": str(lti["context_id"])
                }
                res_class = clickhouse_client.query(sql_class_stats, params_class)
                
                for row in res_class.result_rows:
                    u_id = str(row[0])
                    resp = str(row[1]) if row[1] else ""
                    class_latest_responses[u_id] = resp

                # マーカー取得
                if answer_contents_id != "" and str(answer_page_start).isdigit() and str(answer_page_end).isdigit():
                    sql2 = f"""
                    SELECT
                        last_text AS marker_text,
                        last_color AS marker_color
                    FROM (
                        SELECT
                            marker_position,
                            argMax(CAST(operation_name AS String), timestamp) AS last_op,
                            argMax(CAST(marker_text AS String), timestamp) AS last_text,
                            argMax(CAST(marker_color AS String), timestamp) AS last_color,
                            argMax(CAST(page_no AS Int32), timestamp) AS last_page
                        FROM
                            {db_name}.statements_target
                        WHERE
                            actor_name_id = {{user:String}}
                            AND contents_id = {{answer_contents_id:String}}
                        GROUP BY
                            marker_position
                    )
                    WHERE
                        last_op = 'ADD_MARKER'
                        AND last_page BETWEEN {{answer_page_start:Integer}} AND {{answer_page_end:Integer}}                        
                    """
                    params2 = {
                        "user": str(user), 
                        "answer_contents_id": str(answer_contents_id), 
                        "answer_page_start": int(answer_page_start),
                        "answer_page_end": int(answer_page_end)
                    }
                    brmarkerdata = clickhouse_client.query(sql2, params2)
                    for row in brmarkerdata.result_rows:
                        text, color = row[0], row[1]
                        if text:
                            if color == "rgb(255,255,0)":
                                text_highlighted_yellow += text + " "
                            elif color == "rgb(255,0,0)":
                                text_highlighted_red += text + " "

            except Exception as e:
                print(f"ClickHouse query failed: {e}")

        # 追加: PRIME上でのそのまま解く履歴
        query_prime_orig = {
            "user": user,
            "school_id": lti["school_id"],
            "contents_id": contents_id,
            "page": page
        }
        try:
            prime_orig_docs = list(history_col.find(query_prime_orig))
            num_workingquiz += len(prime_orig_docs)
            for doc in prime_orig_docs:
                ts = doc.get("timestamp")
                if ts and isinstance(ts, str):
                    try: ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except: pass
                resp = doc.get("understanding", "")
                user_history_raw.append({"type": "original", "result": resp, "timestamp": ts})
        except Exception as e:
            pass

        # C-1. クラスのPRIME履歴マージ
        pipeline = [
            {
                "$match": {
                    "school_id": lti["school_id"], 
                    "course_id": lti["context_id"],
                    "contents_id": contents_id, 
                    "page": page, 
                    "no": no
                }
            },
            {"$sort": {"timestamp": 1}},
            {"$group": {"_id": "$user", "latest_understanding": {"$last": "$understanding"}}}
        ]
        try:
            for doc in list(history_col.aggregate(pipeline)):
                if doc.get("latest_understanding"):
                    class_latest_responses[doc["_id"]] = doc["latest_understanding"]
        except Exception as e:
            pass

        # === NEW: クラス全員の復習（類題）回数を集計 ===
        class_review_counts = {}
        pipeline_reviews = [
            {
                "$match": {
                    "school_id": lti["school_id"],
                    "course_id": lti["context_id"],
                    "previous_quiz.contents_id": contents_id,
                    "previous_quiz.page": page,
                    "previous_quiz.no": no
                }
            },
            {
                "$group": {
                    "_id": "$user",
                    "review_count": {"$sum": 1}
                }
            }
        ]
        try:
            for doc in list(history_col.aggregate(pipeline_reviews)):
                class_review_counts[doc["_id"]] = doc["review_count"]
        except Exception as e:
            pass

        # C-2. クラス集計（人数ではなく、ユーザーごとのデータを配列に格納）
        for u_id, resp in class_latest_responses.items():
            r_count = class_review_counts.get(u_id, 0)
            if "まったく分からなかった" in resp or "解説を見てもわからなかった" in resp or "不正解" in resp:
                class_stats["red"].append({"user": u_id, "review_count": r_count})
            elif "一部解説を見て解いた" in resp or "解説を見てわかった" in resp or "一部正解" in resp:
                class_stats["yellow"].append({"user": u_id, "review_count": r_count})
            elif "すべて自力で解けた" in resp or "正解" in resp:
                class_stats["green"].append({"user": u_id, "review_count": r_count})

        # D. 個人の復習問題（類題）の解答履歴
        query_review = {"user": user, "previous_quiz.school_id": lti["school_id"], "previous_quiz.contents_id": contents_id, "previous_quiz.page": page, "previous_quiz.no": no}
        try:
            review_docs = list(history_col.find(query_review))
            num_reviewquiz += len(review_docs)
            for doc in review_docs:
                ts = doc.get("timestamp")
                if ts and isinstance(ts, str):
                    try: ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except: pass
                resp = doc.get("understanding", "")
                user_history_raw.append({"type": "review", "result": resp, "timestamp": ts})
        except Exception as e:
            pass

    # E. 履歴をソート
    def normalize_tz(dt):
        if not dt: return datetime.min.replace(tzinfo=timezone.utc)
        if getattr(dt, "tzinfo", None) is None: return dt.replace(tzinfo=timezone.utc)
        return dt

    try:
        user_history_raw.sort(key=lambda x: normalize_tz(x["timestamp"]))
    except:
        pass

    user_history = [{"type": x["type"], "result": x["result"]} for x in user_history_raw]
    user_latest_result = user_history[-1]["result"] if len(user_history) > 0 else ""

    return num_workingquiz, num_reviewquiz, text_highlighted_yellow, text_highlighted_red, user_history, class_stats, user_latest_result

def classify_binary(binary_string, knowledge):
    if all(c == '1' for c in binary_string):  # すべて1の場合
        text = """生徒がこの問題で分からなかった部分はないので、この問題に使われている知識に別の知識を組み合わせた新しい問題を作ってください。"""
        return text, 0
    elif all(c == '0' for c in binary_string):  # すべて0の場合
        text = """生徒はこの問題が全くわかっていないので、この問題に使われている最初の知識を簡単に復習する問題を作ってください。"""
        return text, 4
    elif binary_string.endswith('0') and all(c == '1' for c in binary_string[:-1]):  # 最後だけが0の場合
        text = "生徒は最後の最後でミスをしているので、最後まで解き切らせるような問題を作ってください。"
        return text, 1
    elif '01' not in binary_string:  # 1が左、0が右（交互にならない）
        text = "生徒は問題の「{}」から「{}」までの知識はわかっているので、「{}」から「{}」までの知識を扱う問題を作成してください。".format(knowledge[0], knowledge[binary_string.index('0')], knowledge[0], knowledge[binary_string.index('0') + 1])
        return text, 2
    else:  # 1と0が交互の場合、最初の0の位置を返す
        text = "生徒はところどころ理解していない部分があるので、「{}」を確認する問題を作成してください。".format(knowledge[binary_string.index('0') + 1])
        return text, 3

def check_if_solvable(question, knowledge, num, model, grade):
    if num == "type1":
        prompt = '''
        以下はある数学の問題です。 \n {} \n
        この問題を解いて、最終的な答えを出しなさい。
        - 以下の知識を用いても良い。 \n {} \n
        - 日本の{}の生徒が理解できる回答を出力すること。
        - 以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
        例：$$\\int_{}^{} f(x)\\\\,dx = F(b) - F(a)$$
        例：$$\\beta + \\gamma \\{} \\\\ \\alpha \\{}$$
        - ただし、以下の点に注意すること。
            - mathjaxフォーマットの&は使わないこと。
            - \\text フォーマットは使わないこと。
            - <, >の２つの記号は、必ず"<\\," ">\\," という形で出力すること。
            - $という記号は、フォーマットで与えられている4組の$$を除き使用しないこと。
        - 解答の過程を出力すること。
        - 以下のフォーマットで、XXXXに解答の過程、YYYYに最終的な答えを挿入して答えること。
        [解答の過程] \n
        $$ \\begin{}{}{} XXXX \\end{}{} $$
        [最終的な答え] \n
        $$YYYY$$
        '''.format(question, knowledge, grade, r"{a}", r"{b}", r"text{の値から、}", r"text{を求める}", "{array", "}{l", "}", "{array", "}")
    start_time = time.time()
    ans = gpt_exection(model, prompt)
    ans = ans.replace("$$", "__two_dollars__").replace("$", "").replace("__two_dollars__", "$$")
    end_time = time.time()
    elapsed_time_solve = end_time - start_time
    return ans, prompt, f"{elapsed_time_solve:.2f}"

def execute0006_ks(question, answer, knowledge, tags, model, quiz_base, grade, yellow_marker, red_marker, custom_prompt=""):
    start_time = time.time()
    reason = ["この問題についてはよくできています。さらに知識を応用した問題で復習しましょう！\n",
          "最後の最後でミスをしています。最後まで気を抜かずに、しっかり解き切りましょう！\n",
          "途中までよくできています。元の問題を解くために必要なステップを確認するために、以下の問題に取り組みましょう！\n",
          "ところどころ間違っているようです。怪しいポイントを確認して、カンペキに解けるようになりましょう！\n",
          "ちょっと難しすぎましたね。でも大丈夫。ひとつずつ確認しましょう。\n"]
    base1 = '''
        あなたのタスクは、生徒が解いた数学の問題の内容とその結果に応じて、生徒に適した新しい数学の問題を作成することです。
        生徒がある問題を解きました。
        '''
    description = ""
    prompt_main = ""

    # 解答のポイントにチェックを入れた場合
    if len(tags) > 0:
        stats_bit = ''.join(str(1 if tag == '_o_' else 0) for tag in tags)
        stats, bittype = classify_binary(stats_bit, knowledge)
        description = reason[bittype]

        if quiz_base:
            base1 = base1 + '''
            この問題の設定は次のとおりです。 \n {} \n
            '''.format(quiz_base)

        prompt_type = 1

        if stats == "生徒がこの問題で分からなかった部分はないので、この問題に使われている知識に別の知識を組み合わせた新しい問題を作ってください。":
            prompt_type = random.choice([1, 2])

        if prompt_type == 1:
            prompt_main = '''
            問題に必要な数学的思考・計算技術・注意すべき点は次の通りです。 \n {} \n
            {} \n
            '''.format(knowledge, stats)
            description_query = """
            {} \n
            この情報を１行で{}の生徒にわかりやすく伝えてください。
            復習すべき理由と、復習すべき事項を含め、「〜しましょう！」の形で結果のみを出力すること。
            """.format(prompt_main, grade)
            description = gpt_exection("gpt-4.1-nano", description_query) + "\n"
        else:
            prompt_main = '''
            もとの問題の問題文は次の通りです。 \n {} \n
            この問題の数値や条件を変えて、新たな問題を作成してください。 \n
            '''.format(question)
            description = "この問題についてはよくできています。数値や条件を変えた問題を解いて定着させましょう！\n"
    
    yellow = ""
    if len(yellow_marker) > 0:
        yellow_marker_prompt = """
        ある{}の生徒が数学の問題に取り組みました。 \n
        問題は以下の通りです。 \n {} \n
        この問題の解答は以下の通りです。 \n {} \n
        この問題について、生徒は解答のうち以下の部分について分からないといっています。 \n {} \n
        この生徒が復習すべき事項を理由とともに説明してください。
        """.format(grade, question, answer, yellow_marker)
        yellow = gpt_exection(model, yellow_marker_prompt)
    
    red = ""
    if len(red_marker) > 0:
        red_marker_prompt = """
        ある{}の生徒が数学の問題に取り組みました。 \n
        問題は以下の通りです。 \n {} \n
        この問題の解答は以下の通りです。 \n {} \n
        この問題について、生徒は解答のうち以下の部分について理解しており、応用力を試したいといっています。 \n {} \n
        この生徒が応用として取り組むべき事項を理由とともに説明してください。
        """.format(grade, question, answer, red_marker)
        red = gpt_exection(model, red_marker_prompt)

    markers = ""
    if yellow + red != "":
        markers = """\n
        生徒の取り組みの様子から、生徒が復習すべき事項は以下のように推測されます。
        {} \n
        {} \n
        """.format(yellow, red)

        description_query = """
        {} \n
        この情報を１行で{}の生徒にわかりやすく伝えてください。
        復習すべき理由と、復習すべき事項を含め、「〜しましょう！」の形で結果のみを出力すること。
        """.format(markers, grade)
        description = gpt_exection("gpt-4.1-nano", description_query) + "\n"
    
    base1 += markers

    if prompt_main == "":
        prompt_main = "これらの情報から、生徒の復習に適した数学の問題を生成してください。"

    condition = '''
    - 新しい問題のみを結果として出力すること。
    - 日本の{}の生徒が理解できる問題を出題すること。
    - 問題は以下のフォーマットのように**$$で囲み**mathjax形式で出力すること。
    例：$$\\int_{}^{} f(x)\\\\,dx = F(b) - F(a)$$
    例：$$\\beta + \\gamma \\{} \\\\ \\alpha \\{}$$
    ただし、以下の点に注意すること。
    - mathjaxフォーマットの&は使わないこと。
    - \\text フォーマットは使わないこと。
    - <, >の２つの記号は、必ず"<\\," ">\\," という形で出力すること。
    - $という記号は、フォーマットで与えられている2組の$$を除き使用しないこと。
    以下のフォーマットで、XXXXに問題を挿入して、左揃えで答えること。
    [問題] \n
    $$ \\begin{}{}{} XXXX \\end{}{} $$
    '''.format(grade, r"{a}", r"{b}", r"text{の値から、}", r"text{を求める}", "{array", "}{l", "}", "{array", "}")

    if custom_prompt:
        # 1. 判定用のプロンプトを作成
        check_prompt = f"""
        以下の「生徒からの追加要望」は、数学の復習問題を自動生成するシステムへの指示です。
        この要望を満たすためには、システムが「もとの問題文」の内容や設定（場面、登場人物、具体的な状況など）を直接知っている必要がありますか？

        （判断基準の例）
        - 「もとの問題と同じ形式で」「数値を変更して」「少し難しくして」など、元の問題がベースとなるニュアンスがあれば「True」
        - 「英語で出して」「図形の問題にして」「全く別の設定にして」など、元の設定に依存しないものは「False」

        必要な場合は「True」、不要な場合は「False」のみを出力してください。

        生徒からの追加要望:
        {custom_prompt}
        """
        needs_original = False
        try:
            # 2. 高速なモデルで判定を実行
            check_result = gpt_exection("gpt-4.1-nano", check_prompt)
            if "True" in check_result or "true" in check_result.lower():
                needs_original = True
        except Exception as e:
            print(f"Original question context check failed: {e}")
            # エラー時は念のため含めない方向にする
            pass

        # 3. 判定結果に応じて指示を組み立てる
        custom_instruction = f"- 生徒からの追加要望（必ず考慮し、要望に沿った内容にすること）: {custom_prompt}\n"
        if needs_original:
            custom_instruction += f"- ※追加要望を満たすために、以下の【もとの問題】の設定や形式を参照して問題を作成してください。\n【もとの問題】\n{question}\n"
            
        condition = custom_instruction + "\n" + condition

    prompt = base1 + prompt_main + condition

    
    ans = gpt_exection(model, prompt)
    ans = ans.replace("$$", "__two_dollars__").replace("$", "").replace("__two_dollars__", "$$")
    end_time = time.time()
    elapsed_time_creation = end_time - start_time
    return description, ans, f"{elapsed_time_creation:.2f}", prompt

# rubricの説明を抽出する関数
def get_main_explanations(quiz_title, quiz_text_dict, lti):
    quiz_text, contents_id, page, no, school, ans, ans_page_s, ans_page_e = quiz_text_dict[quiz_title]
    exercise_info = _scoped_exercise_info(lti, {"school_id": school, "contents_id": contents_id, "page": page, "no": no})
    rubrics = exercise_info.get("rubric", {})
    main_list = [item["main"] for item in rubrics.values() if "main" in item]
    if (len(main_list) > 0) and ("上記の項目について、ひとつも理解できなかった" not in main_list):
        main_list.append("上記の項目について、ひとつも理解できなかった")
    return main_list

def initial_register():
    #quiz.json読み込み
    quiz_path = os.path.join(os.path.dirname(__file__), "static", "quiz.json")

    if not os.path.exists(quiz_path):
        return

    with open(quiz_path, encoding="utf-8") as f:
        quiz_list = json.load(f)

    if not quiz_list:
        return

    # 一括保存用のリストを作成
    requests = []
    for quiz in quiz_list:
        contents_id = quiz["contents_id"]
        page = quiz["page"]
        no = quiz["no"]
        school = quiz["school_id"]
        user = quiz["user"]
        
        # 1件ずつ通信せず、リクエストとしてリストにためる
        requests.append(
            ReplaceOne(
                {"contents_id": contents_id, "page": page, "no": no, "school_id": school, "user": user}, 
                quiz, 
                upsert=True
            )
        )
    
    # リストにデータがあれば、1回の通信で一括更新 (めちゃくちゃ早くなります)
    if requests:
        try:
            exercise_col.bulk_write(requests)
            print(f"Quiz JSON loaded successfully: {len(requests)} items.")
        except Exception as e:
            print(f"Quiz JSON load failed: {e}")

    return

# ▼ 追加：Gradioの画面ロード時ではなく、サーバー(アプリ)起動時に1回だけ実行させる
initial_register()

with gr.Blocks() as demo:
    lti_state = gr.State()  # ここにユーザー情報を保存   
    user_state = gr.State()
    session_state = gr.State()
    operationname_state = gr.State()
    contentsid_state = gr.State()
    page_state = gr.State()
    no_state = gr.State()
    highlighted_yellow_state = gr.State("")
    highlighted_red_state = gr.State("")
    isrubric_state = gr.State()
    ismarker_state = gr.State()
    quiz_map_state = gr.State()
    exercise_saving_state = gr.State(True)

    exercise_state = gr.State()
    exercise_creation_time_state = gr.State()
    answer_creation_time_state = gr.State()
    overall_creation_time_state = gr.State()
    prompt_exercise_state = gr.State("")
    prompt_answer_state = gr.State("")
    custom_prompt_state = gr.State("")
    can_use_custom_prompt_state = gr.State(False)
    user_answer_state = gr.State("")
    new_contentsid_state = gr.State()
    new_page_state = gr.State()
    new_no_state = gr.State()
    tags_state = gr.State()
    grade_state = gr.State()
    school_state = gr.State()
    gen_state = gr.State()

    gr.Markdown(
        """
        <div style="background-color: #2196f3; padding: 24px; border-radius: 8px; text-align: center; color: black;">
        <h1> $$\\Huge \\mathfrak{PRIME} - \\textsf{AI数学復習エンジン}$$ </h1>
        </div>
        """, visible=False
    )
    
    report_result = gr.Markdown(
        "### 正常に送信されました！",
        visible=False
    )
    
    # 新しい宇宙の旅UIを配置
    journey_display = gr.Markdown(visible=False)

    # 古いUIはコードの連鎖を壊さないために、Rowごと非表示(visible=False)にして残す
    with gr.Row(elem_classes="notranslate", visible=False):
        with gr.Column(scale=3): 
            title = gr.Markdown(
                "## " + phrases[0],
                visible=False
            )
        with gr.Column(scale=2): 
            vanish_btn = gr.Button(
                value="メッセージを消す",
                visible=False,
                interactive=False,
                variant="secondary"
            )
    title_state = gr.State()

    contents_dropdown = gr.Dropdown(
        choices=[],
        label="復習する教材",
        value=None
    )
    contents_dropdown_state = gr.State()
    contents_state = gr.State()

    contents_summary = gr.Markdown(visible=False)
    
    quiz_dropdown = gr.Dropdown(
        choices=[],
        label="復習する問題",
        value=None
    )
    quiz_dropdown_state = gr.State()

    quiz_text_display = gr.Markdown(visible=False)
    figure_warning_display = gr.Markdown(visible=False)

    with gr.Row(elem_classes="notranslate"):  
        with gr.Column(scale=1):    
            status_msg = gr.Markdown(
                value='',
                visible=False
            )

        with gr.Column(scale=1):
            
            marker_btn_state = gr.State(False)
            rubric_btn_state = gr.State(False)

            with gr.Row():
                btn_from_marker = gr.Button("マーカーからつくる", interactive=False, variant="secondary")
                btn_from_rubric = gr.Button("解答のポイントからつくる", interactive=False, variant="secondary")
            
            with gr.Column(scale=1):
                rev_quiz_btn = gr.Button("そのまま解く", visible=False, interactive=False, variant="primary")
            
            checkboxes = gr.CheckboxGroup(
                choices=[], 
                label="できたポイントをチェックしよう", 
                visible=False, 
                show_label = False
            )

            marker_checkboxes = gr.CheckboxGroup(
                choices=["赤：応用力を試す", "黄：わからなかった部分を復習する"], 
                label="以下をチェックすると、マーカーを引いた箇所を考慮した問題を生成します", 
                visible=False, 
                show_label = False
            )

            custom_prompt_input = gr.Textbox(
                label="問題生成への追加要望（オプション）",
                placeholder="例：もっと難しい問題にしてほしい、文章題にしてほしい など",
                visible=False,
                interactive=True
            )

            model_options = gr.Dropdown(
                choices=["o4-mini(速さ重視、普段使いにおすすめ)", "gpt-5(正確さ重視・遅い。より深い学習向け)", "llm-jp-4(日本製のモデル)"],
                label="復習問題を作成するモデルを選んでください",
                interactive=True,
                value="o4-mini(速さ重視、普段使いにおすすめ)",
                visible=False
            )
    quiz_dropdown_state = gr.State()
    status_msg_state = gr.State()
    checkbox_state = gr.State()
    checkbox_all_items_state = gr.State()
    current_checkbox_state = gr.State([])
    check_flaw_state = gr.State()
    new_checkbox_all_items_state = gr.State()
    new_checkbox_state = gr.State()
    new_current_checkbox_state = gr.State([])
    cnt_work_state = gr.State()
    cnt_review_state = gr.State()
    model_state = gr.State("o4-mini")
    can_use_custom_prompt_state = gr.State(False)
    description_state = gr.State()
    
    with gr.Row(elem_classes="notranslate"):  
        with gr.Column(scale=1):    
            gen_quiz_btn = gr.Button("類題をつくる（まだ押せません）", visible=True, interactive=False, variant="stop")

    with gr.Row():
        with gr.Column(scale=1):
            exercise_output = gr.Markdown(
                value="復習問題はここに出てきます",
                visible=True
            )

        with gr.Column(scale=1):
            student_answer = gr.Textbox(
                label="左側の問題の解答を書いてみよう",
                lines=1,
                placeholder="(まだ入力できません)",
                visible=True,
                interactive=False
            )

    answer_btn = gr.Button("模範解答を表示", visible=False)

    with gr.Row(elem_classes="notranslate"):
        with gr.Column(scale=3):
            answer_output = gr.Markdown(
                "",
                visible=False
            )
        answer_state = gr.State()

        with gr.Column(scale=2):
            note_mkdwn = gr.Markdown("### 解いた問題を振り返ろう", visible=False)

            understanding = gr.Radio(
                choices=["すべて自力で解けた", "解説を見てわかった", "解説を見てもわからなかった"],
                label="この問題はどの程度理解できましたか？",
                visible=False
            )

            new_checkboxes = gr.CheckboxGroup(
                choices=[],
                label="いま解いた問題について、できたポイントをすべてチェックしよう。復習できましたか？", 
                visible=False, 
                show_label = False
            )

            fluency = gr.Radio(
                choices=["自然だった", "不自然な箇所があった", "全体的に不自然だった"],
                label="この問題の問題文は自然な日本語でしたか？",
                visible=False
            )

            difficulty = gr.Radio(
                choices=["5(難しい)", "4", "3", "2", "1(簡単)"],
                label="この問題はどのくらい難しかったですか？",
                visible=False
            )

            relevance = gr.Radio(
                choices=["5(関連していた)", "4", "3", "2", "1(関連していなかった)"],
                label="この問題はもとの問題とどのくらい関連していましたか？",
                visible=False
            )

            rating = gr.Radio(
                choices=["5(役に立った)", "4", "3", "2", "1(役に立たなかった)"],
                label="この問題はどのぐらい復習の役に立ちましたか？",
                visible=False
            )

            report_markdown = gr.Markdown(
                "この問題について、感想や意見を自由に書いてみよう（任意）。(模範解答が間違ってるかも？, こうすればより良い問題になる, 別解がある, など...)",
                visible=False
            )

            report_type = gr.Radio(
            choices=["模範解答が間違ってるかも？", "わからない場所がある...", "こうすればより良い問題になる", "別解がある", "その他"],
            label="報告のカテゴリはどれですか？",
            visible=False,
            interactive=True
            )

            report_text = gr.Textbox(
                label="こちらに詳しく記述してください",
                placeholder="",
                visible=False,
                interactive=False,
                lines=5
            )
        understanding_state = gr.State()
        rating_state = gr.State()
        fluency_state = gr.State()
        difficulty_state = gr.State()
        relevance_state = gr.State()
        check_state = gr.State()
        report_type_state = gr.State()
        report_text_state = gr.State()

    with gr.Row(elem_classes="notranslate"):
        with gr.Column(scale=1):
            report_btn = gr.Button(
                interactive=False, 
                value="結果を送信する(まずは問題を振り返ってください！)", 
                variant="secondary",
                visible=False
            )

    vanish_btn.click(
        fn=lambda: (
            gr.update(visible=False),
            gr.update(visible=False),
            "VanishedTitle"
        ),
        inputs=None,
        outputs=[title, vanish_btn, operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, title],
        outputs=None
    )

    # ⬇️ 引数の一番最後に user_id を追加！
    def generate_status_msg(school, contents_id, count_work, count_review, ans_contents_id, ishighlighted, user_history, class_stats, user_latest_result, user_id):
        color_green = "#00c853"
        color_yellow = "#ffb300"
        color_red = "#f44336"
        html_parts = []
    
        if school == "C126210001533" and contents_id != "ai_generated" and count_work == 0:
            html_parts.append(f"""<div style="background-color: #ffebee; color: #c62828; padding: 12px; border-radius: 8px; font-weight: bold; margin-bottom: 10px; text-align: center; border: 2px solid #ef5350;" translate="no">⚠️ BookRollで問題を解かないと、下のボタンが有効になりません。<br>BookRollで解いてから、システムに入り直してください。</div>""")
    
        if ans_contents_id != "" and not ishighlighted:
            html_parts.append(f"""<div style="background-color: #fff8e1; color: #f57f17; padding: 12px; border-radius: 8px; font-weight: bold; margin-bottom: 10px; text-align: center; border: 2px solid #fbc02d;" translate="no">🖍️ マーカーから類題を作るには、BookRollの解答ページにマーカー（<span style="color:red;">赤色</span>や<span style="color:#f57f17;">黄色</span>）を引いてください。</div>""")
    
        count_html = f"""<div style="display: flex; justify-content: center; gap: 40px; margin-bottom: 4px; border-bottom: 1px solid #e0e0e0; padding-bottom: 12px;"><div style="text-align: center;"><div style="font-size: 14px; color: #616161; font-weight: bold;">演習回数</div><div style="font-size: 26px; font-weight: bold; color: #2196f3;">{count_work}<span style="font-size: 16px; margin-left: 2px; color: #424242;">回</span></div></div><div style="text-align: center;"><div style="font-size: 14px; color: #616161; font-weight: bold;">復習回数</div><div style="font-size: 26px; font-weight: bold; color: #4caf50;">{count_review}<span style="font-size: 16px; margin-left: 2px; color: #424242;">問</span></div></div></div>"""
    
        history_html = ""
        if isinstance(user_history, list) and len(user_history) > 0:
            recent_history = user_history[-10:]
            history_html += '<span style="font-size: 11px; color: #9e9e9e; margin-right: 8px; font-weight: normal; align-self: center;">◀ 古い</span>'
            
            if len(user_history) > 10:
                history_html += '<span style="color: #bdbdbd; font-weight: bold; font-size: 20px; margin-right: 8px; align-self: center;">...</span>'

            total_items = len(recent_history)
            for i, item in enumerate(recent_history):
                if not isinstance(item, dict): continue
                char = "問" if item.get("type") == "original" else "復"
                res = str(item.get("result", ""))
                
                if "まったく分からなかった" in res or "解説を見てもわからなかった" in res or "不正解" in res:
                    color = color_red
                elif "一部解説を見て解いた" in res or "解説を見てわかった" in res or "一部正解" in res:
                    color = color_yellow
                else:
                    color = color_green
                
                # 【UI改善1】左（古い）ほど薄く(0.3)、右（新しい）ほど濃く(1.0)する計算
                opacity = 0.3 + (0.7 * (i / max(1, total_items - 1))) if total_items > 1 else 1.0
                
                # 【UI改善2】最新（一番右）のアイテムだけ少し大きくして影をつけ「これが今」を強調
                if i == total_items - 1:
                    style = f'color: {color}; font-weight: 900; font-size: 26px; margin-right: 4px; text-shadow: 0px 0px 4px {color}80; transform: scale(1.1); display: inline-block; align-self: center;'
                else:
                    style = f'color: {color}; font-weight: bold; font-size: 24px; margin-right: 6px; opacity: {opacity:.2f}; align-self: center;'
                    
                history_html += f'<span style="{style}">{char}</span>'
            
            # 右側に少し目立つ色で「最新 ▶」を添える
            history_html += '<span style="font-size: 11px; color: #2196f3; margin-left: 6px; font-weight: bold; align-self: center;">最新 ▶</span>'
            
        else:
            history_html = '<span style="color: #9e9e9e; font-size: 16px;">まだ解答履歴がありません</span>'
    
        total_students = sum(len(lst) for lst in class_stats.values()) if isinstance(class_stats, dict) and "green" in class_stats else 0
        blocks_html = ""
        
        if total_students > 0:
            # ＝＝＝ 修正：パーセンテージの計算 ＝＝＝
            green_count = len(class_stats.get("green", []))
            yellow_count = len(class_stats.get("yellow", []))
            
            # 合計が必ず100%になるように調整（赤は100から引く）
            green_pct = round((green_count / total_students) * 100)
            yellow_pct = round((yellow_count / total_students) * 100)
            red_pct = max(0, 100 - green_pct - yellow_pct)
            
            blocks_html = f'''
            <div style="font-size: 20px; font-weight: bold; letter-spacing: 1px;">
                <span style="color: {color_green}; margin-right: 16px;">🟩：{green_pct}%</span>
                <span style="color: {color_yellow}; margin-right: 16px;">🟨：{yellow_pct}%</span>
                <span style="color: {color_red};">🟥：{red_pct}%</span>
            </div>
            '''
        else:
            if contents_id == "ai_generated":
                blocks_html = '<span style="color: #9e9e9e; font-size: 16px; letter-spacing: normal;">類題のため、クラスの正答率データはありません</span>'
            else:
                blocks_html = '<span style="color: #9e9e9e; font-size: 16px; letter-spacing: normal;">クラスのデータがありません</span>'
    
        # ＝＝＝ 凡例から「大きい▮: 〜〜」の説明を削除 ＝＝＝
        html_parts.append(
            f"""<div style="background-color: #f5f5f5; padding: 16px; border-radius: 8px; border: 1px solid #e0e0e0; display: flex; flex-direction: column; gap: 12px;" translate="no">{count_html}<div style="display: flex; align-items: center; justify-content: center;"><span style="font-size: 16px; font-weight: bold; margin-right: 12px; color: #424242;">あなたの解答履歴:</span><div style="display: flex; align-items: center; line-height: 1;">{history_html}</div></div><div style="display: flex; align-items: center; justify-content: center;"><span style="font-size: 16px; font-weight: bold; margin-right: 12px; color: #424242;">クラス全体の正答率:</span><div style="display: flex; align-items: center; line-height: 1;">{blocks_html}</div></div></div><div style="margin-top: 8px; font-size: 13px; color: #757575; text-align: center; line-height: 1.6;" translate="no"><span style="font-weight: bold; color: #616161;">【見方】</span><br><span style="color: #00c853; font-weight: bold;">緑</span>: 全部正解/自力で正解 &nbsp;&nbsp;|&nbsp;&nbsp; <span style="color: #ffb300; font-weight: bold;">黄</span>: 一部正解/解説を見て正解 &nbsp;&nbsp;|&nbsp;&nbsp; <span style="color: #f44336; font-weight: bold;">赤</span>: 不正解<br><span style="font-weight: bold;">問</span>: もとの問題 &nbsp;&nbsp;|&nbsp;&nbsp; <span style="font-weight: bold;">復</span>: PRIMEで作った復習問題</div>"""
        )
    
        return "\n".join(html_parts)
    
    def update_when_contents_dropdown(contents_name, contents_dropdown_list, lti):
        if contents_name:
            contents_id = contents_dropdown_list[contents_name]["contents_id"] 
            
            quiz_map, quiz_dropdown_update, summary_update = reload_quiz_map_from_mongo(lti, contents_id)
            
            return quiz_map, quiz_dropdown_update, summary_update
        
        return {}, gr.update(choices=[], value=None), gr.update(visible=False, value="")
    
    contents_dropdown.change(
        fn=update_when_contents_dropdown,
        inputs=[contents_dropdown, contents_dropdown_state, lti_state],
        outputs=[quiz_map_state, quiz_dropdown, contents_summary]
    ).then(
        fn=lambda: (
        "SelectedContents"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, contents_dropdown],
        outputs=None
    )

    @_prime_mongo_budget
    def update_when_dropdown(quiz_title, quiz_text_dict, user, lti):
        if quiz_title:
            quiz_text, contents_id, page, no, school, ans, ans_page_s, ans_page_e = quiz_text_dict[quiz_title]
            rubric_explanations = get_main_explanations(quiz_title, quiz_text_dict, lti)
            
            exercise_info = _scoped_exercise_info(lti, {"school_id": school, "contents_id": contents_id, "page": page, "no": no})
            isfigure = exercise_info.get("isfigure", False) if exercise_info else False
            
            if isfigure:
                figure_warning_update = gr.update(value='<div style="background-color: #fff3e0; color: #e65100; padding: 12px; border-radius: 8px; font-weight: bold; margin-bottom: 12px; text-align: center; border: 2px solid #ffb74d;" translate="no">🖼️ この問題は図が必要な問題です。図は、BookRollを見てください</div>', visible=True)
            else:
                figure_warning_update = gr.update(visible=False)
            
            # --- 1. DBからデータを取得（7つの変数を受け取る） ---
            count_work, count_review, text_highlighted_yellow, text_highlighted_red, user_history, class_stats, user_latest_result = get_result_from_db(school, contents_id, page, no, user, lti, ans, ans_page_s, ans_page_e)
            
            isrubric = True if len(rubric_explanations) > 0 else False
            ishighlighted = True if len(text_highlighted_red) + len(text_highlighted_yellow) > 0 else False
            ismarker = True if ans != "" else False
            
            # --- 2. ボタンの有効化判定（警告文の処理は generate_status_msg に移動したため削除） ---
            btn_marker_interactive = False
            btn_rubric_interactive = False

            if contents_id != "ai_generated" and count_work > 0:
                if isrubric:
                    btn_rubric_interactive = True
                if ismarker and ishighlighted:
                    btn_marker_interactive = True
            
            # --- 3. メッセージと履歴の生成---
            msg = generate_status_msg(
                school=lti["school_id"], 
                contents_id=contents_id, 
                count_work=count_work, 
                count_review=count_review,
                ans_contents_id=ans, 
                ishighlighted=ishighlighted, 
                user_history=user_history, 
                class_stats=class_stats, 
                user_latest_result=user_latest_result,
                user_id=user
            )
            if lti.get("lti_version") == "1.3":
                msg = '<p translate="no">' + _native_history_notice(False) + '</p>' + msg
            
            rubric_label = "できたポイントをチェックしよう！" if isrubric else "この問題には解答のポイントがついていません"
            marker_label = "類題に反映したいマーカーの種類を選ぼう" if ishighlighted else "BookRollにマーカーを引いてみよう！"
            
            selected_quiz = "あなたが選んだ問題"
            if contents_id == "ai_generated":
                selected_quiz = "あなたが選んだ問題"
            elif ismarker:
                selected_quiz = "あなたが選んだ問題 (問題：{}ページ, 解答：{}ページ)".format(str(page), str(ans_page_s))
            else:
                selected_quiz = "あなたが選んだ問題 (問題：{}ページ)".format(str(page))

            # --- 4. ボタンの状態更新 ---
            if btn_marker_interactive:
                update_marker_btn = gr.update(visible=True, interactive=True, variant="primary", value="マーカーからつくる")
            else:
                update_marker_btn = gr.update(visible=True, interactive=False, variant="secondary", value="マーカーからつくる")

            if btn_rubric_interactive:
                update_rubric_btn = gr.update(visible=True, interactive=True, variant="primary", value="解答のポイントからつくる")
            else:
                update_rubric_btn = gr.update(visible=True, interactive=False, variant="secondary", value="解答のポイントからつくる")

            can_custom = (count_review > 0) and (lti["school_id"] != "C126210001533")

            if lti["school_id"] != "F126110107407":
                # 元の問題を１回も解いていない場合
                if count_work == 0:
                    return (
                        gr.update(value=f'<div style="text-align: center;" translate="no"><h1> {selected_quiz} </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;" translate="no"> \n{quiz_text} </div>', visible=True), # quiz_text_display
                        figure_warning_update, # figure_warning_display 
                        gr.update(choices=rubric_explanations, value=[], visible=False, interactive=False, show_label=False), # checkboxes
                        gr.update(visible=True, value=msg), # status_msg
                        quiz_text, # quiz_dropdown_state
                        "SelectedExercise", # operationname_state
                        rubric_explanations, # checkbox_all_items_state
                        contents_id, # contentsid_state
                        page, # page_state
                        no, # no_state
                        count_work, # cnt_work_state
                        count_review, # cnt_review_state
                        text_highlighted_yellow, # highlighted_yellow_state
                        text_highlighted_red, # highlighted_red_state
                        gr.update(visible=False, interactive=False, show_label=False), # marker_checkboxes
                        isrubric, # isrubric_state
                        ismarker, # ismarker_state
                        gr.update(interactive=False), # contents_dropdown
                        update_marker_btn, # btn_from_marker
                        update_rubric_btn, # btn_from_rubric
                        False, # marker_btn_state
                        False, # rubric_btn_state
                        can_custom,
                        gr.update(visible=False, value="")
                    )
                # 元の問題を１回は解いているが、復習問題を１回も解いていない場合
                elif count_review == 0:
                    return (
                        gr.update(value=f'<div style="text-align: center;" translate="no"><h1> {selected_quiz} </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;" translate="no"> \n{quiz_text} </div>', visible=True), # quiz_text_display
                        figure_warning_update, # figure_warning_display 
                        gr.update(choices=rubric_explanations, value=[], visible=False, interactive=isrubric, label=rubric_label, show_label=isrubric), # checkboxes
                        gr.update(visible=True, value=msg), # status_msg
                        quiz_text, # quiz_dropdown_state
                        "SelectedExercise", # operationname_state
                        rubric_explanations, # checkbox_all_items_state
                        contents_id, # contentsid_state
                        page, # page_state
                        no, # no_state
                        count_work, # cnt_work_state
                        count_review, # cnt_review_state
                        text_highlighted_yellow, # highlighted_yellow_state
                        text_highlighted_red, # highlighted_red_state
                        gr.update(visible=False, interactive=ishighlighted, show_label=True, label=marker_label), # marker_checkboxes
                        isrubric, # isrubric_state
                        ismarker, # ismarker_state
                        gr.update(interactive=False), # contents_dropdown
                        update_marker_btn, # btn_from_marker
                        update_rubric_btn, # btn_from_rubric
                        False, # marker_btn_state
                        False, # rubric_btn_state
                        can_custom,
                        gr.update(visible=False, value="")
                    )
                # それ以外（１回以上復習済み）
                else:
                    return (
                        gr.update(value=f'<div style="text-align: center;" translate="no"><h1> {selected_quiz} </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;" translate="no"> \n{quiz_text} </div>', visible=True), # quiz_text_display
                        figure_warning_update, # figure_warning_display 
                        gr.update(choices=rubric_explanations, value=[], visible=False, interactive=True, show_label=True, label=rubric_label), # checkboxes
                        gr.update(visible=True, value=msg), # status_msg
                        quiz_text, # quiz_dropdown_state
                        "SelectedExercise", # operationname_state
                        rubric_explanations, # checkbox_all_items_state
                        contents_id, # contentsid_state
                        page, # page_state
                        no, # no_state
                        count_work, # cnt_work_state
                        count_review, # cnt_review_state
                        text_highlighted_yellow, # highlighted_yellow_state
                        text_highlighted_red, # highlighted_red_state
                        gr.update(visible=False, interactive=ishighlighted, show_label=True, label=marker_label), # marker_checkboxes
                        isrubric, # isrubric_state
                        ismarker, # ismarker_state
                        gr.update(interactive=False), # contents_dropdown
                        update_marker_btn, # btn_from_marker
                        update_rubric_btn, # btn_from_rubric
                        False, # marker_btn_state
                        False, # rubric_btn_state
                        can_custom,
                        gr.update(visible=False, value="")
                    )

            else:
                return (
                    gr.update(value=f'<div style="text-align: center;" translate="no"><h1> {selected_quiz} </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;" translate="no"> \n{quiz_text} </div>', visible=True), # quiz_text_display
                    figure_warning_update, # figure_warning_display 
                    gr.update(choices=rubric_explanations, value=[], visible=False, interactive=isrubric, label=rubric_label, show_label=isrubric), # checkboxes
                    gr.update(visible=True, value=msg), # status_msg
                    quiz_text, # quiz_dropdown_state
                    "SelectedExercise", # operationname_state
                    rubric_explanations, # checkbox_all_items_state
                    contents_id, # contentsid_state
                    page, # page_state
                    no, # no_state
                    count_work, # cnt_work_state
                    count_review, # cnt_review_state
                    text_highlighted_yellow, # highlighted_yellow_state
                    text_highlighted_red, # highlighted_red_state
                    gr.update(visible=False, interactive=ishighlighted, show_label=True, label=marker_label), # marker_checkboxes
                    isrubric, # isrubric_state
                    ismarker, # ismarker_state
                    gr.update(interactive=False), # contents_dropdown
                    update_marker_btn, # btn_from_marker
                    update_rubric_btn, # btn_from_rubric
                    False, # marker_btn_state
                    False, # rubric_btn_state
                    can_custom,
                    gr.update(visible=False, value="")
                )

        # --- dropboxに何も選択されていない場合（初期状態） ---
        else:
            return (
                gr.update(), # quiz_text_display
                gr.update(), # figure_display
                gr.update(), # checkboxes
                gr.update(), # status_msg
                "", # quiz_dropdown_state
                "SelectedExercise", # operationname_state
                [], # checkbox_all_items_state
                "", # contentsid_state
                "", # page_state
                "", # no_state
                0, # cnt_work_state
                0, # cnt_review_state
                "", # highlighted_yellow_state
                "", # highlighted_red_state
                gr.update(), # marker_checkboxes
                False, # isrubric_state
                False, # ismarker_state
                gr.update(), # contents_dropdown
                gr.update(), # btn_from_marker
                gr.update(), # btn_from_rubric
                False, # marker_btn_state
                False, # rubric_btn_state
                False, # ◀ 追加
                gr.update(visible=False, value="")
            )

    def open_when_no_rubrics(quiz_title, all_items, contents_id, count_review):
        if quiz_title:
            # 出現条件：「AI生成問題である」または「復習回数が1回以上」
            is_rev_visible = (contents_id == "ai_generated") or (count_review > 0)
            
            if contents_id == "ai_generated":
                if (len(all_items) == 0):
                    return (
                        gr.update(interactive=False, variant="secondary", value="(この問題は類題生成に対応していません)"),
                        gr.update(visible=is_rev_visible, interactive=True, variant="primary", value="選んだ問題をそのまま解く")
                    )
                else:
                    return (
                        gr.update(interactive=False, variant="secondary", value="類題をつくるには、上のらんに１つ以上チェックを入れてください"),
                        gr.update(visible=is_rev_visible, interactive=True, variant="primary", value="選んだ問題をそのまま解く")
                    )
            else:
                return (
                    gr.update(interactive=False, variant="secondary", value="類題をつくるには、上のらんに１つ以上チェックを入れてください"),
                    gr.update(visible=is_rev_visible, interactive=True, variant="primary", value="選んだ問題をそのまま解く")
                )
        else:
            return (
                gr.update(),
                gr.update(visible=False)
            )
    
    quiz_dropdown.change(
        fn=update_when_dropdown,
        inputs=[quiz_dropdown, quiz_map_state, user_state, lti_state],
        outputs=[
            quiz_text_display,
            figure_warning_display,
            checkboxes,
            status_msg,
            quiz_dropdown_state,
            operationname_state,
            checkbox_all_items_state,
            contentsid_state,
            page_state,
            no_state,
            cnt_work_state,
            cnt_review_state,
            highlighted_yellow_state,
            highlighted_red_state,
            marker_checkboxes,
            isrubric_state,
            ismarker_state,
            contents_dropdown,
            btn_from_marker,
            btn_from_rubric,
            marker_btn_state,
            rubric_btn_state,
            can_use_custom_prompt_state,
            custom_prompt_input
        ]
    ).then(
        fn=open_when_no_rubrics,
        inputs=[quiz_dropdown, checkbox_all_items_state, contentsid_state, cnt_review_state],
        outputs=[gen_quiz_btn, rev_quiz_btn]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, quiz_dropdown_state],
        outputs=None
    )

    def toggle_marker_btn(is_selected, other_btn_state, can_use_custom_prompt):
        new_state = not is_selected
        new_value = "✔️ マーカーからつくる" if new_state else "マーカーからつくる"
        
        dropdown_interactive = not (new_state or other_btn_state)
        model_visible = new_state or other_btn_state
        custom_prompt_visible = model_visible and can_use_custom_prompt # ◀ 追加
        
        return new_state, gr.update(value=new_value, variant="primary"), gr.update(visible=new_state), gr.update(interactive=dropdown_interactive), gr.update(visible=model_visible), gr.update(visible=custom_prompt_visible)

    btn_from_marker.click(
        fn=toggle_marker_btn,
        inputs=[marker_btn_state, rubric_btn_state, can_use_custom_prompt_state],
        outputs=[marker_btn_state, btn_from_marker, marker_checkboxes, quiz_dropdown, model_options, custom_prompt_input]
    ).then(
        fn=lambda: (
        "SelectedBtnFromMarker"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state],
        outputs=None
    )

    def toggle_rubric_btn(is_selected, other_btn_state, can_use_custom_prompt):
        new_state = not is_selected
        new_value = "✔️ 解答のポイントからつくる" if new_state else "解答のポイントからつくる"
        
        dropdown_interactive = not (new_state or other_btn_state)
        model_visible = new_state or other_btn_state
        custom_prompt_visible = model_visible and can_use_custom_prompt # ◀ 追加
        
        return new_state, gr.update(value=new_value, variant="primary"), gr.update(visible=new_state), gr.update(interactive=dropdown_interactive), gr.update(visible=model_visible), gr.update(visible=custom_prompt_visible)

    btn_from_rubric.click(
        fn=toggle_rubric_btn,
        inputs=[rubric_btn_state, marker_btn_state, can_use_custom_prompt_state],
        outputs=[rubric_btn_state, btn_from_rubric, checkboxes, quiz_dropdown, model_options, custom_prompt_input]
    ).then(
        fn=lambda: (
        "SelectedBtnFromRubric"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state],
        outputs=None
    )

    def update_when_checkboxes(all_items, selected, current_status):
        #checkboxのチェックの変更
        current_select = list(set(selected) ^ set(current_status))
        # 上記の項目について、ひとつも理解できなかった を選択した
        if len(selected) > len(current_status):
            if current_select[0] == "上記の項目について、ひとつも理解できなかった":
                updated_status = ["上記の項目について、ひとつも理解できなかった"]
            else:
                updated_status = [x for x in selected if x != "上記の項目について、ひとつも理解できなかった"]
        else:
            updated_status = selected
        
        # 選択結果を "o" / "x" で辞書化
        result = {
            choice: "o" if choice in updated_status else "x"
            for choice in all_items
        }

        return (
            result, 
            updated_status, 
            gr.update(value=updated_status)
        )
    
    def update_genquizbtn_when_checkboxes(selected, lti, count_work, count_review, all_items, marker_selected, isrubric, ismarker, quiz_title, contents_id, h_yellow, h_red):
        selected = selected or []
        marker_selected = marker_selected or []
        h_yellow = h_yellow or ""
        h_red = h_red or ""

        if not quiz_title:
            return gr.update(), gr.update(), gr.update(), gr.update(), gr.update()
            
        gen_btn_val = gr.update()
        rev_btn_val = gr.update()
        dropdown_val = gr.update()

        # 出現条件：「AI生成問題である」または「復習回数が1回以上」
        is_rev_visible = (contents_id == "ai_generated") or (count_review > 0)

        # --- 1. 類題作成・そのまま解くボタンの制御 ---
        if len(selected) + len(marker_selected) == 0:
            if isrubric or ismarker:
                gen_btn_val = gr.update(interactive=False, variant="secondary", value="類題をつくるには、上のらんに１つ以上チェックを入れてください")
                rev_btn_val = gr.update(visible=is_rev_visible, interactive=True, variant="primary", value="選んだ問題をそのまま解く")
            else:
                gen_btn_val = gr.update(interactive=False, variant="secondary", value="(この問題は類題生成に対応していません)")
                rev_btn_val = gr.update(visible=is_rev_visible, interactive=True, variant="primary", value="選んだ問題をそのまま解く")
        else:
            if lti["school_id"] == "C126210001533" and count_review == 0:
                gen_btn_val = gr.update(interactive=True, variant="primary", value="選んだ問題の類題をつくる")
                rev_btn_val = gr.update(visible=False, interactive=False, variant="secondary")
            else:
                gen_btn_val = gr.update(interactive=True, variant="primary", value="選んだ問題の類題をつくる")
                rev_btn_val = gr.update(visible=is_rev_visible, interactive=True, variant="primary", value="選んだ問題をそのまま解く")
            dropdown_val = gr.update(interactive=False)

        # --- 2. トグルボタンを「押せない」状態にする制御 ---
        ishighlighted = True if len(h_yellow) + len(h_red) > 0 else False
        can_use_rubric = (contents_id != "ai_generated") and (count_work > 0) and isrubric
        can_use_marker = (contents_id != "ai_generated") and (count_work > 0) and ismarker and ishighlighted

        if can_use_rubric:
            rubric_btn_state = gr.update(interactive=(len(selected) == 0))
        else:
            rubric_btn_state = gr.update(interactive=False)
            
        if can_use_marker:
            marker_btn_state = gr.update(interactive=(len(marker_selected) == 0))
        else:
            marker_btn_state = gr.update(interactive=False)

        return gen_btn_val, rev_btn_val, dropdown_val, rubric_btn_state, marker_btn_state
    
    checkboxes.change(
        fn=update_when_checkboxes,
        inputs=[checkbox_all_items_state, checkboxes, current_checkbox_state],
        outputs=[checkbox_state, current_checkbox_state, checkboxes]
    ).then(
        fn=update_genquizbtn_when_checkboxes,
        inputs=[checkboxes, lti_state, cnt_work_state, cnt_review_state, checkbox_all_items_state, marker_checkboxes, isrubric_state, ismarker_state, quiz_dropdown, contentsid_state, highlighted_yellow_state, highlighted_red_state],
        outputs=[gen_quiz_btn, rev_quiz_btn, quiz_dropdown, btn_from_rubric, btn_from_marker]
    ).then(
        fn=lambda: (
        "SelectedRubricStatus"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, checkbox_state],
        outputs=None
    )

    marker_checkboxes.change(
        fn=update_genquizbtn_when_checkboxes,
        inputs=[checkboxes, lti_state, cnt_work_state, cnt_review_state, checkbox_all_items_state, marker_checkboxes, isrubric_state, ismarker_state, quiz_dropdown, contentsid_state, highlighted_yellow_state, highlighted_red_state],
        outputs=[gen_quiz_btn, rev_quiz_btn, quiz_dropdown, btn_from_rubric, btn_from_marker]
    ).then(
        fn=lambda: (
        "SelectedMarkerInput"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, marker_checkboxes],
        outputs=None
    )

    MODEL_MAPPING = {
        "o4-mini(速さ重視、普段使いにおすすめ)": "o4-mini",
        "gpt-5(正確さ重視・遅い。より深い学習向け)": "gpt-5",
        "llm-jp-4(日本製のモデル)": "llm-jp-4-32b-a3b-thinking"
    }

    model_options.change(
        fn=lambda selection: MODEL_MAPPING.get(selection, selection.split('(')[0].strip()) if selection else "",
        inputs=[model_options],
        outputs=[model_state]
    ).then(
        fn=lambda: (
        "SelectedModel"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, model_state],
        outputs=None
    )

    custom_prompt_input.blur(
        fn=lambda text: (text, "InputCustomPrompt"),
        inputs=[custom_prompt_input],
        outputs=[custom_prompt_state, operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, custom_prompt_state],
        outputs=None
    )

    def check_sensitive_prompt(text):
        if not text.strip():
            return False
    
        prompt = f"""
            以下のテキストは、中高生向けの学習システムで生徒が入力したテキストです。
            このテキストに、個人情報（具体的な氏名、住所、電話番号、学校名など）や、教育システムに不適切な内容（暴力、暴言、性的、差別的、公序良俗に反する表現など）が含まれているか判定してください。

            含まれている場合は「True」、含まれていない場合は「False」のみを出力してください。

            テキスト:
            {text}
        """
        try:
            # 判定には高速なモデルを使用
            result = gpt_exection("gpt-4.1-nano", prompt)
            if "True" in result or "true" in result.lower():
                return True
            return False
        except Exception as e:
            print(f"Sensitive check failed: {e}")
            # APIエラー等の場合は、生徒の学習を止めないために一旦False(安全)として通す
            return False

    @_prime_mongo_budget
    def update_when_gen_quiz_btn(quiz_title, selections, quiz_text_dict, model, lti, num_review, highlighted_yellow, highlighted_red, isrubric, ismarker, marker_selected, custom_prompt):

        review_point = "この問題は、元の問題の復習問題として、どのくらい役に立ちましたか(どのくらい他の人にオススメしたいですか)？"
        rubrics = []
        tags = []
        if isrubric:
            rubrics = get_main_explanations(quiz_title, quiz_text_dict, lti)
            rubrics = rubrics[:-1]
            selected = selections or []
            tags = ['_o_' if item in selected else '_x_' for item in rubrics]
            review_point = "この問題は、元の問題の応用問題として、どのくらい役に立ちましたか(どのくらい他の人にオススメしたいですか)？"
            for i in range(len(tags)):
                if tags[i] == "_x_":
                    review_point = "この問題は、もとの問題の理解できていなかったポイントを復習する問題として、どのくらい役に立ちましたか(どのくらい他の人にオススメしたいですか)？".format(rubrics[i])
                    break
        if ismarker:
            if "黄：わからなかった部分を復習する" not in marker_selected:
                highlighted_yellow = ""
            if "赤：応用力を試す" not in marker_selected:
                highlighted_red = ""
        quiz_text, contents_id, page, no, school, ans, ans_page_s, ans_page_e = quiz_text_dict[quiz_title]
        exercise_info = _scoped_exercise_info(lti, {"school_id": school, "contents_id": contents_id, "page": page, "no": no})
        standard_answer = exercise_info.get("standard_answer", "")
        additional_explanation = exercise_info.get("figure_explanation", "")

        # reason[bittype], ans, f"{elapsed_time_creation:.2f}", prompt
        description, new_exercise, exercise_creation_time, prompt_exercise = execute0006_ks(quiz_text, standard_answer, rubrics, tags, model, additional_explanation, find_grade(lti["context_title"]), highlighted_yellow, highlighted_red, custom_prompt)

        tags_for_saving = [True if item in selected else False for item in rubrics]

        return (
            new_exercise,
            exercise_creation_time,
            gr.update(value= description + '\n <div style="text-align: center;" translate="no">' + new_exercise + f" </div> <br>問題生成時間:" + exercise_creation_time + "秒" + "<br> 右側の入力欄に解答の過程を入力するか、紙に解いて答えを出した後、模範解答を見て確認しましょう。<br> 注意：AIの生成問題には誤りを含むことがあります。"), 
            gr.update(visible=True, variant="secondary", interactive=False, value="(問題の解答を作成中...)"),
            gr.update(placeholder="ここに記述してください", visible=True, interactive=True, lines=10),
            tags_for_saving,
            school,
            gr.update(label=review_point),
            prompt_exercise,
            description,
            custom_prompt
        )
    
    def update_when_gen_quiz_btn_2(quiz_title, selections, quiz_text_dict, model, lti, num_review, new_exercise):
        rubrics = get_main_explanations(quiz_title, quiz_text_dict, lti)
        rubrics = rubrics[:-1]

        # check_if_solvable(question, knowledge, num, model, grade)
        new_answer, prompt_answer, elapsed_time_solve = check_if_solvable(new_exercise, rubrics, "type1", model, find_grade(lti["context_title"]))

        return (
            new_answer,
            elapsed_time_solve, 
            gr.update(visible=True, variant="primary", interactive=True, value="模範解答を表示"),
            prompt_answer
        )

    def pre_check_and_start(custom_prompt):
        if custom_prompt:
            is_sensitive = check_sensitive_prompt(custom_prompt)
            if is_sensitive:
                # gr.Errorを出すと、画面右上に警告メッセージが表示され、
                # 後続の .then() が全てキャンセルされるため、ボタンを押す前の状態が維持されます。
                raise gr.Error("⚠️ 問題が生成されませんでした。")
        return "SubmittedCheck"

    gen_quiz_btn.click(
        fn=pre_check_and_start,
        inputs=[custom_prompt_input],
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, checkbox_state],
        outputs=None
    ).then(
        fn=lambda: (
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(visible=True, variant="secondary", interactive=False, value="(あなたの理解に最適な問題を作成中...)"),
        "CreatedQuestion",
        1
        ),
        inputs=None,
        outputs=[
            vanish_btn, 
            quiz_dropdown, 
            checkboxes, 
            marker_checkboxes, 
            gen_quiz_btn, 
            rev_quiz_btn, 
            model_options, 
            custom_prompt_input,  # ★ここ
            btn_from_marker, 
            btn_from_rubric, 
            answer_btn, 
            operationname_state, 
            gen_state             # ★1を正しく受け取る
        ]
    ).then(
        fn=update_when_gen_quiz_btn,
        inputs=[quiz_dropdown, checkboxes, quiz_map_state, model_state, lti_state, cnt_review_state, highlighted_yellow_state, highlighted_red_state, isrubric_state, ismarker_state, marker_checkboxes, custom_prompt_state],
        outputs=[exercise_state, 
                 exercise_creation_time_state,
                 exercise_output, 
                 answer_btn, 
                 student_answer,
                 tags_state,
                 school_state,
                 rating,
                 prompt_exercise_state,
                 description_state, 
                 custom_prompt_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, exercise_state],
        outputs=None
    ).then(
        fn=lambda: (
        "CreateDescription"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, description_state],
        outputs=None
    ).then(
        fn=update_when_gen_quiz_btn_2,
        inputs=[quiz_dropdown, checkboxes, quiz_map_state, model_state, lti_state, cnt_review_state, exercise_state],
        outputs=[answer_state,
                 answer_creation_time_state,
                 answer_btn, 
                 prompt_answer_state]
    ).then(
        fn=lambda: (
        "CreatedAnswer"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, answer_state],
        outputs=None
    )

    @_prime_mongo_budget
    def update_when_rev_quiz_btn(quiz_title, quiz_text_dict, lti):
        quiz_text, contents_id, page, no, school, ans, ans_page_s, ans_page_e = quiz_text_dict[quiz_title]
        exercise_info = _scoped_exercise_info(lti, {"school_id": school, "contents_id": contents_id, "page": page, "no": no})
        standard_answer = exercise_info.get("standard_answer", "")
        rubrics = get_main_explanations(quiz_title, quiz_text_dict, lti)
        if (len(rubrics) > 0) and ("上記の項目について、ひとつも理解できなかった" not in rubrics):
            rubrics.append("上記の項目について、ひとつも理解できなかった")

        return (
            quiz_text,
            standard_answer,
            gr.update(value='<div style="text-align: center;" translate="no">' + quiz_text + "</div> <br> 右側の入力欄に解答の過程を入力するか、紙に解いて答えを出した後、模範解答を見て確認しましょう。<br> 注意：AIの生成問題には誤りを含むことがあります。"), 
            gr.update(visible=True, variant="primary", interactive=True, value="模範解答を表示"),
            gr.update(placeholder="ここに記述してください", visible=True, interactive=True, lines=10),
            [],
            school,
            gr.update(choices=rubrics),
            rubrics
        )

    rev_quiz_btn.click(
        fn=lambda: (
        "RevSubmittedCheck"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, checkbox_state],
        outputs=None
    ).then(
        fn=lambda: (
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        gr.update(interactive=False),
        False,
        "ReveiwedQuestion",
        0
        ),
        inputs=None,
        outputs=[vanish_btn, quiz_dropdown, checkboxes, gen_quiz_btn, rev_quiz_btn, model_options, btn_from_rubric, btn_from_marker, exercise_saving_state, operationname_state, gen_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, exercise_state],
        outputs=None
    ).then(
        fn=update_when_rev_quiz_btn,
        inputs=[quiz_dropdown, quiz_map_state, lti_state],
        outputs=[exercise_state,
                 answer_state, 
                 exercise_output, 
                 answer_btn, 
                 student_answer,
                 tags_state,
                 school_state,
                 new_checkboxes,
                 new_checkbox_all_items_state]
    ).then(
        fn=lambda: (
        "ReveiwedAnswer"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, answer_state],
        outputs=None
    )

    student_answer.change(
        fn=lambda text: (text, "InputStudentAnswer"),
        inputs=[student_answer],
        outputs=[user_answer_state, operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, user_answer_state],
        outputs=None
    )

    def update_when_answer_btn(solver, answer_time):
        if answer_time:
            return '<div style="text-align: center;" translate="no">' + solver + f"</div> 解答生成時間: {answer_time}秒" + "<br> 注意：AIの生成した解答には誤りを含むことがあります。"
        else:
            return '<div style="text-align: center;" translate="no">' + solver + "</div> <br> 注意：AIの生成した解答には誤りを含むことがあります。"
    
    def appear_questionnaire_box(is_gen, rubrics):
        rubrics = rubrics or []

        if is_gen == 1: #類題を作った場合
            return (
                gr.update(visible=True),
                gr.update(visible=False, interactive=False, show_label=False), #checkbox
                gr.update(visible=True, interactive=True), #understanding
                gr.update(visible=False, interactive=True), #difficulty
                gr.update(visible=False, interactive=True), #fluency
                gr.update(visible=True, interactive=True), #relevance
                gr.update(visible=True, interactive=True), #rating
                gr.update(visible=True, interactive=False), #answer_btn
                gr.update(visible=False, interactive=True), #report_type
                gr.update(visible=True, interactive=True), #report_text
                gr.update(visible=True),
                gr.update(visible=True, interactive=False),
                gr.update(visible=True, interactive=False),
                gr.update(visible=True),
                0, #understanding
                1, #difficulty
                1, #fluency
                0, #relevance
                0, #rating
                1, #checkbox
                "AnsweredExercise"
            )
        else: #そのまま解いた場合
            if len(rubrics) > 0:
                return (
                    gr.update(visible=True),
                    gr.update(visible=True, interactive=True, show_label=True), # checkbox
                    gr.update(visible=True, interactive=True), #understanding
                    gr.update(visible=False, interactive=True), #difficulty
                    gr.update(visible=False, interactive=False), #fluency
                    gr.update(visible=False, interactive=False), #relevance
                    gr.update(visible=False, interactive=False), #rating
                    gr.update(visible=True, interactive=False), #answer_btn
                    gr.update(visible=False, interactive=True), #report_type
                    gr.update(visible=True, interactive=True), #report_text
                    gr.update(visible=True),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True),
                    0, #understanding
                    1, #difficulty
                    1, #fluency
                    1, #relevance
                    1, #rating
                    0, #checkbox
                    "AnsweredExercise"
                )
            else:
                return (
                    gr.update(visible=True),
                    gr.update(visible=False, interactive=False, show_label=False),
                    gr.update(visible=True, interactive=True), #understanding
                    gr.update(visible=False, interactive=False), #difficulty
                    gr.update(visible=False, interactive=False), #fluency
                    gr.update(visible=False, interactive=False), #relevance
                    gr.update(visible=False, interactive=False), #rating
                    gr.update(visible=True, interactive=False), #answer_btn
                    gr.update(visible=False, interactive=True), #report_type
                    gr.update(visible=True, interactive=True), #report_text
                    gr.update(visible=True),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True, interactive=False),
                    gr.update(visible=True),
                    0, #understanding
                    1, #difficulty
                    1, #fluency
                    1, #relevance
                    1, #rating
                    1, #checkbox
                    "AnsweredExercise"
                )

    @_prime_mongo_budget
    def get_next_no_for_user(user_name, save, contents_id, page, no, lti):
        if lti.get("lti_version") == "1.3":
            scope = _prime_lti_scope(lti)
            store = PrimeStore(quiz_generator_db)
            store.authorize(scope)
            if not save:
                if contents_id == "ai_generated":
                    page = scope.learner_id
                return contents_id, page, no
            # Opaque references remain unique across concurrent generation attempts.
            return "ai_generated", scope.learner_id, uuid.uuid4().hex
        if save:
            # 該当ユーザのレコードを全件取得
            records = list(exercise_col.find({
                "contents_id": "ai_generated",
                "page": user_name
            }))

            if len(records)==0:
                return "ai_generated", user_name, "1"  # 該当がなければ1からスタート

            # ◀ 【修正】noを整数に変換する際、文字列("sample_1"等)が含まれていたら無視する安全な処理
            valid_nos = []
            for record in records:
                try:
                    val = int(record.get("no", 0))
                    valid_nos.append(val)
                except (ValueError, TypeError):
                    pass # 数字に変換できないものはスキップ
            
            # 最大値を取得（もし全て無効な文字列だった場合は0を返す）
            max_no = max(valid_nos) if valid_nos else 0

            return "ai_generated", user_name, str(max_no + 1)
        else:
            return contents_id, page, no

    answer_btn.click(
        fn=update_when_answer_btn,
        inputs=[answer_state, answer_creation_time_state],
        outputs=answer_output,
    ).then(
        fn=appear_questionnaire_box,
        inputs=[gen_state, new_checkbox_all_items_state],
        outputs=[answer_output,
                 new_checkboxes,
                 understanding,
                 difficulty,
                 fluency,
                 relevance,
                 rating,
                 answer_btn, 
                 report_type, 
                 report_text, 
                 report_markdown, 
                 report_btn, 
                 student_answer, 
                 note_mkdwn, 
                 understanding_state,
                 difficulty_state,
                 fluency_state,
                 relevance_state,
                 rating_state,
                 check_state,
                 operationname_state]
    ).then(
        fn=get_next_no_for_user,
        inputs=[user_state, exercise_saving_state, contentsid_state, page_state, no_state, lti_state],
        outputs=[new_contentsid_state, new_page_state, new_no_state]
    ).then(
        fn=handle_exercise,
        inputs=[contentsid_state, page_state, no_state, exercise_saving_state, new_no_state, exercise_state, answer_state, user_state, exercise_creation_time_state, answer_creation_time_state, model_state, session_state, lti_state, prompt_exercise_state, prompt_answer_state, custom_prompt_state],
        outputs=None
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, student_answer],
        outputs=None
    )

    def enable_submit(understanding_val, rating_val, difficulty_val, fluency_val, relevance_val, check_val):
        if understanding_val * rating_val * difficulty_val * fluency_val * relevance_val * check_val == 1:
            return gr.update(interactive=True, value="結果を送信する", variant="primary")
        else:
            return gr.update(interactive=False, value="結果を送信する(まずは問題を振り返ってください！)", variant="secondary")
        
    def change_questionnairestate(val):
        if val is not None:
            return 1
        else:
            return 0
        
    def change_box(val):
        if len(val) > 0:
            return 1
        else:
            return 0

    new_checkboxes.change(
        fn=update_when_checkboxes,
        inputs=[new_checkbox_all_items_state, new_checkboxes, new_current_checkbox_state],
        outputs=[new_checkbox_state, new_current_checkbox_state, new_checkboxes]
    ).then(
        fn=change_box,
        inputs=[new_checkboxes],
        outputs=check_state
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: (
        "SelectedNewRubricStatus"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, new_checkbox_state],
        outputs=None
    )

    understanding.change(
        fn=change_questionnairestate,
        inputs=[understanding],
        outputs=[understanding_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedComprehensibility"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, understanding],
        outputs=None
    )

    rating.change(
        fn=change_questionnairestate,
        inputs=[rating],
        outputs=[rating_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedUsefulness"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, rating],
        outputs=None
    )

    difficulty.change(
        fn=change_questionnairestate,
        inputs=[difficulty],
        outputs=[difficulty_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedDifficulty"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, difficulty],
        outputs=None
    )

    fluency.change(
        fn=change_questionnairestate,
        inputs=[fluency],
        outputs=[fluency_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedFluency"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, fluency],
        outputs=None
    )

    relevance.change(
        fn=change_questionnairestate,
        inputs=[relevance],
        outputs=[relevance_state]
    ).then(
        fn=enable_submit,
        inputs=[understanding_state, rating_state, difficulty_state, fluency_state, relevance_state, check_state],
        outputs=[report_btn]
    ).then(
        fn=lambda: ("SelectedRelevance"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, relevance],
        outputs=None
    )

    report_type.change(
        fn=lambda _: gr.update(placeholder="例：式が間違っている気がします、〜の部分がわかりません、など。なるべく具体的に", interactive=True, lines=10),
        inputs=report_type,
        outputs=report_text
    ).then(
        fn=lambda: ("SelectedReportType"),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, report_type],
        outputs=None
    )

    # 送信ボタンクリックで「送信しました」と表示
    report_btn.click(
        fn=lambda: (gr.update(interactive=False)),
        inputs=None,
        outputs=[report_btn]
    ).then(
        fn=handle_answer,
        inputs=[exercise_state, answer_state, exercise_saving_state, user_state, session_state, contentsid_state, page_state, no_state, highlighted_yellow_state, highlighted_red_state, checkbox_state, marker_checkboxes, school_state, new_contentsid_state, new_page_state, new_no_state, description_state, user_answer_state, understanding, rating, difficulty, fluency, relevance, new_checkbox_state, lti_state, report_type, report_text],
        outputs=None
    ).then(
        fn=lambda: (
        "Reported"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, report_type],
        outputs=None
    ).then(
        fn=lambda: (
            gr.update(interactive=True), # vanish_btn
            gr.update(visible=True), # report_result
            gr.update(visible=True, interactive=True, value=None), # contents_dropdown
            gr.update(visible=True, interactive=True, value=None), # quiz_dropdown
            gr.update(visible=False), # quiz_text_display
            gr.update(visible=False), # figure_warning_display
            gr.update(visible=False), # status_msg
            gr.update(visible=False, show_label=False, value=None), # checkboxes
            gr.update(visible=False, show_label=False, value=None), # new_checkboxes
            {}, # checkbox_state
            [], # current_checkbox_state
            {}, # new_checkbox_state
            [], # new_current_checkbox_state
            gr.update(visible=False, interactive=False), # btn_from_rubric
            gr.update(visible=False, interactive=False), # btn_from_marker
            gr.update(visible=True, interactive=False, variant="secondary", value="類題をつくる(まだ押せません)"), # gen_quiz_btn
            gr.update(visible=False, interactive=False, variant="primary", value="そのまま解く"), # rev_quiz_btn
            gr.update(value="復習問題はここに出てきます"), # exercise_output
            gr.update(visible=True, interactive=False, placeholder="(まだ入力できません)", value="", lines=1), # student_answer
            gr.update(visible=False, interactive=False), # answer_btn
            gr.update(visible=False, value=""), # answer_output
            gr.update(visible=False, interactive=True, value="o4-mini(速さ重視、普段使いにおすすめ)"), # model_options
            gr.update(visible=False, interactive=True, value=""), # custom_prompt_input
            gr.update(visible=False, interactive=False, value=None), # understanding
            gr.update(visible=False, interactive=False, value=None), # difficulty
            gr.update(visible=False, interactive=False, value=None), # fluency
            gr.update(visible=False, interactive=False, value=None), # relevance
            gr.update(visible=False, interactive=False, value=None), # rating
            gr.update(visible=False), # report_markdown
            gr.update(visible=False, interactive=False, value=None), # report_type
            gr.update(visible=False, interactive=False, placeholder="", value="", lines=5), # report_text
            gr.update(visible=False, interactive=False, value="結果を送信する(まずは問題を振り返ってください！)", variant="secondary"), # report_btn
            gr.update(visible=False, interactive=False, show_label=False, value=None), # marker_checkboxes
            gr.update(value="## " + random.choice(phrases)), # title
            gr.update(visible=False), # note_mkdwn
            gr.update(value=None), # exercise_creation_time_state
            gr.update(value=None), # answer_creation_time_state
            gr.update(value=None), # overall_creation_time_state
            gr.update(value=None), # tags_state
            gr.update(value=None), # school_state
            False, # isrubric_state
            False, # ismarker_state
            0, # understanding_state
            0, # difficulty_state
            0, # fluency_state
            0, # relevance_state
            0, # rating_state
            "", # new_contentsid_state, 
            "", # new_page_state, 
            "", # new_no_state,
            "", # contentsid_state, 
            "", # page_state, 
            "", # no_state,
            "", # description_state
            "", "", # prompt_exercise_state, prompt_answer_state
            "", # custom_prompt_state
            "", # user_answer_state
            "", "", # highlighted_red_state, highlighted_yellow_state
            "o4-mini", # model_state
            True # exercise_saving_state
        ),
        inputs=None,
        outputs=[
            vanish_btn,
            report_result,
            contents_dropdown,
            quiz_dropdown,
            quiz_text_display,
            figure_warning_display,
            status_msg,
            checkboxes,
            new_checkboxes,
            checkbox_state,
            current_checkbox_state,
            new_checkbox_state,
            new_current_checkbox_state,
            btn_from_rubric,
            btn_from_marker,
            gen_quiz_btn,
            rev_quiz_btn,
            exercise_output,
            student_answer,
            answer_btn,
            answer_output,
            model_options,
            custom_prompt_input,
            understanding,
            difficulty,
            fluency,
            relevance,
            rating,
            report_markdown,
            report_type,
            report_text,
            report_btn,
            marker_checkboxes,
            title,
            note_mkdwn,
            exercise_creation_time_state, 
            answer_creation_time_state,
            overall_creation_time_state,
            tags_state,
            school_state,
            isrubric_state,
            ismarker_state,
            understanding_state,
            difficulty_state,
            fluency_state,
            relevance_state,
            rating_state,
            new_contentsid_state, 
            new_page_state, 
            new_no_state,
            contentsid_state, 
            page_state, 
            no_state,
            description_state,
            prompt_exercise_state, prompt_answer_state,
            custom_prompt_state,
            user_answer_state,
            highlighted_red_state, highlighted_yellow_state,
            model_state, 
            exercise_saving_state
        ]
    ).then(
        fn=get_contents_dict_from_clickhouse,
        inputs=[lti_state],
        outputs=[contents_dropdown_state, contents_dropdown]
    ).then(
        fn=lambda: (
        "StartedSession",
        str(uuid.uuid4())
        ),
        inputs=None,
        outputs=[operationname_state, session_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state],
        outputs=None
    ).then(
        fn=get_journey_html,
        inputs=[lti_state],
        outputs=[journey_display]
    ).then(
        fn=lambda: (
        "Traveled"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, journey_display],
        outputs=None
    )

    # ✅ 初回自動読み込み（表示直後に1回実行）
    demo.load(
        fn=load_session_info,
        inputs=None,
        outputs=[lti_state, user_state]
    ).then(
        fn=get_contents_dict_from_clickhouse,
        inputs=[lti_state],
        outputs=[contents_dropdown_state, contents_dropdown]
    ).then(
        fn=lambda: (
        "StartedSession",
        str(uuid.uuid4()),
        "o4-mini"
        ),
        inputs=None,
        outputs=[operationname_state, session_state, model_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state],
        outputs=None
    ).then(
        fn=get_journey_html,
        inputs=[lti_state],
        outputs=[journey_display]
    ).then(
        fn=lambda: (
        "Traveled"
        ),
        inputs=None,
        outputs=[operationname_state]
    ).then(
        fn=handle_logs,
        inputs=[user_state, operationname_state, session_state, lti_state, journey_display],
        outputs=None
    )

demo.queue()