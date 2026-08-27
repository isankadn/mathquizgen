import gradio as gr
from openai import OpenAI
import time
import random
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
import uuid
import json
import base64

JST = timezone(timedelta(hours=9))

load_dotenv()

# Mongo 起動を待ってから接続
mongo_client = MongoClient(os.getenv("MONGO_URL"))

# データベースとコレクションを定義
quiz_generator_db = mongo_client["prime"]
exercise_col = quiz_generator_db["question_bank"]
history_col = quiz_generator_db["history"]
evaluation_col = quiz_generator_db["evaluation"]

#ltiセッション情報読み込み
def load_session_info(request: gr.Request):
    lti = request.session['user']
    user = lti['user_id']
    return lti, user

openai_client = None


def get_openai_client():
    global openai_client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for evaluation.")
    if openai_client is None:
        openai_client = OpenAI(api_key=api_key)
    return openai_client
models = ["gpt-3.5-turbo", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini", "o1-preview", "o1-mini", "o3-mini"]

# genaiのapiを走らせる
def gpt_exection(model, query):
    '''
    str: model, str: query
    '''
    completion = get_openai_client().chat.completions.create(
        model=model,
        messages=[
            {
            "role": "user", "content": query
            }
        ]
    ) 
    return completion.choices[0].message.content

def handle_evaluation(user, session, g1, g2, c1, c2, v1, v2, a1, a2, e1, e2, report_text):   
    evaluation_doc = {
        "user": user,
        "session_id": session,
        "grammar1": g1,
        "grammar2": g2,
        "clarity1": c1,
        "clarity2": c2,
        "validity1": v1,
        "validity2": v2,
        "answerability1": a1,
        "answerability2": a2,
        "explainability1": e1,
        "explainability2": e2,
        "report": report_text,
        "timestamp": datetime.now(JST),
    }
    evaluation_col.replace_one({"user": user, "session_id": session}, evaluation_doc, upsert=True)
    return

# ✅ MongoDBから再読み込みして State と Dropdown を更新する関数
def reload_quiz_map_from_mongo(lti):
    
    documents_browse = list(
        exercise_col.find({
            "user": "prime",
            "show": True
        })
    )

    documents = list(
        exercise_col.find({
            "school_id": "C126210001533",
            "show": True,
            "user": {"$ne": "prime"},
            "timestamp": {"$exists": False}
        })
    )

    evaluation_browse = list(
        evaluation_col.find({
            "user": lti["user_id"]
        })
    )

    if not documents:
        return {}, {}, gr.update(choices=[], value=None)
    
    # quiz_text_dict = short_session_id → (quiz_text, answer, contentid, page, no)
    quiz_text_dict = {}

    for doc in documents:
        text = doc.get("quiz_text")
        answer = doc.get("standard_answer")
        title = doc.get("quiz_title")
        sessionid = doc.get("session_id")
        flag = True
        for quiztext in documents_browse:
            if text == quiztext.get("quiz_text"):
                flag = False
                break
        session_ids = [doc["session_id"] for doc in evaluation_browse]
        if sessionid in session_ids:
            sessionid_text = "✅ " + sessionid
        else:
            sessionid_text = sessionid

        if title and text and answer and flag:
            quiz_text_dict[sessionid_text] = (text, answer, sessionid)
    title_text = "## 評価が完了した問題： " + str(len(evaluation_browse)) + " / " + str(len(quiz_text_dict))
    return quiz_text_dict, gr.update(choices=sorted(quiz_text_dict.keys()), value=None), gr.update(value=title_text)

# userのこれまでのresultを入手する
def get_result_from_db(quiz_title, user):
    
    # MongoDBクエリ
    query = {
        "session_id": quiz_title,
        "user": user
    }
    matching_docs = list(evaluation_col.find(query))
    num_workingquiz = len(matching_docs)

    return num_workingquiz

###################################### interface ######################################

with gr.Blocks() as demo:
    lti_state = gr.State()  # ここにユーザー情報を保存   
    user_state = gr.State()
    session_state = gr.State()
    operationname_state = gr.State()
    contentsid_state = gr.State()
    page_state = gr.State()
    no_state = gr.State()
    quiz_map_state = gr.State()
    exercise_saving_state = gr.State(True)

    exercise_state = gr.State()
    exercise_creation_time_state = gr.State()
    answer_creation_time_state = gr.State()
    overall_creation_time_state = gr.State()
    new_contentsid_state = gr.State()
    new_page_state = gr.State()
    new_no_state = gr.State()
    tags_state = gr.State()
    school_state = gr.State()
    model_state = gr.State("o4-mini")
    gen_state = gr.State()

    gr.Markdown(
        """
        <div style="background-color: #2196f3; padding: 24px; border-radius: 8px; text-align: center; color: black;">
        <h1> $$\\Huge PRIME - \\textsf{評価システム} $$ </h1>
        </div>
        """
    )
    
    report_result = gr.Markdown(
        "### 正常に送信されました！",
        visible=False
    )
    
    with gr.Row():
        with gr.Column(scale=3): 
            title = gr.Markdown(
                "あなたが評価した問題数が出ます",
                visible=True
            )

        with gr.Column(scale=2): 
            vanish_btn = gr.Button(
                value="",
                visible=False,
                interactive=False,
                variant="secondary"
            )
    title_state = gr.State()

    quiz_dropdown = gr.Dropdown(
        choices=[],
        label="問題を選択",
        value=None
    )
    dropdown_state = gr.State()

    quiz_text_display = gr.Markdown(visible=False)

    with gr.Row():  
        with gr.Column(scale=1):    
            status_msg = gr.Markdown(
                value='',
                visible=False
            )

        with gr.Column(scale=1):
            checkboxes = gr.CheckboxGroup(
                choices=[], 
                label="できたポイントをチェックしよう", 
                visible=False, 
                show_label = False
            )
    status_msg_state = gr.State()
    checkbox_state = gr.State()
    checkbox_all_items_state = gr.State()
    current_checkbox_state = gr.State([])
    check_flaw_state = gr.State()
    new_checkbox_all_items_state = gr.State()
    new_checkbox_state = gr.State()
    new_current_checkbox_state = gr.State([])
    cnt_work_state = gr.State()
    cnt_state = gr.State()
    
    with gr.Row():  
        with gr.Column(scale=1):    
            gen_quiz_btn = gr.Button("前の問題を見る", visible=False, interactive=True, variant="stop")

        with gr.Column(scale=1):
            rev_quiz_btn = gr.Button("次の問題を見る", visible=False, interactive=True, variant="stop")

    with gr.Row():
        with gr.Column(scale=1):
            exercise_output = gr.Markdown(
                value="",
                visible=False
            )

        with gr.Column(scale=1):
            student_answer = gr.Textbox(
                label="",
                lines=1,
                placeholder="",
                visible=False,
                interactive=False
            )

    answer_btn = gr.Button("模範解答を表示", visible=False)

    with gr.Row():
        with gr.Column(scale=3):
            answer_output = gr.Markdown(
                "",
                visible=False
            )
        answer_state = gr.State()

        with gr.Column(scale=2):
            note_mkdwn = gr.Markdown("### 評価", visible=False)

            clarity2 = gr.Radio(
                choices=["3", "2", "1"],
                label="問題文は、より簡単に記述できる",
                visible=False
            )

            grammar1 = gr.Radio(
                choices=["3", "2", "1"],
                label="問題文は問いかけになっている",
                visible=False
            )

            answerability2 = gr.Radio(
                choices=["3", "2", "1"],
                label="問題文に矛盾がある",
                visible=False
            )

            clarity1 = gr.Radio(
                choices=["3", "2", "1"],
                label="問題文で問われていることがはっきりしている",
                visible=False
            )

            answerability1 = gr.Radio(
                choices=["3", "2", "1"],
                label="問題の答えが有限個に定まる",
                visible=False
            )

            validity1 = gr.Radio(
                choices=["3", "2", "1"],
                label="解答には正しいことが書かれている",
                visible=False
            )

            validity2 = gr.Radio(
                choices=["3", "2", "1"],
                label="解答は過不足がある",
                visible=False
            )

            explainability1 = gr.Radio(
                choices=["3", "2", "1"],
                label="全体は、中学生が理解できる",
                visible=False
            )

            explainability2 = gr.Radio(
                choices=["3", "2", "1"],
                label="全体で、中学で習わないような用語・表現が使われている",
                visible=False
            )

            grammar2 = gr.Radio(
                choices=["3", "2", "1"],
                label="全文中に、日本語の誤りがある",
                visible=False
            )

            report_text = gr.Textbox(
                label="その他気づいた点",
                placeholder="",
                visible=False,
                interactive=False
            )
        
        grammar1_state = gr.State()
        grammar2_state = gr.State()
        clarity1_state = gr.State()
        clarity2_state = gr.State()
        validity1_state = gr.State()
        validity2_state = gr.State()
        explainability1_state = gr.State()
        explainability2_state = gr.State()
        answerability1_state = gr.State()
        answerability2_state = gr.State()
        check_state = gr.State()
        report_type_state = gr.State()
        report_text_state = gr.State()

    with gr.Row():
        with gr.Column(scale=1):
            report_btn = gr.Button(
                interactive=False, 
                value="送信", 
                variant="secondary",
                visible=False
            )

    def update_when_dropdown(quiz_title, quiz_text_dict, user):
        if quiz_title:
            quiz_text, answer, sessionid = quiz_text_dict[quiz_title]
            count_work = get_result_from_db(sessionid, user)
        
            return (
                    gr.update(visible=True, interactive=True),
                    gr.update(value=f'<div style="text-align: center;"><h1> あなたが選んだ問題 </h1></div><div style="border: 3px solid #2196f3;padding: 24px;border-radius: 8px;text-align: center;"> \n{quiz_text} </div>', visible=True),
                    gr.update(),
                    quiz_text,
                    count_work,
                    answer,
                    sessionid
            )
        else:
            return (
                gr.update(),
                gr.update(),
                gr.update(),
                "",
                0,
                "",
                ""
            )

    quiz_dropdown.change(
        fn=update_when_dropdown,
        inputs=[quiz_dropdown, quiz_map_state, user_state],
        outputs=[
            answer_btn,
            quiz_text_display, 
            checkboxes,
            dropdown_state,
            cnt_work_state,
            answer_state,
            session_state
        ]
    )

    def update_when_answer_btn(solver):
        return (
            gr.update(visible=True, value=solver), #answer_output
            gr.update(visible=True, interactive=True), gr.update(visible=True, interactive=True), #grammar1, grammar2
            gr.update(visible=True, interactive=True), gr.update(visible=True, interactive=True), #clarity1, clarity2
            gr.update(visible=True, interactive=True), gr.update(visible=True, interactive=True), #validity1, validity2
            gr.update(visible=True, interactive=True), gr.update(visible=True, interactive=True), #answerability1, answerability2
            gr.update(visible=True, interactive=True), gr.update(visible=True, interactive=True), #explainability1, explainability2
            gr.update(visible=True, interactive=True), #report_text
            gr.update(visible=True, interactive=False, value="評価を入力していない箇所があります", variant="secondary"), #report_btn
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, #states
            gr.update(interactive=False), gr.update(interactive=False) #quiz_dropdown, answer_btn
        )

    answer_btn.click(
        fn=update_when_answer_btn,
        inputs=[answer_state],
        outputs=[
            answer_output,
            grammar1, grammar2,
            clarity1, clarity2,
            validity1, validity2,
            answerability1, answerability2,
            explainability1, explainability2,
            report_text,
            report_btn,
            grammar1_state, grammar2_state,
            clarity1_state, clarity2_state,
            validity1_state, validity2_state,
            answerability1_state, answerability2_state,
            explainability1_state, explainability2_state,
            quiz_dropdown, answer_btn
            ]
    )

    def enable_submit(g1, g2, c1, c2, v1, v2, a1, a2, e1, e2):
        if g1 * g2 * c1 * c2 * v1 * v2 * a1 * a2 * e1 * e2 == 1:
            return gr.update(interactive=True, value="送信", variant="primary")
        else:
            return gr.update(interactive=False, value="評価を入力していない箇所があります", variant="secondary")
        
    def change_questionnairestate(val):
        if val is not None:
            return 1
        else:
            return 0

    grammar1.change(
        fn=change_questionnairestate,
        inputs=[grammar1],
        outputs=[grammar1_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    grammar2.change(
        fn=change_questionnairestate,
        inputs=[grammar2],
        outputs=[grammar2_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    clarity1.change(
        fn=change_questionnairestate,
        inputs=[clarity1],
        outputs=[clarity1_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    clarity2.change(
        fn=change_questionnairestate,
        inputs=[clarity2],
        outputs=[clarity2_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    validity1.change(
        fn=change_questionnairestate,
        inputs=[validity1],
        outputs=[validity1_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    validity2.change(
        fn=change_questionnairestate,
        inputs=[validity2],
        outputs=[validity2_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    answerability1.change(
        fn=change_questionnairestate,
        inputs=[answerability1],
        outputs=[answerability1_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    answerability2.change(
        fn=change_questionnairestate,
        inputs=[answerability2],
        outputs=[answerability2_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    explainability1.change(
        fn=change_questionnairestate,
        inputs=[explainability1],
        outputs=[explainability1_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    explainability2.change(
        fn=change_questionnairestate,
        inputs=[explainability2],
        outputs=[explainability2_state]
    ).then(
        fn=enable_submit,
        inputs=[grammar1_state, grammar2_state, 
                clarity1_state, clarity2_state, 
                validity1_state, validity2_state, 
                answerability1_state, answerability2_state, 
                explainability1_state, explainability2_state],
        outputs=[report_btn]
    )

    # 送信ボタンクリックで「送信しました」と表示
    report_btn.click(
        fn=lambda: (gr.update(interactive=False)),
        inputs=None,
        outputs=[report_btn]
    ).then(
        fn=handle_evaluation,
        inputs=[user_state, session_state, grammar1, grammar2, clarity1, clarity2, validity1, validity2, answerability1, answerability2, explainability1, explainability2, report_text],
        outputs=None
    ).then(
        fn=lambda: (
            gr.update(visible=True, interactive=True, value=None), # quiz_dropdown
            gr.update(value=""), # quiz_text_display
            gr.update(""), # status_msg
            gr.update(visible=False, interactive=False), # answer_btn
            gr.update(visible=False, value=""), # answer_output
            gr.update(visible=False, interactive=False, value=None), gr.update(visible=False, interactive=False, value=None), # grammar1, grammar2
            gr.update(visible=False, interactive=False, value=None), gr.update(visible=False, interactive=False, value=None), # clarity1, clarity2
            gr.update(visible=False, interactive=False, value=None), gr.update(visible=False, interactive=False, value=None), # validity1, validity2
            gr.update(visible=False, interactive=False, value=None), gr.update(visible=False, interactive=False, value=None), # answerability1, answerability2
            gr.update(visible=False, interactive=False, value=None), gr.update(visible=False, interactive=False, value=None), # explainability1, explainability2
            gr.update(visible=False, interactive=False, placeholder="", value="", lines=1), # report_text
            gr.update(visible=False, interactive=False, value="送信", variant="secondary"), # report_btn
            0, 0, # grammar1_state, grammar2_state
            0, 0, # clarity1_state, clarity2_state
            0, 0, # validity1_state, validity2_state
            0, 0, # answerability1_state, answerability2_state
            0, 0, # explainability1_state, explainability2_state
            "", # session_state
            True # exercise_saving_state
        ),
        inputs=None,
        outputs=[
            quiz_dropdown,
            quiz_text_display,
            status_msg,
            answer_btn,
            answer_output,
            grammar1, grammar2,
            clarity1, clarity2,
            validity1, validity2,
            answerability1, answerability2,
            explainability1, explainability2,
            report_text,
            report_btn,
            grammar1_state, grammar2_state,
            clarity1_state, clarity2_state,
            validity1_state, validity2_state,
            answerability1_state, answerability2_state,
            explainability1_state, explainability2_state,
            session_state,
            exercise_saving_state
        ]
    ).then(
        fn=reload_quiz_map_from_mongo,
        inputs=[lti_state],
        outputs=[quiz_map_state, quiz_dropdown, title]
    )

    # ✅ 初回自動読み込み（表示直後に1回実行）
    demo.load(
        fn=load_session_info,
        inputs=None,
        outputs=[lti_state, user_state]
    ).then(
        fn=reload_quiz_map_from_mongo,
        inputs=[lti_state],
        outputs=[quiz_map_state, quiz_dropdown, title]
    )

demo.queue()