import json
import os
from telegram import send_message, send_photo
from logger import save_message, start_thread, get_chat_history
from analyst import run_analyst
from yandex_cloud_ml_sdk import YCloudML

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

_seen_update_ids: set[int] = set()
_threads: dict[str, str] = {}

# --- YandexGPT для диалогов и маршрутизации ---
FOLDER_ID = os.getenv("YC_FOLDER_ID")
API_KEY = os.getenv("YC_API_KEY")

yc_sdk = YCloudML(folder_id=FOLDER_ID, auth=API_KEY)
dialog_llm = yc_sdk.models.completions("yandexgpt")


def _log(label, obj):
    try:
        print(label, json.dumps(obj, ensure_ascii=False)[:3000])
    except Exception:
        print(label, str(obj)[:3000])


def decide_action(user_message: str) -> str:
    """
    GPT решает: нужно SQL (Analyst) или обычный ответ (Chat).
    Возвращает "SQL" или "CHAT".
    """
    system_prompt = """
    Ты — HR-ассистент. Твоя задача: определить, нужен ли SQL-запрос к базе данных hr_data,
    или достаточно обычного ответа.

    hr_data содержит:
    - report_date (DATE)
    - service (TEXT)
    - hirecount (INT)
    - firecount (INT)

    Если вопрос пользователя про статистику, графики, наймы, увольнения → ответ "SQL".
    Если это общий вопрос или разговор → ответ "CHAT".

    ВАЖНО: Ответь только одним словом: SQL или CHAT.
    """

    model = dialog_llm.configure(temperature=0.0, max_tokens=5)
    result = model.run(f"{system_prompt}\n\nВопрос: {user_message}")
    decision = result.alternatives[0].text.strip().upper()
    return "SQL" if "SQL" in decision else "CHAT"


def chat_with_gpt(thread_id: str, user_message: str) -> str:
    """
    Диалоговый ассистент (YandexGPT), использует историю.
    """
    history = get_chat_history(thread_id, limit=6)

    hist_text = "\n".join(
        [f"{row['role']}({row['agent_name']}): {row['message']}" for row in history]
    )[:2000]

    system_prompt = f"""
    Ты HR-ассистент. Отвечай дружелюбно, но по делу.
    Помогай в вопросах HR-аналитики и данных.
    История диалога:
    {hist_text}

    Новый вопрос: {user_message}
    """

    model = dialog_llm.configure(temperature=0.5, max_tokens=300)
    result = model.run(system_prompt)
    return result.alternatives[0].text.strip()


def handler(event, context):
    # ---------- Healthcheck ----------
    if (event or {}).get("httpMethod") == "GET":
        return {"statusCode": 200, "headers": {"Content-Type": "text/plain"}, "body": "ok"}

    # ---------- Проверка секрета ----------
    headers = (event or {}).get("headers") or {}
    headers_l = {(k or "").lower(): v for k, v in headers.items()}
    got_secret = headers_l.get("x-telegram-bot-api-secret-token")
    if got_secret != WEBHOOK_SECRET:
        _log("forbidden: bad secret", {"got": got_secret, "need": WEBHOOK_SECRET})
        return {"statusCode": 200, "body": "forbidden"}  # всегда 200

    # ---------- Разбор апдейта ----------
    try:
        update = json.loads((event or {}).get("body") or "{}")
        _log("incoming update", update)
    except Exception as e:
        _log("bad json", {"error": repr(e), "body": (event or {}).get("body")})
        return {"statusCode": 200, "body": "bad json"}  # всегда 200

    update_id = update.get("update_id")
    if update_id is not None:
        if update_id in _seen_update_ids:
            return {"statusCode": 200, "body": "dup"}
        _seen_update_ids.add(update_id)

    message = update.get("message") or update.get("edited_message") or {}
    chat_id = str((message.get("chat") or {}).get("id"))
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        _log("no chat_id or empty text", update)
        return {"statusCode": 200, "body": "no chat_id"}

    # ---------- Управление тредами ----------
    if chat_id not in _threads:
        _threads[chat_id] = start_thread(chat_id)  # используем chat_id как user_id
    thread_id = _threads[chat_id]

    # ---------- Сохраняем сообщение пользователя ----------
    save_message(thread_id, chat_id, "user", text, "user")

    # ---------- Бизнес-логика ----------
    try:
        if text == "/start":
            reply = "Привет! Я твой HR-ассистент 👋 Я могу работать с БД, строить графики и помогать в аналитике."
            save_message(thread_id, chat_id, "assistant", reply, "main")
            send_message(chat_id, reply)

        elif text.startswith("/db"):
            result = run_analyst(thread_id, text, chat_id)

            if result["type"] == "clarification":
                save_message(thread_id, chat_id, "assistant", result["text"], "analyst")
                send_message(chat_id, result["text"])
            elif result["type"] == "result":
                save_message(thread_id, chat_id, "assistant", result["text"], "analyst")
                send_message(chat_id, result["text"])
                if result["image"]:
                    send_photo(chat_id, result["image"], "Визуализация 📈")
            else:
                send_message(chat_id, result["text"])

        else:
            # --- GPT решает, звать ли аналитика ---
            action = decide_action(text)

            if action == "SQL":
                send_message(chat_id, "Генерирую аналитику... 📊")
                result = run_analyst(thread_id, text, chat_id)

                if result["type"] == "clarification":
                    save_message(thread_id, chat_id, "assistant", result["text"], "analyst")
                    send_message(chat_id, result["text"])
                elif result["type"] == "result":
                    save_message(thread_id, chat_id, "assistant", result["text"], "analyst")
                    send_message(chat_id, result["text"])
                    if result["image"]:
                        send_photo(chat_id, result["image"], "Визуализация 📈")
                else:
                    send_message(chat_id, result["text"])

            else:
                reply = chat_with_gpt(thread_id, text)
                save_message(thread_id, chat_id, "assistant", reply, "main")
                send_message(chat_id, reply)

        return {"statusCode": 200, "body": "ok"}

    except Exception as e:
        _log("unhandled error", repr(e))
        send_message(chat_id, "⚠️ Произошла внутренняя ошибка, попробуйте ещё раз.")
        return {"statusCode": 200, "body": "error"}  # всегда 200
