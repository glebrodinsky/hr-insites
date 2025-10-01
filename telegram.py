import requests
import csv
import io
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")


def _check_response(resp):
    """Проверка ответа Telegram API"""
    try:
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            print("Telegram API error:", data)
        return data
    except Exception as e:
        print("Telegram request failed:", e)
        return {"ok": False, "error": str(e)}


def send_message(chat_id, text, parse_mode=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    resp = requests.post(url, json=payload)
    return _check_response(resp)


def send_photo(chat_id, photo_bytes, caption=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    files = {"photo": ("image.png", photo_bytes, "image/png")}
    data = {"chat_id": chat_id, "caption": caption or ""}
    resp = requests.post(url, data=data, files=files)
    return _check_response(resp)


def send_table_as_file(chat_id: str, rows: list[dict], filename="result.csv"):
    """Отправляем список словарей как CSV-файл"""
    if not rows:
        return {"ok": False, "error": "Empty rows"}

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys(), delimiter=";")
    writer.writeheader()
    for row in rows:
        safe_row = {k: str(v) if v is not None else "" for k, v in row.items()}
        writer.writerow(safe_row)
    buf.seek(0)

    # Кодируем в байты
    file_bytes = buf.getvalue().encode("utf-8-sig")

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    files = {"document": (filename, io.BytesIO(file_bytes), "text/csv")}
    data = {"chat_id": chat_id}
    resp = requests.post(url, data=data, files=files)
    return _check_response(resp)

