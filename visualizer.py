import os
os.environ["MPLCONFIGDIR"] = "/tmp"  # ✅ do matplotlib

import matplotlib
matplotlib.use("Agg")  # ✅ do pyplot

import matplotlib.pyplot as plt
import io
import json
import pandas as pd
from yandex_cloud_ml_sdk import YCloudML

# --- Фикс для Yandex Cloud Functions ---
matplotlib.rcParams["figure.dpi"] = 100

FOLDER_ID = os.getenv("YC_FOLDER_ID")
API_KEY = os.getenv("API_KEY")

yc_sdk = YCloudML(folder_id=FOLDER_ID, auth=API_KEY)
llm = yc_sdk.models.completions("yandexgpt")


def ask_visualization_schema(user_query: str, columns: list[str], schema: dict | None = None) -> dict:
    schema_text = ""
    if schema:
        schema_text = f"\n\n\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0430\u043b\u044c\u043d\u044b\u0435: {schema.get('categorical')}\n" \
                      f"Числовые: {schema.get('numeric')}\n" \
                      f"Временные: {schema.get('temporal')}"

    prompt = f"""
    Ты — помощник по визуализации HR-данных.

    Запрос: "{user_query}"
    Поля: {columns}{schema_text}

    Ответь в формате JSON без комментариев и объяснений.

    Пример:
    {{
      "type": "bar",
      "x": "department_3",
      "y": "firecount",
      "title": "Увольнения по департаментам",
      "xlabel": "Департамент",
      "ylabel": "Увольнения"
    }}
    """

    model = llm.configure(temperature=0.0, max_tokens=300)
    result = model.run([{"role": "user", "text": prompt}])

    text = result.alternatives[0].text.strip()
    text = text.strip("`")  # remove markdown if exists

    try:
        start = text.find("{")
        json_text = text[start:] if start != -1 else text
        return json.loads(json_text)
    except Exception as e:
        print("[GPT-Visual] \u26a0\ufe0f Ошибка парсинга JSON:", e)
        return {"type": "none"}


# --- Графики ---
def plot_line(x, y, title, xlabel, ylabel):
    plt.figure(figsize=(9, 5))
    plt.plot(x, y, marker="o")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()


def plot_bar(x, y, title, xlabel, ylabel):
    plt.figure(figsize=(9, 5))
    plt.bar(x, y)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(rotation=45)
    plt.tight_layout()


def plot_pie(values, labels, title):
    plt.figure(figsize=(7, 7))
    plt.pie(values, labels=labels, autopct="%1.1f%%")
    plt.title(title)
    plt.tight_layout()


def plot_scatter(x, y, title, xlabel, ylabel):
    plt.figure(figsize=(7, 5))
    plt.scatter(x, y, alpha=0.7)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()


def visualize_with_matplotlib(rows: list[dict], user_query: str, schema: dict | None = None) -> bytes | None:
    if not rows:
        return None

    columns = list(rows[0].keys())
    decision = ask_visualization_schema(user_query, columns, schema=schema)

    if decision.get("type") == "none":
        return None

    x_field = decision.get("x")
    y_field = decision.get("y")
    limited_rows = rows[:50]  # Ограничим объём для графика

    try:
        if decision["type"] == "line" and x_field and y_field:
            x = [r[x_field] for r in limited_rows if x_field in r]
            y = [r[y_field] for r in limited_rows if y_field in r]
            plot_line(x, y, decision.get("title", ""), decision.get("xlabel", x_field), decision.get("ylabel", y_field))

        elif decision["type"] == "bar" and x_field and y_field:
            x = [r[x_field] for r in limited_rows if x_field in r]
            y = [r[y_field] for r in limited_rows if y_field in r]
            plot_bar(x, y, decision.get("title", ""), decision.get("xlabel", x_field), decision.get("ylabel", y_field))

        elif decision["type"] == "pie" and x_field and y_field:
            labels = [r[x_field] for r in limited_rows if x_field in r]
            values = [r[y_field] for r in limited_rows if y_field in r]
            plot_pie(values, labels, decision.get("title", ""))

        elif decision["type"] == "scatter" and x_field and y_field:
            x = [r[x_field] for r in limited_rows if x_field in r]
            y = [r[y_field] for r in limited_rows if y_field in r]
            plot_scatter(x, y, decision.get("title", ""), decision.get("xlabel", x_field), decision.get("ylabel", y_field))

        else:
            return None

        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        plt.close()
        buf.seek(0)
        return buf.read()

    except Exception as e:
        print("[Visualizer] Ошибка рисования:", e)
        return None
