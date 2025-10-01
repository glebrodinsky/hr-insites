from logger import get_chat_history
from db import run_hr_query
from yandex_cloud_ml_sdk import YCloudML
from visualizer import visualize_with_matplotlib
from telegram import send_table_as_file
import os
import datetime
import re

FOLDER_ID = os.getenv("YC_FOLDER_ID")
API_KEY = os.getenv("YC_API_KEY")

yc_sdk = YCloudML(folder_id=FOLDER_ID, auth=API_KEY)
llm = yc_sdk.models.completions("yandexgpt")

# --- Полная схема таблицы hr_data ---
SCHEMA = {
    # временные/даты
    "report_date": "DATE — дата формирования отчёта. Не использовать для анализа трендов, так как доступно всего несколько дат (например, только за 3 месяца). Это дата выгрузки, а не события.",
    "fire_from_company": "DATE — реальная дата увольнения сотрудника. Значение 1970-01-01 означает, что сотрудник всё ещё работает. Используется для анализа увольнений по времени, только если дата больше 1971 года",
    "hire_to_company": "DATE — реальная дата выхода сотрудника в компанию. Используется для анализа количества наймов по времени.",
    "real_day": "INT — число дней в отчётном месяце",

    # числовые показатели
    "hirecount": "INT — (0/1) — флаг (1, если сотрудник был принят в компанию в текущем `report_date`). Не использовать для агрегации по времени. Подходит только для отчётов, связанных с конкретным `report_date`.",
    "firecount": "INT — (0/1) — флаг (1, если сотрудник был уволен в рамках `report_date`). Аналогично, не подходит для временных рядов.",
    "fte": "NUMERIC — ставка (0.2, 0.5, 1.0 и т.д.)",
    "experience": "NUMERIC — стаж сотрудника, исчсляемый в месяцах",
    "fullyears": "INT — Возраст сотрудника в полных годах.",

    # категориальные признаки
    "service": "TEXT — Сервис или подразделение, к которому относится сотрудник.",
    "cluster": "TEXT — кластер",
    "location_name": "TEXT — локация",
    "sex": "TEXT — пол сотрудника, значения M (мужчина) и F (женщина)",
    "age_category": "TEXT — Категориальный диапазон по возрасту",
    "experience_category": "TEXT — Категориальный диапазон по стажу",
    "department_3": "TEXT — Название департамента третьего уровня.",
    "department_4": "TEXT — Название департамента четвёртого уровня.",
    "department_5": "TEXT — Название департамента пятого уровня.",
    "department_6": "TEXT — Название департамента шестого уровня.",
}

CATEGORICAL = [
    "service", "cluster", "location_name", "sex",
    "age_category", "experience_category",
    "department_3", "department_4", "department_5", "department_6",
]
NUMERIC = ["hirecount", "firecount", "fte", "experience", "fullyears"]
TEMPORAL = ["report_date", "fire_from_company", "hire_to_company", "real_day"]


def _schema_text() -> str:
    return "\n".join([f"- {k}: {v}" for k, v in SCHEMA.items()])


def validate_sql(sql: str) -> bool:
    sql_up = sql.strip().upper()
    if not sql_up.startswith("SELECT"):
        return False
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    return not any(word in sql_up for word in forbidden)


def extract_sql(text: str) -> str | None:
    """
    Достаём SQL-запрос из текста ответа (если есть).
    """
    # 1. Ищем внутри блоков ```sql ... ```
    match = re.search(r"```(?:sql)?\s*(SELECT .*?)```", text, flags=re.I | re.S)
    if match:
        return match.group(1).strip()

    # 2. Если блоков нет — ищем просто SELECT
    match = re.search(r"(SELECT .*?);?$", text, flags=re.I | re.S)
    if match:
        return match.group(1).strip()

    return None


def make_filename(user_message: str) -> str:
    date_str = datetime.date.today().isoformat()
    name = "analytics"

    if "увольн" in user_message.lower():
        name = "fires"
    elif "найм" in user_message.lower():
        name = "hires"
    elif "статистик" in user_message.lower():
        name = "stats"
    elif "график" in user_message.lower():
        name = "chart"

    return f"{name}_{date_str}.csv"


def run_analyst(thread_id: str, user_message: str, chat_id: str) -> dict:
    try:
        # --- История чата ---
        history = get_chat_history(thread_id, limit=5)
        hist_text = "\n".join(
            [f"{row['role']}({row['agent_name']}): {row['message']}" for row in history]
        )

        # --- Системный промпт ---
        system_prompt = f"""
Ты — SQL-аналитик, работающий с таблицей hr_data.

📊 Структура таблицы:
{_schema_text()}

📂 Категориальные признаки:
{', '.join(CATEGORICAL)}

🔢 Числовые признаки:
{', '.join(NUMERIC)}

🕒 Временные признаки:
{', '.join(TEMPORAL)}

---

Обрати внимание:

1. Если нужны точные даты найма или увольнения — используй:
   - hire_to_company — дата найма сотрудника
   - fire_from_company — дата увольнения сотрудника
   Это точные события.

2. hirecount и firecount — агрегированные метрики по report_date (возможно с дублированием). Не использовать для точных расчётов.

3. Для анализа динамики наймов (по месяцам/годам) используй:
   DATE_TRUNC('month', hire_to_company) или fire_from_company, а не `report_date` или `hirecount`.

4. report_date — это отчётный месяц (агрегированная метка, не факт события).

5. fte — ставка: 1.0 = полная занятость, 0.5 = половина и т.д.

6. fullyears — возраст (в полных годах), experience — стаж (в годах).

7. age_category и experience_category — категориальные признаки.

8. department_3...6 — иерархия подразделений.

9. Всегда фильтруй и группируй по дате, если упоминается "по месяцам", "по годам", "за период".


---

🎯 Правила:

1. Пиши **только SQL SELECT** на PostgreSQL к таблице `hr_data`. Присваивай признакам информативные имена в стоответствии с запросом. 
2. Если не хватает деталей (год, пол, возраст, сервис) — сначала задай **уточняющий вопрос** на русском.
3. Не придумывай данные, не используй другие таблицы.
4. Отвечай **только одним из двух** вариантов:
   - Уточняющий вопрос (текст).
   - Чистый SQL SELECT (без пояснений).
5. Никогда не используй слова из пользовательского запроса как значения в SQL. Это ЗАПРЕЩЕНО!
    Вместо этого — фильтруй или группируй по существующим значениям категориальных признаков из структуры данных
    (например, department_3, service, cluster и т.д.).

    Если неясно, какое значение имеется в виду (например, "пятый департамент", "город", "мужчины старше 30") —
    сначала задай уточняющий вопрос. Не придумывай значения!
"""


        model = llm.configure(temperature=0.0, max_tokens=500)
        result = model.run(f"{system_prompt}\n\nИстория:\n{hist_text}\n\nВопрос: {user_message}")

        answer = result.alternatives[0].text.strip()
        print("Ответ аналитика:", answer)

        sql = extract_sql(answer)

        # --- Уточняющий вопрос ---
        if not sql:
            return {"type": "clarification", "text": f"❓ {answer}", "image": None}

        # --- Валидация SQL ---
        if not validate_sql(sql):
            return {"type": "error", "text": f"⚠️ Запрос отклонён как небезопасный:\n{sql}", "image": None}

        # --- Выполнение SQL ---
        try:
            rows = run_hr_query(sql)
        except Exception as db_err:
            return {
                "type": "error",
                "text": f"⚠️ Ошибка при выполнении SQL:\n{sql}\n\nОшибка: {db_err}",
                "image": None,
            }

        if not rows:
            return {"type": "result", "text": "⚠️ Данных нет.", "image": None}

        # --- Отправляем результат таблицей (CSV) ---
        filename = make_filename(user_message)
        send_table_as_file(chat_id, rows, filename=filename)

        # --- Визуализация ---
        img = None
        try:
            img = visualize_with_matplotlib(
                rows,
                user_query=user_message,
                schema={"categorical": CATEGORICAL, "numeric": NUMERIC, "temporal": TEMPORAL},
            )
            print("Визуализация:", "есть" if img else "НЕТ")
        except Exception as e:
            print("Matplotlib visualization failed:", e)

        return {"type": "result", "text": f"📊 Результат анализа во вложенном файле: {filename}", "image": img}

    except Exception as e:
        return {"type": "error", "text": f"❌ Ошибка аналитика: {e}", "image": None}
