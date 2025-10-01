from db import run_hr_query, exec_sql
import uuid


def save_message(thread_id: str, user_id: str, role: str, message: str, agent_name: str = "main"):
    """
    Сохраняем сообщение в чат-лог.
    """
    sql = """
        INSERT INTO chat_log (thread_id, user_id, role, message, agent_name, ts)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """
    exec_sql(sql, (thread_id, user_id, role, message, agent_name))


def start_thread(user_id: str) -> str:
    """
    Создаём новый тред (диалог).
    """
    thread_id = str(uuid.uuid4())
    sql = "INSERT INTO chat_threads (thread_id, user_id, started_at) VALUES (%s, %s, NOW())"
    exec_sql(sql, (thread_id, user_id))
    return thread_id


def get_chat_history(thread_id: str, limit: int = 10):
    """
    Возвращает историю чата по треду (от старых к новым).
    """
    sql = """
        SELECT role, message, agent_name, ts
        FROM chat_log
        WHERE thread_id = %s
        ORDER BY ts DESC
        LIMIT %s
    """
    rows = run_hr_query(sql, (thread_id, limit))
    return rows[::-1] if rows else []
