import os
import psycopg2
import psycopg2.extras


DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def _get_conn():
    """Создаём соединение с Supabase Postgres"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def ping_hr_data(limit: int = 5):
    """
    Тест подключения: возвращает список словарей (первые строки из hr_data).
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT report_date, service, hirecount, firecount FROM hr_data LIMIT %s;",
            (limit,),
        )
        rows = cur.fetchall()
        return rows or []


def run_hr_query(sql: str, params: tuple = (), limit: int = 50):
    """
    Универсальный запуск SQL-запроса SELECT.
    Возвращает список словарей (RealDictRow).
    Ограничивает количество возвращаемых строк.
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        if not rows:
            return []
        return rows  # ограничиваем для безопасности


def exec_sql(sql: str, params: tuple = ()):
    """
    Выполнение INSERT/UPDATE/DELETE.
    Возвращает количество изменённых строк.
    """
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcount
