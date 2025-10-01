# 🤖 HR-Insights — Telegram-бот для анализа HR-данных

**HR-Insights** — это умный Telegram-бот на базе Yandex GPT, который позволяет анализировать HR-данные на естественном языке. Он подключён к реальной базе данных, строит графики, генерирует SQL-запросы и отвечает на вопросы на русском языке.

---

## 🔍 Возможности

- 📊 Анализ и агрегация HR-метрик (наймы, увольнения и пр.)
- 🤖 Автоматическая генерация SQL-запросов с помощью Yandex GPT
- 📈 Визуализация данных с помощью Matplotlib (линии, столбцы, круговые диаграммы)
- 🗣 Общение на естественном языке через Telegram
- 🗃 Запись истории чата в Supabase (PostgreSQL)

---

## 🚀 Быстрый старт

1. Клонируйте репозиторий:

```bash
git clone https://github.com/your-username/hr-insights.git
cd hr-insights
```

2. Установите зависимости:

```bash
pip install -r requirements.txt
```

3. Создайте файл `.env` на основе шаблона:

```bash
cp .env.example .env
```

Заполните `.env` своими данными.

4. Запустите локально:

```bash
python main.py
```

> Или задеплойте как Yandex Cloud Function.

---

## 📁 Структура проекта

```
.
├── analyst.py         # Модуль для генерации SQL-запросов (GPT)
├── db.py              # Работа с Supabase (PostgreSQL)
├── logger.py          # Логгирование событий
├── main.py            # Основная точка входа
├── requirements.txt   # Python-зависимости
├── telegram.py        # Интеграция с Telegram Bot API
├── visualizer.py      # Построение графиков на основе данных
└── .env.example       # Пример переменных окружения
```

---

## ⚙️ Переменные окружения

См. `.env.example` для полного списка. Основные:

- `TELEGRAM_TOKEN` — токен Telegram-бота
- `YC_API_KEY` — API-ключ Yandex GPT
- `DB_HOST`, `DB_USER`, `DB_PASSWORD` и т.д. — параметры базы данных

---

## 🧠 Используемые технологии

- **Python 3.12**
- **Yandex GPT** для обработки естественного языка
- **Matplotlib** для визуализации
- **Telegram Bot API**
- **Supabase** (PostgreSQL) для хранения данных

---



