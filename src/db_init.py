import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from settings.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def create_database_if_not_exists():
    """Создает базу данных, если она не существует."""
    try:
        # Подключаемся к postgres (системной БД)
        conn = psycopg2.connect(
            dbname="postgres",  # ВАЖНО: подключаемся к postgres, а не к своей БД
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Проверяем, существует ли нужная БД
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f"CREATE DATABASE {DB_NAME};")
            print(f"База данных '{DB_NAME}' успешно создана.")
        else:
            print(f"База данных '{DB_NAME}' уже существует.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
