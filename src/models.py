import psycopg2
from settings.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def create_tables():
    """Создание таблиц в базе данных PostgreSQL."""
    with psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    ) as conn:
        with conn.cursor() as cur:
            # Таблица компаний
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS companies (
                    company_id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    hh_id INTEGER UNIQUE NOT NULL
                );
            """
            )

            # Таблица вакансий
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES companies(company_id),
                    title TEXT NOT NULL,
                    salary_from INTEGER,
                    salary_to INTEGER,
                    url TEXT NOT NULL
                );
            """
            )

    print("Таблицы успешно созданы.")
