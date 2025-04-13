from typing import List

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from settings.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from src.api import get_employer_info, get_vacancies


def create_database_if_not_exists() -> None:
    """
    Создает БД если она не существует.

    Raises:
        psycopg2.OperationalError: Если не удается подключиться к серверу PostgreSQL
        Exception: При других ошибках создания БД
    """
    try:
        # Подключаемся к серверу PostgreSQL без указания конкретной БД
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname="postgres",  # Подключаемся к стандартной БД
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cur:
            # Проверяем существование БД
            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
            exists = cur.fetchone()

            if not exists:
                cur.execute(f"CREATE DATABASE {DB_NAME}")
                print(f"Создана новая БД: {DB_NAME}")
    except Exception as e:
        print(f"Ошибка при создании БД: {e}")
    finally:
        if "conn" in locals():
            conn.close()


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Устанавливает соединение с базой данных, создавая её при необходимости.

    Returns:
        Активное соединение с БД

    Raises:
        psycopg2.OperationalError: Если не удается подключиться к БД
    """
    create_database_if_not_exists()

    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )


def insert_data(employer_ids: List[int]) -> None:
    """
    Вставляет данные о компаниях и их вакансиях в базу данных.

    Args:
        employer_ids: Список идентификаторов работодателей с HeadHunter

    Raises:
        psycopg2.Error: При ошибках работы с базой данных
        Exception: При других ошибках в процессе вставки данных
    """
    with get_db_connection() as conn:  # Используем нашу новую функцию подключения
        with conn.cursor() as cur:
            for emp_id in employer_ids:
                info = get_employer_info(emp_id)
                print(f"Добавление компании: {info['name']} (ID: {info['id']})")

                cur.execute(
                    """
                    INSERT INTO companies (name, hh_id)
                    VALUES (%s, %s) ON CONFLICT (hh_id) DO NOTHING
                    RETURNING company_id;
                    """,
                    (info["name"], info["id"]),
                )

                company_row = cur.fetchone()
                company_id = company_row[0] if company_row else None
                print(f"company_id: {company_id}")

                vacancies = get_vacancies(emp_id)
                for vac in vacancies:
                    salary_from = vac["salary"]["from"] if vac["salary"] else None
                    salary_to = vac["salary"]["to"] if vac["salary"] else None

                    cur.execute(
                        """
                        INSERT INTO vacancies (company_id, title, salary_from, salary_to, url)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (
                            company_id,
                            vac["name"],
                            salary_from,
                            salary_to,
                            vac["alternate_url"],
                        ),
                    )
    print("Данные успешно добавлены.")
