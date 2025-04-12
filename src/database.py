from src.api import get_employer_info, get_vacancies
from settings.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
import psycopg2


def insert_data(employer_ids: list):
    with psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    ) as conn:
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
                print(f"company_id: {company_id}")  # Отладочный вывод
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

