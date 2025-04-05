import psycopg2
from settings.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


class DBManager:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
            )
            self.cur = self.conn.cursor()
            print("Соединение с базой данных установлено.")
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")
            raise

    def create_tables(self):
        """Создание таблиц в базе данных PostgreSQL."""
        try:
            # Таблица компаний
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS companies (
                    company_id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    hh_id INTEGER UNIQUE NOT NULL
                );
                """
            )

            # Таблица вакансий
            self.cur.execute(
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
            self.conn.commit()
            print("Таблицы успешно созданы.")
        except Exception as e:
            print(f"Ошибка при создании таблиц: {e}")
            self.conn.rollback()

    def add_company(self, company):
        """Добавление компании в таблицу companies."""
        try:
            self.cur.execute(
                """
                INSERT INTO companies (name, hh_id)
                VALUES (%s, %s)
                ON CONFLICT (hh_id) DO NOTHING;
                """,
                (company["name"], company["id"]),
            )
            self.conn.commit()
            print(f"Компания {company['name']} успешно добавлена.")
        except Exception as e:
            print(f"Ошибка при добавлении компании: {e}")
            self.conn.rollback()

    def add_vacancy(self, vacancy, company_id):
        """Добавление вакансии в таблицу vacancies."""
        try:
            self.cur.execute(
                """
                INSERT INTO vacancies (company_id, title, salary_from, salary_to, url)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (
                    company_id,
                    vacancy["name"],
                    vacancy["salary"]["from"] if vacancy["salary"] else None,
                    vacancy["salary"]["to"] if vacancy["salary"] else None,
                    vacancy["alternate_url"],
                ),
            )
            self.conn.commit()
            print(f"Вакансия {vacancy['name']} успешно добавлена.")
        except Exception as e:
            print(f"Ошибка при добавлении вакансии: {e}")
            self.conn.rollback()

    def get_companies_and_vacancies_count(self):
        try:
            self.cur.execute(
                """
                SELECT companies.name, COUNT(vacancies.vacancy_id)
                FROM companies
                JOIN vacancies ON companies.company_id = vacancies.company_id
                GROUP BY companies.name;
                """
            )
            return self.cur.fetchall()
        except Exception as e:
            print(f"Ошибка при получении данных: {e}")
            return []

    def get_all_vacancies(self):
        try:
            self.cur.execute(
                """
                SELECT companies.name, vacancies.title, vacancies.salary_from, vacancies.salary_to, vacancies.url
                FROM vacancies
                JOIN companies ON vacancies.company_id = companies.company_id;
                """
            )
            return self.cur.fetchall()
        except Exception as e:
            print(f"Ошибка при получении вакансий: {e}")
            return []

    def get_avg_salary(self):
        try:
            self.cur.execute(
                """
                SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0))/2)
                FROM vacancies
                WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL;
                """
            )
            return self.cur.fetchone()[0]
        except Exception as e:
            print(f"Ошибка при расчете средней зарплаты: {e}")
            return 0

    def get_vacancies_with_higher_salary(self):
        avg_salary = self.get_avg_salary()
        try:
            self.cur.execute(
                """
                SELECT title, salary_from, salary_to, url
                FROM vacancies
                WHERE ((COALESCE(salary_from, 0) + COALESCE(salary_to, 0))/2) > %s;
                """,
                (avg_salary,),
            )
            return self.cur.fetchall()
        except Exception as e:
            print(f"Ошибка при получении вакансий с зарплатой выше средней: {e}")
            return []

    def get_vacancies_with_keyword(self, keyword: str):
        try:
            self.cur.execute(
                """
                SELECT title, salary_from, salary_to, url
                FROM vacancies
                WHERE title ILIKE %s;
                """,
                (f"%{keyword}%",),
            )
            return self.cur.fetchall()
        except Exception as e:
            print(f"Ошибка при поиске вакансий по ключевому слову '{keyword}': {e}")
            return []

    def close(self):
        try:
            self.cur.close()
            self.conn.close()
            print("Соединение с базой данных закрыто.")
        except Exception as e:
            print(f"Ошибка при закрытии соединения: {e}")
