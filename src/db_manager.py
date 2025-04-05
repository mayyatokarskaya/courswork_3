import psycopg2
from settings.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


class DBManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        self.cur.execute(
            """
            SELECT companies.name, COUNT(vacancies.vacancy_id)
            FROM companies
            JOIN vacancies ON companies.company_id = vacancies.company_id
            GROUP BY companies.name;
        """
        )
        return self.cur.fetchall()

    def get_all_vacancies(self):
        self.cur.execute(
            """
            SELECT companies.name, vacancies.title, vacancies.salary_from, vacancies.salary_to, vacancies.url
            FROM vacancies
            JOIN companies ON vacancies.company_id = companies.company_id;
        """
        )
        return self.cur.fetchall()

    def get_avg_salary(self):
        self.cur.execute(
            """
            SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0))/2)
            FROM vacancies
            WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL;
        """
        )
        return self.cur.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        avg_salary = self.get_avg_salary()
        self.cur.execute(
            """
            SELECT title, salary_from, salary_to, url
            FROM vacancies
            WHERE ((COALESCE(salary_from, 0) + COALESCE(salary_to, 0))/2) > %s;
        """,
            (avg_salary,),
        )
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str):
        self.cur.execute(
            """
            SELECT title, salary_from, salary_to, url
            FROM vacancies
            WHERE title ILIKE %s;
        """,
            (f"%{keyword}%",),
        )
        return self.cur.fetchall()

    def close(self):
        self.cur.close()
        self.conn.close()
