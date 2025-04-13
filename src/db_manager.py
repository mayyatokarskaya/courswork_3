import psycopg2
from typing import List, Tuple, Optional, Dict, Any
from database import get_db_connection
from settings.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from src.api import get_vacancies


class DBManager:
    """Класс для управления взаимодействием с базой данных вакансий."""

    def __init__(self) -> None:
        """
        Инициализирует соединение с базой данных.

        Raises:
            psycopg2.OperationalError: Если не удается подключиться к БД
        """
        self.conn = get_db_connection()
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

    def create_tables(self) -> None:
        """
        Создает таблицы companies и vacancies в базе данных.

        Raises:
            psycopg2.Error: При ошибках выполнения SQL-запросов
        """
        try:
            # Таблица компаний
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS companies (
                    company_id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    hh_id INTEGER UNIQUE NOT NULL,
                    logo TEXT,
                    description TEXT
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
                    url TEXT NOT NULL,
                    description TEXT,
                    published_at TIMESTAMP
                );
                """
            )
            self.conn.commit()
            print("Таблицы успешно созданы.")
        except Exception as e:
            print(f"Ошибка при создании таблиц: {e}")
            self.conn.rollback()

    def add_company(self, company: Dict[str, Any]) -> Optional[int]:
        """
        Добавляет компанию в таблицу companies.

        Args:
            company: Словарь с данными компании (должен содержать ключи 'name' и 'id')

        Returns:
            ID добавленной компании или None при ошибке

        Raises:
            psycopg2.Error: При ошибках выполнения SQL-запросов
        """
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

            # Получаем company_id компании
            self.cur.execute(
                """
                SELECT company_id FROM companies WHERE hh_id = %s;
                """,
                (company["id"],),
            )
            company_id = self.cur.fetchone()[0]
            print(
                f"Компания {company['name']} успешно добавлена с company_id = {company_id}."
            )
            return company_id
        except Exception as e:
            print(f"Ошибка при добавлении компании: {e}")
            self.conn.rollback()
            return None

    def add_vacancy(self, vacancy: Dict[str, Any], hh_id: int) -> None:
        """
        Добавляет вакансию в таблицу vacancies.

        Args:
            vacancy: Словарь с данными вакансии
            hh_id: Идентификатор работодателя на HeadHunter

        Raises:
            psycopg2.Error: При ошибках выполнения SQL-запросов
            ValueError: Если компания с указанным hh_id не найдена
        """
        try:
            # Находим company_id по hh_id
            self.cur.execute(
                """
                SELECT company_id FROM companies WHERE hh_id = %s;
                """,
                (hh_id,),
            )
            company_id = self.cur.fetchone()

            if not company_id:
                print(f"Компания с hh_id = {hh_id} не найдена.")
                return

            company_id = company_id[0]  # Получаем company_id

            # Вставляем вакансию в таблицу vacancies
            self.cur.execute(
                """
                INSERT INTO vacancies (company_id, title, salary_from, salary_to, url, description, published_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    company_id,
                    vacancy["name"],
                    vacancy["salary"]["from"] if vacancy["salary"] else None,
                    vacancy["salary"]["to"] if vacancy["salary"] else None,
                    vacancy["alternate_url"],
                    vacancy.get("description", None),
                    vacancy.get("published_at", None),
                ),
            )
            self.conn.commit()
            print(f"Вакансия {vacancy['name']} успешно добавлена.")
        except Exception as e:
            print(f"Ошибка при добавлении вакансии: {e}")
            self.conn.rollback()

    @staticmethod
    def add_vacancies_to_db(companies: List[Dict[str, Any]], db_manager: 'DBManager') -> None:
        """
        Добавляет вакансии для всех компаний в базу данных.

        Args:
            companies: Список компаний
            db_manager: Экземпляр DBManager

        Raises:
            Exception: При ошибках в процессе добавления данных
        """
        for company in companies:
            # Добавляем компанию в базу и получаем company_id
            company_id = db_manager.add_company(company)

            # Получаем вакансии для компании
            vacancies = get_vacancies(company["id"])

            # Добавляем вакансии в базу данных, передавая hh_id вместо company_id
            for vacancy in vacancies:
                db_manager.add_vacancy(vacancy, company["id"])

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список компаний с количеством их вакансий.

        Returns:
            Список кортежей (название компании, количество вакансий)
        """
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

    def get_all_vacancies(self) -> List[Tuple[str, str, Optional[int], Optional[int], str]]:
        """
        Получает список всех вакансий.

        Returns:
            Список кортежей с информацией о вакансиях:
            (название компании, название вакансии, зарплата от, зарплата до, URL)
        """
        try:
            self.cur.execute(
                """
                SELECT companies.name, vacancies.title, vacancies.salary_from, vacancies.salary_to, vacancies.url
                FROM vacancies
                JOIN companies ON vacancies.company_id = companies.company_id;
                """
            )
            result = self.cur.fetchall()
            print(f"Найдено {len(result)} вакансий.")
            return result
        except Exception as e:
            print(f"Ошибка при получении вакансий: {e}")
            return []

    def get_avg_salary(self) -> float:
        """
        Рассчитывает среднюю зарплату по всем вакансиям.

        Returns:
            Средняя зарплата (округленная)
        """
        try:
            self.cur.execute(
                """
                SELECT ROUND(AVG(COALESCE(salary_from, salary_to)))
                FROM vacancies
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
                """
            )
            return self.cur.fetchone()[0]
        except Exception as e:
            print(f"Ошибка при расчете средней зарплаты: {e}")
            return 0

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, Optional[int], Optional[int], str]]:
        """
        Получает вакансии с зарплатой выше средней.

        Returns:
            Список кортежей с информацией о вакансиях:
            (название вакансии, зарплата от, зарплата до, URL)
        """
        avg_salary = self.get_avg_salary()
        try:
            self.cur.execute(
                """
                SELECT title, salary_from, salary_to, url
                FROM vacancies
                WHERE (
                    (salary_from IS NOT NULL AND salary_from > %s) OR
                    (salary_to IS NOT NULL AND salary_to > %s)
                );
                """,
                (avg_salary, avg_salary),
            )
            return self.cur.fetchall()
        except Exception as e:
            print(f"Ошибка при получении вакансий с зарплатой выше средней: {e}")
            return []

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, Optional[int], Optional[int], str]]:
        """
        Ищет вакансии по ключевому слову в названии.

        Args:
            keyword: Ключевое слово для поиска

        Returns:
            Список кортежей с информацией о найденных вакансиях:
            (название вакансии, зарплата от, зарплата до, URL)
        """
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

    def close(self) -> None:
        """Закрывает соединение с базой данных."""
        try:
            self.cur.close()
            self.conn.close()
            print("Соединение с базой данных закрыто.")
        except Exception as e:
            print(f"Ошибка при закрытии соединения: {e}")