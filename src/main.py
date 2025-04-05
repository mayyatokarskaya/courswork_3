from db_manager import DBManager
from api import get_companies, get_vacancies


def main():
    # Создание базы данных и таблиц
    db_manager = DBManager()
    db_manager.create_tables()

    # Заполнение таблиц данными
    companies = get_companies()
    for company in companies:
        db_manager.add_company(company)  # Добавление компании
        vacancies = get_vacancies(company["id"])

        if vacancies:
            for vacancy in vacancies:
                db_manager.add_vacancy(
                    vacancy, company["id"]
                )  # Передача company_id для каждой вакансии
        else:
            print(f"Список вакансий для компании с id {company['id']} пуст.")

    # Пример работы с данными
    print(db_manager.get_companies_and_vacancies_count())
    print(db_manager.get_all_vacancies())
    print(db_manager.get_avg_salary())
    print(db_manager.get_vacancies_with_higher_salary())
    print(db_manager.get_vacancies_with_keyword("Python"))

    db_manager.close()


if __name__ == "__main__":
    main()
