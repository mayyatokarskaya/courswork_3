from api import get_companies, get_vacancies
from db_manager import DBManager


def setup_database():
    """Создание таблиц и заполнение БД данными из API"""
    db_manager = DBManager()
    db_manager.create_tables()

    companies = get_companies()
    for company in companies:
        db_manager.add_company(company)
        vacancies = get_vacancies(company["id"])

        if vacancies:
            for vacancy in vacancies:
                db_manager.add_vacancy(vacancy, company["id"])
        else:
            print(f"Список вакансий для компании с id {company['id']} пуст.")

    db_manager.close()


def run_menu():
    """Консольное меню для взаимодействия с пользователем"""
    db_manager = DBManager()

    while True:
        print("\nВыберите действие:")
        print("1. Получить список компаний и количество вакансий")
        print("2. Получить список всех вакансий")
        print("3. Получить среднюю зарплату")
        print("4. Показать вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("6. Выйти")

        choice = input("Введите номер действия: ")

        if choice == "1":
            result = db_manager.get_companies_and_vacancies_count()
            for company, count in result:
                print(f"{company}: {count} вакансий")
        elif choice == "2":
            result = db_manager.get_all_vacancies()
            for row in result:
                print(row)
        elif choice == "3":
            avg = db_manager.get_avg_salary()
            print(f"Средняя зарплата: {avg}")
        elif choice == "4":
            result = db_manager.get_vacancies_with_higher_salary()
            for row in result:
                print(row)
        elif choice == "5":
            keyword = input("Введите ключевое слово: ")
            result = db_manager.get_vacancies_with_keyword(keyword)
            for row in result:
                print(row)
        elif choice == "6":
            print("Выход из программы.")
            db_manager.close()
            break
        else:
            print("Неверный ввод. Попробуйте снова.")


if __name__ == "__main__":

    print("Добро пожаловать в систему вакансий!\n")
    while True:
        print("Что вы хотите сделать?")
        print("1. Создать таблицы и загрузить данные")
        print("2. Открыть меню работы с данными")
        print("3. Выйти")

        user_choice = input("Введите номер действия: ")
        if user_choice == "1":
            setup_database()
        elif user_choice == "2":
            run_menu()
        elif user_choice == "3":
            print("До свидания!")
            break
        else:
            print("Неверный ввод. Попробуйте снова.")
