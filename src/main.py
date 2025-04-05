from src.models import create_tables
from src.database import insert_data
from src.db_manager import DBManager

employers = [
    "1740",
    "3529",
    "78638",
    "3776",
    "1122462",
    "9498115",
    "2180",
    "3127",
    "67611",
    "87021",
]

if __name__ == "__main__":
    create_tables()
    insert_data(employers)

    db = DBManager()
    print(db.get_companies_and_vacancies_count())
    print(db.get_all_vacancies())
    print(db.get_avg_salary())
    print(db.get_vacancies_with_higher_salary())
    print(db.get_vacancies_with_keyword("Python"))
    db.close()
