import requests
from typing import List, Dict

HH_API_URL = "https://api.hh.ru"


# Получение списка всех компаний
def get_companies() -> List[Dict]:
    url = f"{HH_API_URL}/employers"
    response = requests.get(url)

    if response.status_code != 200:
        print("Не удалось получить данные о компаниях")
        return []

    companies = response.json().get("items", [])

    if not companies:
        print("Список компаний пуст.")

    return companies


# Получение вакансий по id компании
def get_vacancies(employer_id: str) -> List[Dict]:
    url = f"{HH_API_URL}/vacancies"
    params = {
        "employer_id": employer_id,
        "per_page": 100,  # Получаем до 100 вакансий за один запрос
    }
    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"Не удалось получить вакансии для компании с id {employer_id}")
        return []

    vacancies = response.json().get("items", [])
    print(f"Полученные вакансии для компании {employer_id}: {vacancies}")  # Добавь отладочный вывод

    if not vacancies:
        print(f"Список вакансий для компании с id {employer_id} пуст.")

    return vacancies

# Получение информации о компании
def get_employer_info(employer_id: str) -> Dict:
    url = f"{HH_API_URL}/employers/{employer_id}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Не удалось получить информацию о компании с id {employer_id}")
        return {}

    return response.json()
