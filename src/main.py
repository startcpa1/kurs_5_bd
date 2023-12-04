import json

import psycopg2

from src.client_api import HeadHunterApi
from src.config import config
from src.db_manager import DBManager

params = config()
db_name = 'vacancies'

connection = psycopg2.connect(**params)
cursor = connection.cursor()
connection.autocommit = True

cursor.execute(f'DROP DATABASE IF EXISTS {db_name}')
cursor.execute(f'CREATE DATABASE {db_name}')
connection.commit()
cursor.close()
connection.close()
params.update(database=db_name)

db = DBManager(**params)
db.create_companies_table()
db.create_vacancies_table()

"""Список id заранее выбраных компаний"""

companies = ['1910225', '9395611', '2324020', '1133701']

"""Для каждого id получаем 20 (по умолчанию) открытых вакансий  и добавляем их в таблицу"""

for emp_id in companies:
    hh_vacancies = HeadHunterApi(emp_id)
    data = hh_vacancies.get_vacancies()
    hh_vacancies.save_to_json(data)
    id_list = []  # Список для проверки уникальности id компаний
    with open(hh_vacancies.file_to_save, encoding='utf8') as f:
        data = json.load(f)
        for dict_ in data:
            """Выполняется проверка на значения salary.
            Если данные не указаны работодателем - значения приравниваются к 0
            для возможности дальнейшего сравнения"""
            if not dict_['salary']:
                salary_from = 0
                salary_to = 0
                currency = 'нет'
            else:
                salary_from = dict_['salary']['from'] if dict_['salary']['from'] else 0
                salary_to = dict_['salary']['to'] if dict_['salary']['to'] else 0
                currency = dict_['salary']['currency']

            company_id = dict_['company_id']

            """Создаем временные списки для добавления данных в таблицы"""
            if company_id not in id_list:
                temp_company_list = [company_id, dict_['company']]
                db.fill_companies_table(temp_company_list)
                id_list.append(company_id)

            temp_vacancy_list = [company_id, dict_['employee'], dict_['city'], salary_from, salary_to, currency,
                                 dict_['url'], dict_['requirement']]
            db.fill_vacancies_table(temp_vacancy_list)


def data_base_usage(db_object):
    """Функция для выполнения запросов к базе данных
    В качестве аргумента получает объект класса DBManager"""

    while True:
        print()
        print("Выберите действие:")
        print("1 - Получить список всех компаний и количество вакансий у каждой компании")
        print("2 - Получить список всех вакансий")
        print("3 - Получить среднюю зарплату по вакансиям")
        print("4 - Получить список вакансий с зарплатой выше средней по всем вакансиям")
        print("5 - Получить вакансии по ключевому слову")
        print("0 - Завершение работы")

        answer = input()
        if answer == '0':
            print('Работа программы завершена')
            break
        elif answer == '1':
            db_object.get_companies_and_vacancies_count()
        elif answer == '2':
            db_object.get_all_vacancies()
        elif answer == '3':
            db_object.get_avg_salary()
        elif answer == '4':
            db_object.get_vacancies_with_higher_salary()
        elif answer == '5':
            keyword = input('Введите ключевое слово\n')
            db_object.get_vacancies_with_keyword(keyword)
        else:
            print('Некорректный ввод')


data_base_usage(db)
db.connection.close()
