from flask import Flask, render_template, request, redirect, session, jsonify
from flask_httpauth import HTTPBasicAuth
import requests
import json
from time import sleep
import zipfile
import csv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
import hashlib
import os


app = Flask(__name__)
auth = HTTPBasicAuth() #На данный момент используется Basic-авторизация

users = {"login": "password"}

connection = psycopg2.connect(user='user', password='password', host='192.168.0.1', port='5432', database='db_name')
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) #заметка для себя: никому не рекомендовать автокоммит
cursor = connection.cursor()

@auth.verify_password
def verify_password(username, password):
    if username in users and users[username] == password:
        return username
    return None

# Метод для наполнения БД информацией о звонках
@app.route('/data/prepare', methods=['GET', 'POST'])
@auth.login_required
def submit_form():
    data = request.json
    date_start = data.get('date_start')
    date_end = data.get('date_end')
    direction = data.get('direction') # направление звонка (входящий/исходящий)
    state = data.get('state') # статус звона (ответ/сброс/пропущен)
    if request.method == 'POST':
        url = "https://api.cloudpbx.rt.ru/domain_call_history" # Шаг 1: выгружаем массив звонков от date_start до date_end
        payload = json.dumps({
            "date_start": date_start,
            "date_end": date_end,
            "direction": direction,
            "state": state
        })
        xci = 'xci' # получаем у Ростелекома
        xck = 'xck' # получаем у Ростелекома

        combinate = xci + payload + xck
        signature_256 = hashlib.sha256(combinate.encode('cp1251')).hexdigest()
        headers = {
            'X-Client-ID': 'x-cl-id', # получаем у Ростелекома
            'X-Client-Sign': signature_256,
            'Content-Type': 'application/json',
            'Cookie': 'session-cookie' # заглушка
        }
        response = requests.request("POST", url, headers=headers, data=payload, verify=False) # verify=False - временное решение, в проде лучше использовать сертификат
        data_call_history = json.loads(response.text)
        order_id = data_call_history['order_id']

        cursor.execute(f"insert into db_name (order_id) values ('{order_id}')")

        cursor.execute("select nn_id, o_id from db_name where date_processed is null order by nn_id limit 1")
        pg_order_id = cursor.fetchone()

        sleep(300) # именно sleep и именно 300 - такое значение получено методом проб, РТ формирует запись примерно 5 минут


        url2 = "https://api.cloudpbx.rt.ru/download_call_history" #получаем заветный .zip (!!!) с .csv файлом внутри
        payload2 = json.dumps({"order_id": f"{pg_order_id[1]}"})
        combinate2 = xci + payload2 + xck
        signature_256_2 = hashlib.sha256(combinate2.encode('cp1251')).hexdigest()
        headers_2 = {
            'X-Client-ID': 'x-cl-id',
            'X-Client-Sign': signature_256_2,
            'Content-Type': 'application/json',
            'Cookie': 'session-cookie'
        }
        response = requests.request("POST", url2, headers=headers_2, data=payload2, verify=False)




        cursor.execute(f"update db_name set date_processed='{datetime.now()}' where nn_id = {pg_order_id[0]}")
        sleep(10)
        with open("call_history.zip", "wb") as f:
            f.write(response.content)
            zip_file_path = 'call_history.zip'

        sleep(100) # с этим значением можно поиграться, но только при стабильном интернете

        # Открываем zip-архив
        with zipfile.ZipFile('call_history.zip', 'r') as z:
            with z.open(z.namelist()[0]) as csvfile:
                content = csvfile.read().decode('utf-8')
                # Создаем CSV reader
                reader = csv.reader(content.splitlines(), delimiter=';')

                # Перебираем строки и выводим первый столбец
                for row in reader:
                    if row:  # проверка на пустую строку
                        indices = range(0, len(row), 16)
                        indices2 = range(4, len(row), 20)
                        indices3 = range(6, len(row), 22)
                        indices4 = range(9, len(row), 25)
                        indices5 = range(10, len(row), 26)
                        indices6 = range(2, len(row), 18)
                        session_ids = [row[i] for i in indices if i < len(row)]
                        orig_nums = [row[i] for i in indices2 if i < len(row)]
                        dest_nums = [row[i] for i in indices3 if i < len(row)]
                        call_times = [row[i] for i in indices4 if i < len(row)]
                        durations = [row[i] for i in indices5 if i < len(row)]
                        directions = [row[i] for i in indices6 if i < len(row)]
                        cursor.execute(f"insert into db_name (s_id, ph_from, ph_to, st_call_date, duration, direction) values ('{session_ids[0]}', '{orig_nums[0]}', '{dest_nums[0]}', '{call_times[0]}', {durations[0]}, {directions[0]})")




@app.route('/data/receive', methods=['GET', 'POST']) #метод для выгрузки ссылок на аудио
def getlink():
    xci = 'xci'
    xck = 'xck'
    cursor.execute(
        f"select nn_id, s_id, ph_from, ph_to, dt_rec, st_call_date, duration, direction, ans_suri from db_name where date_processed is null order by nn_id limit 1")
    pg_n_id = cursor.fetchone()
    url3 = "https://api.cloudpbx.rt.ru/get_record" # получаем ссылку на запись
    payload3 = json.dumps({
        "s_id": f"{pg_n_id[1]}", "ip_adress": "195.49.187.78" # ОБЯЗАТЕЛЬНО указываем ip по которому необходимо загрузить ссылку
    })
    combinate3 = xci + payload3 + xck
    signature_256_3 = hashlib.sha256(combinate3.encode('cp1251')).hexdigest()
    headers_3 = {
        'X-Client-ID': 'x-cl-id',
        'X-Client-Sign': signature_256_3,
        'Content-Type': 'application/json',
        'Cookie': 'session-cookie'
    }
    response = requests.request("POST", url3, headers=headers_3, data=payload3, verify=False)
    data_record = json.loads(response.text)
    record_link = data_record.get('url')

    cursor.execute(f"update db_name set dwnld_link='{record_link}', date_processed='{datetime.now()}' where nn_id = {pg_n_id[0]}")
    return jsonify({"nn_id": pg_n_id[0], "s_id": pg_n_id[1],"fr":pg_n_id[2], "to":pg_n_id[3], "dwnld_link":record_link, "st_call_date": pg_n_id[5], "ans_suri": pg_n_id[8]})






if __name__ == '__main__':
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', 'port'))
    except ValueError:
        PORT = 1234
    app.run(host='0.0.0.0', threaded=True, debug=True) # не забываем отключать дебаггер


# P.S. не забывать закрывать соединение с БД