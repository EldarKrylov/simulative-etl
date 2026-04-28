import psycopg2
import requests
from datetime import datetime, timedelta
import ast
import os
import logging

# URL api для подключения
api_url = "https://b2b.itresume.ru/api/statistics"

# Параметры подключения
params = {
    'client': 'Skillfactory',
    'client_key': 'M2MGWS',
    'start': '2023-04-01 12:46:47.860798',
    'end': '2023-04-04 12:46:47.860798'
}

# Данные для подключения в БД
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "grade_statistics",
    "user": "eldarkrylov",
    "password": "Citysl@m1392"
}

# Функция задающая параметры и путь для файла с логами
def log_config_prepare():
    directory = "logs"
    os.makedirs(directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    log_path = os.path.join(directory, log_filename)

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return directory

# Функция очистки файлов с логами старше 3 дней
def cleanup_logs():
    directory = log_config_prepare()
    now = datetime.now()

    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)

        if os.path.isfile(file_path):
            try:
                file_date = datetime.strptime(file.replace(".log", ""), "%Y-%m-%d")
                if now - file_date > timedelta(days=3):
                    os.remove(file_path)
            except:
                continue

# Функция получения данных через API
def get_data_from_api(api_url, params):
  try:
      cleanup_logs()
      response = requests.get(api_url, params=params)
      response.raise_for_status()

      logging.info(f"ETL: этап извлечения данных из API: {api_url} завершен.")
      print('='*100)
      print(f"ETL: этап извлечения данных из API: {api_url} завершен.")
      print('='*100)
      return response.json()

  except requests.exceptions.HTTPError as err:
      logging.error(f"HTTP-ошибка: {err} | Код ответа сервера={response.status_code}")
      print('='*100)
      print(f"HTTP-ошибка: {err} | Код ответа сервера={response.status_code}")
      print('='*100)

  except requests.exceptions.RequestException as err:
      logging.error(f"Ошибка при запросе к API: {err}")
      print('='*100)
      print(f"Ошибка при запросе к API: {err}")
      print('='*100)

  return []

# Функция обработки данных и получения списка кортежей
def get_grade_statistics():
    data = get_data_from_api(api_url, params)

    result = []

    for d in data:
        try:
            user_id = d.get('lti_user_id')
            is_correct = d.get('is_correct')
            attempt_type = d.get('attempt_type')
            created_at = datetime.strptime(d.get('created_at'), '%Y-%m-%d %H:%M:%S.%f')

            if d.get('passback_params'):
                value = ast.literal_eval(d['passback_params'])

                oauth_consumer_key = value.get('oauth_consumer_key')
                lis_result_sourcedid = value.get('lis_result_sourcedid')
                lis_outcome_service_url = value.get('lis_outcome_service_url', None)

                if lis_outcome_service_url:
                    result.append((user_id,
                                   oauth_consumer_key,
                                   lis_result_sourcedid,
                                   lis_outcome_service_url,
                                   is_correct,
                                   attempt_type, created_at))

        except Exception as e:
            logging.error(f"ETL: этап обработки данных: ошибка {e}")
            print(f"ETL: этап обработки данных: ошибка {e}")
            print('='*100)
            break

    logging.info(f"ETL: этап обработки данных завершен: количество обработанных записей {len(result)}.")
    print(f"ETL: этап обработки данных завершен: количество обработанных записей {len(result)}.")

    return result

# Функция подключения к БД и загрузки данных
def download_data_into_db():
    data = get_grade_statistics()
    conn = psycopg2.connect(
            host = db_config.get('host'),
            port = db_config.get('port'),
            database = db_config.get('database'),
            user = db_config.get('user'),
            password = db_config.get('password'),
        )

    table_name = 'student_statistics'
    columns_name = ['user_id',
                    'oauth_consumer_key',
                    'lis_result_sourcedid',
                    'lis_outcome_service_url',
                    'is_correct',
                    'attempt_type',
                    'created_at']

    columns_insert = ', '.join([f"{col}" for col in columns_name])
    insert_values = ', '.join(['%s'] * len(columns_name))

    query_table_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        user_id VARCHAR(250),
        oauth_consumer_key VARCHAR(250),
        lis_result_sourcedid VARCHAR(250),
        lis_outcome_service_url VARCHAR(250),
        is_correct VARCHAR(10),
        attempt_type VARCHAR(10),
        created_at TIMESTAMP
    )"""

    query_data_insert = f"INSERT INTO {table_name} ({columns_insert}) VALUES ({insert_values})"

    try:
        # Создадим таблицу, если ее нет
        cursor = conn.cursor()
        cursor.execute(query_table_create)
        conn.commit()

        # Вставим данные
        cursor.executemany(query_data_insert, data)
        conn.commit()

        logging.info(f"ETL: этап загрузки данных в DWH завершен: загружено {len(data)} строк")
        print('='*100)
        print(f"ETL: этап загрузки данных в DWH завершен: загружено {len(data)} строк")

    except Exception as e:
        logging.error(f"ETL: этап загрузки данных в DWH: ошибка {e}")
        print('='*100)
        print(f"ETL: этап загрузки данных в DWH: ошибка {e}")

    # Закроем подключения
    finally:
        cursor.close()
        conn.close()
        logging.info(f"ETL: этап загрузки данных в DWH: соединение закрыто")
        print('='*100)
        print(f"ETL: этап загрузки данных в DWH: соединение закрыто")

if __name__ == "__main__":
    download_data_into_db()
    logging.info("ETL-процесс выполнен успешно")
    print('='*100)
    print(f"ETL-процесс выполнен успешно")