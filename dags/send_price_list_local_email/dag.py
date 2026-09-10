"""
Следит за папкой прайс-листов в SharePoint:
  https://spledvance.lancloud.ru/Shared%20Documents/.../MSK-files/TRADE/Price%20List

Сотрудник при выходе нового прайс-листа создаёт там новую подпапку с именем
вида "2026.08.17_price_list_". Внутри неё лежит файл, в названии которого
есть "LOCAL" (например "2026.08.17 Прайс-лист LOCAL.xlsx").

DAG периодически проверяет список подпапок, сравнивает имя самой свежей
(по дате в начале имени) с именем, обработанным в прошлый раз (хранится в
Airflow Variable 'price_list_last_folder'). Если появилась более новая
папка - скачивает из неё файл с "LOCAL" в названии и отправляет по почте.

Требуемые Airflow Variables:
  - test_emails                - JSON вида {"to": ["a@b.com"], "cc": []} (уже используется другими DAG-ами)
  - tmp_dir_path                - папка для временных файлов (уже используется другими DAG-ами)
  - price_list_sharepoint_path  - путь до папки "Price List" относительно корня
                                   WebDAV-подключения 'webdav_sharepoint_root'
                                   (по умолчанию 'MSK-files/TRADE/Price List' -
                                   поправить, если корень подключения не совпадает
                                   с "Shared Documents")
  - price_list_last_folder      - служебная переменная, DAG сам её создаёт/обновляет,
                                   вручную заводить не обязательно

Требуемые Airflow connections (уже используются другими DAG-ами):
  - webdav_sharepoint_root (webdav) - тот же, что в copy_invoices_ftp_to_sharepoint
  - smtp_sys_tech (smtp) - тот же, что в send_stock_report / notify_about_retired_stock
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import task, Variable

from constants import TZ_MSK
from hooks.webdav import WebDAVHook
from send_price_list_local_email.libs.sender import send_price_list_email
from send_price_list_local_email.libs.sharepoint import find_latest_date_folder

DAG_ID = "send_price_list_local_email"
SCHEDULE = '0 10 * * 1-5'  # каждый рабочий день в 10:00 МСК

WEBDAV_CONN_ID = 'webdav_sharepoint_root'
DEFAULT_SHAREPOINT_PATH = 'MSK-files/TRADE/Price List'

LAST_FOLDER_VARIABLE = 'price_list_last_folder'


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 9, 1, tzinfo=TZ_MSK),
    schedule=SCHEDULE,
    catchup=False,
    tags=['price_list', 'sharepoint', 'email'],
) as dag:

    @task
    def find_latest_folder_task() -> dict:
        base_path = Variable.get('price_list_sharepoint_path', default=DEFAULT_SHAREPOINT_PATH)
        client = WebDAVHook(WEBDAV_CONN_ID).get_conn()
        return find_latest_date_folder(client, base_path)

    @task.short_circuit
    def check_new_folder_task(folder_info: dict) -> bool:
        last_folder = Variable.get(LAST_FOLDER_VARIABLE, default=None)
        if folder_info['folder_name'] == last_folder:
            logging.info("Новых папок нет, последняя обработанная - '%s'.", last_folder)
            return False
        logging.info("Найдена новая папка: '%s' (была: '%s').", folder_info['folder_name'], last_folder)
        return True

    @task
    def send_local_file_task(folder_info: dict):
        recipients = Variable.get("test_emails", default=None, deserialize_json=True)
        if not recipients or not isinstance(recipients, dict) or not recipients.get('to'):
            raise AirflowException("Airflow Variable 'test_emails' is not set or invalid.")

        client = WebDAVHook(WEBDAV_CONN_ID).get_conn()
        tmp_dir = Variable.get('tmp_dir_path')

        send_price_list_email(client, folder_info, recipients, tmp_dir)
        Variable.set(LAST_FOLDER_VARIABLE, folder_info['folder_name'])

    folder = find_latest_folder_task()
    is_new = check_new_folder_task(folder)
    is_new >> send_local_file_task(folder)
