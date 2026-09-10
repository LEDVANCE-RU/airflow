"""
Ручной (не по расписанию, не из Airflow-воркера) логин на
partner.lemanapro.ru в видимом окне браузера и сохранение сессии
(cookies + localStorage) в Airflow Variable 'lemana_portal_session_state'
для последующего headless-скачивания отчётов в DAG lemana_sales_report.

Портал блокирует headless-браузеры и логин "в лоб": после ввода
логина/пароля может показать капчу или доп. проверку (2FA), которую
способен пройти только человек, наблюдающий за окном браузера. Поэтому
этот скрипт нужно запускать вручную с машины с графическим окружением
каждый раз, когда сессия истекает (download_task в DAG падает с
SessionExpiredError).

Подготовка перед первым запуском:
    pip install -r requirements.txt
    python -m playwright install chromium

Запуск:
    python refresh_session.py

Логин и пароль от портала берутся из Airflow connection 'lemanapro_portal'
(нужен доступ к той же БД Airflow, что и у воркеров). Если доступа нет,
их можно передать явно:
    python refresh_session.py --login T2810443 --password '...'

По завершении скрипт пробует сохранить сессию прямо в Airflow Variable
через `airflow variables set` (если команда airflow доступна локально).
Если нет - печатает эту команду и путь к файлу с сессией, чтобы
выполнить сохранение вручную (или вставить значение в Airflow UI ->
Admin -> Variables).
"""
import argparse
import getpass
import subprocess
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

from lemana_sales_report.libs.common import REPORTS_URL, is_login_page
from lemana_sales_report.libs.login_flow import perform_login, wait_for_authenticated

SESSION_STATE_VARIABLE = "lemana_portal_session_state"
STATE_OUT_PATH = Path(__file__).resolve().parent / "storage_state.json"


def get_credentials(args) -> tuple[str, str]:
    if args.login and args.password:
        return args.login, args.password

    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(args.conn_id)
        if conn.login and conn.password:
            return conn.login, conn.password
    except Exception as e:
        print(f"Не удалось получить connection '{args.conn_id}' из Airflow: {e}")

    login = args.login or input("Логин на partner.lemanapro.ru: ")
    password = args.password or getpass.getpass("Пароль: ")
    return login, password


def save_to_airflow(state_json: str) -> bool:
    try:
        subprocess.run(
            ["airflow", "variables", "set", SESSION_STATE_VARIABLE, state_json],
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except Exception as e:
        print(f"Не удалось выполнить `airflow variables set` автоматически: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--conn-id", default="lemanapro_portal",
                         help="Airflow connection с логином/паролем от портала")
    parser.add_argument("--login", default=None)
    parser.add_argument("--password", default=None)
    args = parser.parse_args()

    login, password = get_credentials(args)

    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(Path(__file__).resolve().parent / "browser_profile"),
            headless=False,
            viewport={"width": 1400, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            page = browser.pages[0] if browser.pages else browser.new_page()
            page.goto(REPORTS_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(1500)

            if is_login_page(page):
                perform_login(page, login, password)
                if not wait_for_authenticated(page):
                    print("Не удалось дождаться завершения авторизации. Попробуйте снова.")
                    sys.exit(1)
            else:
                print("Уже авторизованы (сохранённый профиль браузера ещё валиден).")

            browser.storage_state(path=str(STATE_OUT_PATH))
            print(f"Сессия сохранена в {STATE_OUT_PATH}")
        finally:
            browser.close()

    state_json = STATE_OUT_PATH.read_text(encoding="utf-8")
    if save_to_airflow(state_json):
        print(f"Airflow Variable '{SESSION_STATE_VARIABLE}' обновлена.")
    else:
        print("Сохраните сессию в Airflow вручную одним из способов:")
        print(f"  airflow variables set {SESSION_STATE_VARIABLE} \"$(cat {STATE_OUT_PATH})\"")
        print(f"  или вставьте содержимое {STATE_OUT_PATH} в Admin -> Variables -> {SESSION_STATE_VARIABLE} в Airflow UI.")


if __name__ == "__main__":
    main()
