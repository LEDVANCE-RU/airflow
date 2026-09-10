"""
Интерактивный логин на partner.lemanapro.ru в видимом окне браузера.

Используется только refresh_session.py (запускается вручную человеком),
а не headless-скачиванием в DAG - портал может показать капчу/2FA,
которую способен пройти только человек, наблюдающий за окном браузера.
"""
import re
import time

from lemana_sales_report.libs.common import REPORTS_URL, is_login_page


def first_matching(page, selectors):
    for selector in selectors:
        element = page.query_selector(selector)
        if element:
            return element
    return None


def perform_login(page, login: str, password: str) -> None:
    print("Открылась страница авторизации, пробуем войти автоматически...")
    page.wait_for_load_state("networkidle", timeout=30000)

    login_selectors = [
        "input[name='login']", "input[name='username']", "input[name='email']",
        "input[type='email']", "input[id*='login' i]", "input[id*='email' i]",
        "input[type='text']",
    ]
    password_selectors = ["input[name='password']", "input[type='password']"]

    login_input = first_matching(page, login_selectors)
    password_input = first_matching(page, password_selectors)

    if not login_input or not password_input:
        print("Не удалось автоматически найти поля логина/пароля.")
        print("Войдите вручную в открывшемся окне браузера, затем вернитесь в консоль.")
        input("После входа в личный кабинет нажмите Enter здесь, чтобы продолжить...")
        return

    login_input.fill(login)
    password_input.fill(password)

    submit = page.query_selector("button[type='submit']")
    if not submit:
        try:
            submit = page.get_by_role("button", name=re.compile("вход|войти|log ?in|sign in", re.I)).first
        except Exception:
            submit = None

    if submit:
        submit.click()
    else:
        password_input.press("Enter")


def wait_for_authenticated(page) -> bool:
    print("Ожидаем завершения авторизации (при появлении капчи/доп. проверки пройдите её вручную в окне браузера)...")
    deadline = time.time() + 300
    while time.time() < deadline:
        if not is_login_page(page):
            if "/reports/commercial" not in page.url:
                page.goto(REPORTS_URL, wait_until="domcontentloaded", timeout=60000)
            # Портал иногда на мгновение убирает форму входа и тут же
            # откатывается обратно на страницу логина - убеждаемся, что
            # форма логина не появляется вновь некоторое время, прежде чем
            # считать авторизацию завершённой.
            page.wait_for_timeout(3000)
            if not is_login_page(page):
                return True
        time.sleep(2)
    return False
