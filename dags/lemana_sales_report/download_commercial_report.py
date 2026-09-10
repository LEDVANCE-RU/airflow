"""
Скачивание последнего коммерческого отчёта по продажам с портала
партнёра Лемана ПРО (https://partner.lemanapro.ru/reports/commercial).

Подготовка перед первым запуском:
    1) pip install -r requirements.txt
    2) python -m playwright install chromium
    3) Запустить скрипт один раз - он создаст рядом файл config.ini,
       в который нужно вписать логин и пароль от портала.
    4) Запустить скрипт снова.

Логин выполняется автоматически (логин/пароль берутся из config.ini).
Браузер открывается видимым окном (не headless), т.к. портал блокирует
запросы от headless-браузеров. Если после автологина появится капча или
доп. проверка (2FA) - пройдите её вручную в открывшемся окне, скрипт
подождёт и продолжит сам.

Скрипт ищет в таблице отчётов на странице /reports/commercial строку,
относящуюся к отчёту по продажам ("Продажи" / "sales"), берёт самую
свежую (по дате в строке, либо самую верхнюю, если дат не нашлось) и
скачивает файл в подпапку "Raw data".

Если отчёт по продажам не удалось найти автоматически, скрипт выведет
список всех найденных строк с кнopкой/ссылкой скачивания и их номера -
нужный отчёт можно скачать вручную, указав его номер параметром --index.

Скачанный файл приходит в виде zip-архива - скрипт распаковывает его,
переименовывает содержимое в commreport_10011392_sales.<расширение> и
кладёт в "Raw data", перезаписывая файл с таким же именем, если он уже есть.
"""

import argparse
import configparser
import re
import shutil
import sys
import time
import zipfile
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_DIR = BASE_DIR / "Raw data"
PROFILE_DIR = BASE_DIR / "browser_profile"
CONFIG_PATH = BASE_DIR / "config.ini"
FINAL_REPORT_NAME = "commreport_10011392_sales"
REPORTS_URL = "https://partner.lemanapro.ru/reports/commercial"

SALES_PATTERN = re.compile(r"продаж|sales", re.IGNORECASE)
DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2}")


def load_credentials() -> tuple[str, str]:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(
            "[lemanapro]\nlogin = \npassword = \n", encoding="utf-8"
        )
        print(f"Создан файл настроек: {CONFIG_PATH}")
        print("Впишите туда логин и пароль от partner.lemanapro.ru и запустите скрипт снова.")
        sys.exit(0)

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH, encoding="utf-8")
    login = config.get("lemanapro", "login", fallback="").strip()
    password = config.get("lemanapro", "password", fallback="").strip()

    if not login or not password:
        print(f"В файле {CONFIG_PATH} не заполнены login и password. Заполните их и запустите скрипт снова.")
        sys.exit(1)

    return login, password


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


def is_login_page(page) -> bool:
    return page.query_selector("input[type='password']") is not None


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


def find_report_rows(page):
    page.wait_for_load_state("networkidle", timeout=30000)
    page.wait_for_timeout(2000)

    # Каждый отчёт на странице оформлен как карточка с классом вида
    # "_cardItem_<hash>", внутри которой лежит заголовок отчёта и кнопка
    # скачивания с классом вида "_downloadBtn_<hash>", появляющаяся в блоке
    # "_controls_<hash>" только после того, как сервер подготовит файл - до
    # этого момента там крутится "_loader_<hash>".
    deadline = time.time() + 90
    printed_waiting = False

    while True:
        cards = page.query_selector_all("[class*='cardItem' i]")

        candidates = []
        any_loading = False
        for card in cards:
            download_el = card.query_selector("button[class*='downloadBtn' i]")
            if not download_el:
                download_el = card.query_selector(
                    "[class*='controls' i] button, [class*='controls' i] a[href], "
                    "[class*='fileContainer' i] a[href]"
                )
            if not download_el:
                if card.query_selector("[class*='loader' i]"):
                    any_loading = True
                continue
            text = card.inner_text().strip()
            if not text:
                continue
            candidates.append({"row": card, "text": text, "download_el": download_el})

        if candidates:
            return candidates

        if not any_loading or time.time() >= deadline:
            break

        if not printed_waiting:
            print("Отчёт ещё готовится на сервере, ждём появления кнопки скачивания...")
            printed_waiting = True
        page.wait_for_timeout(3000)

    # Резервный вариант на случай изменения вёрстки: ищем любую кнопку/ссылку
    # со словом "Скачать"/"Download" и берём её ближайший видимый текстовый блок.
    candidates = []
    for el in page.query_selector_all("button, a[href]"):
        text = (el.inner_text() or "").strip()
        if re.search(r"скачать|download", text, re.I):
            candidates.append({"row": el, "text": text, "download_el": el})

    return candidates


def pick_sales_report(candidates):
    sales_candidates = [c for c in candidates if SALES_PATTERN.search(c["text"])]
    pool = sales_candidates if sales_candidates else candidates

    def sort_key(c):
        match = DATE_PATTERN.search(c["text"])
        return match.group(0) if match else ""

    pool_with_dates = [c for c in pool if DATE_PATTERN.search(c["text"])]
    if pool_with_dates:
        pool_with_dates.sort(key=sort_key, reverse=True)
        return pool_with_dates[0], sales_candidates

    return (pool[0] if pool else None), sales_candidates


def download_report(page, download_el) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with page.expect_download(timeout=60000) as download_info:
        download_el.click()
    download = download_info.value
    dest = RAW_DATA_DIR / download.suggested_filename
    download.save_as(str(dest))
    return dest


def finalize_download(dest: Path) -> Path:
    if dest.suffix.lower() == ".zip":
        with zipfile.ZipFile(dest) as zf:
            names = zf.namelist()
            if not names:
                raise ValueError(f"Архив {dest} пуст.")
            inner_name = names[0]
            suffix = Path(inner_name).suffix
            final_path = RAW_DATA_DIR / f"{FINAL_REPORT_NAME}{suffix}"
            with zf.open(inner_name) as src, open(final_path, "wb") as out:
                shutil.copyfileobj(src, out)
        dest.unlink()
        return final_path

    final_path = RAW_DATA_DIR / f"{FINAL_REPORT_NAME}{dest.suffix}"
    if final_path != dest:
        dest.replace(final_path)
    return final_path


class DownloadError(RuntimeError):
    """Не удалось скачать отчёт (проблема с авторизацией или вёрсткой страницы)."""


def run_download(index: int | None = None) -> Path:
    """Логинится на портале, скачивает свежий отчёт и возвращает путь к
    итоговому файлу в "Raw data". Бросает DownloadError, если что-то пошло
    не так (используется как логикой CLI, так и внешними скриптами/DAG-ами)."""
    login, password = load_credentials()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            viewport={"width": 1400, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(REPORTS_URL, wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                pass
            page.wait_for_timeout(1500)

            if is_login_page(page):
                perform_login(page, login, password)
                if not wait_for_authenticated(page):
                    raise DownloadError(
                        "Не удалось дождаться завершения авторизации. Проверьте окно браузера и попробуйте снова."
                    )

            try:
                candidates = find_report_rows(page)
            except PlaywrightTimeoutError:
                raise DownloadError("Страница со списком отчётов не загрузилась вовремя.")

            if not candidates:
                raise DownloadError("На странице не найдено ни одной строки со ссылкой/кнопкой скачивания.")

            if index is not None:
                if not (0 <= index < len(candidates)):
                    raise DownloadError(f"Некорректный --index. Доступно строк: {len(candidates)} (0..{len(candidates) - 1}).")
                chosen = candidates[index]
            else:
                chosen, sales_candidates = pick_sales_report(candidates)
                if not sales_candidates:
                    listing = "\n".join(f"  [{i}] {c['text'][:120]}" for i, c in enumerate(candidates))
                    raise DownloadError(
                        "Не удалось однозначно найти отчёт по продажам. Найденные строки с возможностью скачивания:\n"
                        f"{listing}\n"
                        "Запустите скрипт снова с параметром --index N, указав номер нужной строки."
                    )

            print(f"Скачиваем: {chosen['text'][:120]}")
            dest = download_report(page, chosen["download_el"])
            return finalize_download(dest)
        finally:
            context.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--index", type=int, default=None,
        help="Номер строки отчёта для скачивания вручную (см. список при неудачном автоопределении)",
    )
    args = parser.parse_args()

    try:
        final_path = run_download(index=args.index)
    except DownloadError as e:
        print(str(e))
        sys.exit(1)

    print(f"Готово. Файл сохранён: {final_path}")


if __name__ == "__main__":
    main()
