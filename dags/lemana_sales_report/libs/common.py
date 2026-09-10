"""
Общие для headless-скачивания (report_downloader.py) и интерактивного
логина (login_flow.py) вещи: константы портала и разбор карточек отчётов
на странице /reports/commercial.
"""
import re
import time

REPORTS_URL = "https://partner.lemanapro.ru/reports/commercial"
FINAL_REPORT_NAME = "commreport_10011392_sales"

SALES_PATTERN = re.compile(r"продаж|sales", re.IGNORECASE)
DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2}")


def is_login_page(page) -> bool:
    return page.query_selector("input[type='password']") is not None


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
