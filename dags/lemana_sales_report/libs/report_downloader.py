"""
Headless-скачивание коммерческого отчёта по продажам с портала
partner.lemanapro.ru (https://partner.lemanapro.ru/reports/commercial).

В отличие от ручного логина (см. refresh_session.py), download_report()
никогда не пытается залогиниться сама - портал блокирует автоматический
логин и в любой момент может показать капчу или доп. проверку (2FA),
которую способен пройти только человек, а Airflow-воркер работает без
экрана и без участия человека.

Вместо логина она переиспользует сессию (cookies + localStorage),
заранее сохранённую вручную через refresh_session.py в Airflow Variable
'lemana_portal_session_state'. Если сохранённая сессия недействительна
или истекла, бросается SessionExpiredError - в этом случае нужно вручную
запустить refresh_session.py и обновить Variable.
"""
import shutil
import tempfile
import zipfile
from pathlib import Path

from airflow.sdk import Variable
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from lemana_sales_report.libs.common import (
    REPORTS_URL,
    find_report_rows,
    is_login_page,
    pick_sales_report,
)

SESSION_STATE_VARIABLE = "lemana_portal_session_state"


class SessionExpiredError(RuntimeError):
    """Сохранённая сессия недействительна - нужен ручной re-login (refresh_session.py)."""


class DownloadError(RuntimeError):
    """Не удалось скачать отчёт по причине, не связанной с сессией (например, изменилась вёрстка)."""


def _finalize_download(raw_path: Path, dest_path: Path) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if raw_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(raw_path) as zf:
            names = zf.namelist()
            if not names:
                raise DownloadError(f"Архив {raw_path} пуст.")
            with zf.open(names[0]) as src, open(dest_path, "wb") as out:
                shutil.copyfileobj(src, out)
        return dest_path

    shutil.copyfile(raw_path, dest_path)
    return dest_path


def download_report(dest_path: str, index: int | None = None) -> Path:
    """Скачивает свежий отчёт headless-браузером, используя сохранённую в
    Airflow Variable сессию, и сохраняет его в dest_path (внутри dest_path
    всегда лежит уже распакованный файл, даже если портал отдал zip).

    Бросает SessionExpiredError, если сохранённая сессия недействительна,
    или DownloadError при прочих сбоях (изменилась вёрстка страницы,
    не найден отчёт по продажам и т.п.).
    """
    dest_path = Path(dest_path)

    state_json = Variable.get(SESSION_STATE_VARIABLE, default=None)
    if not state_json:
        raise SessionExpiredError(
            f"Airflow Variable '{SESSION_STATE_VARIABLE}' не задана. "
            "Запустите refresh_session.py и сохраните сессию."
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        state_path = Path(tmp_dir) / "storage_state.json"
        state_path.write_text(state_json, encoding="utf-8")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                storage_state=str(state_path),
                viewport={"width": 1400, "height": 900},
            )
            try:
                page = context.new_page()
                page.goto(REPORTS_URL, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_load_state("networkidle", timeout=30000)
                except PlaywrightTimeoutError:
                    pass
                page.wait_for_timeout(1500)

                if is_login_page(page):
                    raise SessionExpiredError(
                        "Портал перенаправил на страницу логина - сохранённая сессия "
                        "истекла или недействительна."
                    )

                try:
                    candidates = find_report_rows(page)
                except PlaywrightTimeoutError:
                    raise DownloadError("Страница со списком отчётов не загрузилась вовремя.")

                if not candidates:
                    raise DownloadError(
                        "На странице не найдено ни одной строки со ссылкой/кнопкой скачивания."
                    )

                if index is not None:
                    if not (0 <= index < len(candidates)):
                        raise DownloadError(
                            f"Некорректный index. Доступно строк: {len(candidates)} (0..{len(candidates) - 1})."
                        )
                    chosen = candidates[index]
                else:
                    chosen, sales_candidates = pick_sales_report(candidates)
                    if not sales_candidates:
                        listing = "\n".join(
                            f"  [{i}] {c['text'][:120]}" for i, c in enumerate(candidates)
                        )
                        raise DownloadError(
                            "Не удалось однозначно найти отчёт по продажам. Найденные строки:\n"
                            f"{listing}"
                        )

                with page.expect_download(timeout=60000) as download_info:
                    chosen["download_el"].click()
                download = download_info.value
                raw_path = Path(tmp_dir) / download.suggested_filename
                download.save_as(str(raw_path))

                return _finalize_download(raw_path, dest_path)
            finally:
                context.close()
                browser.close()
