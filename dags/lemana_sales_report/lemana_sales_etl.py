"""
ETL для будущего Airflow DAG-а: скачивает коммерческий отчёт по продажам с
портала partner.lemanapro.ru (используя download_commercial_report.py),
приводит его к табличному виду и загружает в retail.lemana_sales (Postgres).

Функции download_report() / transform() / load_to_db() рассчитаны на то,
чтобы их можно было вызывать как отдельные задачи DAG-а. load_to_db()
принимает уже открытое соединение psycopg2 - в реальном DAG-е его можно
получить, например, через PostgresHook(...).get_conn() и передать сюда,
не завязывая ETL-логику на конкретный способ хранения credentials.

Особенности отчёта, которые определяют логику загрузки:
  - Отчёт всегда содержит данные за последние 11 месяцев + текущий месяц
    на дату выгрузки (итого 12 месяцев).
  - Отсутствие строки артикул/магазин в отчёте означает, что по этой
    комбинации сток = 0, товар в пути = 0, продажи = 0 - отчёт не
    выгружает нулевые строки явно.
  - Данные за текущий месяц - это накопительный итог с начала месяца по
    дату выгрузки, то есть меняются при каждой новой ежедневной выгрузке.
  - Данные за истёкший месяц считаются окончательными только начиная с
    6-го числа следующего месяца; до этой даты они могут быть неполными.

  Из-за этого загрузка в БД всегда выполняется по принципу replace-by-month:
  перед вставкой удаляются все строки в БД за месяцы, присутствующие в
  свежем отчёте, и на их место вставляются свежие данные. Это делает
  загрузку идемпотентной (безопасно перезапускать) и не требует отдельной
  логики для "ещё не финализированного" месяца - при каждом запуске данные
  в БД просто приводятся в соответствие с последним состоянием отчёта.

Использование как самостоятельного скрипта:
    python lemana_sales_etl.py                  # скачать отчёт и загрузить в БД
    python lemana_sales_etl.py --csv path.csv    # загрузить уже скачанный файл
    python lemana_sales_etl.py --dry-run         # скачать/трансформировать без записи в БД

Подключение к БД задаётся переменными окружения LEMANA_DB_HOST,
LEMANA_DB_NAME, LEMANA_DB_USER, LEMANA_DB_PASSWORD, либо секцией
[database] в config.ini (по аналогии с логином от портала).
"""

import argparse
import configparser
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

import download_commercial_report as downloader

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.ini"

TABLE = "retail.lemana_sales"

# Соответствие столбцов исходного CSV столбцам таблицы БД - как в
# ручном процессе (Sell Out/Lemana.ipynb), которым эта загрузка заменяется.
COLUMN_MAPPING = {
    "Месяц": "month",
    "Поставщик": "supplier_id",
    "Имя поставщика": "supplier_name",
    "Артикул": "sku",
    "Имя артикула": "sku_name",
    "Гамма": "gamma",
    "Дата AVS": "avs_date",
    "Номер магазина": "store_id",
    "Магазин": "store_name",
    "Топ в магазине на дату выгрузки": "top_in_store",
    "Статус артикула в магазине на дату выгрузки": "sku_status_in_store",
    "Штрихкод": "barcode",
    "Сток на дату выгрузки  (шт.)": "stock",
    "В пути (шт.)": "transit",
    "Продажа (шт.)": "sales_qty",
    "Страна закупки": "purchase_country",
    "Закупочная цена": "purchase_price",
    "Дата обновления": "update_date",
}

DB_COLUMNS = list(COLUMN_MAPPING.values())


def download_report(index: int | None = None) -> Path:
    """Скачивает свежий отчёт с портала и возвращает путь к CSV-файлу."""
    return downloader.run_download(index=index)


def transform(csv_path: Path) -> pd.DataFrame:
    """Читает CSV отчёта и приводит его к виду, готовому для вставки в БД."""
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8", dtype={"Месяц": str})

    missing = set(COLUMN_MAPPING) - set(df.columns)
    if missing:
        raise ValueError(f"В отчёте отсутствуют ожидаемые столбцы: {sorted(missing)}")

    # "Месяц" приходит в формате "M.YYYY" (например "8.2026") - преобразуем
    # в дату первого числа месяца.
    df["Месяц"] = pd.to_datetime(df["Месяц"], format="%m.%Y").dt.date

    df = df.rename(columns=COLUMN_MAPPING)[DB_COLUMNS]
    df = df.replace({np.nan: None})
    return df


def get_db_connection():
    """Открывает соединение с БД по параметрам из переменных окружения
    или config.ini. Используется только для самостоятельного запуска
    скрипта - реальный DAG обычно будет передавать своё соединение
    напрямую в load_to_db()."""
    host = os.environ.get("LEMANA_DB_HOST")
    database = os.environ.get("LEMANA_DB_NAME")
    user = os.environ.get("LEMANA_DB_USER")
    password = os.environ.get("LEMANA_DB_PASSWORD")

    if not all([host, database, user, password]):
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH, encoding="utf-8")
        db_cfg = config["database"] if config.has_section("database") else {}
        host = host or db_cfg.get("host")
        database = database or db_cfg.get("database")
        user = user or db_cfg.get("user")
        password = password or db_cfg.get("password")

    if not all([host, database, user, password]):
        raise RuntimeError(
            "Не заданы параметры подключения к БД. Укажите переменные окружения "
            "LEMANA_DB_HOST / LEMANA_DB_NAME / LEMANA_DB_USER / LEMANA_DB_PASSWORD "
            f"или секцию [database] в {CONFIG_PATH}."
        )

    return psycopg2.connect(host=host, database=database, user=user, password=password)


def load_to_db(df: pd.DataFrame, conn) -> int:
    """Заменяет в БД данные за месяцы, присутствующие в df, на данные из df.

    Удаление и вставка выполняются в одной транзакции: если вставка
    упадёт, удалённые строки вернутся обратно (rollback)."""
    months = sorted(df["month"].dropna().unique().tolist())
    if not months:
        return 0

    data = df[DB_COLUMNS].values.tolist()

    insert_sql = f"INSERT INTO {TABLE} ({', '.join(DB_COLUMNS)}) VALUES %s"

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE month = ANY(%s)", (months,))
            execute_values(cur, insert_sql, data, page_size=5000)

    return len(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", type=Path, default=None,
                         help="Путь к уже скачанному CSV-файлу отчёта (пропускает скачивание с портала)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Скачать и трансформировать данные, но не записывать их в БД")
    args = parser.parse_args()

    if args.csv:
        csv_path = args.csv
    else:
        try:
            csv_path = download_report()
        except downloader.DownloadError as e:
            print(str(e))
            sys.exit(1)

    print(f"Файл отчёта: {csv_path}")

    df = transform(csv_path)
    months = sorted(str(m) for m in df["month"].dropna().unique())
    print(f"Строк после трансформации: {len(df)}, месяцев в отчёте: {months}")

    if args.dry_run:
        print("--dry-run: запись в БД пропущена.")
        return

    conn = get_db_connection()
    try:
        inserted = load_to_db(df, conn)
        print(f"Загружено строк в {TABLE}: {inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
