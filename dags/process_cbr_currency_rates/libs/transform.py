import csv
import json
import os
import uuid
import logging
import pandas as pd
import requests
import xmltodict
from datetime import datetime
from process_cbr_currency_rates.libs.mapping import CbrFieldsMap


def fetch_cbr_xml_daily() -> dict:
    url = 'http://www.cbr.ru/scripts/XML_daily.asp'
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return xmltodict.parse(response.content)


def parse_rates(doc: dict, currency_list: set[str]) -> pd.DataFrame:
    all_rates = doc['ValCurs']['Valute']
    date_str = doc['ValCurs']['@Date']
    columns = CbrFieldsMap.dest_columns()
    out_rows = []
    wanted = set(currency_list)
    if {'RUB', 'RUR'} & wanted:
        out_rows.append({columns[0]: 'RUB', columns[1]: 1.0, columns[2]: date_str})
        wanted.discard('RUB')
        wanted.discard('RUR')
    for rate in all_rates:
        code = rate['CharCode']
        if code in wanted:
            value = float(rate['Value'].replace(',', '.'))
            nominal = float(rate['Nominal'])
            out_rows.append({columns[0]: code, columns[1]: value / nominal, columns[2]: date_str})
    df = pd.DataFrame(out_rows, columns=columns)
    if not df.empty:
        df[columns[2]] = pd.to_datetime(df[columns[2]], format='%d.%m.%Y').dt.date
        df[columns[1]] = df[columns[1]].astype('Float64')
    return df


def transform_cbr_rates(out_dp: str) -> str:
    os.makedirs(out_dp, exist_ok=True)
    doc = fetch_cbr_xml_daily()
    df = parse_rates(doc, {'EUR', 'USD', 'CNY', 'RUB'})
    export_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}_cbr_rates.csv")
    df.to_csv(export_fp,
              index=False,
              encoding='utf-8',
              sep=',',
              quotechar='"',
              quoting=csv.QUOTE_MINIMAL,
              columns=CbrFieldsMap.dest_columns())
    logging.info(f"Transformed CBR rates to {export_fp}")
    return json.dumps({'cbr_rates': export_fp})

