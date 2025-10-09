import csv
import json
import os
import uuid
import logging
import pandas as pd
import requests
import bs4
from process_cbr_currency_rates.libs.mapping import CbrFieldsMap


def fetch_and_parse_cbr_rates(currency_list: set[str]) -> pd.DataFrame:
    url = 'http://www.cbr.ru/scripts/XML_daily.asp'
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    soup = bs4.BeautifulSoup(response.content, 'xml')
    date_str = soup.find('ValCurs').get('Date')
    
    df_rates = pd.read_xml(soup.encode('utf-8'), xpath='.//Valute')
    
    wanted = set(currency_list)
    has_rub = bool({'RUB', 'RUR'} & wanted)
    
    if has_rub:
        wanted.discard('RUB')
        wanted.discard('RUR')
    
    if not df_rates.empty and wanted:
        df = df_rates[df_rates['CharCode'].isin(wanted)].copy()
        
        df['Value'] = df['Value'].astype(str).str.replace(',', '.').astype(float)
        df['Nominal'] = df['Nominal'].astype(float)
        df[CbrFieldsMap.RATE_RUB] = df['Value'] / df['Nominal']
        
        df = df.rename(columns={'CharCode': CbrFieldsMap.CURRENCY})
        df[CbrFieldsMap.DATE] = date_str
        
        df = df[[CbrFieldsMap.CURRENCY, CbrFieldsMap.RATE_RUB, CbrFieldsMap.DATE]]
    else:
        df = pd.DataFrame(columns=CbrFieldsMap.dest_columns())
    
    if has_rub:
        rub_row = pd.DataFrame([{
            CbrFieldsMap.CURRENCY: 'RUB',
            CbrFieldsMap.RATE_RUB: 1.0,
            CbrFieldsMap.DATE: date_str
        }])
        df = pd.concat([rub_row, df], ignore_index=True)
    
    if not df.empty:
        df[CbrFieldsMap.DATE] = pd.to_datetime(df[CbrFieldsMap.DATE], format='%d.%m.%Y').dt.date
        df[CbrFieldsMap.RATE_RUB] = df[CbrFieldsMap.RATE_RUB].astype('Float64')
    
    return df


def transform_cbr_rates(out_dp: str) -> str:
    os.makedirs(out_dp, exist_ok=True)
    df = fetch_and_parse_cbr_rates({'EUR', 'USD', 'CNY', 'RUB'})
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


