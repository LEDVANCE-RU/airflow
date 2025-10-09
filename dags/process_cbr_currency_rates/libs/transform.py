import csv
import json
import os
import uuid
import logging
import pandas as pd
import requests
from io import BytesIO
from process_cbr_currency_rates.libs.mapping import CbrFieldsMap


def fetch_and_parse_cbr_rates(currency_list: set[str]) -> pd.DataFrame:
    url = 'http://www.cbr.ru/scripts/XML_daily.asp'
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    soup = bs4.BeautifulSoup(response.content, 'xml')
    date_str = soup.find('ValCurs').get('Date')
    
    df_rates = pd.read_xml(soup.encode('utf-8'), xpath='.//Valute')
    
    columns = CbrFieldsMap.dest_columns()
    wanted = set(currency_list)
    
    result_data = []
    
    if {'RUB', 'RUR'} & wanted:
        result_data.append({columns[0]: 'RUB', columns[1]: 1.0, columns[2]: date_str})
        wanted.discard('RUB')
        wanted.discard('RUR')
    
    if not df_rates.empty and wanted:
        df_filtered = df_rates[df_rates['CharCode'].isin(wanted)].copy()
        
        df_filtered['Value'] = df_filtered['Value'].astype(str).str.replace(',', '.').astype(float)
        df_filtered['Nominal'] = df_filtered['Nominal'].astype(float)
        df_filtered['rate_rub'] = df_filtered['Value'] / df_filtered['Nominal']
        
        df_filtered = df_filtered.rename(columns={
            'CharCode': columns[0],
            'rate_rub': columns[1]
        })
        df_filtered[columns[2]] = date_str
        
        result_data.extend(df_filtered[[columns[0], columns[1], columns[2]]].to_dict('records'))
    
    df = pd.DataFrame(result_data, columns=columns)
    if not df.empty:
        df[columns[2]] = pd.to_datetime(df[columns[2]], format='%d.%m.%Y').dt.date
        df[columns[1]] = df[columns[1]].astype('Float64')
    
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

