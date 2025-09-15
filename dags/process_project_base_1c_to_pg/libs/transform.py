import csv
import json
import os
import uuid
import pandas as pd
import logging

from process_project_base_1c_to_pg.libs.mapping import ProjectBaseFieldsMap


def transform_project_base(in_fp: str, out_dp: str, src_map: dict, dest_map: dict, file_key: str) -> str:
    df = pd.read_excel(in_fp, dtype=str)
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]

    df.drop(columns=['Краткое описание'], inplace=True, errors='ignore')

    proj_col = 'Проект'
    if proj_col in df.columns:
        df = df[~df[proj_col].astype(str).str.strip().str.startswith('Итого', na=False)]

    df.rename(columns=src_map, inplace=True)

    dest_columns = list(dest_map.keys())
    df = df[df.columns.intersection(dest_columns)]
    for col in dest_columns:
        if col not in df.columns:
            df[col] = None

    df = df[dest_columns]

    export_fp = os.path.join(out_dp, f"{uuid.uuid4().hex}_{file_key}.csv")
    df.to_csv(
        export_fp,
        index=False,
        encoding='utf-8',
        sep=',',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        columns=dest_columns
    )
    
    logging.info(f"Transformed {in_fp} to {export_fp}")
    return export_fp


def transform_project_base_data(downloaded_files_json: str, out_dp: str):
    downloaded_files = json.loads(downloaded_files_json)
    transformed_filepaths = {}
    in_fp = downloaded_files.get("project_base")

    if in_fp:
        transformed_filepaths["project_base"] = transform_project_base(
            in_fp=in_fp,
            out_dp=out_dp,
            src_map=ProjectBaseFieldsMap.src_map(),
            dest_map=ProjectBaseFieldsMap.dest_map(),
            file_key="project_base"
        )
        
    return json.dumps(transformed_filepaths)


