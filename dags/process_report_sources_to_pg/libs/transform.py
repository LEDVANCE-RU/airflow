import json
import os
from process_report_sources_to_pg.libs.handlers import mtd, ltm, md, stock, ic, ean, po, bo, packing


def transform_report_sources(downloaded_files_json: str, out_dp: str) -> str:
    os.makedirs(out_dp, exist_ok=True)
    files = json.loads(downloaded_files_json)

    table_to_file: dict[str, str] = {}

    md.handle(files, out_dp, table_to_file)
    mtd.handle(files, out_dp, table_to_file)
    ltm.handle(files, out_dp, table_to_file)
    stock.handle(files, out_dp, table_to_file)
    ic.handle(files, out_dp, table_to_file)
    ean.handle(files, out_dp, table_to_file)
    po.handle(files, out_dp, table_to_file)
    bo.handle(files, out_dp, table_to_file)
    packing.handle(files, out_dp, table_to_file)

    return json.dumps(table_to_file)


