import json
import os
from process_report_sources_to_pg.libs.handlers import mtd, ltm, md, stock, ic, ean, po, bo, packing


def transform_report_sources(downloaded_files_json: str, out_dp: str) -> str:
    os.makedirs(out_dp, exist_ok=True)
    files = json.loads(downloaded_files_json)

    table_to_file: dict[str, str] = {}

    handlers = [
        md.handle,
        mtd.handle,
        ltm.handle,
        stock.handle,
        ic.handle,
        ean.handle,
        po.handle,
        bo.handle,
        packing.handle
    ]
    for h in handlers:
        h(files, out_dp, table_to_file)

    return json.dumps(table_to_file)


