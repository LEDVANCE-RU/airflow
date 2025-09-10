import pandas as pd
from ..constants import KEY_TO_TABLE
from ..common import export_df, align_columns, drop_trailing_total


def handle(files: dict, out_dp: str, table_to_file: dict):
    if not files.get('1C_packing_AG'):
        return
    df = pd.read_excel(files['1C_packing_AG'])
    df = drop_trailing_total(df)
    df.columns = [
        'pack_type', 'is_dimensionless', 'weight_uom', 'height_uom', 'depth_uom', 'unit',
        'dims_repr', 'volume_uom', 'size_type', 'width_uom', 'tare_characteristic', 'measure_type',
        'full_name', 'intl_abbr', 'package_type', 'accounting_type', 'processing_multiplicity',
        'axelot_guid', 'ic', 'ean', 'is_indivisible', 'pack_level', 'gross_weight', 'height',
        'depth', 'numerator', 'denominator', 'volume', 'width', 'packs_qty', 'layers_per_pallet',
        'transport_boxes_per_pallet'
    ]
    table = KEY_TO_TABLE['1C_packing_AG']
    df = align_columns(df, table)
    table_to_file[table] = export_df(df, out_dp, '1C_packing_AG', table)
