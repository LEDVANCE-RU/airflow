KEY_TO_TABLE = {
    '1C_master_data_AG': 'md.master_data_ag',
    'MTD_report_AG': 'md.mtd_report_ag_data',
    'LTM_report_AG': 'md.ltm_report_ag_data',
    'STOCK_report_AG': 'stocks.stock_report_ag',
    '1C_IC_AG': 'md.ic_ag',
    '1C_EAN_AG': 'md.ean_ag',
    'PO_report_NEW_AG': 'stocks.po_report_new_ag',
    'BO_report_AG': 'stocks.bo_report_ag',
    '1C_packing_AG': 'md.packing_ag',
}

FILE_DTYPES = {
    '1C_master_data_AG': {'Артикул': str},
    'MTD_report_AG': {0: str},
    'LTM_report_AG': {0: str},
    'STOCK_report_AG': {'Артикул': str},
    '1C_IC_AG': str,
    '1C_EAN_AG': str,
    'PO_report_NEW_AG': {'Артикул': str},
    'BO_report_AG': {'Артикул': str},
    '1C_packing_AG': {'Штрихкод (WA)': str},
}

PERIOD_TABLES = {
    'MTD_report_AG': 'md.mtd_report_ag_period',
    'LTM_report_AG': 'md.ltm_report_ag_period',
}

TABLE_COLUMNS = {
    'md.master_data_ag': [
        'description', 'uom', 'article', 'import_', 'kind_of_goods', 'type_of_goods', 'price_group',
        'analytic_group', 'fin_group', 'hs_code', 'co_o', 'nom_group', 'nom_group_group',
        'nom_group_group_group', 'nom_group_group_group_group', 'volume'
    ],
    'md.mtd_report_ag_period': ['mtd_period', 'mtd_from', 'mtd_to'],
    'md.mtd_report_ag_data': ['ean', 'ic', 'open_stock', 'in_', 'out_', 'close_stock', 'sku'],
    'md.ltm_report_ag_period': ['ltm_period', 'ltm_from', 'ltm_to'],
    'md.ltm_report_ag_data': ['ean', 'ic', 'open_stock', 'in_', 'out_', 'close_stock', 'sku'],
    'stocks.stock_report_ag': ['ean', 'description', 'ic', 'uom', 'open_stock_pce', 'open_stock_rub', 'stock_pce', 'stock_rub', 'sku', 'variance_rub'],
    'md.ic_ag': ['ic', 'project', 'hs_code', 'country_of_origin', 'localization', 'life_status', 'wh_status', 'volume'],
    'md.ean_ag': ['description', 'ean', 'hs_code'],
    'stocks.po_report_new_ag': ['project', 'life_status', 'ean', 'ic', 'description', 'date', 'po_qty', 'sku', 'article'],
    'stocks.bo_report_ag': ['ean', 'description', 'ic', 'uom', 'stock', 'in_outbound', 'reserved', 'available', 'sku'],
    'md.packing_ag': [
        'pack_type', 'is_dimensionless', 'weight_uom', 'height_uom', 'depth_uom', 'unit',
        'dims_repr', 'volume_uom', 'size_type', 'width_uom', 'tare_characteristic', 'measure_type',
        'full_name', 'intl_abbr', 'package_type', 'accounting_type', 'processing_multiplicity',
        'axelot_guid', 'ic', 'ean', 'is_indivisible', 'pack_level', 'gross_weight', 'height',
        'depth', 'numerator', 'denominator', 'volume', 'width', 'packs_qty', 'layers_per_pallet',
        'transport_boxes_per_pallet'
    ],
}

SQL_CREATE_TABLE = {
    'md.master_data_ag': (
        'CREATE TABLE IF NOT EXISTS md.master_data_ag ('
        'description VARCHAR(100), uom VARCHAR(50), article VARCHAR(50), import_ BOOLEAN, '
        'kind_of_goods VARCHAR(200), type_of_goods VARCHAR(200), price_group VARCHAR(100), '
        'analytic_group VARCHAR(200), fin_group VARCHAR(100), hs_code VARCHAR(100), co_o VARCHAR(100), '
        'nom_group VARCHAR(100), nom_group_group VARCHAR(100), nom_group_group_group VARCHAR(100), '
        'nom_group_group_group_group VARCHAR(100), volume NUMERIC(20,3)'
        ');'
    ),
    'md.mtd_report_ag_period': (
        'CREATE TABLE IF NOT EXISTS md.mtd_report_ag_period ('
        'mtd_period VARCHAR(100), mtd_from DATE, mtd_to DATE'
        ');'
    ),
    'md.mtd_report_ag_data': (
        'CREATE TABLE IF NOT EXISTS md.mtd_report_ag_data ('
        'ean VARCHAR(50), ic VARCHAR(50), open_stock NUMERIC(20,0), in_ NUMERIC(20,0), out_ NUMERIC(20,0), '
        'close_stock NUMERIC(20,0), sku VARCHAR(100)'
        ');'
    ),
    'md.ltm_report_ag_period': (
        'CREATE TABLE IF NOT EXISTS md.ltm_report_ag_period ('
        'ltm_period VARCHAR(100), ltm_from DATE, ltm_to DATE'
        ');'
    ),
    'md.ltm_report_ag_data': (
        'CREATE TABLE IF NOT EXISTS md.ltm_report_ag_data ('
        'ean VARCHAR(50), ic VARCHAR(50), open_stock NUMERIC(20,0), in_ NUMERIC(20,0), out_ NUMERIC(20,0), '
        'close_stock NUMERIC(20,0), sku VARCHAR(100)'
        ');'
    ),
    'stocks.stock_report_ag': (
        'CREATE TABLE IF NOT EXISTS stocks.stock_report_ag ('
        'ean VARCHAR(50), description VARCHAR(100), ic VARCHAR(50), uom VARCHAR(10), '
        'open_stock_pce NUMERIC(20,0), open_stock_rub NUMERIC(20,2), stock_pce NUMERIC(20,0), stock_rub NUMERIC(20,2), '
        'sku VARCHAR(100), variance_rub NUMERIC(20,2)'
        ');'
    ),
    'md.ic_ag': (
        'CREATE TABLE IF NOT EXISTS md.ic_ag ('
        'ic VARCHAR(50), project VARCHAR(500), hs_code VARCHAR(100), country_of_origin VARCHAR(100), '
        'localization VARCHAR(100), life_status VARCHAR(100), wh_status VARCHAR(100), volume NUMERIC(20,6)'
        ');'
    ),
    'md.ean_ag': (
        'CREATE TABLE IF NOT EXISTS md.ean_ag ('
        'description VARCHAR(100), ean VARCHAR(50), hs_code VARCHAR(100)'
        ');'
    ),
    'stocks.po_report_new_ag': (
        'CREATE TABLE IF NOT EXISTS stocks.po_report_new_ag ('
        'project VARCHAR(500), life_status VARCHAR(100), ean VARCHAR(50), ic VARCHAR(50), description VARCHAR(100), '
        'date DATE, po_qty NUMERIC(20,0), sku VARCHAR(100), article VARCHAR(500)'
        ');'
    ),
    'stocks.bo_report_ag': (
        'CREATE TABLE IF NOT EXISTS stocks.bo_report_ag ('
        'ean VARCHAR(50), description VARCHAR(100), ic VARCHAR(50), uom VARCHAR(50), stock NUMERIC(20,0), '
        'in_outbound NUMERIC(20,0), reserved NUMERIC(20,0), available NUMERIC(20,0), sku VARCHAR(200)'
        ');'
    ),
    'md.packing_ag': (
        'CREATE TABLE IF NOT EXISTS md.packing_ag ('
        'pack_type VARCHAR(100), is_dimensionless VARCHAR(50), weight_uom VARCHAR(50), height_uom VARCHAR(50), depth_uom VARCHAR(50), unit VARCHAR(50), '
        'dims_repr VARCHAR(100), volume_uom VARCHAR(50), size_type VARCHAR(100), width_uom VARCHAR(50), tare_characteristic VARCHAR(100), measure_type VARCHAR(100), '
        'full_name VARCHAR(200), intl_abbr VARCHAR(50), package_type VARCHAR(100), accounting_type VARCHAR(200), processing_multiplicity VARCHAR(200), '
        'axelot_guid VARCHAR(100), ic VARCHAR(200), ean VARCHAR(50), is_indivisible VARCHAR(50), pack_level VARCHAR(100), gross_weight NUMERIC(20,3), height NUMERIC(20,3), '
        'depth NUMERIC(20,3), numerator NUMERIC(20,6), denominator NUMERIC(20,6), volume NUMERIC(20,6), width NUMERIC(20,3), packs_qty NUMERIC(20,0), layers_per_pallet NUMERIC(20,0), '
        'transport_boxes_per_pallet NUMERIC(20,0)'
        ');'
    ),
}
