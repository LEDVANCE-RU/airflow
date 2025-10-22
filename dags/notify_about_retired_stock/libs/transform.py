import openpyxl.styles as pyxl_styles
import pandas

from db_model.mapping import QuerySiblingIcMap
from utils.excel import ExcelPrettifier

OUT_FIELD_MAP = {
    QuerySiblingIcMap.IC: 'IC',
    QuerySiblingIcMap.IC_LIFECYCLE_STATUS: 'Статус жизненного цикла',
    QuerySiblingIcMap.ARTICLE: 'Артикул',
    QuerySiblingIcMap.SIBLING_IC: 'Альтернативный IC',
    QuerySiblingIcMap.SIBLING_IC_LIFECYCLE_STATUS: 'Статус жизненного цикла (альт. IC)'
}


def transform(df_fp: str):
    df = pandas.read_parquet(df_fp)
    df = df[[QuerySiblingIcMap.IC,
             QuerySiblingIcMap.IC_LIFECYCLE_STATUS,
             QuerySiblingIcMap.ARTICLE,
             QuerySiblingIcMap.SIBLING_IC,
             QuerySiblingIcMap.SIBLING_IC_LIFECYCLE_STATUS]]
    df.rename(columns=OUT_FIELD_MAP, inplace=True)
    return df


def save_to_excel(df: pandas.DataFrame, out_fp: str):
    df.to_excel(out_fp, index=False)

    with ExcelPrettifier(out_fp, autosave=True) as prettifier:
        prettifier.merge_cells_by_unique_row_values(1, column_indices=[1, 2, 3])

        border_side = pyxl_styles.Side(style='thin', color='808080')
        border_style = pyxl_styles.Border(left=border_side, right=border_side, top=border_side, bottom=border_side)
        alignment = pyxl_styles.Alignment('left', 'top')
        font = pyxl_styles.Font(size=10, bold=False)
        prettifier.apply_style(ranges=None,
                               border_style=border_style,
                               alignment=alignment,
                               font=font)

        font = pyxl_styles.Font(size=10, bold=True)
        prettifier.apply_style(ranges=[(1, 1, None, 1)], font=font)

        prettifier.auto_fit_columns_advanced()
