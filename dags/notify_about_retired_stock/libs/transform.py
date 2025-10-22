import openpyxl.styles as pyxl_styles
import pandas

from db_model.mapping import QuerySuccessorIcMap
from utils.excel import ExcelPrettifier

OUT_FIELD_MAP = {
    QuerySuccessorIcMap.IC: 'IC',
    QuerySuccessorIcMap.IC_LIFECYCLE_STATUS: 'Статус жизненного цикла',
    QuerySuccessorIcMap.IC_PRIORITY: 'Приоритет',
    QuerySuccessorIcMap.ARTICLE: 'Артикул',
    QuerySuccessorIcMap.SUCCESSOR_IC: 'Следующий IC',
    QuerySuccessorIcMap.SUCCESSOR_IC_LIFECYCLE_STATUS: 'Статус жизненного цикла (след. IC)',
    QuerySuccessorIcMap.SUCCESSOR_IC_PRIORITY: 'Приоритет (след. IC)'
}


def transform(df_fp: str):
    df = pandas.read_parquet(df_fp)
    df = df[[QuerySuccessorIcMap.IC,
             QuerySuccessorIcMap.IC_LIFECYCLE_STATUS,
             QuerySuccessorIcMap.IC_PRIORITY,
             QuerySuccessorIcMap.ARTICLE,
             QuerySuccessorIcMap.SUCCESSOR_IC,
             QuerySuccessorIcMap.SUCCESSOR_IC_LIFECYCLE_STATUS,
             QuerySuccessorIcMap.SUCCESSOR_IC_PRIORITY]]
    df.drop_duplicates([QuerySuccessorIcMap.IC, QuerySuccessorIcMap.ARTICLE], inplace=True)
    df.rename(columns=OUT_FIELD_MAP, inplace=True)
    return df


def save_to_excel(df: pandas.DataFrame, out_fp: str):
    df.to_excel(out_fp, index=False)

    with ExcelPrettifier(out_fp, autosave=True) as prettifier:
        # prettifier.merge_cells_by_unique_row_values(1, column_indices=[1, 2, 3])
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
