import pandas

from models.db.mapping import QuerySiblingIcMap


OUT_FIELD_MAP = {
    QuerySiblingIcMap.IC: 'IC',
    QuerySiblingIcMap.IC_LIFECYCLE_STATUS: 'Статус жизненного цикла',
    QuerySiblingIcMap.ARTICLE: 'Артикул',
    QuerySiblingIcMap.SIBLING_IC: 'Альтернативный IC',
    QuerySiblingIcMap.SIBLING_IC_LIFECYCLE_STATUS: 'Статус жизненного цикла (альт. IC)'
}


def transform(df_fp: str, out_fp: str):
    df = pandas.read_parquet(df_fp)
    df = df[[QuerySiblingIcMap.IC,
             QuerySiblingIcMap.IC_LIFECYCLE_STATUS,
             QuerySiblingIcMap.ARTICLE,
             QuerySiblingIcMap.SIBLING_IC,
             QuerySiblingIcMap.SIBLING_IC_LIFECYCLE_STATUS]]
    df.set_index([QuerySiblingIcMap.IC,
                  QuerySiblingIcMap.IC_LIFECYCLE_STATUS,
                  QuerySiblingIcMap.ARTICLE],
                 inplace=True)
    df.rename(OUT_FIELD_MAP, inplace=True)
    df.to_excel(out_fp, merge_cells=True)