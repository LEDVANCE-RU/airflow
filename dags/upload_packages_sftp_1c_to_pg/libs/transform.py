import csv
import logging

import pandas

from upload_packages_sftp_1c_to_pg.libs.mapping import PackageFieldsMap


def transform(in_fp: str, out_fp: str):
    fmap = PackageFieldsMap
    src_map = fmap.src_map()
    dest_map = fmap.dest_map()

    df = pandas.read_excel(in_fp,
                           usecols=list(src_map.keys()),
                           dtype={k: v.type for k, v in src_map.items()})
    df.rename(columns={k: v.name for k, v in src_map.items()},
              inplace=True)

    df = df[df[fmap.level].isin(fmap.allowed_levels())]

    unique_key = [fmap.ic, fmap.level]
    df_duplicates = df[df.duplicated(unique_key, keep=False)]
    if not df_duplicates.empty:
        logging.warning(
            'Обнаружены дубли уровней упаковок по IC:\n%s\n',
            df_duplicates
            .sort_values(unique_key)
            .fillna('')
            .to_markdown(index=False, tablefmt="github")
        )
        df.drop_duplicates(unique_key, inplace=True)

    df[fmap.qty] = df[fmap.enum] / df[fmap.denom]
    df = (
        df.pivot(
            index=[fmap.code,
                   fmap.article,
                   fmap.ic,
                   fmap.description],
            columns=fmap.level,
            values=[fmap.qty, fmap.ean])
        .reset_index()
    )

    df.columns = ['.'.join([c for c in col if c]) for col in df.columns.values]

    # forcibly add columns from mapping for case when levels are missing in source dataframe
    df = df.reindex(columns=list(dest_map.keys()))
    d = {
        **{(str(c[0]), c[1]): 'float64' for c in df.columns if c[0] == fmap.enum},
        **{(str(c[0]), c[1]): 'string' for c in df.columns if c[0] == fmap.ean}
    }
    df = df.astype(d)
    df.rename(columns={k: v.name for k, v in dest_map.items()}, inplace=True)

    df_wrong_pce_qty = df[
       (df['pce_qty'] != 1)
       & (df[[col for col in df.columns.values if col.endswith('_qty')]].min(axis='columns') != 1)
    ]
    if not df_wrong_pce_qty.empty:
        logging.warning(
            'Отсутствует штучная упаковка:\n%s\n',
            df_wrong_pce_qty
            .fillna('')
            .to_markdown(index=False, tablefmt="github")
        )

    df.to_csv(out_fp,
              index=False,
              encoding='utf-8',
              sep=',',
              quotechar='"',
              quoting=csv.QUOTE_MINIMAL,
              columns=[c.name for c in dest_map.values()])
    return True