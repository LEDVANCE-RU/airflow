import csv
import logging

import pandas

from upload_packages_sftp_1c_to_pg.mapping import PackageFieldsMap


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
    df.drop_duplicates([fmap.code, fmap.level],
                       inplace=True)
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

    d = {
        **{(str(c[0]), c[1]): 'float64' for c in df.columns if c[0] == fmap.enum},
        **{(str(c[0]), c[1]): 'string' for c in df.columns if c[0] == fmap.ean}
    }
    df = df.astype(d)

    df.columns = ['.'.join([c for c in col if c]) for col in df.columns.values]
    (
        df
        .rename(columns={k: v.name for k, v in dest_map.items()})
        .to_csv(out_fp,
                index=False,
                encoding='utf-8',
                sep=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                columns=[c.name for c in dest_map.values()])
    )
    return True