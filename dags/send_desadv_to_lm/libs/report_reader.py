import time
from typing import Any

import pandas
from bidict import bidict

from send_desadv_to_lm.libs.constants import (
    SHIP_TO_GLN, SELLER_GLN,
    IN_DATE_FORMAT, IN_DATETIME_FORMAT,
    IN_PACKAGES_WORKSHEET, IN_ITEMS_WORKSHEET,
    IN_VALUES_DELIMITER, PACKAGE_TYPES, OrderTypes
)
from send_desadv_to_lm.libs.exceptions import (
    ValuesNumDiscrepancyError, NoExpenditureOrdersFoundError, PackageItemsInconsistencyError,
    UnknownPackageTypeError, NonUniquePackageError, NonOccupiedPackageError, OrphanedItemsError
)
from send_desadv_to_lm.libs.field_map import (
    BaseFields,
    PackageFieldsBase, ItemFieldsBase,
    PackageFieldsXD, ItemFieldsXD,
    PackageFieldsBBXD, ItemFieldsBBXD
)


class ReportReader:
    def __init__(self, report_path: str, order_type: OrderTypes):
        self.path = report_path
        self.order_type = order_type
        self.df = None

    def parse(self) -> pandas.DataFrame:
        df_base = self._parse_base_data()

        if self.order_type == OrderTypes.XD:
            df_packages = self._parse_packages_xd()
            df_items = self._parse_items_xd()
        else:
            df_packages = self._parse_packages_bbxd()
            df_items = self._parse_items_bbxd()

        self._validate_expenditure_orders(df_base, df_packages)

        df_base_packages = pandas.merge(df_base, df_packages, how='inner',
                                        left_on=BaseFields.EXPENDITURE_ORDER,
                                        right_on=PackageFieldsBase.EXPENDITURE_ORDER)

        self._validate_orphaned_items(df_base_packages, df_items)

        if self.order_type == OrderTypes.XD:
            self.df = self._get_package_items_xd(df_base_packages, df_items)
        else:
            self.df = self._get_package_items_bbxd(df_base_packages, df_items)

        self.df = self.df.astype(BaseFields.types())
        self.df.sort_values([PackageFieldsBase.SORT_INDEX,
                             PackageFieldsBase.NUM,
                             ItemFieldsBase.CUSTOMER_ORDER_LINE],
                            inplace=True)
        return self.df

    def iter_invoices(self):
        for num in self.df[BaseFields.INVOICE].unique():
            if pandas.isna(num):
                continue
            yield self.df[self.df[BaseFields.INVOICE] == num]


    def _get_package_items_xd(self, df_base_packages: pandas.DataFrame, df_items: pandas.DataFrame):
        df_single_packages_items = self._get_single_packages_items_xd(df_base_packages)
        df_multiple_packages_items = self._get_multiple_packages_items_xd(df_base_packages, df_items)

        list_df = [df for df in [df_single_packages_items, df_multiple_packages_items] if not df.empty]
        df = pandas.concat(list_df, ignore_index=True)

        self._validate_packages_items(df)

        df = df[~pandas.isna(df[ItemFieldsXD.QTY])]
        df = df.astype(ItemFieldsBBXD.types())
        return df

    def _get_package_items_bbxd(self, df_base_packages: pandas.DataFrame, df_items: pandas.DataFrame):
        df = pandas.merge(
            df_base_packages, df_items, how='left',
            left_on=[BaseFields.EXPENDITURE_ORDER, BaseFields.CUSTOMER_ORDER_LINE, PackageFieldsBBXD.NUM],
            right_on=[ItemFieldsBBXD.EXPENDITURE_ORDER, ItemFieldsBBXD.CUSTOMER_ORDER_LINE, ItemFieldsBBXD.PACKAGE_NUM]
        )

        self._validate_packages_items(df)

        df = df[~pandas.isna(df[ItemFieldsXD.QTY])]
        df = df.astype(ItemFieldsBBXD.types())
        return df

    def _validate_packages_items(self, df: pandas.DataFrame):
        self._validate_packages_items_qty(df)
        self._validate_packages_occupation(df)

    def _parse_base_data(self):
        df = pandas.read_excel(self.path, header=1, usecols=BaseFields.map().keys(),
                               dtype=BaseFields.src_types())
        df.rename(columns=BaseFields.map(), inplace=True)
        df.drop_duplicates([BaseFields.ORDER,
                            BaseFields.CUSTOMER_ORDER_LINE,
                            BaseFields.EXPENDITURE_ORDER,
                            BaseFields.ISSUE,
                            BaseFields.INVOICE], inplace=True)
        df[BaseFields.SHIP_TO_GLN] = SHIP_TO_GLN
        df[BaseFields.SELLER_GLN] = SELLER_GLN
        df[[BaseFields.DELIVERY_NOTE, BaseFields.DELIVERY_NOTE_DATE]] =(
            pandas.DataFrame(
                df[BaseFields.DELIVERY_NOTE].apply(
                    lambda x:
                    # take the latest delivery note and then split the string into columns;
                    # several delivery notes - very rare case
                    x.split('\n')[-1].split('; ', 1)
                    if isinstance(x, str) and not pandas.isna(x)
                    else [None] * 2
                ).to_list(),
                index=df.index
            )
        )
        df[BaseFields.date_fields()] = df[BaseFields.date_fields()].apply(
            lambda s: pandas.to_datetime(s, format=IN_DATE_FORMAT)
        )
        df[BaseFields.datetime_fields()] = df[BaseFields.datetime_fields()].apply(
            lambda s: pandas.to_datetime(s, format=IN_DATETIME_FORMAT)
        )
        df[BaseFields.doc_num_fields()] = df[BaseFields.doc_num_fields()].apply(self._strip_doc_num_series)
        return df

    def _parse_packages_xd(self) -> pandas.DataFrame:
        fmap = PackageFieldsXD

        df = pandas.read_excel(self.path, header=0, sheet_name=IN_PACKAGES_WORKSHEET,
                               usecols=fmap.map().keys())
        df.rename(columns=fmap.map(), inplace=True)
        df[fmap.SORT_INDEX] = df.index.values
        df[fmap.EXPENDITURE_ORDER] = (
            self._strip_doc_num_series(df[fmap.EXPENDITURE_ORDER]))
        df[fmap.PALLET_QTY] = df[fmap.PALLET_QTY].replace(0, 1)
        multiple_valued_cols = fmap.multiple_valued_cols()

        for col in multiple_valued_cols:
            df[col] = df.apply(lambda s: self._explode_value(s[col], s[fmap.PALLET_QTY]),
                               axis='columns')
        self._validate_packages_consistency_xd(df)
        df = df.explode(multiple_valued_cols)
        df[fmap.NUM] = df.groupby(fmap.EXPENDITURE_ORDER).cumcount() + 1
        df[fmap.SSCC] = df.apply(
            lambda s: self._get_sscc(s[fmap.EXPENDITURE_ORDER],
                                     s[fmap.NUM]),
            axis='columns'
        )

        df = df.astype(fmap.types())
        df[fmap.WEIGHT] = df[fmap.WEIGHT].round()

        self._validate_packages(df, fmap.map().inverse)
        return df

    def _parse_packages_bbxd(self) -> pandas.DataFrame:
        fmap = PackageFieldsBBXD

        df = pandas.read_excel(self.path, header=0, sheet_name=IN_PACKAGES_WORKSHEET,
                               usecols=fmap.map().keys())
        df.rename(columns=fmap.map(), inplace=True)
        df[fmap.SORT_INDEX] = df.index.values
        df[fmap.EXPENDITURE_ORDER] = (
            self._strip_doc_num_series(df[fmap.EXPENDITURE_ORDER]))

        df[fmap.SSCC] = df.apply(
            lambda s: self._get_sscc(s[fmap.EXPENDITURE_ORDER],
                                     s[fmap.NUM]),
            axis='columns'
        )

        df = df.astype(fmap.types())
        df[fmap.WEIGHT] = df[fmap.WEIGHT].round()

        self._validate_packages(df, fmap.map().inverse)
        return df

    def _validate_packages(self, df: pandas.DataFrame, mandatory_fields: bidict):
        self._validate_mandatory_fields(df, mandatory_fields, IN_PACKAGES_WORKSHEET)
        self._validate_packages_types(df)
        self._validate_packages_uniqueness(df)

    def _parse_items_xd(self) -> pandas.DataFrame:
        fmap = ItemFieldsXD

        df = pandas.read_excel(self.path, header=0, sheet_name=IN_ITEMS_WORKSHEET,
                               usecols=fmap.map().keys())
        df.rename(columns=fmap.map(), inplace=True)
        df[fmap.EXPENDITURE_ORDER] = (
            self._strip_doc_num_series(df[fmap.EXPENDITURE_ORDER]))
        multiple_valued_cols = fmap.multiple_valued_cols()
        for col in multiple_valued_cols:
            df[col] = df.apply(lambda s: self._explode_value(s[col]),
                               axis='columns')
        self._validate_items_line_consistency_xd(df)

        df = df.explode(multiple_valued_cols)

        self._validate_items(df, fmap.map().inverse)
        df = df.astype(fmap.types())
        return df

    def _parse_items_bbxd(self) -> pandas.DataFrame:
        fmap = ItemFieldsBBXD

        df = pandas.read_excel(self.path, header=0, sheet_name=IN_ITEMS_WORKSHEET,
                               usecols=fmap.map().keys())
        df.rename(columns=fmap.map(), inplace=True)
        df[fmap.EXPENDITURE_ORDER] = (
            self._strip_doc_num_series(df[fmap.EXPENDITURE_ORDER]))

        group_fname = '_group'
        index_fname = 'index'

        group_fields = [fmap.EXPENDITURE_ORDER, fmap.IC]
        # increment group when values of group fields differ from previous row
        df[group_fname] = (df[group_fields] != df.shift(1)[group_fields]).apply(any, axis='columns').cumsum()

        # fill possible gaps in order line numbers
        gapped_fname = fmap.CUSTOMER_ORDER_LINE
        df[gapped_fname] = (
            # save original index into separate column
            df.reset_index()
            .groupby(group_fname)
            .apply(lambda x: x[[index_fname, gapped_fname]].ffill().bfill())
            # set original index
            .set_index(index_fname, drop=True)
        )[gapped_fname]

        self._validate_items(df, fmap.map().inverse)
        df = df.drop(columns=[group_fname]).astype(fmap.types())
        return df


    @staticmethod
    def _get_sscc(doc_num: int, package_num: int):
        base_code = f"{doc_num:0>6}{package_num:0>3}{str(int(time.time()))[-7:]}"
        return f"{base_code:0>18}"

    @staticmethod
    def _explode_value(value: Any, expected_num: int = 1):
        if isinstance(value, str):
            vals = list(map(str.strip, value.split(IN_VALUES_DELIMITER)))
            if len(vals) > 1:
                return vals

        return [value] * expected_num

    @staticmethod
    def _strip_doc_num_series(s: pandas.Series) -> pandas.Series:
        return s.str.lstrip('0-').str.strip()

    @staticmethod
    def _validate_packages_consistency_xd(df_packages: pandas.DataFrame):
        matches = []
        s_expected_num = df_packages[PackageFieldsXD.PALLET_QTY]
        for col in PackageFieldsXD.multiple_valued_cols():
            s = df_packages[col].apply(len) == s_expected_num
            s.name = col
            matches.append(s)
        df_mask = pandas.concat(matches, axis=1)
        df_mask_all_by_rows = df_mask.all(axis=1)
        if df_mask_all_by_rows.all():
            return True
        df = df_packages.mask(df_mask)[~df_mask_all_by_rows]
        df[PackageFieldsXD.EXPENDITURE_ORDER] = df_packages[PackageFieldsXD.EXPENDITURE_ORDER]
        df[PackageFieldsXD.PALLET_QTY] = df_packages[PackageFieldsXD.PALLET_QTY]
        df.dropna(axis=1, how='all', inplace=True)
        df.rename(columns=PackageFieldsXD.map().inverse, inplace=True)
        msg = (f"Лист \"{IN_PACKAGES_WORKSHEET}\": число паллет не совпадает с числом перечисленных значений.\n"
               f"{df.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise ValuesNumDiscrepancyError(msg)

    @staticmethod
    def _validate_items_line_consistency_xd(df_items: pandas.DataFrame):
        fmap = ItemFieldsXD

        matches = []
        s_expected_num = df_items[fmap.PACKAGE_NUM].apply(len)
        for col in fmap.multiple_valued_cols():
            s = df_items[col].apply(len) == s_expected_num
            s.name = col
            matches.append(s)
        df_mask = pandas.concat(matches, axis=1)
        df_mask_all_by_rows = df_mask.all(axis=1)
        if df_mask_all_by_rows.all():
            return True
        df = df_items.mask(df_mask)[~df_mask_all_by_rows]
        df[fmap.EXPENDITURE_ORDER] = df_items[fmap.EXPENDITURE_ORDER]
        df[fmap.CUSTOMER_ORDER_LINE] = df_items[fmap.CUSTOMER_ORDER_LINE]
        df[fmap.PACKAGE_NUM] = df_items[fmap.PACKAGE_NUM]
        df.dropna(axis=1, how='all', inplace=True)
        df.rename(columns=fmap.map().inverse, inplace=True)
        msg = (f"Лист \"{IN_ITEMS_WORKSHEET}\": число паллет не совпадает с числом перечисленных значений.\n"
               f"{df.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise ValuesNumDiscrepancyError(msg)

    @staticmethod
    def _validate_expenditure_orders(df_base: pandas.DataFrame, df_packages: pandas.DataFrame):
        df_base_packages = pandas.merge(df_base, df_packages, how='outer',
                                        left_on=BaseFields.EXPENDITURE_ORDER,
                                        right_on=PackageFieldsBase.EXPENDITURE_ORDER)
        df = df_base_packages[
            pandas.isna(df_base_packages[BaseFields.EXPENDITURE_ORDER])
            & ~pandas.isna(df_base_packages[PackageFieldsBase.EXPENDITURE_ORDER])
        ]
        if not df.empty:
            raise NoExpenditureOrdersFoundError(df[PackageFieldsBase.EXPENDITURE_ORDER].unique().tolist())

    @staticmethod
    def _validate_packages_items_qty(df_package_items: pandas.DataFrame):
        df_group = df_package_items.groupby([BaseFields.EXPENDITURE_ORDER, BaseFields.CUSTOMER_ORDER_LINE])
        df = (df_group
              .agg({BaseFields.ISSUED_ITEM_QTY: 'max',
                    ItemFieldsBase.QTY: 'sum'})
              .reset_index())
        df = df[df[BaseFields.ISSUED_ITEM_QTY] != df[ItemFieldsBase.QTY]]
        if df.empty:
            return True

        col_names = {
            BaseFields.EXPENDITURE_ORDER: "Расходный ордер",
            BaseFields.CUSTOMER_ORDER_LINE: "№ стр. ИЗ",
            BaseFields.ISSUED_ITEM_QTY: "Фактическое кол-во",
            ItemFieldsBase.QTY: "Указанное кол-во"
        }
        df = df[col_names.keys()].rename(columns=col_names)
        msg = (f"Фактическое количество товара в расходном ордере не совпадает с количеством, "
               f"указанным на листе \"{IN_ITEMS_WORKSHEET}\".\n"
               f"{df.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise PackageItemsInconsistencyError(msg)

    @staticmethod
    def _validate_packages_types(df_exploded_packages: pandas.DataFrame):
        df = df_exploded_packages[~df_exploded_packages[PackageFieldsBase.TYPE].isin(PACKAGE_TYPES)]
        if df.empty:
            return True
        df= (df[[PackageFieldsBase.EXPENDITURE_ORDER,
                PackageFieldsBase.TYPE]]
             .rename(columns=PackageFieldsBase.map().inverse))
        msg = (f"Лист \"{IN_PACKAGES_WORKSHEET}\": неизвестные типы упаковок. "
               f"Допустимые значения: {PACKAGE_TYPES}.\n"
               f"{df.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise UnknownPackageTypeError(msg)

    @staticmethod
    def _get_single_packages_items_xd(df_base_packages: pandas.DataFrame) -> pandas.DataFrame:
        fmap = ItemFieldsXD

        df = df_base_packages[df_base_packages[PackageFieldsXD.PALLET_QTY] == 1].copy()
        df[[fmap.EXPENDITURE_ORDER,
            fmap.PACKAGE_NUM,
            fmap.CUSTOMER_ORDER_LINE,
            fmap.QTY]] \
            = df[[BaseFields.EXPENDITURE_ORDER,
                  PackageFieldsXD.NUM,
                  BaseFields.CUSTOMER_ORDER_LINE,
                  BaseFields.ISSUED_ITEM_QTY]]
        return df

    def _get_multiple_packages_items_xd(self, df_base_packages: pandas.DataFrame, df_items: pandas.DataFrame) \
            -> pandas.DataFrame:
        fmap = ItemFieldsXD
        fmap_package = PackageFieldsXD

        df = df_base_packages[df_base_packages[fmap_package.PALLET_QTY] > 1].copy()
        df = pandas.merge(
            df, df_items, how='left',
            left_on=[BaseFields.EXPENDITURE_ORDER, BaseFields.CUSTOMER_ORDER_LINE, fmap_package.NUM],
            right_on=[fmap.EXPENDITURE_ORDER, fmap.CUSTOMER_ORDER_LINE, fmap.PACKAGE_NUM]
        )
        return df

    def _validate_packages_occupation(self, df: pandas.DataFrame):
        group_fnames = {
            PackageFieldsBase.EXPENDITURE_ORDER: "Расходный ордер",
            PackageFieldsBase.NUM: "№ паллеты"
        }
        df_g = df.groupby(list(group_fnames.keys()))
        s_g = df_g[ItemFieldsBase.QTY].sum()
        df_v = s_g[s_g <= 0].reset_index()
        if df_v.empty:
            return True

        df_v = df_v[group_fnames.keys()].rename(columns=group_fnames)
        msg = (f"Обнаружены пустые паллеты:\n"
               f"{df_v.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise NonOccupiedPackageError(msg)

    def _validate_packages_uniqueness(self, df: pandas.DataFrame):
        group_fnames = {
            PackageFieldsBase.EXPENDITURE_ORDER: "Расходный ордер",
            PackageFieldsBase.NUM: "№ паллеты"
        }
        df_g = df.groupby(list(group_fnames.keys()))
        df_v = df[df_g.transform('size') > 1]
        if df_v.empty:
            return True

        df_v = df_v[group_fnames.keys()].rename(columns=group_fnames)
        msg = (f"Обнаружены дубли номеров паллет на листе \"{IN_PACKAGES_WORKSHEET}\".\n"
               f"{df_v.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise NonUniquePackageError(msg)

    def _validate_items(self, df: pandas.DataFrame, validation_fields: bidict):
        self._validate_mandatory_fields(df, validation_fields, IN_ITEMS_WORKSHEET)

    def _validate_mandatory_fields(self, df: pandas.DataFrame, validation_fields: bidict, sheet_name: str):
        def is_empty(x):
            return pandas.isna(x) or x == ''

        s_v = df[validation_fields].map(is_empty).any(axis='columns')
        if not s_v.any():
            return

        df_v = df[s_v][validation_fields].rename(columns=validation_fields)
        msg = (f"Обнаружены пустые значения на листе \"{sheet_name}\".\n"
               f"{df_v.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise NonUniquePackageError(msg)

    def _validate_orphaned_items(self, df_base_packages: pandas.DataFrame,
                                 df_items: pandas.DataFrame):
        columns = [
            ItemFieldsBase.EXPENDITURE_ORDER,
            ItemFieldsBase.PACKAGE_NUM,
            ItemFieldsBase.CUSTOMER_ORDER_LINE,
            ItemFieldsBase.QTY
        ]
        df = pandas.merge(
            df_items, df_base_packages, how='left',
            left_on=[ItemFieldsBase.EXPENDITURE_ORDER, ItemFieldsBase.CUSTOMER_ORDER_LINE, ItemFieldsBBXD.PACKAGE_NUM],
            right_on=[BaseFields.EXPENDITURE_ORDER, BaseFields.CUSTOMER_ORDER_LINE, PackageFieldsBBXD.NUM]
        )
        df = df[pandas.isna(df[BaseFields.EXPENDITURE_ORDER])]

        if df.empty:
            return True

        df = df[columns].rename(columns={k: v for k, v in ItemFieldsBase.map().inverse.items() if k in columns})
        msg = (f"Для позиций на листе \"{IN_ITEMS_WORKSHEET}\" не обнаружены соответствия "
               f"по (1) номеру расходного ордера, "
               f"либо (2) номеру строки заказа, "
               f"либо (3) номеру паллеты:\n"
               f"{df.fillna('').to_markdown(index=False, tablefmt="github")}")
        raise OrphanedItemsError(msg)

