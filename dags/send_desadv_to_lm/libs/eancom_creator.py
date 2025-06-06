import copy
from datetime import datetime

import pandas
from bs4 import BeautifulSoup, Tag

from send_desadv_to_lm.libs.field_map import BaseFields, PackageFieldsBase, ItemFieldsBase


class Constants:
    DESADV_MSG_TYPE_NODE = BeautifulSoup(
        f"""
            <S009>
                <E0065>DESADV</E0065>
                <E0052>D</E0052>
                <E0054>01B</E0054>
                <E0051>UN</E0051>
                <E0057>EAN010</E0057>
            </S009>
        """,
        features='xml'
    )
    DESADV_QUALIFICATOR_NODE = BeautifulSoup(
        f"""
            <C002>
                <E1001>351</E1001>
            </C002>
        """,
        features='xml'
    )
    DESADV_FUNC_CODE = BeautifulSoup(
        "<E1225>9</E1225>",
        features='xml'
    )
    DESADV_DELIVERY_CONDITIONS_NODE = BeautifulSoup(
        f"""
            <ALI>
                <E4183>NO</E4183>
            </ALI>
        """,
        features='xml'
    )
    DESADV_SIDE_UNLOADING_NODE = BeautifulSoup(
        f"""
            <FTX>
                <E4451>OSI</E4451>
                <C108>
                    <E4440>NO</E4440>
                </C108>
            </FTX>
        """,
        features='xml'
    )
    SSCC_PACKAGE_NODE = BeautifulSoup(
        f"""
            <PCI>
                <E4233>33E</E4233>
            </PCI>        
        """,
        features='xml'
    )
    SSCC_TYPE_NODE = BeautifulSoup("<E7405>BJ</E7405>", features='xml')
    EAN_TYPE_NODE = BeautifulSoup("<E7143>SRV</E7143>", features='xml')
    CUSTOMERS_ARTICLE_TYPE_NODE = BeautifulSoup("<E7143>IN</E7143>", features='xml')
    ITEM_VAT_NODE = BeautifulSoup(
        f"""
            <FTX>
                <E4451>ZZZ</E4451>
                <C108>
                    <E4440>20</E4440>
                </C108>
            </FTX>   
        """,
        features='xml'
    )


class DesadvCreator:
    def __init__(self, data: pandas.DataFrame):
        self.data = data
        self._soup = None

        self._root = None
        self._header = None

    def generate(self):
        self._soup = BeautifulSoup(features='xml')
        self._generate_root_node()
        self._generate_header_node()
        return copy.deepcopy(self._root)

    @property
    def _h(self) -> pandas.Series:
        """Return header row."""
        return self.data.iloc[0]

    def _generate_root_node(self):
        self._root = self._soup.new_tag('DESADV')  # type: Tag

    def _generate_header_node(self):
        msg_type_node = self._generate_msg_type_node(str(self._h[BaseFields.INVOICE]))
        msg_function_node = self._generate_msg_function_node(str(self._h[BaseFields.INVOICE]))
        doc_date_node = self._generate_date_node('137', self._h[BaseFields.INVOICE_DATE])
        actual_delivery_date_node = self._generate_date_node('11', self._h[BaseFields.INVOICE_DATE])
        delivery_date_node = self._generate_date_node('17', self._h[BaseFields.DELIVERY_DATE])
        doc_sum_with_vat_node = self._generate_sum_node('86', str(self._h[BaseFields.INVOICE_SUM_WITH_VAT]))
        doc_sum_without_vat_node = self._generate_sum_node('125', str(self._h[BaseFields.INVOICE_SUM_WITHOUT_VAT]))
        doc_sum_vat_node = self._generate_sum_node('124', str(self._h[BaseFields.INVOICE_VAT]))
        order_info_node = self._generate_related_doc_node('ON',
                                                          str(self._h[BaseFields.CUSTOMER_ORDER_NUM]),
                                                          self._h[BaseFields.CUSTOMER_ORDER_DATE])
        utd_info_node = self._generate_related_doc_node('DQ', str(self._h[BaseFields.INVOICE]),
                                                        self._h[BaseFields.INVOICE_DATE])

        delivery_note = self._h[BaseFields.DELIVERY_NOTE]
        delivery_note_info_node = None
        if delivery_note and not pandas.isna(delivery_note):
            delivery_note_info_node = self._generate_related_doc_node(
                'BM',
                str(self._h[BaseFields.DELIVERY_NOTE]),
                self._h[BaseFields.DELIVERY_NOTE_DATE]
            )

        buyer_node = self._generate_participant_node('BY', str(self._h[BaseFields.BUYER_GLN]))
        seller_node = self._generate_participant_node('SU', str(self._h[BaseFields.SELLER_GLN]))
        delivery_point_node = self._generate_participant_node('DP', str(self._h[BaseFields.SHIP_TO_GLN]))
        end_customer_node = self._generate_participant_node('UD', str(self._h[BaseFields.END_CUSTOMER_GLN]))
        package_node = self._generate_package_node('1', '1')
        subpackage_nodes = self._generate_subpackage_nodes()
        total_sections_node = self._generate_total_sections_node(str(self._h[BaseFields.INVOICE]))
        self._root.append(msg_type_node)
        self._root.append(msg_function_node)
        self._root.append(doc_date_node)
        self._root.append(actual_delivery_date_node)
        self._root.append(delivery_date_node)
        self._root.append(copy.deepcopy(Constants.DESADV_DELIVERY_CONDITIONS_NODE))
        self._root.append(copy.deepcopy(Constants.DESADV_SIDE_UNLOADING_NODE))
        self._root.append(doc_sum_with_vat_node)
        self._root.append(doc_sum_without_vat_node)
        self._root.append(doc_sum_vat_node)
        self._root.append(order_info_node)
        self._root.append(utd_info_node)
        if delivery_note_info_node:
            self._root.append(delivery_note_info_node)
        self._root.append(buyer_node)
        self._root.append(seller_node)
        self._root.append(delivery_point_node)
        self._root.append(end_customer_node)
        self._root.append(package_node)
        self._root.extend(subpackage_nodes)
        self._root.append(total_sections_node)

    def _generate_msg_type_node(self, msg_num: str):
        node = self._soup.new_tag('UNH')
        num_node = self._soup.new_tag('E0062')
        num_node.string = msg_num
        node.append(num_node)
        node.append(copy.deepcopy(Constants.DESADV_MSG_TYPE_NODE))
        return node

    def _generate_msg_function_node(self, msg_num: str):
        node = self._soup.new_tag('BGM')
        node.append(copy.deepcopy(Constants.DESADV_QUALIFICATOR_NODE))
        subnode = self._soup.new_tag('C106')
        num_node = self._soup.new_tag('E1004')
        num_node.string = msg_num
        subnode.append(num_node)
        node.append(subnode)
        node.append(copy.deepcopy(Constants.DESADV_FUNC_CODE))
        return node

    def _generate_date_node(self, date_qual: str, date_value: datetime):
        node = self._soup.new_tag('DTM')
        subnode = self._soup.new_tag('C507')
        qual_node = self._soup.new_tag('E2005')
        qual_node.string = date_qual
        date_node = self._soup.new_tag('E2380')
        date_node.string = date_value.strftime('%Y%m%d')
        format_node = self._soup.new_tag('E2379')
        format_node.string = '102'
        subnode.append(qual_node)
        subnode.append(date_node)
        subnode.append(format_node)
        node.append(subnode)
        return node

    def _generate_sum_node(self, sum_qual: str, value: str):
        node = self._soup.new_tag('MOA')
        subnode = self._soup.new_tag('C516')
        qual_node = self._soup.new_tag('E5025')
        qual_node.string = sum_qual
        sum_node = self._soup.new_tag('E5004')
        sum_node.string = value

        subnode.append(qual_node)
        subnode.append(sum_node)

        node.append(subnode)
        return node

    def _generate_related_doc_node(self, doc_type: str, doc_num: str, doc_date: datetime):
        node = self._soup.new_tag('SG1')
        subnode = self._soup.new_tag('RFF')
        doc_node = self._soup.new_tag('C506')
        qual_node = self._soup.new_tag('E1153')
        qual_node.string = doc_type
        num_node = self._soup.new_tag('E1154')
        num_node.string = doc_num

        doc_node.append(qual_node)
        doc_node.append(num_node)
        subnode.append(doc_node)

        date_node = self._generate_date_node('171', doc_date)

        node.append(subnode)
        node.append(date_node)
        return node

    def _generate_participant_node(self, participant_role: str, gln: str):
        node = self._soup.new_tag('SG2')
        subnode = self._soup.new_tag('NAD')

        qual_node = self._soup.new_tag('E3035')
        qual_node.string = participant_role

        id_node = self._soup.new_tag('C082')
        gln_node = self._soup.new_tag('E3039')
        gln_node.string = gln
        gln_qual_node = self._soup.new_tag('E3055')
        gln_qual_node.string = '9'
        id_node.append(gln_node)
        id_node.append(gln_qual_node)

        subnode.append(qual_node)
        subnode.append(id_node)

        node.append(subnode)
        return node

    def _generate_package_node(self, package_id: str, doc_count: str):
        node = self._soup.new_tag('SG10')

        level_node = self._soup.new_tag('CPS')
        package_id_node = self._soup.new_tag('E7164')
        package_id_node.string = package_id
        level_node.append(package_id_node)

        doc_count_header_node = self._soup.new_tag('SG11')
        doc_count_subnode = self._soup.new_tag('PAC')
        doc_count_node = self._soup.new_tag('E7224')
        doc_count_node.string = doc_count
        doc_count_subnode.append(doc_count_node)
        doc_count_header_node.append(doc_count_subnode)

        node.append(level_node)
        node.append(doc_count_header_node)
        return node

    def _generate_subpackage_nodes(self):
        nodes = []
        for subpackage_num in self.data[ItemFieldsBase.PACKAGE_NUM].unique():
            df_items = self.data[self.data[ItemFieldsBase.PACKAGE_NUM] == subpackage_num]
            h = df_items.iloc[0]
            node = self._generate_subpackage_node(package_id=int(subpackage_num),
                                                  parent_package_id='1',
                                                  package_type=str(h[PackageFieldsBase.TYPE]),
                                                  doc_count='1',
                                                  sscc_code=h[PackageFieldsBase.SSCC],
                                                  length=str(h[PackageFieldsBase.LENGTH]),
                                                  width=str(h[PackageFieldsBase.WIDTH]),
                                                  height=str(h[PackageFieldsBase.HEIGHT]),
                                                  weight=str(h[PackageFieldsBase.WEIGHT]),
                                                  df_items=df_items)
            nodes.append(node)
        return nodes

    def _generate_subpackage_node(self, package_id: int, parent_package_id: str, package_type: str, doc_count: str,
                                  sscc_code: str, length: str, width: str, height: str, weight: str,
                                  df_items: pandas.DataFrame):
        node = self._soup.new_tag('SG10')

        level_node = self._soup.new_tag('CPS')
        package_id_node = self._soup.new_tag('E7164')
        package_id_node.string = str(package_id + 1)
        parent_package_node = self._soup.new_tag('E7166')
        parent_package_node.string = parent_package_id
        level_node.append(package_id_node)
        level_node.append(parent_package_node)

        package_info_node = self._soup.new_tag('SG11')

        doc_count_1_node = self._soup.new_tag('PAC')
        doc_count_2_node = self._soup.new_tag('E7224')
        doc_count_2_node.string = doc_count
        package_type_header_node = self._soup.new_tag('C202')
        package_type_node = self._soup.new_tag('E7065')
        package_type_node.string = package_type
        package_type_header_node.append(package_type_node)
        doc_count_1_node.append(doc_count_2_node)
        doc_count_1_node.append(package_type_node)

        length_node = self._generate_weight_and_dimensions_node('LN', length)
        width_node = self._generate_weight_and_dimensions_node('WD', width)
        height_node = self._generate_weight_and_dimensions_node('HT', height)
        weight_node = self._generate_weight_and_dimensions_node('AAB', weight)
        sscc_node = self._generate_sscc_node(sscc_code)

        package_info_node.append(doc_count_1_node)
        package_info_node.append(length_node)
        package_info_node.append(width_node)
        package_info_node.append(height_node)
        package_info_node.append(weight_node)
        package_info_node.append(sscc_node)

        node.append(level_node)
        node.append(package_info_node)

        for idx, row in df_items.iterrows():
            item_node = self._generate_item_node(line_num=str(row[BaseFields.CUSTOMER_ORDER_LINE]),
                                                 ean=str(row[BaseFields.ARTICLE]),
                                                 customers_article_code=str(row[BaseFields.CUSTOMERS_ARTICLE]),
                                                 article_name=str(row[BaseFields.ITEM_NAME]),
                                                 issued_qty=str(row[ItemFieldsBase.QTY]))
            node.append(item_node)

        return node

    def _generate_weight_and_dimensions_node(self, type_: str, value: str):
        node = self._soup.new_tag('MEA')

        qual_node = self._soup.new_tag('E6311')
        qual_node.string = 'PD'

        type_header_node = self._soup.new_tag('C502')
        type_node = self._soup.new_tag('E6313')
        type_node.string = type_
        type_header_node.append(type_node)

        value_header_node = self._soup.new_tag('C174')
        value_node = self._soup.new_tag('E6314')
        value_node.string = value
        value_header_node.append(value_node)

        node.append(qual_node)
        node.append(type_header_node)
        node.append(value_header_node)

        return node

    def _generate_sscc_node(self, code: str):
        node = self._soup.new_tag('SG13')

        subnode = self._soup.new_tag('SG15')

        sscc_1_node = self._soup.new_tag('GIN')
        sscc_2_node = self._soup.new_tag('C208')
        sscc_3_node = self._soup.new_tag('E7402')
        sscc_3_node.string = code
        sscc_2_node.append(sscc_3_node)
        sscc_1_node.append(copy.deepcopy(Constants.SSCC_TYPE_NODE))
        sscc_1_node.append(sscc_2_node)

        subnode.append(sscc_1_node)

        node.append(copy.deepcopy(Constants.SSCC_PACKAGE_NODE))
        node.append(subnode)
        return node

    def _generate_total_sections_node(self, msg_id: str):
        node = self._soup.new_tag('UNT')

        cnt_node = self._soup.new_tag('E0074')
        cnt_node.string = 'null'
        msg_id_node = self._soup.new_tag('E0062')
        msg_id_node.string = msg_id

        node.append(cnt_node)
        node.append(msg_id_node)
        return node

    def _generate_item_node(self, line_num: str, ean: str, customers_article_code: str, article_name: str,
                            issued_qty: str):
        node = self._soup.new_tag('SG17')

        line_node = self._generate_line_node(line_num, ean)
        customers_article_code_node = self._generate_customers_article_code_node(customers_article_code)
        article_name_node = self._generate_article_name_node(article_name)
        issued_qty_node = self._generate_qty_node('12', issued_qty, 'PCE')
        # ordered_qty_node = self._generate_qty_node('21', '', '')  # TODO
        # qty_in_package_node = self._generate_qty_node('59', '', '')  # TODO
        # sum_without_vat_node = self._generate_sum_node('203', '')  # TODO
        # sum_vat_node = self._generate_sum_node('124', '')  # TODO
        # sum_with_vat_node = self._generate_sum_node('79', '')  # TODO
        # price_without_vat_node = self._generate_sum_node('146', '')  # TODO
        # price_with_vat_node = self._generate_sum_node('XB5', '')  # TODO

        node.append(line_node)
        node.append(customers_article_code_node)
        node.append(article_name_node)
        node.append(issued_qty_node)
        # node.append(ordered_qty_node)
        # node.append(qty_in_package_node)
        # node.append(Constants.ITEM_VAT_NODE)
        # node.append(sum_without_vat_node)
        # node.append(sum_vat_node)
        # node.append(sum_with_vat_node)
        # node.append(price_without_vat_node)
        # node.append(price_with_vat_node)
        return node

    def _generate_line_node(self, line_num: str, ean: str):
        node = self._soup.new_tag('LIN')

        line_node = self._soup.new_tag('E1082')
        line_node.string = line_num

        ean_1_node = self._soup.new_tag('C212')
        ean_2_node = self._soup.new_tag('E7140')
        ean_2_node.string = ean
        ean_1_node.append(ean_2_node)
        ean_1_node.append(copy.deepcopy(Constants.EAN_TYPE_NODE))

        node.append(line_node)
        node.append(ean_1_node)
        return node

    def _generate_customers_article_code_node(self, code: str):
        node = self._soup.new_tag('PIA')

        extra_id_type_node = self._soup.new_tag('E4347')
        extra_id_type_node.string = '1'

        code_1_node = self._soup.new_tag('C212')
        code_2_node = self._soup.new_tag('E7140')
        code_2_node.string = code
        code_1_node.append(code_2_node)
        code_1_node.append(copy.deepcopy(Constants.CUSTOMERS_ARTICLE_TYPE_NODE))

        node.append(extra_id_type_node)
        node.append(code_1_node)
        return node

    def _generate_article_name_node(self, name: str):
        node = self._soup.new_tag('IMD')

        article_name_type_node = self._soup.new_tag('E7077')
        article_name_type_node.string = 'F'

        name_1_node = self._soup.new_tag('C273')
        name_2_node = self._soup.new_tag('E7008')
        name_2_node.string = name
        name_1_node.append(name_2_node)
        name_1_node.append(copy.deepcopy(Constants.CUSTOMERS_ARTICLE_TYPE_NODE))

        node.append(article_name_type_node)
        node.append(name_1_node)
        return node

    def _generate_qty_node(self, qual: str, qty: str, uom: str):
        node = self._soup.new_tag('QTY')
        subnode = self._soup.new_tag('C186')

        qual_node = self._soup.new_tag('E6063')
        qual_node.string = qual
        qty_node = self._soup.new_tag('E6060')
        qty_node.string = qty
        uom_node = self._soup.new_tag('E6411')
        uom_node.string = uom

        subnode.append(qual_node)
        subnode.append(qty_node)
        subnode.append(uom_node)

        node.append(subnode)
        return node
