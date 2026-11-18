from enum import StrEnum


class QuerySuccessorIcMap(StrEnum):
    IC_UUID = 'ic_uuid'
    IC = 'ic'
    IC_LIFECYCLE_STATUS = 'lifecycle_status'
    IC_PRIORITY = 'priority'
    NOMENCLATURE_UUID = 'nomenclature_uuid'
    ARTICLE = 'article'
    SUCCESSOR_IC_UUID = 'successor_ic_uuid'
    SUCCESSOR_IC = 'successor_ic'
    SUCCESSOR_IC_LIFECYCLE_STATUS = 'successor_ic_lifecycle_status'
    SUCCESSOR_IC_PRIORITY = 'successor_ic_priority'


class QueryIcPackageMap(StrEnum):
    SKU_UUID = 'sku_uuid'
    SKU = 'sku'
    NAME = 'name'
    FULL_NAME = 'full_name'
    TNVED = 'tnved'
    OKPD2 = 'okpd2'
    IC_UUID = 'ic_uuid'
    IC = 'ic'
    IC_LIFECYCLE_STATUS = 'lifecycle_status'
    IC_PRIORITY = 'priority'
    CONTAINS_BATTERY = 'contains_battery'
    SUPPLIER_UUID = 'supplier_uuid'
    SUPPLIER_NAME = 'supplier_name'
    EAN = 'ean'
    LEVEL = 'level'
    QTY = 'qty'
    LENGTH = 'length'
    WIDTH = 'width'
    HEIGHT = 'height'
    WEIGHT = 'weight'


class QuerySKUCertificateMap(StrEnum):
    CERT_UUID = 'cert_uuid'
    SKU_UUID = 'sku_uuid'
    SKU_CODE = 'sku_code'
    SKU = 'sku'
    SKU_NAME = 'sku_name'
    CERT_NAME = 'cert_name'
    CERT_NUM = 'cert_num'
    CERT_TYPE = 'cert_type'
    CERT_ACTIVE_FROM = 'cert_active_from'
    CERT_ACTIVE_TILL = 'cert_active_till'
    CERT_IS_UNLIMITED = 'cert_is_unlimited'


class QuerySKUCommodityCertificatesPivotedMap(StrEnum):
    SKU_UUID = 'sku_uuid'
    SKU_CODE = 'sku_code'
    SKU = 'sku'
    CONF_CERT_NAME = 'conf_cert_name'
    CONF_CERT_NUM = 'conf_cert_num'
    CONF_CERT_ACTIVE_FROM = 'conf_cert_active_from'
    CONF_CERT_ACTIVE_TILL = 'conf_cert_active_till'
    CONF_DECL_CERT_NAME = 'conf_decl_cert_name'
    CONF_DECL_CERT_NUM = 'conf_decl_cert_num'
    CONF_DECL_CERT_ACTIVE_FROM = 'conf_decl_cert_active_from'
    CONF_DECL_CERT_ACTIVE_TILL = 'conf_decl_cert_active_till'
