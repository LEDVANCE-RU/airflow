from enum import StrEnum


class ApiResponseField(StrEnum):
    RESULT = 'result'


class ProductListField(StrEnum):
    PRODUCTS = 'goods'


class ProductListItemField(StrEnum):
    GOOD_ID = 'good_id'
    ETAG = 'etag'


class ProductField(StrEnum):
    NAME = 'good_name'
    STATUS = 'good_status'
    IS_SIGNED = 'good_signed'
    CREATED_AT = 'create_date'
    UPDATED_AT = 'update_date'
    IDENTITY = 'identified_by'
    ATTRS = 'good_attrs'


class ProductAttrField(StrEnum):
    NAME = 'attr_name'
    VALUE = 'attr_value'


class ProductAttrName(StrEnum):
    TNVED = 'Код ТНВЭД'
    OKPD2 = 'Код ОКПД2'
    VARCHAR = 'Тип переменной характеристики'
    CONF_CERT = 'Сертификат соответствия'
    CONF_DECL_CERT = 'Декларация о соответствии'


class ProductIdentityField(StrEnum):
    VALUE = 'value'
