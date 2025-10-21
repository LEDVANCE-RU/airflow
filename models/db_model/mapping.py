from enum import StrEnum


class QuerySiblingIcMap(StrEnum):
    IC_UUID = 'ic_uuid'
    IC = 'ic'
    IC_LIFECYCLE_STATUS = 'lifecycle_status'
    NOMENCLATURE_UUID = 'nomenclature_uuid'
    ARTICLE = 'article'
    SIBLING_IC_UUID = 'sibling_ic_uuid'
    SIBLING_IC = 'sibling_ic'
    SIBLING_IC_LIFECYCLE_STATUS = 'sibling_ic_lifecycle_status'
