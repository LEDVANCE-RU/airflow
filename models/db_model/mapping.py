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
