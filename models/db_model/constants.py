from enum import StrEnum

ONEC_EXTRACT_SCHEMA = '1c_extract'
MAIN_SCHEMA = 'public'
MARKETPROVIDER_SCHEMA = 'marketprovider'


class PackageLevels(StrEnum):
    PCE = 'PCE'
    EAN10 = 'EAN10'
    EAN20 = 'EAN20'
    EAN40 = 'EAN40'
    EAN50 = 'EAN50'
    EAN60 = 'EAN60'
