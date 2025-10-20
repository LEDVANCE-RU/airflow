import uuid

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from models.db.constants import ONEC_EXTRACT_SCHEMA
from models.db.main import Base
from models.db.types import NullableUUID, NullableUUIDString


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': ONEC_EXTRACT_SCHEMA}


class Nomenclature(AbstractBaseModel):
    __tablename__ = "Nomenklatura"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    article = sa.Column(sa.String, name='Artikul')
    full_name = sa.Column(sa.String, name='NaimenovaniePolnoe')


class Ic(AbstractBaseModel):
    __tablename__ = "KHarakteristikiNomenklatury"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    nomenclature_uuid = sa.Column(UUID(as_uuid=True), name='VladeletsGuid')
    name = sa.Column(sa.String, name='Naimenovanie')
    lifecycle_status = sa.Column(sa.String, name="WA_StatusZHiznennogoTSikla")


class Packages(AbstractBaseModel):
    __tablename__ = "UpakovkiEdinitsyIzmereniya"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    nomenclature_uuid = sa.Column(UUID(as_uuid=True), name='VladeletsGuid')
    ic_name = sa.Column(sa.String, name='WA_KHarakteristika')
    numerator = sa.Column(sa.Numeric, name='CHislitel')
    denominator = sa.Column(sa.Numeric, name='Znamenatel')


class WmsStockHistory(AbstractBaseModel):
    __tablename__ = "WA_OstatkiIzWMSIstoriya"

    nomenclature_uuid = sa.Column(NullableUUIDString, name='NomenklaturaGuid', primary_key=True)
    ic_uuid = sa.Column(NullableUUID(as_uuid=True), name='KHarakteristikaGuid', primary_key=True)
    period = sa.Column(sa.DateTime, name='Period', primary_key=True)
    cut_date = sa.Column(sa.DateTime, name='DataSreza')
    status = sa.Column(sa.String, name='Sostoyanie')
    stock = sa.Column(sa.Numeric, name='Kolichestvo')
    article = sa.Column(sa.String, name='NomenklaturaArtikul')
    code = sa.Column(sa.String, name='NomenklaturaKod')


class FutureArrivalsStock(AbstractBaseModel):
    __tablename__ = "TovaryKPostupleniyuOstatki"

    nomenclature_uuid = sa.Column(UUID(as_uuid=True), name='NomenklaturaGuid', primary_key=True)
    ic_uuid = sa.Column(NullableUUID(as_uuid=True), name='KHarakteristikaGuid', primary_key=True)
    document_date = sa.Column(sa.DateTime, name='DokumentPostupleniyaData', primary_key=True)
    document_num = sa.Column(sa.String, name='DokumentPostupleniyaNomer', primary_key=True)
    document = sa.Column(sa.String, name='DokumentPostupleniya')
    warehouse_uuid = sa.Column(NullableUUID(as_uuid=True), name='SkladGuid')
    receipt_in_progress_qty = sa.Column(sa.Numeric, name='PrinimaetsyaOstatok')
