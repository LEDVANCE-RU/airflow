import uuid

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from db_model.constants import ONEC_EXTRACT_SCHEMA
from db_model.main import Base
from db_model.types import NullableUUID, NullableUUIDString, DateTimeMSK


class AbstractBaseModel(Base):
    __abstract__ = True
    __table_args__ = {'schema': ONEC_EXTRACT_SCHEMA}


class DeletionMarkMixin:
    is_deleted = sa.Column(sa.Boolean, name='PometkaUdaleniya')


class Nomenclature(AbstractBaseModel, DeletionMarkMixin):
    __tablename__ = "Nomenklatura"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    code = sa.Column(sa.String, name='Kod')
    article = sa.Column(sa.String, name='Artikul')
    full_name = sa.Column(sa.String, name='NaimenovaniePolnoe')
    tnved = sa.Column(sa.String, name='KodTNVED')
    okpd2 = sa.Column(sa.String, name='KodOKPD2')


class Ic(AbstractBaseModel, DeletionMarkMixin):
    __tablename__ = "KHarakteristikiNomenklatury"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    nomenclature_uuid = sa.Column(UUID(as_uuid=True), name='VladeletsGuid')
    name = sa.Column(sa.String, name='Naimenovanie')
    lifecycle_status = sa.Column(sa.String, name='WA_StatusZHiznennogoTSikla')
    priority = sa.Column(sa.Integer, name='WA_Prioritet')
    contains_battery = sa.Column(sa.Boolean, name='erpit_SoderzhitBatareykiAkkumulyatory')
    supplier_uuid = sa.Column(UUID(as_uuid=True), name='WA_PostavshchikGuid')


class Packages(AbstractBaseModel, DeletionMarkMixin):
    __tablename__ = "UpakovkiEdinitsyIzmereniya"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    ean = sa.Column(sa.String, name='WA_SHtrikhkod')
    nomenclature_uuid = sa.Column(UUID(as_uuid=True), name='VladeletsGuid')
    ic_name = sa.Column(sa.String, name='WA_KHarakteristika')
    level = sa.Column(sa.String, name='WA_UrovenUpakovki')
    numerator = sa.Column(sa.Numeric, name='CHislitel')
    denominator = sa.Column(sa.Numeric, name='Znamenatel')
    height = sa.Column(sa.Numeric, name='Vysota')
    width = sa.Column(sa.Numeric, name='SHirina')
    length = sa.Column(sa.Numeric, name='Glubina')
    weight = sa.Column(sa.Numeric, name='Ves')


class PackagesDepartments(AbstractBaseModel):
    __tablename__ = "UpakovkiEdinitsyIzmereniyaWA_Podrazdeleniya"

    package_uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', default=uuid.uuid4, primary_key=True)
    department_uuid = sa.Column(UUID(as_uuid=True), name='"PodrazdelenieGuid"', default=uuid.uuid4,
                                primary_key=True)


class WmsStockHistory(AbstractBaseModel):
    __tablename__ = "WA_OstatkiIzWMSIstoriya"

    nomenclature_uuid = sa.Column(NullableUUIDString, name='NomenklaturaGuid', primary_key=True)
    ic_uuid = sa.Column(NullableUUID(as_uuid=True), name='KHarakteristikaGuid', primary_key=True)
    period = sa.Column(DateTimeMSK, name='Period', primary_key=True)
    cut_date = sa.Column(DateTimeMSK, name='DataSreza')
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
    yet_to_arrive_qty = sa.Column(sa.Numeric, name='KOformleniyuNakladnykhPoRasporyazheniyuOstatok')


class StockHistory(AbstractBaseModel):
    __tablename__ = "TovaryNaSkladakhOstatki"

    stock_date = sa.Column(DateTimeMSK, name='DataOstatka', primary_key=True)
    warehouse_uuid = sa.Column(NullableUUID(as_uuid=True), name='SkladGuid', primary_key=True)
    nomenclature_uuid = sa.Column(UUID(as_uuid=True), name='NomenklaturaGuid', primary_key=True)
    ic_uuid = sa.Column(NullableUUID(as_uuid=True), name='KHarakteristikaGuid', primary_key=True)
    stock = sa.Column(sa.Numeric, name='VNalichiiOstatok')
    stock_to_issue = sa.Column(sa.Numeric, name='KOtgruzkeOstatok')


class Warehouse(AbstractBaseModel):
    __tablename__ = "Sklady"

    warehouse_uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', primary_key=True)
    name = sa.Column(sa.String, name='Naimenovanie')


class Partner(AbstractBaseModel, DeletionMarkMixin):
    __tablename__ = "Partnery"

    uuid = sa.Column(UUID(as_uuid=True), name='SsylkaGuid', primary_key=True)
    name = sa.Column(sa.String, name='Naimenovanie')
