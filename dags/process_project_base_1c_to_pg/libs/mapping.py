from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    type: str


class ProjectBaseFieldsMap:
    @classmethod
    def src_map(cls) -> dict[str, str]:
        return {
            'Проект': 'opp_num_header',
            'Проект.Number': 'opp_id',
            'Название проекта': 'opp_name',
            'Номер (старый)': 'opp_id_old',
            'Номер в системе партнера': 'opp_partner_id',
            'Партнер': 'partner',
            'Ответственный': 'responsible',
            'Заказчик': 'customer',
            'Генподрядчик': 'general_contractor',
            'Состояние проекта': 'status',
            'Бизнес-регион': 'region',
            'Плановая дата начала': 'start_date',
            'Плановая дата окончания': 'closing_date',
            'Адрес объекта': 'facility_adress',
            'Отрасль': 'verticals',
            'Подотрасль': 'sub_vertical',
            'Последняя актуализация': 'last_refresh_date',
            'Причина отказа': 'reason_for_rejection',
            'Причина отказа (комментарий)': 'reason_comment',
            'Источник проекта': 'project_origin',
            'Маркетинговое мероприятие': 'marketing_event',
            'Тип объекта': 'object_type',
            'Вид инициативы': 'type_of_initiative',
            'Наименование объекта': 'description',
            'Этап объекта': 'project_stage',
            'Дата начала строительства': 'construction_start_date',
            'Дата окончания строительства': 'construction_completion_date',
            'Тендер': 'tender',
            'Дата начала действия скидки': 'discount_effective_date',
            'Дата окончания действия скидки': 'discount_expiration_date',
            'Головной проект': 'parent_project',
            'Автор': 'author',
            'Плановый бюджет проекта': 'budget',
        }

    @classmethod
    def dest_map(cls) -> dict[str, Field]:
        return {
            'opp_num_header': Field('opp_num_header', 'character varying(1000)'),
            'opp_id': Field('opp_id', 'character varying(200)'),
            'opp_name': Field('opp_name', 'character varying(500)'),
            'opp_id_old': Field('opp_id_old', 'character varying(50)'),
            'opp_partner_id': Field('opp_partner_id', 'character varying(100)'),
            'partner': Field('partner', 'character varying(500)'),
            'responsible': Field('responsible', 'character varying(200)'),
            'customer': Field('customer', 'character varying(200)'),
            'general_contractor': Field('general_contractor', 'character varying(200)'),
            'status': Field('status', 'character varying(100)'),
            'region': Field('region', 'character varying(500)'),
            'start_date': Field('start_date', 'date'),
            'closing_date': Field('closing_date', 'date'),
            'facility_adress': Field('facility_adress', 'character varying(500)'),
            'verticals': Field('verticals', 'character varying(200)'),
            'sub_vertical': Field('sub_vertical', 'character varying(200)'),
            'last_refresh_date': Field('last_refresh_date', 'date'),
            'reason_for_rejection': Field('reason_for_rejection', 'character varying(1000)'),
            'reason_comment': Field('reason_comment', 'character varying(1000)'),
            'project_origin': Field('project_origin', 'character varying(200)'),
            'marketing_event': Field('marketing_event', 'character varying(1000)'),
            'object_type': Field('object_type', 'character varying(500)'),
            'type_of_initiative': Field('type_of_initiative', 'character varying(500)'),
            'description': Field('description', 'character varying(1000)'),
            'project_stage': Field('project_stage', 'character varying(500)'),
            'construction_start_date': Field('construction_start_date', 'date'),
            'construction_completion_date': Field('construction_completion_date', 'date'),
            'tender': Field('tender', 'boolean'),
            'discount_effective_date': Field('discount_effective_date', 'date'),
            'discount_expiration_date': Field('discount_expiration_date', 'date'),
            'parent_project': Field('parent_project', 'boolean'),
            'author': Field('author', 'character varying(200)'),
            'budget': Field('budget', 'numeric(20, 2)'),
        }


