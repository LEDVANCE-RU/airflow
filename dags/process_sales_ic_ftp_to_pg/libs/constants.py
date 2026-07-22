SALES_DELETE_SQL = '''
delete from sales.since2024_current ss where ss.period in (select distinct period from sales.rawdata_since2024);
'''

SALES_INSERT_SQL = '''
insert into sales.since2024_current (
    period,
    ean,
    name,
    ic,
    customer,
    customer_id,
    agreement,
    customer_corr,
    customer_id_corr,
    aug_key,
    project_ic,
    pcs,
    niv,
    ns,
    coef
)
with bu_corr as (
    select distinct
        mp.ean,
            CASE mp.bu
                WHEN 'L3 - LED Components' THEN 'LED comp'
                WHEN 'L4 - LUM' THEN 'LUM'
                WHEN 'L5 - ECS' THEN 'ECS'
                WHEN 'L6 - Trad. Lamps' THEN 'TRAD'
                WHEN 'L7 - LED Lamps' THEN 'LED'
                WHEN 'L8 - CM CS' THEN 'CM CS'
                WHEN 'L9 - Lamps CC' THEN 'Lamps CC'
                ELSE mp.bu
            END AS bu
    from md.products mp
),
coeffs as (
    select
        customer_id_1c,
        bu,
        "01.01.2024" as c2024,
        "01.01.2025" as c2025q1,
        "01.04.2025" as c2025q2,
        "01.07.2025" as c2025q3,
        "01.10.2025" as c2025q4,
        "01.01.2026" as c2026q1,
        "01.05.2026" as c2026may,
        "01.06.2026" as c2026june
    from md.md_for_ns
),
corr_sr as (
    select
        sr.period,
        sr.ean,
        sr.name,
        sr.ic,
        sr.customer,
        sr.customer_id,
        sr.agreement,
        case
            when sr.agreement = 'ЛМ_МАРКЕТ_ПЛЕЙС' then 'ЛМ маркетплейс'
            when sr.project_ic like '%ADEO%' then 'ЛМ ADEO'
            when sr.customer like '%Электротехмонтаж%' then 'ЭЛЕКТРОТЕХМОНТАЖ ТД АО'
            when sr.customer_id in ('1100271', '1100276', '1100295', '1100296', '00-ЦБ000102') then 'ДОМЛЕНТА ООО'
            else sr.customer
        end as customer_corr,
        case
            when sr.agreement = 'ЛМ_МАРКЕТ_ПЛЕЙС' then '1111930'
            when sr.project_ic like '%ADEO%' then '1111498'
            when sr.customer_id = '1100532' then '00-00000531'
            else sr.customer_id
        end as customer_id_corr,
        sr.aug_key,
        sr.project_ic,
        sr.pcs,
        sr.niv,
        bc.bu
    from sales.rawdata_since2024 sr
    left join bu_corr bc on bc.ean = sr.ean
)
select
    sr.period,
    sr.ean,
    sr.name,
    sr.ic,
    sr.customer,
    sr.customer_id,
    sr.agreement,
    sr.customer_corr,
    sr.customer_id_corr,
    sr.aug_key,
    sr.project_ic,
    sr.pcs,
    sr.niv,
    sr.niv * (1 - coalesce(
        case
            when sr.period between '2024-01-01'::date and '2024-12-31'::date then cf.c2024
            when sr.period between '2025-01-01'::date and '2025-03-31'::date then cf.c2025q1
            when sr.period between '2025-04-01'::date and '2025-06-30'::date then cf.c2025q2
            when sr.period between '2025-07-01'::date and '2025-09-30'::date then cf.c2025q3
            when sr.period between '2025-10-01'::date and '2025-12-31'::date then cf.c2025q4
            when sr.period between '2026-01-01'::date and '2026-04-30'::date then cf.c2026q1
            when sr.period between '2026-05-01'::date and '2026-05-31'::date then cf.c2026may
            when sr.period >= '2026-06-01'::date then cf.c2026june
            else 0
        end, 0)
    ) as ns,
    coalesce(
        case
            when sr.period between '2024-01-01'::date and '2024-12-31'::date then cf.c2024
            when sr.period between '2025-01-01'::date and '2025-03-31'::date then cf.c2025q1
            when sr.period between '2025-04-01'::date and '2025-06-30'::date then cf.c2025q2
            when sr.period between '2025-07-01'::date and '2025-09-30'::date then cf.c2025q3
            when sr.period between '2025-10-01'::date and '2025-12-31'::date then cf.c2025q4
            when sr.period between '2026-01-01'::date and '2026-04-30'::date then cf.c2026q1
            when sr.period between '2026-05-01'::date and '2026-05-31'::date then cf.c2026may
            when sr.period >= '2026-06-01'::date then cf.c2026june
            else 0
        end, 0
    ) as coef
from corr_sr sr
left join coeffs cf on sr.customer_id_corr = cf.customer_id_1c and sr.bu = cf.bu;
'''


