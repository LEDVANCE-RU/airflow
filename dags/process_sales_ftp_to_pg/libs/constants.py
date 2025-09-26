BU_REPLACEMENTS = {
    'L9 - Lamps CC': 'Lamps CC',
    'L6 - Trad. Lamps': 'TRAD',
    'L4 - LUM': 'LUM',
    'L7 - LED Lamps': 'LED',
    'L8 - CM CS': 'CM CS',
    'L5 - ECS': 'ECS',
}

SALES_DELETE_SQL = '''
delete from sales.since_2024 ss where ss.period in (select distinct period from sales.sales_raw);
'''

SALES_INSERT_SQL = '''
insert into sales.since_2024 (
    period,
    channel,
    channel_wa,
    manager,
    ean,
    name,
    ic,
    opp_num,
    ord_opp_num,
    sales_org,
    customer,
    customer_id,
    agreement,
    country,
    currency,
    order_info,
    order_from_customer,
    order_id,
    order_date,
    shipment_date,
    opp_num_header,
    opp_id,
    opp_name,
    opp_date,
    opp_partner_id,
    opp_id_old,
    offer,
    offer_date,
    offer_id,
    responsible,
    bu,
    bs,
    ag_key,
    aug_key,
    project_ic,
    pcs,
    niv,
    ns,
    coef
)
select
    sr.period,
    sr.channel,
    sr.channel_wa,
    sr.manager,
    sr.ean,
    sr.name,
    sr.ic,
    sr.opp_num,
    sr.ord_opp_num,
    sr.sales_org,
    sr.customer,
    sr.customer_id,
    sr.agreement,
    sr.country,
    sr.currency,
    sr.order_info,
    sr.order_from_customer,
    sr.order_id,
    sr.order_date,
    sr.shipment_date,
    sr.opp_num_header,
    sr.opp_id,
    sr.opp_name,
    sr.opp_date,
    sr.opp_partner_id,
    sr.opp_id_old,
    sr.offer,
    sr.offer_date,
    sr.offer_id,
    sr.responsible,
    sr.bu,
    sr.bs,
    sr.ag_key,
    sr.aug_key,
    sr.project_ic,
    sr.pcs,
    sr.niv,
    sr.niv * (1-
    case
        when sr.period between '01.01.2024' and '31.12.2024' then mdns."01.01.2024"
        when sr.period between '01.01.2025' and '30.06.2025' then mdns."01.01.2025"
        when sr.period >= '01.07.2025' then mdns."01.07.2025"
        else 0
    end
    ) as ns,
    case
        when sr.period between '01.01.2024' and '31.12.2024' then mdns."01.01.2024"
        when sr.period between '01.01.2025' and '30.06.2025' then mdns."01.01.2025"
        when sr.period >= '01.07.2025' then mdns."01.07.2025"
        else 0
    end as coef
from sales.sales_raw sr
left join md.md_for_ns mdns on (
    sr.customer_id = mdns.customer_key 
    --and sr.channel = mdns.channel 
    and sr.bu = mdns.bu
);
'''


