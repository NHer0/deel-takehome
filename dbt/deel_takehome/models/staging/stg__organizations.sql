{{
    config(
        materialized='view',
        schema='staging'
    )
}}

{% set organizations_raw = source('landing', 'organizations') %}

with curated as (
    select
        abs(organization_id) as organization_id,
        first_payment_date,
        last_payment_date,
        abs(legal_entity_country_code) as legal_entity_country_code,
        count_total_contracts_active,
        convert_timezone('UTC', created_date)::timestamp_ntz as created_at_utc
    from {{ organizations_raw }}
)

select * from curated
