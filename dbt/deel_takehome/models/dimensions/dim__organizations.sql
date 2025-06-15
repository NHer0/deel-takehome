{{
    config(
        materialized='table',
        schema='dimensions'
    )
}}

{% set stg_organizations = ref('stg__organizations') %}

with organizations_context as (
    select
        organization_id,
        legal_entity_country_code,
        count_total_contracts_active,
        first_payment_date,
        last_payment_date,
        created_at_utc,
        datediff('day', created_at_utc, current_date) as organization_age_days,
        datediff('day', last_payment_date, current_date) as days_since_last_payment,
        case 
            when count_total_contracts_active = 1 then 'single_contract'
            when count_total_contracts_active <= 5 then 'small_portfolio'
            when count_total_contracts_active <= 20 then 'medium_portfolio'
            else 'large_portfolio'
        end as contract_portfolio_size,
        case 
            when datediff('day', created_at_utc, current_date) <= 30 then 'new'
            when datediff('day', created_at_utc, current_date) <= 180 then 'growing'
            when datediff('day', created_at_utc, current_date) <= 365 then 'established'
            else 'mature'
        end as organization_maturity
    from {{ stg_organizations }}
)

select * from organizations_context