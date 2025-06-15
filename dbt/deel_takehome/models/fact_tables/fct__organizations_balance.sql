{{
    config(
        materialized='table',
        schema='fact_tables'
    )
}}

{% set stg_invoices = ref('stg__invoices') %}

with daily_agg_invoice_data as (
    select
        organization_id,
        created_at_utc::date as balance_date,
        sum(payment_amount * payment_fx_rate) as daily_balance_change_usd,
        count(invoice_id) as daily_invoices_paid_count
    from {{ stg_invoices }}
    where 1 = 1
        and invoice_status = 'paid'
    group by all
),

daily_balances as (
    select
        organization_id,
        balance_date,
        daily_balance_change_usd,
        daily_invoices_paid_count,
        sum(daily_balance_change_usd) over (
            partition by organization_id 
            order by balance_date
            rows between unbounded preceding and current row
        ) as balance_usd
    from daily_agg_invoice_data d
),

daily_balances_with_previous_balance as (
    select
        *,
        lag(balance_usd) over (
            partition by organization_id 
            order by balance_date
        ) as previous_balance_usd
    from daily_balances
)

select
    *,
    (daily_balance_change_usd / previous_balance_usd * 100) as balance_change_percentage
from daily_balances_with_previous_balance