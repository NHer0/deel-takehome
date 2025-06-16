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
        sum(case
                when invoice_status = 'paid' then (payment_amount * payment_fx_rate)
                when invoice_status = 'refunded' then (-1 * payment_amount * invoice_fx_rate)
                else 0
            end
        ) as daily_balance_change_usd,
        count(invoice_id) as daily_invoices_count,
        count(case
                when invoice_status = 'paid' then invoice_id
              end
        ) as daily_invoices_paid_count,
        count(case
                when invoice_status = 'refunded' then invoice_id
              end
        ) as daily_invoices_refunded_count
    from {{ stg_invoices }}
    group by all
),

daily_balances as (
    select
        organization_id,
        balance_date,
        daily_balance_change_usd,
        daily_invoices_count,
        daily_invoices_paid_count,
        daily_invoices_refunded_count,
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
        ) as previous_balance_usd,
        lag(balance_date) over (
            partition by organization_id 
            order by balance_date
        ) as previous_balance_date
    from daily_balances
)

select
    *,
    (balance_date - previous_balance_date) as days_since_last_balance_change,
    (case
        when previous_balance_usd != 0 then ((balance_usd - previous_balance_usd) / abs(previous_balance_usd) * 100)
    end) as balance_change_percentage  -- abs to take into account the case when both are negative
from daily_balances_with_previous_balance