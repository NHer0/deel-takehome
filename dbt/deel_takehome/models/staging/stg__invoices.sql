{{
    config(
        materialized='view',
        schema='staging'
    )
}}

{% set invoices_raw = source('landing', 'invoices') %}

with curated as (
    select
        abs(invoice_id) as invoice_id,
        abs(parent_invoice_id) as parent_invoice_id,
        abs(transaction_id) as transaction_id,
        abs(organization_id) as organization_id,
        type as invoice_type,
        lower(status) as invoice_status,
        lower(currency) as invoice_currency,
        lower(payment_currency) as payment_currency,
        lower(payment_method) as payment_method,
        amount as invoice_amount,
        payment_amount,
        fx_rate as invoice_fx_rate,
        fx_rate_payment as payment_fx_rate,
        convert_timezone('UTC', created_at)::timestamp_ntz as created_at_utc
    from {{ invoices_raw }}
)

select * from curated
