{{ config(
    materialized='incremental',
    unique_key='BillingAccountKey',
    on_schema_change='sync_all_columns'
) }}

-- Incremental load of Billing Accounts from source to EDW
-- Source: dbo.BillingAccount (SQL_SOURCE_SERVER.SourceDB)
-- Destination: dbo.DimBillingAccount (EDW_SERVER.EDW)
-- This model implements the logic from the SSIS package 'EDW_BC_Load_DimBillingAccount'

with source_billing_accounts as (
    select
        AccountID,
        AccountName,
        Status,
        CreatedDate,
        LastModifiedDate
    from {{ source('source_db', 'BillingAccount') }}
    {% if is_incremental() %}
        where LastModifiedDate > (
            select coalesce(max(LastModifiedDate), '1900-01-01')
            from {{ this }}
        )
    {% endif %}
)

select
    AccountID as BillingAccountKey,
    AccountName,
    Status,
    CreatedDate,
    LastModifiedDate
from source_billing_accounts

-- NOTE: The original SSIS package included an Aggregate Count of AccountID for record counting,
-- but this value was not used downstream. If needed, add a separate model or test for row count.
