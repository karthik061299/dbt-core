{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    aliases=['dim_claim_v2']
) }}

{% set src_dataset = var('staging_dataset', 'stg') %}
{% set tgt_dataset = var('warehouse_dataset', 'warehouse') %}
{% set dim_table = tgt_dataset + '.dim_claim' %}

/*
 Version 2: Uses MERGE to perform inserts/updates.
 Small intentional bug: matching condition uses surrogate_key (which is null/new) instead of business_key (claim_id).
*/

with latest_src as (
  select
    claim_id as business_key,
    claimant_name,
    claim_status,
    cast(claim_amount as numeric) as claim_amount,
    cast(claim_date as timestamp) as claim_date
  from `{{ src_dataset }}.claims_staging`
  where load_ts > (select coalesce(max(processed_at), '1970-01-01') from `{{ tgt_dataset }}.dim_claim_meta`)
),

dedup as (
  select
    business_key,
    claimant_name,
    claim_status,
    claim_amount,
    claim_date,
    row_number() over (partition by business_key order by claim_date desc) as rn
  from latest_src
)

-- MERGE into the dimension table to handle inserts/updates (SCD Type 2)
MERGE `{{ dim_table }}` T
USING (
  select * from dedup where rn = 1
) S
ON T.surrogate_key = S.business_key   -- ❌ BUG: matching on surrogate_key vs business_key (incorrect)
WHEN MATCHED AND (
      T.claimant_name != S.claimant_name
   OR T.claim_status != S.claim_status
   OR T.claim_amount != S.claim_amount
) THEN
  -- close current record and insert a new current row below (simplified)
  UPDATE SET
    T.is_current = false,
    T.effective_to = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (surrogate_key, business_key, claimant_name, claim_status, claim_amount, claim_date, effective_from, effective_to, is_current, processed_at)
  VALUES (GENERATE_UUID(), S.business_key, S.claimant_name, S.claim_status, S.claim_amount, S.claim_date, CURRENT_TIMESTAMP(), NULL, true, CURRENT_TIMESTAMP());
