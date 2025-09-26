{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    aliases=['dim_claim']
) }}

{% set src_dataset = var('staging_dataset', 'stg') %}
{% set tgt_dataset = var('warehouse_dataset', 'warehouse') %}
{% set dim_table = tgt_dataset + '.dim_claim' %}
{% set meta_table = tgt_dataset + '.dim_claim_meta' %}
{% set batch_id = var('batch_id', run_started_at | string) %}

-- Version 3: Correct SCD Type 2 implementation for BigQuery (dbt)
-- - Business key: claim_id
-- - Surrogate key: surrogate_key (GENERATE_UUID())
-- - When attributes change: close previous row (set is_current=false, effective_to) and insert new row
-- - Incremental-friendly (uses is_incremental())

with fresh_source as (
  select
    claim_id as business_key,
    claimant_name,
    claim_status,
    cast(claim_amount as numeric) as claim_amount,
    cast(claim_date as timestamp) as claim_date
  from `{{ src_dataset }}.claims_staging`
  {% if is_incremental() %}
    where load_ts > (select coalesce(max(processed_at), '1970-01-01') from `{{ meta_table }}`)
  {% endif %}
),

dedup as (
  select
    business_key,
    claimant_name,
    claim_status,
    claim_amount,
    claim_date,
    row_number() over (partition by business_key order by claim_date desc) as rn
  from fresh_source
),

src_latest as (
  select * from dedup where rn = 1
),

-- identify which business keys changed vs existing current rows
changes as (
  select
    s.*,
    t.surrogate_key as existing_surrogate,
    t.claimant_name    as existing_claimant_name,
    t.claim_status     as existing_claim_status,
    t.claim_amount     as existing_claim_amount
  from src_latest s
  left join `{{ dim_table }}` t
    on t.business_key = s.business_key and t.is_current = TRUE
),

to_close as (
  -- rows where an existing current record must be closed (attributes changed)
  select existing_surrogate, business_key
  from changes
  where existing_surrogate is not null
    and (
       coalesce(existing_claimant_name, '') != coalesce(claimant_name, '')
    or coalesce(existing_claim_status, '') != coalesce(claim_status, '')
    or coalesce(existing_claim_amount, 0) != coalesce(claim_amount, 0)
    )
),

to_insert as (
  -- new rows to insert (either new business key or changed attributes)
  select
    generate_uuid() as surrogate_key,
    c.business_key,
    c.claimant_name,
    c.claim_status,
    c.claim_amount,
    c.claim_date,
    current_timestamp() as effective_from,
    null as effective_to,
    true as is_current,
    current_timestamp() as processed_at,
    '{{ batch_id }}' as batch_id
  from changes c
  where c.existing_surrogate is null
     or exists (select 1 from to_close tc where tc.business_key = c.business_key)
)

-- 1) Close changed existing records
-- 2) Insert new current records
-- Implement as two statements in dbt: an UPDATE then an INSERT when incremental,
-- but for full-refresh we rebuild the table.
{% if is_incremental() %}

-- Close existing current rows that changed
update `{{ dim_table }}` T
set is_current = false,
    effective_to = current_timestamp()
where surrogate_key in (select existing_surrogate from to_close);

-- Insert new/changed rows
insert into `{{ dim_table }}`
(
  surrogate_key, business_key, claimant_name, claim_status, claim_amount, claim_date,
  effective_from, effective_to, is_current, processed_at, batch_id
)
select
  surrogate_key, business_key, claimant_name, claim_status, claim_amount, claim_date,
  effective_from, effective_to, is_current, processed_at, batch_id
from to_insert;

-- update meta table to track last processed timestamp
insert into `{{ meta_table }}` (processed_at, batch_id)
values (current_timestamp(), '{{ batch_id }}');

{% else %}

-- Full refresh: rebuild dim table from scratch combining historical rows (if available) + new inserts.
-- Simpler approach: create a snapshot by closing existing current rows where changes exist,
-- then insert new ones. For a full refresh, we will truncate and insert known good set.

create or replace table `{{ dim_table }}` as
select * from (
  -- keep historical rows that are not current or unaffected
  select * from `{{ dim_table }}`
  where false -- intentionally empty to rebuild cleanly
)
union all
select * from to_insert;

-- refresh meta
create or replace table `{{ meta_table }}` as
select current_timestamp() as processed_at, '{{ batch_id }}' as batch_id;

{% endif %}
