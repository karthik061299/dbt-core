{{ config(
    materialized='incremental',
    unique_key=['PublicID', 'LegacySourceSystem'],
    on_schema_change='sync_all_columns'
) }}

--
-- DBT Model: EDW_BC_Load_DimBillingAccount_Converted_DBT_Model.sql
-- Source: SSIS Package 'EDW_BC_Load_DimBillingAccount'
-- Description: Incremental load of DimBillingAccount with SCD1-like logic, using batch and BeanVersion for change detection.
--

{% set batch_id = var('batch_id', 'auto_batch') %}
{% set days_back = var('days_back', -1) %}

with ParentAcct as (
    select pa.OwnerID,
           cast(act.AccountNumber as int) as ParentAccountNumber,
           concat(pa.BeanVersion, act.BeanVersion) as BeanVersion,
           act.UpdateTime
    from {{ source('bc', 'ParentAcct') }} pa
    join {{ source('bc', 'account') }} act on act.ID = pa.ForeignEntityID
),
InsuredInfo as (
    select ac.InsuredAccountID as AccountID,
           c.FirstName, c.LastName,
           a.AddressLine1, a.AddressLine2, a.AddressLine3, a.City, a.PostalCode,
           tls.NAME as State,
           concat(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) as BeanVersion,
           ac.UpdateTime as ac_UpdateTime, acr.UpdateTime as acr_UpdateTime,
           c.UpdateTime as c_UpdateTime, a.UpdateTime as a_UpdateTime
    from {{ source('bc', 'accountcontact') }} ac
    join {{ source('bc', 'accountcontactrole') }} acr on acr.AccountContactID = ac.ID
    join {{ source('bctl', 'accountrole') }} tlar on tlar.ID = acr.Role
    left join {{ source('bc', 'contact') }} c on c.ID = ac.ContactID
    left join {{ source('bc', 'address') }} a on a.ID = c.PrimaryAddressID
    left join {{ source('bctl', 'state') }} tls on tls.ID = a.State
    where tlar.TYPECODE = 'insured'
),
source_data as (
    select distinct
        dt.AccountNumber,
        dt.AccountName,
        cast(at.NAME as varchar(50)) as AccountTypeName,
        ParentAcct.ParentAccountNumber,
        cast(bl.NAME as varchar(100)) as BillingLevelName,
        cast(bas.NAME as varchar(50)) as Segment,
        cast(cst.NAME as varchar(50)) as ServiceTierName,
        sz.Name as SecurityZone,
        InsuredInfo.FirstName, InsuredInfo.LastName,
        InsuredInfo.AddressLine1, InsuredInfo.AddressLine2, InsuredInfo.AddressLine3,
        cast(InsuredInfo.City as varchar(50)) as City,
        cast(InsuredInfo.State as varchar(50)) as State,
        cast(InsuredInfo.PostalCode as varchar(50)) as PostalCode,
        dt.CloseDate as AccountCloseDate,
        dt.CreateTime as AccountCreationDate,
        cast(tlds.NAME as varchar(50)) as DeliquencyStatusName,
        dt.FirstTwicePerMthInvoiceDOM as FirstTwicePerMonthInvoiceDayOfMonth,
        dt.SecondTwicePerMthInvoiceDOM as SecondTwicePerMonthInvoiceDayOfMonth,
        dt.PublicID,
        dt.ID as GWRowNumber,
        cast(concat(dt.BeanVersion, ParentAcct.BeanVersion, sz.BeanVersion) as varchar(20)) as BeanVersion,
        case dt.Retired when 0 then 1 else 0 end as IsActive,
        'WC' as LegacySourceSystem,
        '{{ batch_id }}' as BatchID
    from {{ source('bc', 'account') }} dt
    left join {{ source('bctl', 'accounttype') }} at on at.ID = dt.AccountType
    left join ParentAcct on ParentAcct.OwnerID = dt.ID
    left join {{ source('bctl', 'billinglevel') }} bl on bl.ID = dt.BillingLevel
    left join {{ source('bctl', 'customerservicetier') }} cst on cst.ID = dt.ServiceTier
    left join {{ source('bc', 'securityzone') }} sz on sz.ID = dt.SecurityZoneID
    left join InsuredInfo on InsuredInfo.AccountID = dt.ID
    left join {{ source('bctl', 'delinquencystatus') }} tlds on tlds.ID = dt.DelinquencyStatus
    left join {{ source('bctl', 'accountsegment') }} bas on bas.ID = dt.Segment
    where (
        dt.UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date)) or
        ParentAcct.UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date)) or
        sz.UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date)) or
        InsuredInfo.ac_UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date)) or
        InsuredInfo.acr_UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date)) or
        InsuredInfo.c_UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date)) or
        InsuredInfo.a_UpdateTime >= dateadd(day, {{ days_back }}, cast(current_date as date))
    )
),
lookup_existing as (
    select PublicID, BeanVersion, LegacySourceSystem
    from {{ this }}
)

select
    src.AccountNumber,
    src.AccountName,
    src.AccountTypeName,
    src.ParentAccountNumber,
    src.BillingLevelName,
    src.Segment,
    src.ServiceTierName,
    src.SecurityZone,
    src.FirstName,
    src.LastName,
    src.AddressLine1,
    src.AddressLine2,
    src.AddressLine3,
    src.City,
    src.State,
    src.PostalCode,
    src.AccountCloseDate,
    src.AccountCreationDate,
    src.DeliquencyStatusName,
    src.FirstTwicePerMonthInvoiceDayOfMonth,
    src.SecondTwicePerMonthInvoiceDayOfMonth,
    src.PublicID,
    src.GWRowNumber,
    src.BeanVersion,
    src.IsActive,
    src.LegacySourceSystem,
    src.BatchID,
    current_timestamp as DateUpdated
from source_data src
left join lookup_existing tgt
    on src.PublicID = tgt.PublicID and src.LegacySourceSystem = tgt.LegacySourceSystem
where (
    tgt.PublicID is null -- Insert
    or tgt.BeanVersion != src.BeanVersion -- Update
)

{% if is_incremental() %}
    -- Only update/insert changed or new records
{% endif %}

--
-- Notes:
-- - This model implements incremental upsert logic based on PublicID + LegacySourceSystem and BeanVersion.
-- - Use dbt run with --vars '{"batch_id": 12345, "days_back": -1}' for parameterization.
-- - Row counts for auditing can be implemented in dbt tests or exposures.
-- - Error logging and process state management should be handled outside dbt (e.g., orchestration layer).
