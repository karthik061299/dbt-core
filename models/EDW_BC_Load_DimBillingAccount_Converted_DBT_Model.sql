{{ config(
    materialized='incremental',
    unique_key='PublicID',
    on_schema_change='sync_all_columns'
) }}

--
-- Model: DimBillingAccount incremental upsert from Guidewire BC sources
-- Source: OLE_SRC - GuideWire (see schema.yml for column docs)
--

{% set batch_id = var('batch_id', 'BATCH_PLACEHOLDER') %}

with ParentAcct as (
    select pa.OwnerID,
           cast(act.AccountNumber as int) as ParentAccountNumber,
           concat(pa.BeanVersion, act.BeanVersion) as BeanVersion,
           act.UpdateTime
      from {{ source('guidewire', 'bc_ParentAcct') }} pa
      join {{ source('guidewire', 'bc_account') }} act on act.ID = pa.ForeignEntityID
),
InsuredInfo as (
    select ac.InsuredAccountID as AccountID,
           c.FirstName, c.LastName,
           a.AddressLine1, a.AddressLine2, a.AddressLine3, a.City, a.PostalCode,
           tls.NAME as State,
           concat(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) as BeanVersion,
           ac.UpdateTime as ac_UpdateTime, acr.UpdateTime as acr_UpdateTime,
           c.UpdateTime as c_UpdateTime, a.UpdateTime as a_UpdateTime
      from {{ source('guidewire', 'bc_accountcontact') }} ac
      join {{ source('guidewire', 'bc_accountcontactrole') }} acr on acr.AccountContactID = ac.ID
      join {{ source('guidewire', 'bctl_accountrole') }} tlar on tlar.ID = acr.Role
      left join {{ source('guidewire', 'bc_contact') }} c on c.ID = ac.ContactID
      left join {{ source('guidewire', 'bc_address') }} a on a.ID = c.PrimaryAddressID
      left join {{ source('guidewire', 'bctl_state') }} tls on tls.ID = a.State
     where tlar.TYPECODE = 'insured'
)
,
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
    from {{ source('guidewire', 'bc_account') }} dt
    left join {{ source('guidewire', 'bctl_accounttype') }} at on at.ID = dt.AccountType
    left join ParentAcct on ParentAcct.OwnerID = dt.ID
    left join {{ source('guidewire', 'bctl_billinglevel') }} bl on bl.ID = dt.BillingLevel
    left join {{ source('guidewire', 'bctl_customerservicetier') }} cst on cst.ID = dt.ServiceTier
    left join {{ source('guidewire', 'bc_securityzone') }} sz on sz.ID = dt.SecurityZoneID
    left join InsuredInfo on InsuredInfo.AccountID = dt.ID
    left join {{ source('guidewire', 'bctl_delinquencystatus') }} tlds on tlds.ID = dt.DelinquencyStatus
    left join {{ source('guidewire', 'bctl_accountsegment') }} bas on bas.ID = dt.Segment
    where (
        dt.UpdateTime >= dateadd(day, -1, cast(current_date as date)) or
        ParentAcct.UpdateTime >= dateadd(day, -1, cast(current_date as date)) or
        sz.UpdateTime >= dateadd(day, -1, cast(current_date as date)) or
        InsuredInfo.ac_UpdateTime >= dateadd(day, -1, cast(current_date as date)) or
        InsuredInfo.acr_UpdateTime >= dateadd(day, -1, cast(current_date as date)) or
        InsuredInfo.c_UpdateTime >= dateadd(day, -1, cast(current_date as date)) or
        InsuredInfo.a_UpdateTime >= dateadd(day, -1, cast(current_date as date))
    )
)
,
lookup_existing as (
    select PublicID, BeanVersion as EDWBeanVersion
    from {{ this }}
)
,
final as (
    select
        s.*
    from source_data s
    left join lookup_existing lkp
      on s.PublicID = lkp.PublicID
    where (
        lkp.EDWBeanVersion is null -- new record
        or s.BeanVersion != lkp.EDWBeanVersion -- changed record
    )
)

select * from final

{% if is_incremental() %}
    -- Update logic handled by dbt incremental merge
{% endif %}

--
-- Notes:
-- - Row count and error logging logic from SSIS is not implemented here; consider dbt artifacts or downstream reporting for row counts.
-- - All business logic is preserved; non-SQL/script tasks are not present in this package.
