{{ config(
    materialized='incremental',
    unique_key='PublicID',
    on_schema_change='sync_all_columns'
) }}

--
-- DBT Model: EDW_BC_Load_DimBillingAccount_Converted_DBT_Model.sql
-- Source: SSIS Package 'EDW_BC_Load_DimBillingAccount.dtsx'
-- Description: Loads and maintains DimBillingAccount dimension table using SCD logic (update if BeanVersion changes, insert if new)
--

{% set batch_id = var('batch_id', 'manual_batch') %}
{% set legacy_source_system = var('legacy_source_system', 'WC') %}

with ParentAcct as (
    select pa.OwnerID,
           cast(act.AccountNumber as int) as ParentAccountNumber,
           concat(pa.BeanVersion, act.BeanVersion) as BeanVersion,
           act.UpdateTime
      from bc_ParentAcct pa
      join bc_account act on act.ID = pa.ForeignEntityID
),
InsuredInfo as (
    select ac.InsuredAccountID as AccountID,
           c.FirstName, c.LastName,
           a.AddressLine1, a.AddressLine2, a.AddressLine3,
           a.City, a.PostalCode,
           tls.NAME as State,
           concat(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) as BeanVersion,
           ac.UpdateTime as ac_UpdateTime, acr.UpdateTime as acr_UpdateTime,
           c.UpdateTime as c_UpdateTime, a.UpdateTime as a_UpdateTime
      from bc_accountcontact ac
      join bc_accountcontactrole acr on acr.AccountContactID = ac.ID
      join bctl_accountrole tlar on tlar.ID = acr.Role
 left join bc_contact c on c.ID = ac.ContactID
 left join bc_address a on a.ID = c.PrimaryAddressID
 left join bctl_state tls on tls.ID = a.State
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
        '{{ legacy_source_system }}' as LegacySourceSystem,
        '{{ batch_id }}' as BatchID
    from bc_account dt
    left join bctl_accounttype at on at.ID = dt.AccountType
    left join ParentAcct on ParentAcct.OwnerID = dt.ID
    left join bctl_billinglevel bl on bl.ID = dt.BillingLevel
    left join bctl_customerservicetier cst on cst.ID = dt.ServiceTier
    left join bc_securityzone sz on sz.ID = dt.SecurityZoneID
    left join InsuredInfo on InsuredInfo.AccountID = dt.ID
    left join bctl_delinquencystatus tlds on tlds.ID = dt.DelinquencyStatus
    left join bctl_accountsegment bas on bas.ID = dt.Segment
    where (
        dt.UpdateTime >= dateadd(day, -1, cast(getdate() as date)) or
        ParentAcct.UpdateTime >= dateadd(day, -1, cast(getdate() as date)) or
        sz.UpdateTime >= dateadd(day, -1, cast(getdate() as date)) or
        InsuredInfo.ac_UpdateTime >= dateadd(day, -1, cast(getdate() as date)) or
        InsuredInfo.acr_UpdateTime >= dateadd(day, -1, cast(getdate() as date)) or
        InsuredInfo.c_UpdateTime >= dateadd(day, -1, cast(getdate() as date)) or
        InsuredInfo.a_UpdateTime >= dateadd(day, -1, cast(getdate() as date))
    )
)

select * from source_data

{% if is_incremental() %}
    --
    -- Incremental logic: update if PublicID+LegacySourceSystem exists and BeanVersion differs, else insert
    --
    merge into {{ this }} as target
    using source_data as source
    on target.PublicID = source.PublicID
       and target.LegacySourceSystem = source.LegacySourceSystem
    when matched and target.BeanVersion != source.BeanVersion then
        update set
            DateUpdated = getdate(),
            AccountCloseDate = source.AccountCloseDate,
            AccountCreationDate = source.AccountCreationDate,
            AccountName = source.AccountName,
            AccountNumber = source.AccountNumber,
            AccountTypeName = source.AccountTypeName,
            AddressLine1 = source.AddressLine1,
            AddressLine2 = source.AddressLine2,
            AddressLine3 = source.AddressLine3,
            BatchID = source.BatchID,
            BeanVersion = source.BeanVersion,
            BillingLevelName = source.BillingLevelName,
            City = source.City,
            DeliquencyStatusName = source.DeliquencyStatusName,
            FirstName = source.FirstName,
            GWRowNumber = source.GWRowNumber,
            IsActive = source.IsActive,
            LastName = source.LastName,
            ParentAccountNumber = source.ParentAccountNumber,
            PostalCode = source.PostalCode,
            SecurityZone = source.SecurityZone,
            Segment = source.Segment,
            ServiceTierName = source.ServiceTierName,
            State = source.State
    when not matched then
        insert (
            AccountNumber, AccountName, AccountTypeName, ParentAccountNumber, BillingLevelName, Segment, ServiceTierName, SecurityZone, FirstName, LastName, AddressLine1, AddressLine2, AddressLine3, City, State, PostalCode, AccountCloseDate, AccountCreationDate, DeliquencyStatusName, FirstTwicePerMonthInvoiceDayOfMonth, SecondTwicePerMonthInvoiceDayOfMonth, PublicID, GWRowNumber, BeanVersion, IsActive, LegacySourceSystem, BatchID
        )
        values (
            source.AccountNumber, source.AccountName, source.AccountTypeName, source.ParentAccountNumber, source.BillingLevelName, source.Segment, source.ServiceTierName, source.SecurityZone, source.FirstName, source.LastName, source.AddressLine1, source.AddressLine2, source.AddressLine3, source.City, source.State, source.PostalCode, source.AccountCloseDate, source.AccountCreationDate, source.DeliquencyStatusName, source.FirstTwicePerMonthInvoiceDayOfMonth, source.SecondTwicePerMonthInvoiceDayOfMonth, source.PublicID, source.GWRowNumber, source.BeanVersion, source.IsActive, source.LegacySourceSystem, source.BatchID
        );
{% endif %}

--
-- Notes:
-- - This model is incremental and uses a MERGE statement for upsert logic.
-- - Variables 'batch_id' and 'legacy_source_system' can be set in dbt Cloud job or profile.
-- - Row count metrics are not persisted but can be added via dbt tests or exposures.
-- - Error handling and logging are handled outside of dbt (e.g., via orchestration or warehouse logs).
-- - If your warehouse does not support MERGE, refactor to use dbt's incremental update pattern.
