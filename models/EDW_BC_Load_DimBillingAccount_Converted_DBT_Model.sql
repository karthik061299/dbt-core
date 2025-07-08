{{ config(materialized="incremental") }}

--
-- DBT Model: EDW_BC_Load_DimBillingAccount_Converted_DBT_Model.sql
-- Source: SSIS Package 'EDW_BC_Load_DimBillingAccount.dtsx'
-- This model implements SCD1-like logic: inserts new records and updates changed ones in DimBillingAccount.
--

{% set batch_id = var('batch_id', 'UNKNOWN_BATCH') %}

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
           c.FirstName,
           c.LastName,
           a.AddressLine1,
           a.AddressLine2,
           a.AddressLine3,
           a.City,
           a.PostalCode,
           tls.NAME as State,
           concat(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) as BeanVersion,
           ac.UpdateTime as ac_UpdateTime,
           acr.UpdateTime as acr_UpdateTime,
           c.UpdateTime as c_UpdateTime,
           a.UpdateTime as a_UpdateTime
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
        InsuredInfo.FirstName,
        InsuredInfo.LastName,
        InsuredInfo.AddressLine1,
        InsuredInfo.AddressLine2,
        InsuredInfo.AddressLine3,
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
        dt.UpdateTime >= dateadd(day, -7, current_date) or
        ParentAcct.UpdateTime >= dateadd(day, -7, current_date) or
        sz.UpdateTime >= dateadd(day, -7, current_date) or
        InsuredInfo.ac_UpdateTime >= dateadd(day, -7, current_date) or
        InsuredInfo.acr_UpdateTime >= dateadd(day, -7, current_date) or
        InsuredInfo.c_UpdateTime >= dateadd(day, -7, current_date) or
        InsuredInfo.a_UpdateTime >= dateadd(day, -7, current_date)
    )
)

-- SCD1 Merge: Insert new, update changed, do nothing for unchanged
select * from source_data

{% if is_incremental() %}
    -- Only update/insert changed/new records
    {% set dest = ref('DimBillingAccount') %}
    merge into {{ dest }} as target
    using source_data as source
    on target.PublicID = source.PublicID and target.BeanVersion = source.BeanVersion and target.LegacySourceSystem = source.LegacySourceSystem
    when matched then update set
        target.DateUpdated = current_timestamp,
        target.AccountCloseDate = source.AccountCloseDate,
        target.AccountCreationDate = source.AccountCreationDate,
        target.AccountName = source.AccountName,
        target.AccountNumber = source.AccountNumber,
        target.AccountTypeName = source.AccountTypeName,
        target.AddressLine1 = source.AddressLine1,
        target.AddressLine2 = source.AddressLine2,
        target.AddressLine3 = source.AddressLine3,
        target.BatchID = source.BatchID,
        target.BeanVersion = source.BeanVersion,
        target.BillingLevelName = source.BillingLevelName,
        target.City = source.City,
        target.DeliquencyStatusName = source.DeliquencyStatusName,
        target.FirstName = source.FirstName,
        target.GWRowNumber = source.GWRowNumber,
        target.IsActive = source.IsActive,
        target.LastName = source.LastName,
        target.ParentAccountNumber = source.ParentAccountNumber,
        target.PostalCode = source.PostalCode,
        target.SecurityZone = source.SecurityZone,
        target.Segment = source.Segment,
        target.ServiceTierName = source.ServiceTierName,
        target.State = source.State
    when not matched then insert (
        AccountNumber, AccountName, AccountTypeName, ParentAccountNumber, BillingLevelName, Segment, ServiceTierName, SecurityZone,
        FirstName, LastName, AddressLine1, AddressLine2, AddressLine3, City, State, PostalCode, AccountCloseDate, AccountCreationDate,
        DeliquencyStatusName, FirstTwicePerMonthInvoiceDayOfMonth, SecondTwicePerMonthInvoiceDayOfMonth, PublicID, GWRowNumber,
        BeanVersion, IsActive, LegacySourceSystem, BatchID
    ) values (
        source.AccountNumber, source.AccountName, source.AccountTypeName, source.ParentAccountNumber, source.BillingLevelName, source.Segment, source.ServiceTierName, source.SecurityZone,
        source.FirstName, source.LastName, source.AddressLine1, source.AddressLine2, source.AddressLine3, source.City, source.State, source.PostalCode, source.AccountCloseDate, source.AccountCreationDate,
        source.DeliquencyStatusName, source.FirstTwicePerMonthInvoiceDayOfMonth, source.SecondTwicePerMonthInvoiceDayOfMonth, source.PublicID, source.GWRowNumber,
        source.BeanVersion, source.IsActive, source.LegacySourceSystem, source.BatchID
    );
{% endif %}

--
-- Manual intervention required for the following:
-- - Row count/audit variables (SourceCount, UpdateCount, InsertCount, UnChangeCount) are not natively tracked in DBT and should be implemented via DBT tests or post-hook logging if needed.
-- - Error logging and process state management (initiate/conclude/failure) should be handled via orchestration or DBT run hooks.
-- - If any non-SQL or script tasks exist, they must be manually ported.
