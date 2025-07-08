{{ config(
    materialized='incremental',
    unique_key='PublicID',
    on_schema_change='fail',
    merge_update_columns=[
        'DateUpdated',
        'AccountCloseDate',
        'AccountCreationDate',
        'AccountName',
        'AccountNumber',
        'AccountTypeName',
        'AddressLine1',
        'AddressLine2',
        'AddressLine3',
        'BatchId',
        'BeanVersion',
        'BillingLevelName',
        'City',
        'DeliquencyStatusName',
        'FirstName',
        'GWRowNumber',
        'IsActive',
        'LastName',
        'ParentAccountNumber',
        'PostalCode',
        'SecurityZone',
        'Segment',
        'ServiceTierName',
        'State'
    ]
) }}

WITH ParentAcct AS (
    SELECT 
        pa.OwnerID,
        CAST(act.AccountNumber AS INT) AS ParentAccountNumber,
        CONCAT(pa.BeanVersion, act.BeanVersion) AS BeanVersion,
        act.UpdateTime
    FROM bc_ParentAcct pa
    JOIN bc_account act ON act.ID = pa.ForeignEntityID
),

InsuredInfo AS (
    SELECT 
        ac.InsuredAccountID AS AccountID,
        c.FirstName,
        c.LastName,
        a.AddressLine1,
        a.AddressLine2,
        a.AddressLine3,
        a.City,
        a.PostalCode,
        tls.NAME AS "State",
        CONCAT(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) AS BeanVersion,
        ac.UpdateTime AS ac_UpdateTime,
        acr.UpdateTime AS acr_UpdateTime,
        c.UpdateTime AS c_UpdateTime,
        a.UpdateTime AS a_UpdateTime
    FROM bc_accountcontact ac
    JOIN bc_accountcontactrole acr ON acr.AccountContactID = ac.ID
    JOIN bctl_accountrole tlar ON tlar.ID = acr.Role
    LEFT JOIN bc_contact c ON c.ID = ac.ContactID
    LEFT JOIN bc_address a ON a.ID = c.PrimaryAddressID
    LEFT JOIN bctl_state tls ON tls.ID = a.State
    WHERE tlar.TYPECODE = 'insured'
),

source_data AS (
    SELECT DISTINCT 
        dt.AccountNumber,
        dt.AccountName,
        CAST(at.NAME AS VARCHAR(50)) AS AccountTypeName,
        ParentAcct.ParentAccountNumber,
        CAST(bl.NAME AS VARCHAR(100)) AS BillingLevelName,
        CAST(bas.NAME AS VARCHAR(50)) AS Segment,
        CAST(cst.NAME AS VARCHAR(50)) AS ServiceTierName,
        sz.Name AS SecurityZone,
        InsuredInfo.FirstName,
        InsuredInfo.LastName,
        InsuredInfo.AddressLine1,
        InsuredInfo.AddressLine2,
        InsuredInfo.AddressLine3,
        CAST(InsuredInfo.City AS VARCHAR(50)) AS City,
        CAST(InsuredInfo."State" AS VARCHAR(50)) AS State,
        CAST(InsuredInfo.PostalCode AS VARCHAR(50)) AS PostalCode,
        dt.CloseDate AS AccountCloseDate,
        dt.CreateTime AS AccountCreationDate,
        CAST(tlds.NAME AS VARCHAR(50)) AS DeliquencyStatusName,
        dt.FirstTwicePerMthInvoiceDOM AS FirstTwicePerMonthInvoiceDayOfMonth,
        dt.SecondTwicePerMthInvoiceDOM AS SecondTwicePerMonthInvoiceDayOfMonth,
        dt.PublicID,
        dt.ID AS GWRowNumber,
        CAST(CONCAT(dt.BeanVersion, ParentAcct.BeanVersion, sz.BeanVersion) AS VARCHAR(20)) AS BeanVersion,
        CASE dt.Retired WHEN 0 THEN 1 ELSE 0 END AS IsActive,
        'WC' AS LegacySourceSystem,
        dt.UpdateTime,
        ParentAcct.UpdateTime AS ParentAcct_UpdateTime,
        sz.UpdateTime AS sz_UpdateTime,
        InsuredInfo.ac_UpdateTime,
        InsuredInfo.acr_UpdateTime,
        InsuredInfo.c_UpdateTime,
        InsuredInfo.a_UpdateTime
    FROM bc_account dt
    LEFT JOIN bctl_accounttype at ON at.ID = dt.AccountType
    LEFT JOIN ParentAcct ON ParentAcct.OwnerID = dt.ID
    LEFT JOIN bctl_billinglevel bl ON bl.ID = dt.BillingLevel
    LEFT JOIN bctl_customerservicetier cst ON cst.ID = dt.ServiceTier
    LEFT JOIN bc_securityzone sz ON sz.ID = dt.SecurityZoneID
    LEFT JOIN InsuredInfo ON InsuredInfo.AccountID = dt.ID
    LEFT JOIN bctl_delinquencystatus tlds ON tlds.ID = dt.DelinquencyStatus
    LEFT JOIN bctl_accountsegment bas ON bas.ID = dt.Segment
)

SELECT
    s.AccountNumber,
    s.AccountName,
    s.AccountTypeName,
    s.ParentAccountNumber,
    s.BillingLevelName,
    s.Segment,
    s.ServiceTierName,
    s.SecurityZone,
    s.FirstName,
    s.LastName,
    s.AddressLine1,
    s.AddressLine2,
    s.AddressLine3,
    s.City,
    s.State,
    s.PostalCode,
    s.AccountCloseDate,
    s.AccountCreationDate,
    s.DeliquencyStatusName,
    s.FirstTwicePerMonthInvoiceDayOfMonth,
    s.SecondTwicePerMonthInvoiceDayOfMonth,
    s.PublicID,
    s.GWRowNumber,
    s.BeanVersion,
    s.IsActive,
    s.LegacySourceSystem,
    {{ var('batch_id', 'default_batch_id') }} as BatchID,
    GETDATE() as DateUpdated
FROM source_data s

{% if is_incremental() %}

WHERE
    s.UpdateTime >= (select max(DateUpdated) from {{ this }}) OR
    s.ParentAcct_UpdateTime >= (select max(DateUpdated) from {{ this }}) OR
    s.sz_UpdateTime >= (select max(DateUpdated) from {{ this }}) OR
    s.ac_UpdateTime >= (select max(DateUpdated) from {{ this }}) OR
    s.acr_UpdateTime >= (select max(DateUpdated) from {{ this }}) OR
    s.c_UpdateTime >= (select max(DateUpdated) from {{ this }}) OR
    s.a_UpdateTime >= (select max(DateUpdated) from {{ this }})

{% endif %}
