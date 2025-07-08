{{ config(
    materialized='incremental',
    unique_key='PublicID',
    pre_hook=[
        "{{- fivetran_utils.drop_if_exists(this.schema, this.name ~ '_source_cte') -}}"
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
        GREATEST(dt.UpdateTime, ParentAcct.UpdateTime, sz.UpdateTime, InsuredInfo.ac_UpdateTime, InsuredInfo.acr_UpdateTime, InsuredInfo.c_UpdateTime, InsuredInfo.a_UpdateTime) as LastUpdated
    FROM bc_account dt
    LEFT JOIN bctl_accounttype at ON at.ID = dt.AccountType
    LEFT JOIN ParentAcct ON ParentAcct.OwnerID = dt.ID
    LEFT JOIN bctl_billinglevel bl ON bl.ID = dt.BillingLevel
    LEFT JOIN bctl_customerservicetier cst ON cst.ID = dt.ServiceTier
    LEFT JOIN bc_securityzone sz ON sz.ID = dt.SecurityZoneID
    LEFT JOIN InsuredInfo ON InsuredInfo.AccountID = dt.ID
    LEFT JOIN bctl_delinquencystatus tlds ON tlds.ID = dt.DelinquencyStatus
    LEFT JOIN bctl_accountsegment bas ON bas.ID = dt.Segment
    {% if is_incremental() %}
    WHERE GREATEST(dt.UpdateTime, ParentAcct.UpdateTime, sz.UpdateTime, InsuredInfo.ac_UpdateTime, InsuredInfo.acr_UpdateTime, InsuredInfo.c_UpdateTime, InsuredInfo.a_UpdateTime) > (SELECT MAX(DateUpdated) FROM {{ this }})
    {% endif %}
)

SELECT 
    *,
    GETDATE() as DateUpdated
FROM source_data