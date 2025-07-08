{{ config(
    materialized='incremental',
    unique_key='PublicID',
    on_schema_change='sync_all_columns'
) }}

--
-- Incremental upsert model for DimBillingAccount
-- Source: Guidewire BC
--

WITH ParentAcct AS (
    SELECT pa.OwnerID,
           CAST(act.AccountNumber AS INT) AS ParentAccountNumber,
           CONCAT(pa.BeanVersion, act.BeanVersion) AS BeanVersion,
           act.UpdateTime
    FROM {{ source('guidewire', 'bc_ParentAcct') }} pa
    JOIN {{ source('guidewire', 'bc_account') }} act ON act.ID = pa.ForeignEntityID
),
InsuredInfo AS (
    SELECT ac.InsuredAccountID AS AccountID,
           c.FirstName,
           c.LastName,
           a.AddressLine1,
           a.AddressLine2,
           a.AddressLine3,
           a.City,
           a.PostalCode,
           tls.NAME AS State,
           CONCAT(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) AS BeanVersion,
           ac.UpdateTime AS ac_UpdateTime,
           acr.UpdateTime AS acr_UpdateTime,
           c.UpdateTime AS c_UpdateTime,
           a.UpdateTime AS a_UpdateTime
    FROM {{ source('guidewire', 'bc_accountcontact') }} ac
    JOIN {{ source('guidewire', 'bc_accountcontactrole') }} acr ON acr.AccountContactID = ac.ID
    JOIN {{ source('guidewire', 'bctl_accountrole') }} tlar ON tlar.ID = acr.Role
    LEFT JOIN {{ source('guidewire', 'bc_contact') }} c ON c.ID = ac.ContactID
    LEFT JOIN {{ source('guidewire', 'bc_address') }} a ON a.ID = c.PrimaryAddressID
    LEFT JOIN {{ source('guidewire', 'bctl_state') }} tls ON tls.ID = a.State
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
        CAST(InsuredInfo.State AS VARCHAR(50)) AS State,
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
        {{ var('batch_id', 'manual_batch') }} AS BatchID
    FROM {{ source('guidewire', 'bc_account') }} dt
    LEFT JOIN {{ source('guidewire', 'bctl_accounttype') }} at ON at.ID = dt.AccountType
    LEFT JOIN ParentAcct ON ParentAcct.OwnerID = dt.ID
    LEFT JOIN {{ source('guidewire', 'bctl_billinglevel') }} bl ON bl.ID = dt.BillingLevel
    LEFT JOIN {{ source('guidewire', 'bctl_customerservicetier') }} cst ON cst.ID = dt.ServiceTier
    LEFT JOIN {{ source('guidewire', 'bc_securityzone') }} sz ON sz.ID = dt.SecurityZoneID
    LEFT JOIN InsuredInfo ON InsuredInfo.AccountID = dt.ID
    LEFT JOIN {{ source('guidewire', 'bctl_delinquencystatus') }} tlds ON tlds.ID = dt.DelinquencyStatus
    LEFT JOIN {{ source('guidewire', 'bctl_accountsegment') }} bas ON bas.ID = dt.Segment
    WHERE (
        {% if is_incremental() %}
            dt.UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }} OR
            ParentAcct.UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }} OR
            sz.UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }} OR
            InsuredInfo.ac_UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }} OR
            InsuredInfo.acr_UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }} OR
            InsuredInfo.c_UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }} OR
            InsuredInfo.a_UpdateTime >= {{ dbt.dateadd('day', -1, 'current_date') }}
        {% else %}
            1=1
        {% endif %}
    )
),
target AS (
    SELECT PublicID, BeanVersion
    FROM {{ this }}
)

SELECT
    sd.PublicID,
    sd.AccountNumber,
    sd.AccountName,
    sd.AccountTypeName,
    sd.ParentAccountNumber,
    sd.BillingLevelName,
    sd.Segment,
    sd.ServiceTierName,
    sd.SecurityZone,
    sd.FirstName,
    sd.LastName,
    sd.AddressLine1,
    sd.AddressLine2,
    sd.AddressLine3,
    sd.City,
    sd.State,
    sd.PostalCode,
    sd.AccountCloseDate,
    sd.AccountCreationDate,
    sd.DeliquencyStatusName,
    sd.FirstTwicePerMonthInvoiceDayOfMonth,
    sd.SecondTwicePerMonthInvoiceDayOfMonth,
    sd.GWRowNumber,
    sd.BeanVersion,
    sd.IsActive,
    sd.LegacySourceSystem,
    sd.BatchID
FROM source_data sd
LEFT JOIN target t
    ON sd.PublicID = t.PublicID
WHERE
    t.PublicID IS NULL -- Insert new
    OR sd.BeanVersion != t.BeanVersion -- Update changed

--
-- Notes:
-- - This model performs an incremental upsert based on PublicID and BeanVersion.
-- - BatchID is injected via dbt variable (default: 'manual_batch').
-- - LegacySourceSystem is always 'WC'.
-- - Row counts for audit are not persisted in this model (consider separate audit logging if required).
-- - Error handling is managed by dbt run status.
