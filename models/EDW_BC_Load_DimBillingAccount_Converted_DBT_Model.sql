{{ config(
    materialized='incremental',
    unique_key='PublicID',
    on_schema_change='fail',
    pre_hook="INSERT INTO dbt_audit_log (model_name, run_started_at, status) VALUES ('EDW_BC_Load_DimBillingAccount_Converted_DBT_Model', GETDATE(), 'STARTED')",
    post_hook="INSERT INTO dbt_audit_log (model_name, run_completed_at, status) VALUES ('EDW_BC_Load_DimBillingAccount_Converted_DBT_Model', GETDATE(), 'COMPLETED')"
) }}

-- DBT Model converted from SSIS Package: EDW_BC_Load_DimBillingAccount.dtsx
-- This model performs incremental upsert operations on DimBillingAccount
-- Original SSIS logic included lookup transformations, conditional splits, and row counting

WITH ParentAcct AS (
    SELECT 
        pa.OwnerID,
        CAST(act.AccountNumber AS INT) AS ParentAccountNumber,
        CONCAT(pa.BeanVersion, act.BeanVersion) AS BeanVersion,
        act.UpdateTime
    FROM {{ source('guidewire', 'bc_ParentAcct') }} pa
    JOIN {{ source('guidewire', 'bc_account') }} act ON act.ID = pa.ForeignEntityID
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
        {{ dbt_utils.generate_surrogate_key(['dt.PublicID', "'WC'"]) }} AS BatchID,
        GETDATE() AS DateCreated,
        GETDATE() AS DateUpdated
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
            dt.UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)) OR 
            ParentAcct.UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)) OR
            sz.UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)) OR
            InsuredInfo.ac_UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)) OR
            InsuredInfo.acr_UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)) OR
            InsuredInfo.c_UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE)) OR
            InsuredInfo.a_UpdateTime >= DATEADD(DAY, -7, CAST(GETDATE() AS DATE))
        {% else %}
            1=1  -- Full refresh loads all data
        {% endif %}
    )
)

SELECT 
    PublicID,
    AccountNumber,
    AccountName,
    AccountTypeName,
    ParentAccountNumber,
    BillingLevelName,
    Segment,
    ServiceTierName,
    SecurityZone,
    FirstName,
    LastName,
    AddressLine1,
    AddressLine2,
    AddressLine3,
    City,
    State,
    PostalCode,
    AccountCloseDate,
    AccountCreationDate,
    DeliquencyStatusName,
    FirstTwicePerMonthInvoiceDayOfMonth,
    SecondTwicePerMonthInvoiceDayOfMonth,
    GWRowNumber,
    BeanVersion,
    IsActive,
    LegacySourceSystem,
    BatchID,
    DateCreated,
    DateUpdated
FROM source_data

{% if is_incremental() %}
    -- This replaces the SSIS lookup and conditional split logic
    -- Only process records that are new or have changed BeanVersion
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ this }} existing
        WHERE existing.PublicID = source_data.PublicID
        AND existing.LegacySourceSystem = source_data.LegacySourceSystem
        AND existing.BeanVersion = source_data.BeanVersion
    )
{% endif %}