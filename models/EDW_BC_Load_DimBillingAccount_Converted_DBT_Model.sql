{{ config(
    materialized='incremental',
    unique_key='PublicID',
    on_schema_change='sync_all_columns',
    database='DBT',
    schema='GUIDEWIRE',
    alias='EDW_BC_LOAD_DIMBILLINGACCOUNT'
) }}

--
-- DBT Model: DimBillingAccount Incremental Upsert
-- Converted from SSIS Package: EDW_BC_Load_DimBillingAccount.dtsx
-- Source: GuideWire BC (multiple tables via complex CTEs)
-- Target: DimBillingAccount
-- Unique Key: PublicID
-- LegacySourceSystem is always 'WC' (Workers Comp)
-- BatchID is passed as a variable
--

WITH parent_acct AS (
    SELECT
        pa.OwnerID,
        CAST(act.AccountNumber AS INT) AS ParentAccountNumber,
        CONCAT(pa.BeanVersion, act.BeanVersion) AS ParentBeanVersion,
        act.UpdateTime AS ParentUpdateTime
    FROM {{ source('guidewire', 'bc_ParentAcct') }} pa
    JOIN {{ source('guidewire', 'bc_account') }} act
        ON act.ID = pa.ForeignEntityID
),

insured_info AS (
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
        CONCAT(ac.BeanVersion, acr.BeanVersion, c.BeanVersion, a.BeanVersion) AS InsuredBeanVersion,
        ac.UpdateTime AS ac_UpdateTime,
        acr.UpdateTime AS acr_UpdateTime,
        c.UpdateTime AS c_UpdateTime,
        a.UpdateTime AS a_UpdateTime
    FROM {{ source('guidewire', 'bc_accountcontact') }} ac
    JOIN {{ source('guidewire', 'bc_accountcontactrole') }} acr
        ON acr.AccountContactID = ac.ID
    JOIN {{ source('guidewire', 'bctl_accountrole') }} tlar
        ON tlar.ID = acr.Role
    LEFT JOIN {{ source('guidewire', 'bc_contact') }} c
        ON c.ID = ac.ContactID
    LEFT JOIN {{ source('guidewire', 'bc_address') }} a
        ON a.ID = c.PrimaryAddressID
    LEFT JOIN {{ source('guidewire', 'bctl_state') }} tls
        ON tls.ID = a.State
    WHERE tlar.TYPECODE = 'insured'
),

source_data AS (
    SELECT DISTINCT
        dt.AccountNumber,
        dt.AccountName,
        CAST(at.NAME AS VARCHAR(50)) AS AccountTypeName,
        parent_acct.ParentAccountNumber,
        CAST(bl.NAME AS VARCHAR(100)) AS BillingLevelName,
        CAST(bas.NAME AS VARCHAR(50)) AS Segment,
        CAST(cst.NAME AS VARCHAR(50)) AS ServiceTierName,
        sz.Name AS SecurityZone,
        insured_info.FirstName,
        insured_info.LastName,
        insured_info.AddressLine1,
        insured_info.AddressLine2,
        insured_info.AddressLine3,
        CAST(insured_info.City AS VARCHAR(50)) AS City,
        CAST(insured_info.State AS VARCHAR(50)) AS State,
        CAST(insured_info.PostalCode AS VARCHAR(50)) AS PostalCode,
        dt.CloseDate AS AccountCloseDate,
        dt.CreateTime AS AccountCreationDate,
        CAST(tlds.NAME AS VARCHAR(50)) AS DeliquencyStatusName,
        dt.FirstTwicePerMthInvoiceDOM AS FirstTwicePerMonthInvoiceDayOfMonth,
        dt.SecondTwicePerMthInvoiceDOM AS SecondTwicePerMonthInvoiceDayOfMonth,
        dt.PublicID,
        dt.ID AS GWRowNumber,
        CAST(CONCAT(
            dt.BeanVersion,
            COALESCE(parent_acct.ParentBeanVersion, ''),
            COALESCE(sz.BeanVersion, '')
        ) AS VARCHAR(20)) AS BeanVersion,
        CASE dt.Retired WHEN 0 THEN 1 ELSE 0 END AS IsActive,
        'WC' AS LegacySourceSystem,
        -- Derived columns (equivalent to SSIS Derived Column transformation)
        '{{ var("batch_id", "default_batch") }}' AS BatchID,
        -- Update times for incremental filter
        dt.UpdateTime AS dt_UpdateTime,
        parent_acct.ParentUpdateTime,
        sz.UpdateTime AS sz_UpdateTime,
        insured_info.ac_UpdateTime,
        insured_info.acr_UpdateTime,
        insured_info.c_UpdateTime,
        insured_info.a_UpdateTime
    FROM {{ source('guidewire', 'bc_account') }} dt
    LEFT JOIN {{ source('guidewire', 'bctl_accounttype') }} at
        ON at.ID = dt.AccountType
    LEFT JOIN parent_acct
        ON parent_acct.OwnerID = dt.ID
    LEFT JOIN {{ source('guidewire', 'bctl_billinglevel') }} bl
        ON bl.ID = dt.BillingLevel
    LEFT JOIN {{ source('guidewire', 'bctl_customerservicetier') }} cst
        ON cst.ID = dt.ServiceTier
    LEFT JOIN {{ source('guidewire', 'bc_securityzone') }} sz
        ON sz.ID = dt.SecurityZoneID
    LEFT JOIN insured_info
        ON insured_info.AccountID = dt.ID
    LEFT JOIN {{ source('guidewire', 'bctl_delinquencystatus') }} tlds
        ON tlds.ID = dt.DelinquencyStatus
    LEFT JOIN {{ source('guidewire', 'bctl_accountsegment') }} bas
        ON bas.ID = dt.Segment
    {% if is_incremental() %}
    -- Incremental filter (equivalent to SSIS parameterized date filter)
    WHERE
        (
            dt.UpdateTime >= DATEADD(day, -7, CURRENT_DATE)
            OR parent_acct.ParentUpdateTime >= DATEADD(day, -7, CURRENT_DATE)
            OR sz.UpdateTime >= DATEADD(day, -7, CURRENT_DATE)
            OR insured_info.ac_UpdateTime >= DATEADD(day, -7, CURRENT_DATE)
            OR insured_info.acr_UpdateTime >= DATEADD(day, -7, CURRENT_DATE)
            OR insured_info.c_UpdateTime >= DATEADD(day, -7, CURRENT_DATE)
            OR insured_info.a_UpdateTime >= DATEADD(day, -7, CURRENT_DATE)
        )
    {% endif %}
),

-- Lookup existing DimBillingAccount records for upsert logic
-- (equivalent to SSIS Lookup transformation)
{% if is_incremental() %}
existing_dim AS (
    SELECT
        PublicID,
        BeanVersion AS EDWBeanVersion
    FROM {{ this }}
    WHERE LegacySourceSystem = 'WC'
),
{% else %}
-- On full-refresh or first run, create empty CTE
existing_dim AS (
    SELECT
        CAST(NULL AS VARCHAR) AS PublicID,
        CAST(NULL AS VARCHAR) AS EDWBeanVersion
    WHERE 1=0
),
{% endif %}

-- Join source to existing to determine insert/update
-- (equivalent to SSIS Conditional Split transformation)
joined AS (
    SELECT
        s.*,
        e.EDWBeanVersion
    FROM source_data s
    LEFT JOIN existing_dim e
        ON s.PublicID = e.PublicID
),

-- Split logic equivalent to SSIS Conditional Split:
-- - If EDWBeanVersion is null, it's an insert (new record)
-- - If BeanVersion != EDWBeanVersion, it's an update (changed record)
-- - If BeanVersion == EDWBeanVersion, it's unchanged (skip)
to_upsert AS (
    SELECT *
    FROM joined
    WHERE
        EDWBeanVersion IS NULL -- insert
        OR BeanVersion != EDWBeanVersion -- update
)

-- Final SELECT with all required columns
-- (equivalent to SSIS OLEDB Destination)
SELECT
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
    PublicID,
    GWRowNumber,
    BeanVersion,
    IsActive,
    LegacySourceSystem,
    BatchID
FROM to_upsert

--
-- CONVERSION NOTES:
-- 1. SSIS Row Count transformations are replaced by DBT run results and logging
-- 2. SSIS OLEDB Command (UPDATE) is handled by DBT incremental materialization
-- 3. SSIS Control Flow error handling is replaced by DBT's built-in error handling
-- 4. SSIS Variables are replaced by DBT variables and Jinja templating
-- 5. SSIS Event Handlers are replaced by DBT hooks and on-run-end macros if needed
--
-- MANUAL INTERVENTION REQUIRED:
-- - Review batch_id variable configuration in dbt_project.yml
-- - Verify source table names and schema references
-- - Test incremental logic with sample data
-- - Configure appropriate DBT hooks for error logging if needed
--