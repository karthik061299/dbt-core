{{ config(materialized='incremental', unique_key=['PublicID', 'LegacySourceSystem'], alias='DimBillingAccount') }}

WITH source_data AS (
    -- This CTE replicates the logic from the OLE_SRC - GuideWire source query in the SSIS package
    SELECT
        acc.AccountNumber,
        acc.AccountName,
        acctyp.TypeName AS AccountTypeName,
        pacc.AccountNumber AS ParentAccountNumber,
        bl.TypeName AS BillingLevelName,
        seg.TypeName AS Segment,
        st.TypeName AS ServiceTierName,
        sz.Name AS SecurityZone,
        con.FirstName,
        con.LastName,
        addr.AddressLine1,
        addr.AddressLine2,
        addr.AddressLine3,
        addr.City,
        stc.TypeName AS State,
        addr.PostalCode,
        acc.CloseDate AS AccountCloseDate,
        acc.CreateTime AS AccountCreationDate,
        ds.TypeName AS DeliquencyStatusName,
        acc.InvoiceDayOfMonth AS FirstTwicePerMonthInvoiceDayOfMonth,
        acc.SecondInvoiceDayOfMonth AS SecondTwicePerMonthInvoiceDayOfMonth,
        acc.PublicID,
        acc.BeanVersion,
        1 AS IsActive,
        'WC' AS LegacySourceSystem, -- Hardcoded value from 'DRV - BatchID & Legacy' component
        '{{ var('batch_id', 'dbt_run') }}' AS BatchID -- Replicates the use of User::BatchID
    FROM
        bc_account acc
    LEFT JOIN bc_accounttype acctyp ON acc.AccountType = acctyp.ID
    LEFT JOIN bc_account pacc ON acc.ParentAccountID = pacc.ID
    LEFT JOIN bc_billinglevel bl ON acc.BillingLevel = bl.ID
    LEFT JOIN bc_segment seg ON acc.Segment = seg.ID
    LEFT JOIN bc_servicetier st ON acc.ServiceTier = st.ID
    LEFT JOIN bc_securityzone sz ON acc.SecurityZoneID = sz.ID
    LEFT JOIN bc_accountcontact ac ON acc.ID = ac.AccountID
    LEFT JOIN bc_contact con ON ac.ContactID = con.ID
    LEFT JOIN bc_address addr ON con.PrimaryAddressID = addr.ID
    LEFT JOIN bc_state stc ON addr.State = stc.ID
    LEFT JOIN bc_delinquencystatus ds ON acc.DelinquencyStatus = ds.ID
    WHERE acc.LoadCommandID IS NULL
    {% if is_incremental() %}
        -- In an incremental run, only process records updated since the last run
        AND acc.UpdateTime > (SELECT MAX(DateUpdated) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    GETDATE() as DateCreated,
    GETDATE() as DateUpdated
FROM source_data