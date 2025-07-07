{{ config(
    materialized='incremental',
    unique_key='Id',
    on_schema_change='append_new_columns'
) }}

--
-- Salesforce_TFS_Converted_DBT_Model.sql
--
-- This model implements the core incremental sync logic from the SSIS package 'Salesforce_TFS.dtsx'.
-- It covers:
--   - Incremental extraction of Salesforce Cases and Attachments
--   - Incremental extraction of TFS Work Items and Attachments
--   - Variable-driven windowing (LastPullDate, StartTime)
--   - Error logging and last-run tracking are NOT implemented here (manual intervention required)
--   - Non-SQL transformations (e.g., BytesToString, MD5 hashing, file uploads) are flagged for manual implementation
--
-- NOTE: This model assumes that the necessary Salesforce, TFS, and logging tables are available in your warehouse.
--       Replace source references as needed for your warehouse schema.
--

-- 1. Get Last Pull Date (for incremental loads)
with last_pull as (
    select max(LastPullDate) as last_pull_date
    from {{ ref('integration_last_pull_date') }}
),

-- 2. Define Start Time (current UTC minus 30 seconds)
start_time as (
    select dateadd(second, -30, current_timestamp) as start_time
),

-- 3. Incremental Salesforce Cases (SFDC - TFS)
new_cases as (
    select
        AccountId, AssetId, CaseNumber, ClosedDate, Comments, ContactEmail, ContactFax, ContactId, ContactMobile, ContactPhone,
        CreatedById, CreatedDate, Description, HasCommentsUnreadByOwner, HasSelfServiceComments, Id, IsClosed, IsDeleted, IsEscalated,
        kws_np__Cost__c, kws_np__EngineeringReqNumber__c, kws_np__NCDR_Series__c, kws_np__PotentialLiability__c, kws_np__Product__c,
        kws_np__SLAViolation__c, kws_np__Total_Amount__c, kws_np__WorkItemId__c, LastModifiedById, LastModifiedDate, LastReferencedDate,
        LastViewedDate, Origin, OwnerId, ParentId, Priority, Reason, Status, Subject, SuppliedCompany, SuppliedEmail, SuppliedName,
        SuppliedPhone, SystemModstamp, Type
    from {{ source('salesforce', 'case') }} c
    cross join last_pull lp
    cross join start_time st
    where c.SystemModstamp > lp.last_pull_date
      and c.SystemModstamp <= st.start_time
      and c.LastModifiedById != '{{ var("sf_integration_user") }}'
),

-- 4. Incremental Salesforce Attachments (SFDC - TFS)
case_workitems as (
    select id, kws_np__WorkItemId__c
    from {{ source('salesforce', 'case') }}
),

new_attachments as (
    select
        LinkedEntityId, ContentDocument_Title as Title, ContentDocument_FileExtension as FileExtension,
        ContentDocument_LatestPublishedVersion_VersionData as VersionData, ContentDocument_CreatedBy_Name as CreatedByName,
        SystemModstamp, ContentDocument_CreatedById
    from {{ source('salesforce', 'contentdocumentlink') }} cdl
    cross join last_pull lp
    cross join start_time st
    where cdl.LinkedEntityId in (select id from {{ source('salesforce', 'case') }})
      and cdl.SystemModstamp > lp.last_pull_date
      and cdl.SystemModstamp <= st.start_time
      and cdl.ContentDocument_CreatedById != '{{ var("sf_integration_user") }}'
),

attachments_with_workitem as (
    select
        na.LinkedEntityId,
        cw.kws_np__WorkItemId__c as WorkItemId,
        na.Title,
        na.FileExtension,
        na.VersionData,
        na.CreatedByName
    from new_attachments na
    left join case_workitems cw on na.LinkedEntityId = cw.id
    where na.FileExtension is null or na.FileExtension != 'snote'
),

-- 5. Incremental TFS WorkItem updates (TFS - SFDC)
workitem_updates as (
    select
        System_Id, System_Title, System_Description, SalesforceID, System_State, System_ChangedDate, System_ChangedBy
    from {{ source('tfs', 'workitems') }} wi
    cross join last_pull lp
    cross join start_time st
    where wi.System_WorkItemType = 'Issue'
      and wi.SalesforceID is not null and wi.SalesforceID != ''
      and wi.System_ChangedDate > lp.last_pull_date
      and wi.System_ChangedDate <= st.start_time
      and wi.System_ChangedBy != '{{ var("tfs_integration_user") }}'
),

-- 6. TFS Attachments to Salesforce (TFS - SFDC Files)
tfs_attachments as (
    select
        *
    from {{ source('tfs', 'attachments') }} ta
    cross join last_pull lp
    cross join start_time st
    where ta.Attachment_AttachedTime > lp.last_pull_date
      and ta.Attachment_AttachedTime <= st.start_time
    -- Additional filters and lookups required for ContentDocument matching (manual intervention required)
)

-- Final SELECT: This is a placeholder. In a real DBT project, you would split each flow into its own model and use exposures/macros for orchestration.
select
    'new_cases' as flow, *
from new_cases
union all
select
    'attachments_with_workitem' as flow, *
from attachments_with_workitem
union all
select
    'workitem_updates' as flow, *
from workitem_updates
union all
select
    'tfs_attachments' as flow, *
from tfs_attachments

--
-- Manual Intervention Required:
--   - File binary handling, MD5 hashing, and file uploads must be implemented outside SQL (e.g., in Python or via Fivetran connectors).
--   - Error logging to dbo.ErrorLog is not implemented here; use DBT tests or external logging.
--   - ContentDocumentLink creation (Foreach loop) must be handled via orchestration or external scripts.
--   - Update LastPullDate logic should be implemented as a post-hook or via orchestration.
--
-- Variables required in dbt_project.yml or as environment variables:
--   - sf_integration_user
--   - tfs_integration_user
--
