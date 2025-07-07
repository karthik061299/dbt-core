-- DBT Model: Salesforce - TFS_Converted_DBT_Model.sql
-- Source: SSIS Package 'Salesforce - TFS.dtsx'
-- This model represents the ETL logic migrated from SSIS to DBT Cloud.
-- All dynamic variables are implemented as Jinja/DBT variables or sources.
-- Manual intervention may be required for non-SQL transformations or external API actions.

{{ config(materialized='table') }}

-- Step 1: Get LastPullDate (Incremental Load Anchor)
with last_pull as (
    select max(LastPullDate) as last_pull_date
    from {{ source('logging', 'IntegrationLastPullDate') }}
),

-- Step 2: Set StartTime (UTC now minus 30 seconds)
-- DBT does not support variable assignment in SQL; use Jinja for dynamic timestamps if needed.
-- For demo, we use current_timestamp - interval '30 seconds'.
start_time as (
    select (current_timestamp - interval '30 seconds') as start_time
),

-- Step 3: Sync Newly Created Salesforce Cases (SFDC - TFS)
new_cases as (
    select
        AccountId, AssetId, CaseNumber, ClosedDate, Comments, ContactEmail, ContactFax, ContactId, ContactMobile, ContactPhone, CreatedById, CreatedDate, Description, HasCommentsUnreadByOwner, HasSelfServiceComments, Id, IsClosed, IsDeleted, IsEscalated, kws_np__Cost__c, kws_np__EngineeringReqNumber__c, kws_np__NCDR_Series__c, kws_np__PotentialLiability__c, kws_np__Product__c, kws_np__SLAViolation__c, kws_np__Total_Amount__c, kws_np__WorkItemId__c, LastModifiedById, LastModifiedDate, LastReferencedDate, LastViewedDate, Origin, OwnerId, ParentId, Priority, Reason, Status, 
        coalesce(Subject, 'Subject') as Subject, -- ReplaceNULL equivalent
        SuppliedCompany, SuppliedEmail, SuppliedName, SuppliedPhone, SystemModstamp, Type,
        current_timestamp as dbt_run_time, -- GetUtcDate() equivalent
        'Sync Newly created Cases' as task_name
    from {{ source('salesforce', 'Case') }}
    where SystemModstamp > (select last_pull_date from last_pull)
      and SystemModstamp <= (select start_time from start_time)
      and LastModifiedById != '{{ var("SFIntegrationUser", "integration_user_id") }}'
),

-- Step 4: Sync Case Attachments (SFDC - TFS)
case_attachments as (
    select
        l.LinkedEntityId,
        d.Title as file_title,
        d.FileExtension,
        v.VersionData,
        d.CreatedById,
        d.CreatedBy.Name as created_by_name,
        c.kws_np__WorkItemId__c,
        case when d.FileExtension != 'snote' then d.Title || '.' || d.FileExtension else null end as file_name,
        current_timestamp as dbt_run_time,
        'Sync Case Attachments' as task_name
    from {{ source('salesforce', 'ContentDocumentLink') }} l
    join {{ source('salesforce', 'ContentDocument') }} d on l.ContentDocumentId = d.Id
    join {{ source('salesforce', 'ContentVersion') }} v on d.LatestPublishedVersionId = v.Id
    join {{ source('salesforce', 'Case') }} c on l.LinkedEntityId = c.Id
    where l.SystemModstamp > (select last_pull_date from last_pull)
      and l.SystemModstamp <= (select start_time from start_time)
      and d.CreatedById != '{{ var("SFIntegrationUser", "integration_user_id") }}'
      and d.FileExtension != 'snote'
),

-- Step 5: Sync WorkItem Updates (TFS - SFDC)
workitem_updates as (
    select
        System_Id,
        System_Title,
        System_Description,
        SalesforceID,
        System_State,
        current_timestamp as dbt_run_time,
        'Sync WorkItem updates' as task_name
    from {{ source('tfs', 'WorkItems') }}
    where System_WorkItemType = 'Issue'
      and SalesforceID != ''
      and System_ChangedDate > '{{ var("LastPullDateUTC", "2022-01-01T00:00:00Z") }}'
      and System_ChangedDate <= '{{ var("StartTimeUTC", "2022-01-01T00:00:00Z") }}'
      and System_ChangedBy != '{{ var("TFSIntegrationUser", "tfs_integration_user") }}'
),

-- Step 6: Sync TFS - SFDC Files
workitem_files as (
    select
        System_Id,
        SalesforceID,
        -- File hash and name extraction would require a custom macro or UDF; flag for manual intervention
        null as file_md5_hash, -- MANUAL: Implement BytesToHex(ComputeMD5Hash([Attachment_BinaryContent]))
        null as file_name -- MANUAL: Extract from attachment
    from {{ source('tfs', 'WorkItems') }}
    where System_WorkItemType = 'Issue'
      and SalesforceID != ''
      and System_ChangedDate > '{{ var("LastPullDateUTC", "2022-01-01T00:00:00Z") }}'
      and System_ChangedDate <= '{{ var("StartTimeUTC", "2022-01-01T00:00:00Z") }}'
      and System_ChangedBy != '{{ var("TFSIntegrationUser", "tfs_integration_user") }}'
),

-- Step 7: ContentDocumentLink Creation (Foreach Loop)
content_document_links as (
    select
        v.Id as ContentVersionId,
        v.ContentDocumentId,
        '{{ var("SFLinkedEntityID", "") }}' as LinkedEntityId, -- MANUAL: Set via macro or variable
        current_timestamp as dbt_run_time,
        'Create ContentDocumentLink' as task_name
    from {{ source('salesforce', 'ContentVersion') }} v
    -- MANUAL: Implement foreach logic if needed
)

-- Final SELECT: Union all flows for audit/logging/demo purposes
select * from new_cases
union all
select * from case_attachments
union all
select * from workitem_updates
union all
select * from workitem_files
union all
select * from content_document_links

--
-- MANUAL INTERVENTION REQUIRED:
-- 1. Implement file hash and file name extraction logic for attachments (BytesToHex, ComputeMD5Hash, etc.).
-- 2. Implement foreach logic for ContentDocumentLink creation if required.
-- 3. Map error handling to DBT logging or downstream error tables if needed.
-- 4. Validate all source references (use correct DBT source definitions).
-- 5. Replace variable defaults with production values or DBT variables/macros.
