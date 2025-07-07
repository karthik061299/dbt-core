{{ config(materialized="table") }}

-- DBT Model: Salesforce_TFS_Converted_DBT_Model.sql
-- This model represents the core SQL-based ETL logic extracted and modernized from the SSIS package 'Salesforce_TFS.dtsx'.
-- Non-SQL operations (e.g., writing to Salesforce, TFS, or logging) are flagged for manual intervention.

-- 1. Get LastPullDate (used for incremental loads)
with last_pull as (
    select max(LastPullDate) as lastpulldate
    from {{ source('dbo', 'IntegrationLastPullDate') }}
),

-- 2. Define StartTime (current UTC minus 30 seconds)
start_time as (
    select dateadd(second, -30, getutcdate()) as starttime
),

-- 3. Sync Newly Created Salesforce Cases (for TFS integration)
new_cases as (
    select
        AccountId, AssetId, CaseNumber, ClosedDate, Comments, ContactEmail, ContactFax, ContactId, ContactMobile, ContactPhone,
        CreatedById, CreatedDate, Description, HasCommentsUnreadByOwner, HasSelfServiceComments, Id, IsClosed, IsDeleted, IsEscalated,
        kws_np__Cost__c, kws_np__EngineeringReqNumber__c, kws_np__NCDR_Series__c, kws_np__PotentialLiability__c, kws_np__Product__c,
        kws_np__SLAViolation__c, kws_np__Total_Amount__c, kws_np__WorkItemId__c, LastModifiedById, LastModifiedDate, LastReferencedDate,
        LastViewedDate, Origin, OwnerId, ParentId, Priority, Reason, Status, Subject, SuppliedCompany, SuppliedEmail, SuppliedName,
        SuppliedPhone, SystemModstamp, Type
    from {{ source('salesforce', 'case') }}
    where SystemModstamp > (select lastpulldate from last_pull)
      and SystemModstamp <= (select starttime from start_time)
      and LastModifiedById != '{{ var("SFIntegrationUser") }}'
),

-- 4. Sync Case Attachments (Salesforce to TFS)
case_workitems as (
    select id, kws_np__WorkItemId__c
    from {{ source('salesforce', 'case') }}
),

new_attachments as (
    select
        LinkedEntityId,
        ContentDocument.Title,
        ContentDocument.FileExtension,
        ContentDocument.LatestPublishedVersion.VersionData,
        ContentDocument.CreatedBy.Name
    from {{ source('salesforce', 'ContentDocumentLink') }}
    where LinkedEntityId in (select id from case_workitems)
      and SystemModstamp > (select lastpulldate from last_pull)
      and SystemModstamp <= (select starttime from start_time)
      and ContentDocument.CreatedById != '{{ var("SFIntegrationUser") }}'
),

-- 5. Sync WorkItem updates (TFS to Salesforce)
workitem_updates as (
    select
        System.Id,
        System.Title,
        System.Description,
        SalesforceID,
        System.State
    from {{ source('tfs', 'WorkItems') }}
    where System.WorkItemType = 'Issue'
      and SalesforceID <> ''
      and System.ChangedDate > '{{ var("LastPullDateUTC") }}'
      and System.ChangedDate <= '{{ var("StartTimeUTC") }}'
      and System.ChangedBy <> '{{ var("TFSIntegrationUser") }}'
),

-- 6. Sync TFS Attachments to Salesforce ContentVersion
-- (Requires manual intervention for binary/file handling and API integration)
tfs_attachments as (
    select *
    from {{ source('tfs', 'Attachments') }}
),

salesforce_contentdocs as (
    select *
    from {{ source('salesforce', 'ContentDocument') }}
)

-- Final SELECTs for review (modular outputs)
select * from last_pull
union all
select * from start_time
union all
select * from new_cases
union all
select * from case_workitems
union all
select * from new_attachments
union all
select * from workitem_updates
union all
select * from tfs_attachments
union all
select * from salesforce_contentdocs

-- NOTE:
-- 1. Actual writing to Salesforce, TFS, and logging destinations must be implemented via external orchestration or manual steps.
-- 2. File/binary handling (attachments) and API-based upserts are not natively supported in DBT SQL and require custom scripts or external tools.
-- 3. All dynamic variables (e.g., SFIntegrationUser, LastPullDateUTC, StartTimeUTC, TFSIntegrationUser) should be defined in dbt_project.yml or as environment variables.
-- 4. Error handling and logging are not implemented in this SQL model and should be handled externally.
