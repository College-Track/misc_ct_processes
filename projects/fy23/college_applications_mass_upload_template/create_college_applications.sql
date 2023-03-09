-- create query that compares SFDC data to the data in the template
-- only import different data

WITH 
 template AS (
  SELECT *
  FROM `data-warehouse-289815.google_sheets.mass_import_template_create_college_application` 
)

, accounts AS (
  SELECT 
    account_id, account_name
  FROM `data-studio-260217.prod_core.dim_school_account`
)

, add_account_id AS ( -- map account ID based on College Name
  SELECT 
    template.*, account_id
  FROM template      AS template
  LEFT JOIN accounts AS accounts ON template.College_University=accounts.account_name
)

, pull_changes_only AS (
  SELECT * 
  FROM add_account_id
  WHERE EditTracker > 0 
    -- account for edits made by staff, but where data needed may still be missing
  AND College_University IS NOT NULL 
  AND Application_Status IS NOT NULL
)

, identify_existing_records AS ( -- see if there are already application records before creating new ones
  SELECT 
    fct.college_app_id
    ,fct.college_name
    ,template.*
  FROM pull_changes_only                                           AS template 
  LEFT JOIN `data-studio-260217.prod_core.fct_college_application` AS fct
    ON  fct.contact_id   = template._18_Digit_ID
    AND fct.college_name = template.College_University
  WHERE college_app_id IS NULL  -- if there is not a record ID, then the application is not in the system yet. Ok to upload!
)

, data_to_import AS ( -- pull edited data, modify field names for import
  SELECT 
      Full_Name
    , Site                               AS site__c
    , College_University                 AS college_name
    , _18_Digit_ID                       AS id                                      -- import this
    , account_id                         AS college_university__c                   -- from account table: import this
    , Application_Status                 AS application_status__c                   -- import this
    , Admission_Status                   AS admission_status__c                     -- import this
    , Requested_application_fee_waiver   AS Requested_application_fee_waiver__c     -- import this
    , Application_fee_waiver_granted_    AS Application_fee_waiver_granted__c       -- import this
    , Requested_Application_Fee_Payment_ AS Requested_Application_Fee_Payment__c    -- import this
  FROM identify_existing_records
)
SELECT * FROM data_to_import
