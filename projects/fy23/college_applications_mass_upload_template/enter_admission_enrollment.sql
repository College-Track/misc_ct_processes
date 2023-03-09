-- create query that compares SFDC data to the data in the template
-- only import differet data

WITH 

 accounts AS (
  SELECT 
    account_id, account_name
  FROM `data-studio-260217.prod_core.dim_school_account`
)

, template AS (
  SELECT *
  FROM `data-warehouse-289815.google_sheets.mass_import_template_enter_admission_enrollment`
)

, add_account_id AS ( -- map account ID based on College Name
  SELECT 
    template.*, accounts.account_id
  FROM template AS template
  LEFT JOIN accounts AS accounts ON template.College_University__Account_Name=accounts.account_name
)

, pull_changes_only AS (
  SELECT * 
  FROM add_account_id
  WHERE EditTracker > 0
      -- account for edits made by staff, but where data needed may still be missing
  AND Admission_Status IS NOT NULL
)

, data_to_import AS ( -- pull edited data, modify field names for import
  SELECT 
      Full_Name
    , Site                              AS site__c 
    , College_University__Account_Name  AS college_name
    , College_Application__ID           AS id                     -- import this
    , Application_Status                AS application_status__c  -- import this
    , Admission_Status                  AS admission_status__c    -- import this
    , Enrollment_Deposit                AS enrollment_deposit__c  -- import this
    , Housing_Application               AS housing_application__c -- import this
    , Financial_Aid_Verification_Status AS verification_status__c -- import this
    , Award_Letter                      AS award_letter__c        -- import this

  FROM pull_changes_only
)

SELECT * FROM data_to_import
