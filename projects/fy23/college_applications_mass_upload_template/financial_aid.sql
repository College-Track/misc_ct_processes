-- create query that compares SFDC data to the data in the template
-- only import differet data

WITH 

  template AS (
  SELECT *
  FROM `data-warehouse-289815.google_sheets.mass_import_template_financial_aid`
)

, pull_changes_only AS (
  SELECT * 
  FROM template
  WHERE EditTracker > 0
)

, data_to_import AS ( -- pull edited data, modify field names for import
  SELECT 
      Full_Name
    , grade
    , Site                                        AS site__c 
    , _18_Digit_ID                                AS id                                           -- record id: import this
    , FA_Req__FAFSA_Alternative_Financial_Aid     AS FA_Req__FAFSA_Alternative_Financial_Aid__c   -- import this
    , FA_Req__EFC_Source                          AS FA_Req__EFC_Source__c                        -- import this
    , FA_Req__Exp_Financial_Contribution__EFC_    AS FA_Req__Exp_Financial_Contribution__EFC__c   -- import this
    , FA_Req__Annual_Adjusted_Gross_Income        AS FA_Req__Annual_Adjusted_Gross_Income__c      -- import this

  FROM pull_changes_only
)

SELECT * FROM data_to_import
