/*
Use this query to create career readiness activity records for BAY AREA only
Will only work if:
  - Career readiness records have already been generated
  - Visit dates have been updated, entered in the google sheet table used by BigQuery
  - The CDE lead has set the Visit Dates for each student (e.g. here: https://docs.google.com/spreadsheets/d/1Z7xKROSOhnqGqDS5R3ZvC1hoNeyV2RmRa-UWheVAWME/edit#gid=861113995)
*/

WITH
    reporting_group AS ( -- cde records - students accepted into program; pull fields to include in import file
        SELECT
            cde.student_c
          , contact.full_name_c
          , contact.site_short                 AS Site
          , contact.region_short               AS Region
          , cde.program_participation_status_c AS Program_Participation_Status
          , cde.organization_c                 AS region_crp
          , cde.id                             AS Career_Readiness_Engagement
        FROM
            `data-warehouse-289815.salesforce.career_readiness_c`    AS cde
                LEFT JOIN `data-studio-260217.prod_core.dim_contact` AS contact ON cde.student_c = contact.contact_id
        WHERE
              cde.record_type_id = '0121M000001cnVvQAI'                           # Career Discovery Externship
          AND cde.global_academic_year_c = 'a1b46000000dRRAAA2'                   # 2022-23
          AND program_participation_status_c = 'Interviewed, offered, and accepted'
          AND contact.site_short IN ('East Palo Alto', 'Oakland', 'San Francisco') # Bay Area only
        )
    
  , visit_dates AS ( -- company visits for cde
    SELECT *
    FROM `data-warehouse-289815.google_sheets.cde_fy23_visit_dates` -- UPDATE WITH FY23 TEMPLATE LINKED ABOVE
    )

  , join_visit_dates_to_cde AS (
    SELECT cde.*, dates.*
    
    -- align visit dates & accounts to cde student records; join on contact_id
    FROM
        reporting_group           AS cde
            LEFT JOIN visit_dates AS schedule ON cde.contact_id = schedule.contact_id
    )

  , organize_fields_for_import AS (
    SELECT
        student_c
      , full_name_c
      , Site
      , Region
      , Program_Participation_Status
      , Account
      , Career_Readiness_Engagement AS Career_Readiness_Engagement
      , '0121M000001cnVtQAI'        AS recordtypeid
      , Account_ID                  AS Event_Specific_Organization
      , DATE                        AS Date
      , type_of_event               AS Type_of_Event
    FROM join_visit_dates_to_cde
    )

SELECT *
FROM organize_fields_for_import
       
