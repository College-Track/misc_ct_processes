WITH
  gather_current_term_data AS (
  SELECT
    -- POPULATE THESE EVERY UPDATE
    "Spring" AS term_to_create,
    "2023-24" AS year_to_create,
    date(2024,01,01) as start_date_of_term_to_create,

    -- END MANAUL UPDATE
    case when at_name like '%(Semester)%' then "Semester" when at_name like '%(Quarter)%' then 'Quarter' else 'Check' end as school_calendar,
    contact_id,
    at_id AS previous_academic_semester_c,
    GAS_Name AS current_gas_name,
    college_track_status_c,
    school_c,
    cc_advisor_at_user_id_c,
    major_c,
    major_other_c,
    second_major_c,
    minor_c,
    indicator_years_since_hs_graduation_c,
    calendar_type_c,
    enrollment_status_c,
    cumulative_credits_awarded_most_recent_c,
    persistence_at_prev_enrollment_status_c,
    '01246000000RNnHAAW' AS record_type_id,
    -- Advising Rubric Fields to Carry Over
    financial_aid_package_c,
    filing_status_c,
    repayment_plan_c,
    repayment_policies_c,
    loan_exit_c,
    academic_networking_50_cred_c,
    academic_networking_over_50_credits_c,
    exploration_career_c,
    internship_5075_credits_c,
    post_graduate_opportunities_75_cred_c,
    interested_in_graduate_school_50_credits_c,
    alumni_network_75_credits_c,
    missing_advising_rubric_academic_v_2_c, 
    missing_advising_rubric_wellness_v_2_c, 
    missing_advising_rubric_financial_v_2_c, 
    missing_advising_rubric_career_v_2_c



  FROM
    `data-studio-260217.prod_core.contact_at_template`
  WHERE
    current_as_c = TRUE
    AND (college_track_status_c = '15A'
      OR (college_track_status_c IN ('16A',
          '17A')
        AND indicator_years_since_hs_graduation_c <5.99) ) ),
  term_ids AS (
  SELECT
    GAS_id,
    academic_year_c,
    GAS_Name,
    term_c,
    academic_calendar_category_c,
    gas_start_date,
  FROM
    `data-studio-260217.prod_staging.stg_salesforce__global_academic_semester_c` ),
  join_term_ids AS (
  SELECT
    gather_current_term_data.*,
    term_ids.GAS_id,
    term_ids.academic_year_c,
    term_ids.GAS_Name AS gas_name_to_create,
    term_ids.term_c as gas_term_to_create

  
  FROM
    gather_current_term_data
  INNER JOIN
    term_ids
  ON
    term_ids.gas_start_date = gather_current_term_data.start_date_of_term_to_create and gather_current_term_data.school_calendar = term_ids.academic_calendar_category_c ),
  filter_data AS (
  SELECT
    *
  FROM
    join_term_ids
  WHERE
    current_gas_name != gas_name_to_create ),
  
  prep_data_for_upload AS (
  
  SELECT
  gas_name_to_create,
    -- These fields are static
    contact_id,
    college_track_status_c,
    previous_academic_semester_c,
    academic_year_c,
    GAS_id,
    -- These fields require a student to be active to be populated
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    major_c
  END
    AS major_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    major_other_c
  END
    AS major_other_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    second_major_c
  END
    AS second_major_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    minor_c
  END
    AS minor_c,
     -- If creating the Fall term, use the Spring term's enrollment status
CASE
        WHEN college_track_status_c != '15A' THEN NULL 
        WHEN (college_track_status_c = '15A'
        AND term_to_create = 'Fall'
        AND enrollment_status_c IS NULL) THEN persistence_at_prev_enrollment_status_c
        ELSE enrollment_status_c
    END AS enrollment_status_c,
    CASE
        WHEN college_track_status_c !='16A' THEN NULL
        ELSE cumulative_credits_awarded_most_recent_c
    END AS Cumulative_Credits_Awarded_All_Terms,



    -- These fields are null for alumni
    CASE
      WHEN college_track_status_c = '17A' THEN NULL
    ELSE
    school_c
  END
    AS school_c,
    CASE
      WHEN college_track_status_c = '17A' THEN NULL
    ELSE
    cc_advisor_at_user_id_c
  END
    AS cc_advisor_at_user_id_c,
------------------ ADVISING RUBRIC SECTION ------------------     
-- Details are found here: 
-- https://docs.google.com/spreadsheets/d/1xoz7mKWl8U1wyVSAz4mcsbE7I86rBRXwUawYdcfpcEY


case when gas_term_to_create IN ('Winter', 'Summer') then false else true end as missing_advising_rubric_career_v_2_c,
case when gas_term_to_create IN ('Summer') then false else true end as missing_advising_rubric_financial_v_2_c,
case when gas_term_to_create IN ('Summer') then false else true end as missing_advising_rubric_wellness_v_2_c,
case when gas_term_to_create IN ('Summer') then false else true end as missing_advising_rubric_academic_v_2_c,
 
-- FIELDS THAT CARRY OVER EVERY TERM
    repayment_plan_c,
    repayment_policies_c,
    loan_exit_c,
    academic_networking_50_cred_c,
    academic_networking_over_50_credits_c,
    exploration_career_c,
    internship_5075_credits_c,
    post_graduate_opportunities_75_cred_c,
    interested_in_graduate_school_50_credits_c,
    alumni_network_75_credits_c,

-- FIELDS THAT RESET EVERY NEW ACADEMIC YEAR
    case when term_to_create = 'Fall' then null
    else financial_aid_package_c end as financial_aid_package_c,
    case when term_to_create = 'Fall' then null
    else filing_status_c end as filing_status__c,



------------------ END ADVISING RUBRIC SECTION ------------------

    '01246000000RNnHAAW' as record_type_id
  FROM
    filter_data ),

    check_record_count as (
      select gas_name_to_create, count(contact_id) as n_students, count(*) as n_records
      from prep_data_for_upload
      group by 1
    )
SELECT
  *
FROM
  prep_data_for_upload
  
