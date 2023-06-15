WITH
  gather_current_term_data AS (
  SELECT
    -- POPULATE THESE EVERY UPDATE
    "Spring" AS term_to_create,
    "2022-23" AS year_to_create,
    -- END MANAUL UPDATE
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
    -- Advising Rubric Fields to Cary Over Term to Term
    financial_aid_package_c,
    free_checking_account_c,
    e_fund_c,
    repayment_plan_c,
    loan_exit_c,
    academic_networking_50_cred_c,
    academic_networking_over_50_credits_c,
    extracurricular_activity_c,
    finding_opportunities_75_c,
    resume_cover_letter_c,
    career_counselor_25_credits_c,
    career_field_2550_credits_c,
    resources_2550_credits_c,
    internship_5075_credits_c,
    post_graduate_plans_5075_creds_c,
    post_graduate_opportunities_75_cred_c,
    alumni_network_75_credits_c
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
    academic_calendar_category_c
  FROM
    `data-studio-260217.prod_staging.stg_salesforce__global_academic_semester_c` ),
  join_term_ids AS (
  SELECT
    gather_current_term_data.*,
    term_ids.GAS_id,
    term_ids.academic_year_c,
    term_ids.GAS_Name AS gas_name_to_create
  FROM
    gather_current_term_data
  INNER JOIN
    term_ids
  ON
    term_ids.GAS_Name = CONCAT(gather_current_term_data.term_to_create, " ", gather_current_term_data.year_to_create, " ", "(", calendar_type_c,")") ),
  filter_data AS (
  SELECT
    *
  FROM
    join_term_ids
  WHERE
    current_gas_name != gas_name_to_create ),
  prep_data_for_upload AS (
  SELECT
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
    -- These are the advising rubric fields. They are null for non-active students
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    financial_aid_package_c
  END
    AS financial_aid_package_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    free_checking_account_c
  END
    AS free_checking_account_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    e_fund_c
  END
    AS e_fund_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    repayment_plan_c
  END
    AS repayment_plan_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    loan_exit_c
  END
    AS loan_exit_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    academic_networking_50_cred_c
  END
    AS academic_networking_50_cred_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    academic_networking_over_50_credits_c
  END
    AS academic_networking_over_50_credits_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    extracurricular_activity_c
  END
    AS extracurricular_activity_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    finding_opportunities_75_c
  END
    AS finding_opportunities_75_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    resume_cover_letter_c
  END
    AS resume_cover_letter_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    career_counselor_25_credits_c
  END
    AS career_counselor_25_credits_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    career_field_2550_credits_c
  END
    AS career_field_2550_credits_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    resources_2550_credits_c
  END
    AS resources_2550_credits_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    internship_5075_credits_c
  END
    AS internship_5075_credits_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    post_graduate_plans_5075_creds_c
  END
    AS post_graduate_plans_5075_creds_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    post_graduate_opportunities_75_cred_c
  END
    AS post_graduate_opportunities_75_cred_c,
    CASE
      WHEN college_track_status_c != '15A' THEN NULL
    ELSE
    alumni_network_75_credits_c
  END
    AS alumni_network_75_credits_c,
    '01246000000RNnHAAW' as record_type_id
  FROM
    filter_data )
SELECT
  *
FROM
  prep_data_for_upload
