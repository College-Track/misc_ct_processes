-- Victoria Mora is included in the NSC return data set (and submitted by us), but did not actually complete the CT HS Program

WITH
    nsc AS (
        SELECT *, REPLACE(Requester_Return_Field, "_", "") AS contact_id_nsc -- remove ' _ '
        FROM `data-warehouse-289815`.external_datasets.nsc_studenttracker_fall_2022_23_ay
        )

  , sfdc AS (
    SELECT
        Contact_Id
      , AT_Id
      , full_name_c
      , high_school_graduating_class_c
      , site_short
      , College_Track_Status_Name
      , years_since_hs_grad_c
      , academic_year_4_year_degree_earned_c
      , AT_Name
      , AT_Enrollment_Status_c
      , AT_School_Name
      , school_account_18_id_c
      , cc_advisor_c
      , GAS_start_date
      , GAS_end_date
      , GAS_Name
      , GAS_Name_short
      , term_c
      , AY_Name
      , global_academic_semester_c
    FROM `data-studio-260217`.prod_core.contact_at_template
    WHERE
        AT_record_type_name = 'College/University Semester'
    )

  , ipeds AS ( -- sourced from college scorecard
    SELECT
        INSTNM                 AS cs_college_name
      , CAST(UNITID AS STRING) AS cs_ipeds_id -- cast as STR to be able to compare with sfdc ipeds id field
      , OPEID6                 AS cs_college_branch_code_6 -- use college branch code to join to nsc and append ipeds #
    FROM
        `data-warehouse-289815`.external_datasets.college_scorecard_2020_21 --`data-warehouse-289815`.external_datasets.ipeds_2021
    )
  , global_at AS (
    SELECT
        GAS_id
      , GAS_Name
      , GAS_Name_short
      , term_c
      , GAS_start_date
      , GAS_end_date
      , academic_calendar_category_c
      , EXTRACT(YEAR FROM DATE(GAS_start_date)) AS GAS_Year
    FROM `data-studio-260217`.prod_staging.stg_salesforce__global_academic_semester_c
    )

  , mod_sfdc AS ( -- modify field, add academic calendar category
    SELECT
        pat.*
      , UPPER(Contact_Id)                 AS contact_id_mod -- capitalize contact_id letters to match on NSC return field
      , global_at.academic_calendar_category_c
      , account.ipeds_id_c
      , account.school_permanently_closed_c
      , account.academic_calendar_value_c AS sfdc_college_academic_calendar
    FROM
        sfdc                                                            AS pat
            LEFT JOIN global_at                                         AS global_at
                          ON pat.global_academic_semester_c = global_at.GAS_id
            LEFT JOIN `data-studio-260217`.prod_core.dim_school_account AS account
                          ON pat.school_account_18_id_c = account.account_id
    )

  , prep_nsc AS ( -- select certain fields, modify fields, filter to matches and certain enrollment statuses
    SELECT
        College_Track_Status_Name                                                       AS sfdc_ct_status -- include CT status to see what status students are for those with no PAT matches
      , years_since_hs_grad_c                                                           AS sfdc_years_since_hs_grad
      --,  REPLACE(Requester_Return_Field, "_", "")                                       AS contact_id_nsc -- remove ' _ '
      , contact_id_nsc
      , First_Name                                                                      AS first_name_nsc
      , Last_Name                                                                       AS last_name_nsc
      , Record_Found_Y_N -- see KEY below
      , Search_Date
      , REGEXP_REPLACE(SPLIT(College_Code_Branch, '-')[OFFSET(0)], r'^0+',
                       '')                                                              AS nsc_college_code_branch -- remove dash; remove leading and trailing ZEROs
      , College_Name
      , College_State
      , _2_year___4_year -- see KEY below
      , Public___Private
      , DATE(PARSE_DATE('%Y%m%d', CAST(Enrollment_Begin AS STRING)))                    AS nsc_enrollment_start -- see KEY below (Enrollment_Begin)
      , DATE(PARSE_DATE('%Y%m%d', CAST(Enrollment_End AS STRING)))                      AS nsc_enrollment_end -- see KEY below (Enrollment_End)
      , CONCAT(DATE(PARSE_DATE('%Y%m%d', CAST(Enrollment_Begin AS STRING))), ' to ',
               DATE(PARSE_DATE('%Y%m%d', CAST(Enrollment_End AS STRING))))              AS nsc_date_concat
      , EXTRACT(YEAR FROM DATE(PARSE_DATE('%Y%m%d', CAST(Enrollment_Begin AS STRING)))) AS nsc_Year
      , Enrollment_Status                                                               AS nsc_enrollment_status -- see KEY below
      , CASE
            WHEN Enrollment_Status = 'F'
                THEN 'Full-time'
            WHEN Enrollment_Status = 'Q'
                THEN 'Part-time'
            WHEN Enrollment_Status = 'H'
                THEN 'Part-time'
            WHEN Enrollment_Status = 'L'
                THEN 'Part-time'
                ELSE 'Error or Not Enrolled'
            END                                                                         AS nsc_enrollment_status_ft_pt
      , College_Sequence
      , Class_Level
      , CASE
            WHEN Class_Level = 'F'
                THEN 'Undergraduate'
            WHEN Class_Level = 'S'
                THEN 'Undergraduate'
            WHEN Class_Level = 'J'
                THEN 'Undergraduate'
            WHEN Class_Level = 'R'
                THEN 'Undergraduate'
            WHEN Class_Level = 'C'
                THEN 'Undergraduate'
            WHEN Class_Level = 'N'
                THEN 'Undergraduate'
            WHEN Class_Level = 'B'
                THEN 'Undergraduate'
            WHEN Class_Level = 'A'
                THEN "Associate's"
            WHEN Class_Level = 'M'
                THEN 'Graduate'
            WHEN Class_Level = 'D'
                THEN 'Graduate'
            WHEN Class_Level = 'P'
                THEN 'Graduate'
            WHEN Class_Level = 'L'
                THEN 'Graduate'
            WHEN Class_Level = 'G'
                THEN 'Graduate or Professional'
            WHEN Class_Level = 'T'
                THEN "Graduate or Post-Bacc"
                ELSE 'Error'
            END                                                                         AS Class
      , Enrollment_Major_1
      , Enrollment_Major_2
      , Graduated_
      , Graduation_Date
      , Degree_Title
      , Degree_Major_1
      , Degree_Major_2
      , Degree_Major_3
      , Degree_Major_4
    FROM
        nsc
            LEFT JOIN mod_sfdc AS sf ON nsc.contact_id_nsc = sf.contact_id_mod
    WHERE
          Record_Found_Y_N IS TRUE
      AND Enrollment_Status <> ' ' -- exclude students missing enrollment info; IS NOT NULL still returns blanks
      AND Enrollment_Status IN ('F', 'Q', 'H', 'L') -- full-time, quarter-FT, half FT, less than half time
    )

  , mod_nsc AS (
    SELECT nsc.*, ipeds.cs_ipeds_id, ipeds.cs_college_branch_code_6, ipeds.cs_college_name
    FROM
        prep_nsc            AS nsc
            LEFT JOIN ipeds AS ipeds ON nsc.nsc_college_code_branch = ipeds.cs_college_branch_code_6
    )

  , sfdc_nsc_matches AS ( -- join nsc data to sfdc data; filter sfdc data to nsc matches only
    SELECT sfdc.*
    FROM
        mod_nsc                 AS nsc
            INNER JOIN mod_sfdc AS sfdc ON nsc.contact_id_nsc = sfdc.contact_id_mod
    )

  , prep_nsc_enrollment_to_gas
        AS ( -- map nsc enrollment data to appropriate PAT record; rank based on variance from start/end date of global ATs
        SELECT
            -- nsc-term data
            term.*

            -- nsc enrollment data
          , nsc.nsc_date_concat
          , nsc.nsc_enrollment_start
          , nsc.nsc_enrollment_end

            -- create rank
          , DENSE_RANK() OVER (PARTITION BY nsc_date_concat ORDER BY ABS(DATE_DIFF(nsc_enrollment_start, term.GAS_start_date, DAY))) AS start_rank
          , ABS(DATE_DIFF(nsc_enrollment_start, term.GAS_start_date, DAY))                                                           AS start_date_diff
          , DENSE_RANK() OVER (PARTITION BY nsc_date_concat ORDER BY ABS(DATE_DIFF(nsc_enrollment_end, term.GAS_end_date, DAY)))     AS end_rank
          , ABS(DATE_DIFF(nsc_enrollment_end, term.GAS_end_date, DAY))                                                               AS end_date_diff
        FROM
            global_at             AS term
                LEFT JOIN mod_nsc AS nsc ON nsc.nsc_Year = term.GAS_Year
            --WHERE  --nsc.nsc_date_concat = "2023-01-09 to 2023-03-31" -- nsc.nsc_date_concat = "2022-08-22 to 2022-12-16"      --nsc.nsc_date_concat = "2023-01-17 to 2023-05-11"
        WHERE
            nsc.nsc_enrollment_end <= term.GAS_end_date -- < term.GAS_start_date
        GROUP BY
            1
          , 2
          , 3
          , 4
          , 5
          , 6
          , 7
          , 8
          , 9
          , 10
          , 11
        )

  , nsc_enrollment_to_gas AS ( -- map GAS to nsc enrollment period to join to PAT in next cte
    SELECT
        nsc.*
      , gas.GAS_id
      , gas.GAS_Name_short -- COUNT(nsc.nsc_date_concat) -- # of terms mapped to nsc enrollment (e.g. both semester AND quarter terms)
    FROM
        mod_nsc                                  AS nsc
            LEFT JOIN prep_nsc_enrollment_to_gas AS gas ON nsc.nsc_date_concat = gas.nsc_date_concat
    WHERE
          start_rank = 1
      AND end_rank = 1 -- align start/end dates with terms that have the highest rank only
    )

  , count_term_matches AS (
    SELECT DISTINCT
        nsc_date_concat
      , nsc_enrollment_start
      , nsc_enrollment_end
      , GAS_Name_short
      , start_rank
      , end_rank
      , COUNT(GAS_Name_short) AS num_terms -- # of terms mapped to nsc enrollment (e.g. both semester AND quarter terms)
    FROM prep_nsc_enrollment_to_gas
    GROUP BY
        1
      , 2
      , 3
      , 4
      , 5
      , 6
    )

    -- will be used in match_nsc_to_pat
  , match_nsc_to_gas AS (-- map GAS (short name) to nsc enrollment period to join to PAT in next cte; this excludes whether the GAS quarter/semester, and includes a count of # of times a nsc enrollment period is mapped to a GAS
    SELECT DISTINCT
        nsc.*
      , gas.GAS_Name_short
      , gas.num_terms
      , start_rank
      , end_rank -- do not include global academic semester Id; include # of times a term (short) is mapped to a nsc_concat_date (exclude Quarter and Semester specificity)
    FROM
        mod_nsc                          AS nsc
            LEFT JOIN count_term_matches AS gas ON nsc.nsc_date_concat = gas.nsc_date_concat -- map enrollment date to nsc data; it will pull in its mapped GAS name (short), and the # of times the nsc date has the GAS mapped to it
    WHERE
          start_rank = 1 AND end_rank = 1 -- for the term data with a # of times it is mapped to nsc_concat, only pull in top ranking alignment
    )

  , nsc_to_pat_data AS ( -- map nsc enrollment data to student PAT record
    SELECT DISTINCT
        pat.* EXCEPT (GAS_Name_short)
      , nsc.*
      , CASE
            WHEN AT_Id IS NULL
                THEN "No PAT Found for NSC Enrollment Dates"
                ELSE "Yes"
            END AS is_pat_available
    FROM
        sfdc_nsc_matches                     AS pat
            INNER JOIN nsc_enrollment_to_gas AS nsc -- nsc enrollment data with associated PAT record
                           ON pat.global_academic_semester_c = nsc.GAS_id AND pat.contact_id_mod = nsc.contact_id_nsc
    )

  , enrollment_and_college_discrepancies AS ( -- sfdc/nsc colleges do not match, and enrollment status does not match
    SELECT
        CASE
            WHEN nsc_enrollment_status_ft_pt <> AT_Enrollment_Status_c
                THEN 1
                ELSE 0
            END         AS indicator_enrollment_discr
      , Contact_Id
      , AT_Id
      , full_name_c
      , high_school_graduating_class_c
      , site_short
      , College_Track_Status_Name
      , years_since_hs_grad_c
      , academic_year_4_year_degree_earned_c
      , AT_Name
      , nsc_enrollment_start
      , nsc_enrollment_end
      , AT_School_Name
      , College_Name    AS nsc_college_name
      , AT_Enrollment_Status_c
      , nsc_enrollment_status
      , nsc_enrollment_status_ft_pt
      , nsc_Year
      , Class_Level
      , Class
      , Enrollment_Major_1
      , Enrollment_Major_2
      , Graduated_
      , Graduation_Date
      , Degree_Title
      , Degree_Major_1
      , Degree_Major_2
      , Degree_Major_3
      , Degree_Major_4
      , users.user_name AS cc_advisor
      , users.is_active AS active_user
      , cc_advisor_c    AS cc_advisor_user_id
      , sfdc_college_academic_calendar
      , academic_calendar_category_c
      , GAS_start_date
      , GAS_end_date
      , GAS_Name
      , school_account_18_id_c
      , school_permanently_closed_c
    FROM
        nsc_to_pat_data                                       AS enrollment
            LEFT JOIN `data-studio-260217`.prod_core.dim_user AS users
                          ON enrollment.cc_advisor_c = users.user_id -- bring in name of advisor
    WHERE
          AT_Enrollment_Status_c NOT IN ("Full-time", "Part-time") -- not ft/pt in sfdc, but ft/pt in nsc
      AND ipeds_id_c <> cs_ipeds_id -- sfdc college does not match nsc college (college enrollment discrepancy)
    )

  , enrollment_discrepancies AS ( -- sfdc and nsc colleges match, but enrollment status does not
    SELECT
        enrollment.Contact_Id
      , AT_Id
      , full_name_c
      , high_school_graduating_class_c
      , site_short
      , College_Track_Status_Name
      , years_since_hs_grad_c
      , academic_year_4_year_degree_earned_c
      , AT_Name
      , nsc_enrollment_start
      , nsc_enrollment_end
      , AT_School_Name
      , College_Name    AS nsc_college_name
      , AT_Enrollment_Status_c
      , nsc_enrollment_status
      , nsc_enrollment_status_ft_pt
      , nsc_Year
      , Class_Level
      , Class
      , Enrollment_Major_1
      , Enrollment_Major_2
      , Graduated_
      , Graduation_Date
      , Degree_Title
      , Degree_Major_1
      , Degree_Major_2
      , Degree_Major_3
      , Degree_Major_4
      , users.user_name AS cc_advisor
      , users.is_active AS active_user
      , cc_advisor_c    AS cc_advisor_user_id
      , sfdc_college_academic_calendar
      , academic_calendar_category_c
      , GAS_start_date
      , GAS_end_date
      , GAS_Name
      , school_account_18_id_c
      , school_permanently_closed_c
    FROM
        nsc_to_pat_data                                                      AS enrollment
            LEFT JOIN `data-studio-260217`.prod_staging.stg_salesforce__user AS users
                          ON enrollment.cc_advisor_c = users.user_id -- bring in name of advisor
    WHERE
          AT_Enrollment_Status_c NOT IN ("Full-time", "Part-time") -- not ft/pt in sfdc, but ft/pt in nsc
      AND ipeds_id_c = cs_ipeds_id -- sfdc college matches nsc college
    )
-- 2 categories for enrollment discepancy:
-- 1. sfdc enrollment status does not match nsc enrollment status AND the colleges listed for enrollment are NOT the same
-- 2. sfdc enrollment status does not match nsc enrollment status and the colleges are the SAME


SELECT *
FROM enrollment_discrepancies

-- SELECT DISTINCT College_Name, cs_college_name, ipeds_id, cs_college_branch_code_6, nsc_college_code_branch FROM mod_nsc
--match_nsc_to_pat


/*
 KEY
- 2 year, 4 year:
    Type of college that the student attended:
    4 = 4‐year or higher institution
    2 = 2‐year institution
    L = less than 2‐year institution
- Enrollment Status:
    The last enrollment status reported for the student:
    F = Full‐time
    Q = Three‐quarter time
    H = Half‐time
    L = Less than half‐time
    A = Leave of absence
    W = Withdrawn
    D = Deceased
    This field will be blank if the reporting college has not
    defined the student’s enrollment status as directory
    information.
- Enrollment Begin: Begin date for the student’s period of attendance
- Enrollment End: End date for the student’s period of attendance
- Class Level:
    If available, the Class level associated with the student
    as provided by the reporting college:
    F = Freshman (Undergraduate)
    S = Sophomore (Undergraduate)
    J = Junior (Undergraduate)
    R = Senior (Undergraduate)
    C = Certificate (Undergraduate)
    N = Unspecified (Undergraduate)
    B = Bachelor's (Undergraduate)
    M = Master’s (Graduate)
    D = Doctoral (Graduate)
    P = Postdoctorate (Graduate)
    L = First Professional (Graduate)
    G = Unspecified (Graduate/Professional)
    A = Associate's
    T = Post Baccalaureate Certificate
- Degree Title: If available, the title of the degree the student received as provided by the reporting college.
 */
