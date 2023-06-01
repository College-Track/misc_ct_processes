-- Purpose: Identify students who earned an Associate's degree or certificate of some kind.
-- Students that do not have the PAT record to store the data, so create it

WITH
    nsc AS (
        SELECT *, REPLACE(Requester_Return_Field, "_", "") AS contact_id_nsc -- remove ' _ '
        FROM `data-warehouse-289815`.external_datasets.nsc_studenttracker_fall_2022_23_ay
        )

  , sfdc AS (
    SELECT
        -- contact data
        Contact_Id
      , full_name_c
      , high_school_graduating_class_c
      , site_short
      , College_Track_Status_Name

        -- PAT data
      , school_account_18_id_c
      , cc_advisor_c
      , AT_Id
      , AT_Name
      , AT_Enrollment_Status_c
      , AT_School_Name
      , type_of_degree_earned_c

        -- GAS data
      , GAS_start_date
      , GAS_end_date
      , GAS_Name
      , GAS_Name_short
      , term_c
      , AY_Name
      , global_academic_semester_c
    FROM `data-studio-260217`.prod_core.contact_at_template
    WHERE
          indicator_completed_ct_hs_program_c = TRUE
      AND College_Track_Status_Name IN ("Active: Post-Secondary", "Inactive: Post-Secondary")
      AND AT_record_type_name = 'College/University Semester'
      --AND type_of_degree_earned_c IS NULL -- BA/BS, AA/AS, or Certificate not recorded on this PAT - move this filter downstream, otherwise missing pats CTE may pull these terms in
    )

  , sfdc_ipeds AS (
    SELECT
        account_id
      , account_name
      , predominant_degree_awarded_c
      , CASE
            WHEN predominant_degree_awarded_c = "Predominantly bachelor's-degree granting"
                THEN 1
            WHEN predominant_degree_awarded_c = "Predominantly associate's-degree granting"
                THEN 2
                ELSE 3
            END    AS rank_college_type -- use for ranking downstream if multiple schools recorded for a single pat record
      , academic_calendar_value_c
      , billing_state
      , billing_state_code
      , ipeds_id_c AS sfdc_ipeds
    FROM `data-studio-260217`.prod_core.dim_school_account
    )

  , global_at AS ( -- includes some field modification to reduce CTE count
    SELECT
        GAS_id
      , GAS_Name
      , GAS_Name_short
      , academic_year_c
      , GAS_start_date
      , GAS_end_date
      , academic_calendar_category_c -- used in map ranking further downstream
      , EXTRACT(YEAR FROM DATE(GAS_start_date)) AS GAS_Year
    FROM `data-studio-260217`.prod_staging.stg_salesforce__global_academic_semester_c
    )

  , cs_ipeds
        AS ( -- extract ipeds for colleges in nsc data; sourced from college scorecard; some modification to reduce CTE count
        SELECT
            INSTNM                 AS cs_college_name
          , CAST(UNITID AS STRING) AS cs_ipeds_id -- cast as STR to be able to compare with sfdc ipeds id field
          , OPEID6                 AS cs_college_branch_code_6 -- use college branch code to join to nsc and append ipeds #
        FROM `data-warehouse-289815`.external_datasets.college_scorecard_2020_21
        )

-- Begin additional modifications, joins --

  , combine_ipeds AS (
    SELECT
        sfdc.*, cs.*, SPLIT(academic_calendar_value_c, ' ')[OFFSET(0)] AS college_academic_calendar -- extract first word of the STRING
    FROM
        sfdc_ipeds             AS sfdc
            LEFT JOIN cs_ipeds AS cs ON sfdc.sfdc_ipeds = cs.cs_ipeds_id
    )

  , mod_sfdc AS ( -- modify fields
    SELECT
        pat.* EXCEPT (global_academic_semester_c)
      , pat.global_academic_semester_c AS PAT_GAS_id
      , UPPER(Contact_Id)              AS contact_id_mod -- capitalize contact_id letters to match on NSC contact_id field
    FROM sfdc AS pat
    )

  , prep_nsc AS ( -- filter for graduates only; select certain fields, modify fields, filter to matches
    SELECT
        contact_id_nsc
      , First_Name                                                            AS first_name_nsc
      , Last_Name AS last_name_nsc
      , REGEXP_REPLACE(SPLIT(College_Code_Branch, '-')[OFFSET(0)], r'^0+','') AS nsc_college_code_branch -- remove dash; remove leading and trailing ZEROs; this field will be used to retrieve ipeds id
      , College_Name
      , College_State

      -- graduation data
      , Graduated_
      , DATE(PARSE_DATE('%Y%m%d', CAST(Graduation_Date AS STRING)))                    AS nsc_graduation_date -- see KEY below (Enrollment_Begin)
      , EXTRACT(YEAR FROM DATE(PARSE_DATE('%Y%m%d', CAST(Graduation_Date AS STRING)))) AS nsc_grad_year

      , Degree_Title
      , Degree_Major_1
      , Degree_Major_2
      , Degree_Major_3
      , Degree_Major_4
      , Class_Level
    FROM nsc
    WHERE
          Record_Found_Y_N IS TRUE
      AND _2_year___4_year IN ('4', '2', 'L') -- graduated from 4-year -- "2" for 2-year, "L" for certificate and other
      AND Graduated_ IS TRUE
      AND Degree_Title NOT LIKE '%POST-BACCALAUREATE%'
      AND Degree_Title NOT LIKE '%GRADUATE%'
      AND ((Degree_Title LIKE '%ASSOCIATE%' OR Degree_Title LIKE '%CERTIF%' OR Degree_Title LIKE '%AA %' OR
            Degree_Title LIKE '%AS %' OR Degree_Title LIKE '%CA %')
               OR  Degree_Title IS NULL) -- student earned a AA/CERT OR it is not listed
    )

  , mod_nsc AS ( -- append ipeds id to college in nsc data
    SELECT
        nsc.*
      , ipeds.account_name
      , ipeds.account_id
      , ipeds.academic_calendar_value_c
      , CASE
        WHEN ipeds.college_academic_calendar = 'Trimester'
            THEN 'Semester'
            ELSE ipeds.college_academic_calendar
                END AS college_academic_calendar
      , ipeds.sfdc_ipeds
      , ipeds.rank_college_type -- salesforce account degree type ranking
      , ipeds.billing_state_code
      , ipeds.cs_ipeds_id
      , ipeds.cs_college_branch_code_6
      , ipeds.cs_college_name
    FROM
        prep_nsc                    AS nsc
            LEFT JOIN combine_ipeds AS ipeds ON nsc.nsc_college_code_branch = ipeds.cs_college_branch_code_6
    )

  , sfdc_nsc_matches AS ( -- join sfdc data to nsc data; filter sfdc data to nsc matches only
    SELECT sfdc.*, nsc.nsc_graduation_date
    FROM
        mod_nsc                 AS nsc
            INNER JOIN mod_sfdc AS sfdc ON nsc.contact_id_nsc = sfdc.contact_id_mod
    )

    -- Begin mapping to Global Academic Semesters and PAT records --

  , prep_nsc_grad_date_to_gas
        AS ( -- prep mapping nsc AA/Cert data to appropriate PAT record; rank based on variance from start/end date of global ATs
        SELECT
            -- nsc-term data
            term.*

            -- nsc grad data
          , nsc.nsc_graduation_date
          , nsc.nsc_grad_year

            -- create rank
          , DENSE_RANK() OVER (PARTITION BY contact_id_nsc,nsc_graduation_date ORDER BY ABS(DATE_DIFF(nsc_graduation_date, term.GAS_start_date, DAY))) AS rank
        FROM
            global_at             AS term
                LEFT JOIN mod_nsc AS nsc ON nsc.nsc_grad_year = term.GAS_Year
        WHERE
              nsc.nsc_graduation_date <= term.GAS_end_date
          AND nsc.nsc_graduation_date >= term.GAS_start_date
          AND GAS_Name <> 'TEST ONLY  WINTER PREV AT'
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
          , contact_id_nsc
        )

  , nsc_grad_date_to_gas
        AS ( -- filter previous cte for global academic terms that we will use to map back to PATs; some students may have 2 GAS mapped to their grad date for this reason
        SELECT DISTINCT
            nsc.*
            , gas.GAS_id
            , gas.GAS_Name
            , gas.academic_calendar_category_c AS GAS_calendar
            , gas.academic_year_c
        FROM
            mod_nsc                                 AS nsc
                LEFT JOIN prep_nsc_grad_date_to_gas AS gas ON nsc.nsc_graduation_date = gas.nsc_graduation_date
        WHERE
             rank = 1
          OR rank = 2
        )

  , nsc_to_pat_data
        AS ( -- map nsc grad data (with GAS term) to graduating PAT record, use this to identify PATs that need to be created in next CTE
        SELECT DISTINCT
            -- id fields
            pat.Contact_Id
          , pat.AT_Id
          , pat.PAT_GAS_id
          , nsc.account_id -- for prod
          , nsc.contact_id_nsc -- will still return if a PAT match was not found

            -- contact data
          , pat.full_name_c
          , pat.College_Track_Status_Name
          , pat.high_school_graduating_class_c
          , pat.site_short

            -- college data
          , nsc.College_Name   AS nsc_college_name -- nsc college name
          , nsc.cs_college_name -- college scorecard college name
          , nsc.college_academic_calendar
          , nsc.rank_college_type
          , nsc.sfdc_ipeds -- ipeds aligned to nsc college
          , nsc.cs_ipeds_id -- ipeds (source: college scorecard)
          , nsc.nsc_graduation_date
          , Degree_Title
          , Degree_Major_1

            -- PAT & GAS data
          , pat.AT_Enrollment_Status_c
          , pat.type_of_degree_earned_c
          , pat.AT_Name
          , pat.GAS_Name

        FROM
            sfdc_nsc_matches                    AS pat
                INNER JOIN nsc_grad_date_to_gas AS nsc -- nsc enrollment data with associated PAT record
                               ON pat.PAT_GAS_id = nsc.GAS_id AND pat.contact_id_mod = nsc.contact_id_nsc
        )

    -- identify students without a PAT record to house the data --

  , missing_pats AS ( -- identify PATs that need to be created to house nsc grad data
    SELECT DISTINCT
        -- contact fields
        Contact_Id
      , full_name_c
      , College_Track_Status_Name
      , high_school_graduating_class_c
      , site_short

        -- grad data
      , sfdc_nsc.nsc_graduation_date
      , gas.GAS_Name
      , gas.GAS_calendar
      , gas.academic_year_c

        -- college data
      , gas.college_academic_calendar
      , account_name    AS sfdc_college_name
      , College_Name    AS nsc_college_name
      , rank_college_type
      , Degree_Title
      , Degree_Major_1
      , sfdc_ipeds
      , cs_ipeds_id
      , account_id      AS sfdc_account_id
      , gas.GAS_id

    FROM
        sfdc_nsc_matches                   AS sfdc_nsc
            --pull in needed GAS to create PAT
            LEFT JOIN nsc_grad_date_to_gas AS gas ON sfdc_nsc.nsc_graduation_date = gas.nsc_graduation_date AND
                                                     sfdc_nsc.contact_id_mod = gas.contact_id_nsc
    WHERE
            contact_id_mod NOT IN (
            SELECT contact_id_nsc
            FROM nsc_to_pat_data AS n
            )
      AND College_State = billing_state_code -- to match more precisely on colleges that sit in various states (e.g. DeVry university)
      AND type_of_degree_earned_c IS NULL    -- validate student PAT not being mapped to grad date; exclude PATs that do have a degree-earned recorded on the PAT already
    )

  , align_acdemic_calendars
        AS ( -- filter missing PAT records: create PAT records where GAS academic calendar aligns with college academic calendar
        SELECT *
        FROM missing_pats
        WHERE
            GAS_calendar = college_academic_calendar
        )

  , school_for_pat AS ( -- major, degree type + college that should be listed on PAT record
    SELECT
        account.account_id   AS for_prod_account_id
      , account.account_name AS for_prod_college_name
      , pat.*
        -- degree type
      , CASE
            WHEN Degree_Title LIKE "%CERTIF%"
                THEN 'Certificate'
                ELSE '2-year degree'
            END              AS sfdc_degree_earned
        -- add major
      , CASE
            WHEN Degree_Major_1 IS NULL
                THEN Degree_Major_1
            WHEN Degree_Major_1 = "ASSOCIATE OF ARTS"
                THEN "Liberal Arts or History"
            WHEN Degree_Major_1 = "CAD (EARLY CHILDHOOD)-BA"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 = "SPANISH"
                THEN "Literature or Languages"
            WHEN Degree_Major_1 = "NURSING"
                THEN "Health Sciences"
            WHEN Degree_Major_1 = "ACCOUNTING"
                THEN "Business"
            WHEN Degree_Major_1 = "MANAGEMENT"
                THEN "Business"
            WHEN Degree_Major_1 = "BUSINESS ADMINISTRATION"
                THEN "Business"
            WHEN Degree_Major_1 = "MULTIMEDIA DESIGN & DEVELOPMENT"
                THEN "Arts: Design, Performing, or Visual"
            WHEN Degree_Major_1 = "ASSOCIATE OF SCIENCE"
                THEN "Biological, Environmental, or Agricultural Sciences"
            WHEN Degree_Major_1 = "URBAN AND REGIONAL PLANNING"
                THEN "Engineering or Architecture"
            WHEN Degree_Major_1 = "ARCHITECTURE"
                THEN "Engineering or Architecture"

            WHEN Degree_Major_1 LIKE "%BIOLOG%"
                THEN "Biological, Environmental, or Agricultural Sciences"
            WHEN Degree_Major_1 LIKE "%KINES%"
                THEN "Chemistry, Physics, or other Physical Sciences"
            WHEN Degree_Major_1 LIKE "%NATURAL SCI%"
                THEN "Chemistry, Physics, or other Physical Sciences"
            WHEN Degree_Major_1 LIKE "%PUBLIC HEALTH%"
                THEN "Health Sciences"
            WHEN Degree_Major_1 LIKE "%NURS%"
                THEN "Biological, Environmental, or Agricultural Sciences"
            WHEN Degree_Major_1 LIKE "%PHARM%"
                THEN "Health Sciences"
            WHEN Degree_Major_1 LIKE "%MATH%"
                THEN "Math or Statistics"
            WHEN Degree_Major_1 LIKE "%ECON%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%COMPUTER SCIENCE%"
                THEN "Computer Science or Information Technology"
            WHEN Degree_Major_1 LIKE "%ELECTRICAL%"
                THEN "Engineering or Architecture"
            WHEN Degree_Major_1 LIKE "%MECHANICAL%"
                THEN "Engineering or Architecture"
            WHEN Degree_Major_1 LIKE "%TECH%"
                THEN "Computer Science or Information Technology"
            WHEN Degree_Major_1 LIKE "%PSYCHOLOGY%"
                THEN "Psychology"
            WHEN Degree_Major_1 LIKE "%HISTORY%"
                THEN "Liberal Arts or History"
            WHEN Degree_Major_1 LIKE "%SOCIOLOGY%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%SOCIAL%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%HUMAN DEVELOPMENT%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%BEHAV%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%CHILD%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%CLINIC%"
                THEN "Health Sciences"
            WHEN Degree_Major_1 LIKE "%ADOLESCENT%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%THERAPY%"
                THEN "Health Sciences"
            WHEN Degree_Major_1 LIKE "%BROADCAST%"
                THEN "Communications or Marketing"
            WHEN Degree_Major_1 LIKE "%BUSINESS%"
                THEN "Business"
            WHEN Degree_Major_1 LIKE "%MGMT%"
                THEN "Business"
            WHEN Degree_Major_1 LIKE "%COMMUNICATION %"
                THEN "Communications or Marketing"
            WHEN Degree_Major_1 LIKE "%JUSTICE%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%CRIMINAL%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%HUMANITIES%"
                THEN "Liberal Arts or History"
            WHEN Degree_Major_1 LIKE "%ASSOCIATE OF ARTS %"
                THEN "Liberal Arts or History"
            WHEN Degree_Major_1 LIKE "%SIGN LANGUAGE%"
                THEN "Literature or Languages"
            WHEN Degree_Major_1 LIKE "%AMERICAN%"
                THEN "Sociology, Political Science, Economics, or other Social Sciences"
            WHEN Degree_Major_1 LIKE "%CULINARY%"
                THEN "Arts: Design, Performing, or Visual"
            WHEN Degree_Major_1 LIKE "%FIREFIGHTER%"
                THEN "Biological, Environmental, or Agricultural Sciences"
            WHEN Degree_Major_1 LIKE "%IGETC%"
                THEN "Undeclared"
            WHEN Degree_Major_1 LIKE "%TRANS%"
                THEN "Undeclared"
            WHEN Degree_Major_1 LIKE "%BREADTH%"
                THEN "Undeclared"
            WHEN Degree_Major_1 LIKE "%LIB%"
                THEN "Liberal Arts or History"
            WHEN Degree_Major_1 LIKE "%GENERAL %"
                THEN "Liberal Arts or History"
                ELSE "ERROR!"
            END              AS sfdc_major
    FROM
        align_acdemic_calendars  AS pat
            LEFT JOIN sfdc_ipeds AS account ON pat.cs_ipeds_id = account.sfdc_ipeds
    )

    -- students with 1+ majors, or earning more than 1 degree in the same PAT: decide which to keep --

  , rank
        AS ( -- rank majors/degrees, and take the first major and degree earned for a specific pat (ranked #1) (some PATs have multiple rows worth of degrees earned data)
        SELECT
            *
          -- calculate ranking
          , DENSE_RANK() OVER (PARTITION BY GAS_id, Contact_Id ORDER BY sfdc_degree_earned ASC) AS row_number_degree --ASC (rank 2-year above Certificate)
          , DENSE_RANK() OVER (PARTITION BY GAS_id, Contact_Id ORDER BY sfdc_major ASC)              AS row_number_major --ASC (put row number 2 in 'second major' field)
          , DENSE_RANK() OVER (PARTITION BY GAS_id, Contact_Id ORDER BY rank_college_type ASC)       AS row_degree_type

          -- Concatenate list of degrees and majors from nsc, and for salesforce, into a single row
          , STRING_AGG(sfdc_degree_earned, "|") OVER (PARTITION BY GAS_id, Contact_Id )              AS partition_sfdc_degree -- concatenate degrees if student earned more than 1
          , STRING_AGG(sfdc_major, "|") OVER (PARTITION BY GAS_id, Contact_Id)                       AS partition_sfdc_major -- concatenate majors if student majored in more than 1
          , STRING_AGG(Degree_Title, "|") OVER (PARTITION BY GAS_id, Contact_Id)                     AS partition_nsc_degree -- concatenate list of degrees into single row based on PAT record
          , STRING_AGG(Degree_Major_1, "|") OVER (PARTITION BY GAS_id, Contact_Id)                   AS partition_nsc_major -- concatenate list of majors into single row based on PAT record

        FROM school_for_pat
        QUALIFY
            (DENSE_RANK() OVER (PARTITION BY GAS_id, Contact_Id ORDER BY sfdc_degree_earned ASC) = 1 -- row_number_degree --ASC (rank 2-year above Certificate)
                AND DENSE_RANK() OVER (PARTITION BY GAS_id, Contact_Id ORDER BY rank_college_type ASC) = 1-- pull highest ranking in degree type (BA/BS is 1st, Associate's 2nd, everything else is 3rd)
                )
        )

  , for_prod AS ( -- add degree to PAT record
    -- this may have more records than the query for for advisors, because the final SELECT includes the college account ID for prod.
    -- Salesforce may have specific account ids for certain colleges (e.g. an id for Carrington College - San Jose, and an id for Carrington College - Sacramento)
    SELECT DISTINCT
        full_name_c
      , site_short
      , for_prod_college_name
      , GAS_Name

        -- nsc data
      , nsc_graduation_date
      , nsc_college_name
      , partition_nsc_degree    AS nsc_degree_earned
      , partition_nsc_major     AS nsc_major
      
        -- import fields
      , Contact_Id                          AS Student__c
      , College_Track_Status_Name           AS student_audit_status__c
      , College_Track_Status_Name           AS ct_status_at_c
      , "01246000000RNnHAAW"                AS RecordTypeId         -- PS record type
      , GAS_id                              AS Global_Academic_Semester__c
      , academic_year_c                     AS Academic_Year__c     -- AY id
      , CONCAT(GAS_Name, ' ', full_name_c)  AS Name
      , for_prod_account_id                 AS School__c
     , CASE
            WHEN INSTR(partition_sfdc_major, '|') > 0
                THEN SUBSTR(partition_sfdc_major, 1, INSTR(partition_sfdc_major, '|') - 1)
                ELSE partition_sfdc_major
            END                             AS Major__c -- if there is a comma "|" then extract first string, otherwise just return the value
     , CASE
            WHEN INSTR(partition_sfdc_degree, '|') > 0
                THEN SUBSTR(partition_sfdc_degree, 1, INSTR(partition_sfdc_degree, '|') - 1)
            ELSE partition_sfdc_degree
            END                             AS Type_of_Degree_Earned__c -- if there is a comma "|" then extract first string, otherwise just return the value
    FROM rank
    )

SELECT *
FROM for_prod
