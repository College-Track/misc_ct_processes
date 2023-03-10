import pandas as pd
from pandas import DataFrame
from google.cloud import bigquery, language
from google.oauth2 import service_account
import gspread
gc = gspread.oauth()
import os
from dotenv import load_dotenv
load_dotenv()

######## CREDENTIALS ########
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "[ENTER THE LOCATION OF YOUR JSON CREDENTIALS FROM YOUR DIRECTORY HERE]"
##############################

######## API REQUEST ########
# This client.query() performs API request
client = bigquery.Client() #manages connections to the BigQuery API
############################## 

######## QUERY STUFF ########
query_job = client.query("""
SELECT 
    first_name
    , middle_name
    , last_name
    , birthdate
    , high_school_graduating_class_c AS hs_class
    , contact_id
FROM prod_core.dim_contact
WHERE indicator_completed_ct_hs_program_c = TRUE
"""
)

# main dataframe to transform
df_contact = query_job.to_dataframe()

######## BEGIN TRANSFORMATIONS ########

# -- Suffix -- #
def suffix(df):
    """
    Define pattern: suffix

    REGEXP for "Jr" or Roman numerals:
    The | character denotes a logical OR operation 
    The $ character matches the end of the string

    returns TRUE if string in column matches the regexp pattern, 
    returns FALSE otherwise

    Note: 
    - regex assumes the suffix is always at the end of the string
    - Ensure to enclose the REGEXP in parenthesis! This creates a capture group that you can then extract
    """
    suffix_regexp = r"((Jr|Jr.|I{1,3}|IV|V|VI|VII|VIII|IX)$)"
    # 1. 1st: create new column 'suffix', 2nd: extract the matched strings from the 'last_name' column and store them in a new column 'suffix'
    df['suffix'] = df['last_name'].str.extract(suffix_regexp)[0]    # [0] extracts first capturing group from the REGEXP match
                                                                    # [0] matters because the REGEXP has 2 capturing groups (looks for 2 types f matches as in the parenthesis)
                                                                    # By adding [0] at the end, we're telling Pandas to only give us the 1st capturing group
                                                                    # AKA it returns the part of the string that matches the REGEXP pattern

    # 2. remove all characters from new 'suffix' column
    df['suffix'] = df['suffix'].str.replace(r'[\d\W]+', '')

    # 3. Remove suffix from last_name
    df['last_name'] = df['last_name'].str.replace(r"((Jr|Jr.|I{1,3}|IV|V|VI|VII|VIII|IX)$)", '')

    """ 
    4. Old, DEP steps - merging and dropping was needed when we were combining to an orig df, before functions were implemented
        # 1st: drop 'last_name' still containing suffix substrings, 
        # 2nd: add 'last_name' and 'suffix' columns to original df (df_contact) - KEY is not added, 
    #df = df.drop(['last_name'], axis = 1).merge(df[['contact_id','suffix', 'last_name']], on='contact_id')
    """
    return df

# -- Middle Initial -- #
def middle_initial(df):
    """
    - Extract initial from middle_name, store in new column 'middle_initial'
    - Pull middle name from first_name with REGEXP:  r'(\w\.)'
    - Ensure to enclose the REGEXP in parenthesis! This creates a capture group that you can then extract
    """
    # 1. Remove characters from middle_name. Will become important further downstream
    df['middle_name'] = df['middle_name'].str.replace(r'[\d\W]+', '')

    # 2. 
        # 1st: create new column 'middle_initial', 
        # 2nd: pull substring from first_name if it has a period, 
        # 3rd: store in new `middle_initial` column; (omit potential suffixes, legit multiple first names)
    df['middle_initial'] = df['first_name'].str.extract(r'(\w\.)') # only pull names with periods

    # 3. Replace names with periods in 'first_name' with whitespace
    df['first_name'] = df['first_name'].str.replace(r'(\w\.)', '')

    # 4. get first element (first letter), make it uppercase
    df['middle_initial'] = df['middle_initial'].str[0].str.upper()

    # 5. from #1, get first initial from middle_name and add to middle_initial; maintain data already in middle_initial - do not overwrite, just add new data
    df['middle_initial'] = df['middle_initial'].fillna(df['middle_name'].str[0].str.upper())

    """
        # 6. Old, DEP steps - merging and dropping was needed when we were combining to an orig df, before functions were implemented
            # 1st: drop 'first_name' containing middle names (names with periods), 
            # 2nd: add middle_initial to original df (df_contact), 
            # 3rd: add cleaned 'first_name' without middle names (w/ periods)
        df = df.drop(['first_name'], axis = 1).merge(df[['contact_id','middle_initial', 'first_name']], on = 'contact_id')
    """
    return df

# -- Drop middle_name -- #
def drop_middle_name(df):
    """
    No longer need middle name column, we use middle initial
    """
    df = df.drop(['middle_name'], axis = 1)
    return df

# -- Apostrophe -- #
def apostrophe(df):
    """
    Replace apostrophe character with whitespace (e.g. A'kira >> A kira)
    """
    # 1. Replace apostrophe with white space from first_name
    df[['first_name', 'last_name']] = df[['first_name', 'last_name']].apply(lambda x: x.str.replace("'"," "))
    
    """
    Old, DEP steps - merging and dropping was needed when we were combining to an orig df, before functions were implemented
    # 2. 1st: drop first_name, last_name columns from df; 2nd: Add revised first_name, last_name 
    df = df.drop(['first_name', 'last_name'], axis = 1).merge(df[['contact_id', 'first_name', 'last_name']], on = 'contact_id')
    """
    return df

# -- Remove Characters, keep Hyphen -- #
def remove_characters(df):
    """
    - Remove characters from first_name, last_name 
    - EXCEPT hyphens. Keep the hyphens
    - Apostrophes should already be accounted for before this using apostrophe() function
    """
    # 1. Remove characters (keep hyphens) from first_name, last_name
    df[['first_name','last_name']] = df[['first_name', 'last_name']].replace(r'[^\w\s-]', '', regex=True)
    
    """
    Old, DEP steps - merging and dropping was needed when we were combining to an orig df, before functions were implemented
    # 2. Replace first_name, last_name columns with new, cleaner columns, no characters
    df = df.drop(['first_name', 'last_name'], axis = 1).merge(df[['contact_id', 'first_name', 'last_name']], on = 'contact_id')
    """
    
    return df

# -- DOB-- #
def dob(df):
    """
    Modify date of birth to YYYYMMDD format
    """
    # 1. convert string birthdate column to pandas datetime object, then convert to YYYYMMDD format
    df['birthdate'] = pd.to_datetime(df['birthdate'], format = '%Y-%m-%d').dt.strftime('%Y%m%d').str.zfill(8) # .zfill(8) fills date with leading zeros (e.g. 6 to 06)

    return df

# -- Search Date -- #
def search_date(df):
    """
    Use student's HS class to create the start search date for batch query
    - Append '0601' to HS class
    - For example, if HS class is 2014, then Start Date is 06012014
    """
    # 1. convert HS Class to a string to append '0601', convert back to an int
    df['hs_class'] = df['hs_class'].apply(lambda x: int(str(x) + '0601'))   # lambda is applied to each value in `hs_class`
                                                                            # concatenates '0601' to a str version of hs_class; str(x) converts the integer value x to a new str to enable concat
                                                                            # it is then converted back to an int with int()

    return df

##########################
# -- Prep Batch Query -- #
##########################

# -- Re-order columns -- #
def header_reorder(df):
    """
    Rearrange columns to align with NSC formatting guidelines
    """
    # 1. prep re-arrange existing columns in df
    new_order01 = ['first_name', 'middle_initial', 'last_name', 'suffix', 'birthdate', 'hs_class','contact_id']

    # 2. re-arrange existing columns 
    df = df.reindex(columns = new_order01)

    # 3. insert columns after x column based on NSC formatting guidelines
    df['H1'] = 'D1'
    df[693622] = ''
    df[''] = ''
    df[''] = ''
    df["00"] = "00"    # assign 0 to all in this column to 0. You will need to convert this to '00' in your exported file before submitting
    # 4. reorder columns again
    new_order02 = ['H1', '693622', 'first_name', 'middle_initial', 'last_name', 'suffix', 'birthdate', 'hs_class', '', '', '00', 'contact_id']
    df = df.reindex(columns = new_order02)
    
    return df

# -- Rename columns -- #
def header_rename_col(df):
    """
    Rename column headers to align with NSC formatting guidelines
    """
    # 1. rename columns; columns = {'old_column' : 'new_column'}
    rename_header = ['H1',693622, 00, 'College Track', 20230308, 'DA', 'S', '', '']
    index = 8
    for i in range(index+1): # range is # of columns in df - for loop iterates through column names 
        df = df.rename(columns = {df.columns[i] : rename_header[i]})    #  Creates a dict where the old column name is the KEY, and the corresponding new column name is the VALUE.
                                                                        # `i` will take on the values 0, 1, 2, etc. in each iteration of the loop.
                                                                        #  selects the old column name corresponding to the current value of i on the left
    return df


def prep_batch_query(df):
    #header_rename_col(df_contact)
    suffix_df = suffix(df)
    mi_df = middle_initial(suffix_df)
    drop_middle_df = drop_middle_name(mi_df)
    apos_df = apostrophe(drop_middle_df)
    no_char_df = remove_characters(apos_df)
    birthdate_df = dob(no_char_df)
    search_df = search_date(birthdate_df)
    header1_df = header_reorder(search_df)
    final_df = header_rename_col(header1_df)

    YOUR_FINAL_DF_HERE.to_csv(r'/LOCATION_HERE/NAME_OF_YOUR_FILE_HERE.csv', index = False)
    return final_df

prep_batch_query(df_contact)
