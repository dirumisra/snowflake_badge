---------------Badge 2: Collaboration, Marketplace & Cost Estimation Workshop-------------------------------------

--YOUR UNI ID:  005VI000007zJHqYAM
--YOUR ASSIGNED UUID: 132c9158-0d4d-4336-bb45-17a1ae46c0ab

// Setting the Sample Share Name Back to the Original Name
ALTER DATABASE THAT_REALLY_COOL_SAMPLE_STUFF
RENAME TO SNOWFLAKE_SAMPLE_DATA

//Grant Privileges to the Share for the SYSADMIN Role?
grant imported privileges
on database SNOWFLAKE_SAMPLE_DATA
to role SYSADMIN;

// Use Select Statements to Look at Sample Data

--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);

//  Join and Aggregate Shared Data

-- Nations Table
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table

SELECT R_REGIONKEY,R_NAME,
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION

-- Join the Tables and Sort

SELECT R_NAME AS REGION, N_NAME AS NATION
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION R
ON R.R_REGIONKEY = N.N_REGIONKEY
ORDER BY R_NAME, N_NAME ASC

// Group and Count Rows Per Region

--Group and Count Rows Per Region AND THEN Export Native and Shared Data
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;


//Run the Two Dora Setup Scripts

// Can You Find the Function Using Code? 

-- where did you put the function?
show user functions in account;

-- did you put it here?
select * 
from util_db.information_schema.functions
where function_name = 'GRADER'
and function_catalog = 'UTIL_DB'
and function_owner = 'ACCOUNTADMIN';

//Give the SYSADMIN Role Access to the Grader Function
//Edit the code below to grant usage to the function to the SYSADMIN role.

grant usage 
on function UTIL_DB.PUBLIC.GRADER(VARCHAR, BOOLEAN, NUMBER, NUMBER, VARCHAR) 
to ROLE SYSADMIN;

-- GRANT USAGE ON DATABASE UTIL_DB TO ROLE sysadmin;
-- GRANT USAGE ON SCHEMA UTIL_DB.PUBLIC TO ROLE sysadmin;
-- GRANT USAGE ON FUNCTION UTIL_DB.PUBLIC.GRADER(VARCHAR, BOOLEAN, NUMBER, NUMBER, VARCHAR) TO ROLE sysadmin;

// Is DORA Working? Run This to Find Out!

select GRADER(step,(actual = expected), actual, expected, description) as graded_results from (
SELECT 'DORA_IS_WORKING' as step
 ,(select 223 ) as actual
 ,223 as expected
 ,'Dora is working!' as description
); 

//Set Up a New Database Called INTL_DB

use role SYSADMIN;
create OR REPLACE database INTL_DB;
use schema INTL_DB.PUBLIC;

//Create a Warehouse for Loading INTL_DB

use role SYSADMIN;
create OR REPLACE warehouse INTL_WH 
with 
warehouse_size = 'XSMALL' 
warehouse_type = 'STANDARD' 
auto_suspend = 600 --600 seconds/10 mins
auto_resume = TRUE;

use warehouse INTL_WH;

//Create Table INT_STDS_ORG_3166

create or REPLACE table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);


// USE Role 
USE ROLE ACCOUNTADMIN;

//Grant necessary privileges to your role.
GRANT USAGE ON FILE FORMAT util_db.public.PIPE_DBLQUOTE_HEADER_CR TO ROLE sysadmin;

//Create a File Format to Load the Table

create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;

USE ROLE SYSADMIN

// Load the ISO Table Using Your File Format
show stages in account; 

list @util_db.public.aws_s3_bucket;

create or replace stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';


-------------------bug fix-----------------------------
//COPY INTO: 
copy into intl_db.public.INT_STDS_ORG_3166
from @util_db.public.AWS_S3_BUCKET
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name='util_db.public.PIPE_DBLQUOTE_HEADER_CR' );


//Check That You Created and Loaded the Table Properly

select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166;

/*
not required this step
ALTER DATABASE INTRL_DB
RENAME TO INTL_DB;
*/
// TRUNCATE TABLE INT_STDS_ORG_3166

TRUNCATE TABLE INTL_DB.PUBLIC.INT_STDS_ORG_3166;

-- set your worksheet drop lists or write and run USE commands
-- YOU WILL NEED TO USE ACCOUNTADMIN ROLE on this test.

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
 SELECT 'CMCW01' as step
 ,( select count(*) 
   from snowflake.account_usage.databases
   where database_name = 'INTL_DB' 
   and deleted is null) as actual
 , 1 as expected
 ,'Created INTL_DB' as description
 );

select count(*) as found, '249' as expected 
from intl_db.public.INT_STDS_ORG_3166; 

//

select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema = 'PUBLIC' 
and table_name= 'INT_STDS_ORG_3166';

// How to Test That You Loaded the Expected Number of Rows

select row_count
from  INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC' 
and table_name= 'INT_STDS_ORG_3166';

--- CMCW02

-- set your worksheet drop lists to the location of your GRADER function
-- role can be set to either SYSADMIN or ACCOUNTADMIN for this check

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW02' as step
 ,( select count(*) 
   from INTL_DB.INFORMATION_SCHEMA.TABLES 
   where table_schema = 'PUBLIC' 
   and table_name = 'INT_STDS_ORG_3166') as actual
 , 1 as expected
 ,'ISO table created' as description
);

-- CMCW03
-- set your worksheet drop lists to the location of your GRADER function 
-- either role can be used

-- DO NOT EDIT BELOW THIS LINE 
select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
SELECT 'CMCW03' as step 
 ,(select row_count 
   from INTL_DB.INFORMATION_SCHEMA.TABLES  
   where table_name = 'INT_STDS_ORG_3166') as actual 
 , 249 as expected 
 ,'ISO Table Loaded' as description 
); 


---- Join Local Data with Shared Data

select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;


// Convert the Select Statement into a View

create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
( iso_country_name
  ,country_name_official
  ,alpha_code_2digit
  ,region) AS
  select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r on n_regionkey = r_regionkey;

//Run a SELECT on the View You Created
select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;

-- SET YOUR DROPLISTS PRIOR TO RUNNING THE CODE BELOW 

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW04' as step
 ,( select count(*) 
   from INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO) as actual
 , 249 as expected
 ,'Nations Sample Plus Iso' as description
);

//  Create Table Currencies

create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

SELECT * FROM INTL_DB.PUBLIC.CURRENCIES


// Create Table Country to Currency
create or replace table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';

SELECT * FROM INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE

// Create a File Format to Process files with Commas, Linefeeds and a Header Row

create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 ;

-- set your worksheet drop lists

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW05' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'COUNTRY_CODE_TO_CURRENCY_CODE') as actual
 , 265 as expected
 ,'CCTCC Table Loaded' as description
);

-- set your worksheet context menus
--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW06' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'CURRENCIES') as actual
 , 151 as expected
 ,'Currencies table loaded' as description
);

// Create a View that Will Return The Result Set Shown

SELECT * FROM SIMPLE_CURRENCY


-- don't forget your droplists
--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
 SELECT 'CMCW07' as step 
,( select count(*) 
  from INTL_DB.PUBLIC.SIMPLE_CURRENCY ) as actual
, 265 as expected
,'Simple Currency Looks Good' as description
);

// Convert "Regular" Views to Secure Views

alter view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
set secure; 

alter view intl_db.public.SIMPLE_CURRENCY
set secure;

// An Outbound Share Has Been Created

-- set your worksheet drop lists to the location of your GRADER function
--DO NOT EDIT ANYTHING BELOW THIS LINE

--This DORA Check Requires that you RUN two Statements, one right after the other
show shares in account;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the SHOW command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW08' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "kind" = 'OUTBOUND'
    and "database_name" = 'INTL_DB') as actual
 , 1 as expected
 ,'Outbound Share Created From INTL_DB' as description
); 


-- set your worksheet drop lists to the location of your GRADER function
--DO NOT EDIT ANYTHING BELOW THIS LINE

--This DORA Check Requires that you RUN two Statements, one right after the other
show resource monitors in account;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the SHOW command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW09' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "name" = 'DAILY_3_CREDIT_LIMIT'
    and "credit_quota" = 3
    and "frequency" = 'DAILY') as actual
 , 1 as expected
 ,'Resource Monitors Exist' as description
);

-------------------------------------GCP------------------------------------

// Use ORGADMIN in WDE to Enable ORGADMIN in ADU

USE ROLE orgadmin;
ALTER ACCOUNT AUTO_DATA_UNLIMITED
SET IS_ORG_ADMIN = TRUE;


// DORA CHECK L9
-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT
select grader(step, (actual = expected), actual, expected, description) as graded_results from ( SELECT 'CMCW12' as step ,( select count(*) from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS where account_name = 'ACME' 
 and region like 'AZURE_%' and deleted_on is null) as actual , 1 as expected ,'ACME Account Added on Azure Platform' as description ); 

//

-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW13' as step
 ,( select count(*) 
   from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS 
   where account_name = 'AUTO_DATA_UNLIMITED' 
   and region like 'GCP_%'
   and deleted_on is null) as actual
 , 1 as expected
 ,'ADU Account Added on GCP' as description
); 

-- set the worksheet drop lists to match the location of your GRADER function
-- DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ACME ACCOUNT

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW14' as step
 ,( select count(*) 
   from STOCK.UNSOLD.LOTSTOCK
   where engine like '%.5 L%'
   or plant_name like '%z, Sty%'
   or desc2 like '%xDr%') as actual
 , 145 as expected
 ,'Intentionally cryptic test' as description
);