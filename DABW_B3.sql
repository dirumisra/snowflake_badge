//  CREATE SMOOTHIES DATABASE

CREATE OR REPLACE DATABASE SMOOTHIES

// Create Your Smoothie Order Form SIS App

// Create Streamlit app 'Custom Smoothie Order Form'

// Create a FRUIT_OPTIONS Table

CREATE OR REPLACE TABLE FRUIT_OPTIONS
(FRUIT_ID INT,
 FRUIT_NAME VARCHAR(25))

SELECT * FROM FRUIT_OPTIONS
 
// CREATE FILE FORMAT

-- COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
-- FROM '@"SMOOTHIES"."PUBLIC"."%FRUIT_OPTIONS"/__snowflake_temp_import_files__/'
-- FILES = ('fruits_available_for_smoothies.txt')
-- FILE_FORMAT = (
--     TYPE=CSV,
--     SKIP_HEADER=2,
--     FIELD_DELIMITER='%',
--     TRIM_SPACE=FALSE,
--     FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
--     REPLACE_INVALID_CHARACTERS=TRUE,
--     DATE_FORMAT=AUTO,
--     TIME_FORMAT=AUTO,
--     TIMESTAMP_FORMAT=AUTO
-- )
-- ON_ERROR=ABORT_STATEMENT
-- PURGE=TRUE;

CREATE or REPLACE FILE FORMAT SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM

// CREATE FILE FORMAT
CREATE OR REPLACE FILE FORMAT SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM
    TYPE=CSV,
    SKIP_HEADER=2,
    FIELD_DELIMITER='%',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO;

// Use the File Format Name in the COPY INTO Statement
COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM '@"SMOOTHIES"."PUBLIC"."%FRUIT_OPTIONS"/__snowflake_temp_import_files__/'
FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = (FORMAT_NAME = SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM)
ON_ERROR = ABORT_STATEMENT
PURGE = TRUE;

//  Create the Internal Stage and Load the File Into It
// follow GUI process to create internal stage


// FEW CHANGED IN COPY INTO 

COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM @smoothies.public.my_internal_stage
FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = (FORMAT_NAME = SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM)
ON_ERROR = ABORT_STATEMENT
VALIDATION_MODE = RETURN_ERRORS
PURGE = TRUE

// Create the Internal Stage and Load the File Into It


// Query the Not-Yet-Loaded Data Using the File Format

SELECT $1, $2, $3, $4, $5
FROM @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt
(file_format => SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM);

SELECT $1, $2
FROM @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt
(file_format => SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM);

//Reorder Columns During the COPY INTO LOAD
COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM (SELECT $2 AS FRUIT_ID,$1 AS FRUIT_NAME from @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt)
FILE_FORMAT = (FORMAT_NAME = SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM)
ON_ERROR = ABORT_STATEMENT
PURGE = TRUE;

-- Display the Fruit Options List in Your Streamlit in Snowflake (SiS) App. 
-- Navigate back to your SiS App and add the bit of code included below. 

-- session = get_active_session()
-- my_dataframe = session.table("smoothies.public.fruit_options")
-- st.dataframe(data=my_dataframe, use_container_width=True)

// Check that DORA is Working In Your Current Trial Account

-- Remember that you MUST USE ACCOUNTADMIN and UTIL_DB.PUBLIC as your context anytime you run DORA checks!!
-- DO NOT EDIT ANYTHING BELOW THIS LINE

USE ROLE ACCOUNTADMIN;
USE SCHEMA PUBLIC;
USE DATABASE UTIL_DB;

select grader(step, (actual = expected), actual, expected, description) as graded_results from 
  ( SELECT 
  'DORA_IS_WORKING' as step
 ,(select 223) as actual
 , 223 as expected
 ,'Dora is working!' as description
); 

//  Did You Create and Load the FRUIT_OPTIONS Table? 

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
-- remove duplicate value form table

-- Create a temporary table to store row numbers

TRUNCATE TABLE SMOOTHIES.PUBLIC.FRUIT_OPTIONS

SELECT * FROM SMOOTHIES.PUBLIC.FRUIT_OPTIONS

// Dora check

USE ROLE ACCOUNTADMIN;
USE SCHEMA PUBLIC;
USE DATABASE UTIL_DB;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW001' as step
 ,( select count(*) 
   from SMOOTHIES.PUBLIC.FRUIT_OPTIONS) as actual
 , 25 as expected
 ,'Fruit Options table looks good' as description
);

---------------------------Moving Data from SiS to Snowflake l2--------------------------------------------
// Remove the SelectBox

// Create a Place to Store Order Data
USE ROLE SYSADMIN

CREATE OR REPLACE TABLE SMOOTHIES.PUBLIC.ORDERS
(ingredients varchar(200));


INSERT INTO SMOOTHIES.PUBLIC.ORDERS(INGREDIENTS)
VALUES ('Cantaloupe Guava Jackfruit Elderberries Figs')

SELECT * FROM SMOOTHIES.PUBLIC.ORDERS


-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE

USE ROLE ACCOUNTADMIN;
USE SCHEMA PUBLIC;
USE DATABASE UTIL_DB;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 'DABW002' as step
 ,(select IFF(count(*)>=5,5,0)
    from (select ingredients from smoothies.public.orders
    group by ingredients)
 ) as actual
 ,  5 as expected
 ,'At least 5 different orders entered' as description
);


// Truncate the Orders Table

SELECT * FROM SMOOTHIES.PUBLIC.ORDERS

-- Set your worksheet drop lists

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW003' as step
 ,(select ascii(fruit_name) from smoothies.public.fruit_options
where fruit_name ilike 'z%') as actual
 , 90 as expected
 ,'A mystery check for the inquisitive' as description
);

// Use the ALTER Command to Add a New Column to Your Orders Table

ALTER TABLE SMOOTHIES.PUBLIC.ORDERS
ADD COLUMN NAME_ON_ORDER VARCHAR(100)

// insert
DELETE FROM SMOOTHIES.PUBLIC.ORDERS
WHERE NAME_ON_ORDER = 'MellyMel';

insert into smoothies.public.orders(ingredients,name_on_order)
values('Dragon Fruit Honeydew Guava Apples Kiwi','MellyMel');


SELECT * FROM SMOOTHIES.PUBLIC.ORDERS
WHERE NAME_ON_ORDER IS NOT NULL


// AD ORDER FIELD COLUMN IN ORDER TABLE

ALTER TABLE SMOOTHIES.PUBLIC.ORDERS
DROP COLUMN ORDER_FILLED

ALTER TABLE SMOOTHIES.PUBLIC.ORDERS
ADD COLUMN ORDER_FILLED BOOLEAN DEFAULT FALSE;



SELECT * FROM SMOOTHIES.PUBLIC.ORDERS


SELECT * FROM SMOOTHIES.PUBLIC.ORDERS


update smoothies.public.orders
set order_filled = true
where name_on_order is null;

SELECT * FROM SMOOTHIES.PUBLIC.ORDERS


-----------------Bugs---------------------------------

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW004' as step
 ,( select count(*) from smoothies.information_schema.columns
    where table_schema = 'PUBLIC' 
    and table_name = 'ORDERS'
    and column_name = 'ORDER_FILLED'
    and column_default = 'FALSE'
    and data_type = 'BOOLEAN') as actual
 , 1 as expected
 ,'Order Filled is Boolean' as description
);

    
// Create a Sequence to Use as a Row ID

//Truncate the Orders Table
TRUNCATE TABLE SMOOTHIES.PUBLIC.ORDERS

// Add the Unique ID Column
alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column;

SELECT * FROM SMOOTHIES.PUBLIC.ORDERS


-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE


USE ROLE ACCOUNTADMIN;
USE SCHEMA PUBLIC;
USE DATABASE UTIL_DB;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW005' as step
 ,(select count(*) from SMOOTHIES.INFORMATION_SCHEMA.STAGES
where stage_name like '%(Stage)') as actual
 , 2 as expected
 ,'There seem to be 2 SiS Apps' as description
);

// Create & Set a Local SQL Variable

SET mystery_bag = 'what is in here?';

// Run a Select that Displays the Variable

select $mystery_bag

// Change the Value and Run the Select Again

SET mystery_bag = 'this bag is empty!!';
select $mystery_bag

// Do More With More Variables

set VAR1 = 2;
set VAR2 = 5;
set VAR3 = 7;

select $VAR1 + $VAR2 + $VAR3 AS TOTAL;

// Create a Simple User Defined Function (UDF)

CREATE OR REPLACE FUNCTION sum_mystery_bag_vars(VAR1 NUMBER, VAR2 NUMBER, VAR3 NUMBER)
RETURNS NUMBER AS 'SELECT VAR1+VAR2+VAR3'


SELECT sum_mystery_bag_vars(12,36,204) AS TOTALVAL

//  Combine Local Variables & Function Calls

SET EENY = 4;
SET MEENY = 67.2;
SET MINEY_MO = -39;

SELECT sum_mystery_bag_vars($EENY,$MEENY,$MINEY_MO);


-- Set your worksheet drop lists

-- Set these local variables according to the instructions
set this = -10.5;
set that = 2;
set the_other = 1000 ;

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW006' as step
 ,( select util_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);


-- Set your worksheet drop lists

-- Set these local variables according to the instructions
set this = -10.5;
set that = 2;
set the_other = 1000 ;

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW006' as step
 ,( select util_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);

//  Using a System Function to Fix a Variable Value

set alternating_caps_phrase = 'aLtErNaTiNg Caps!';

select $alternating_caps_phrase;


set alternating_caps_phrase = 'wHy ArE yOu likE tHiS?';

select initcap($alternating_caps_phrase);

CREATE OR REPLACE FUNCTION NEUTRALIZE_WHINING(input_text TEXT)
RETURNS TEXT
LANGUAGE SQL
AS
$$
  INITCAP(input_text)
$$;

select initcap($NEUTRALIZE_WHINING)

---------------Bugs---------------------------------------

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW007' as step
 ,( select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))) as actual
 , -4759027801154767056 as expected
 ,'WHINGE UDF Works' as description
);

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DABW008' as step 
   ,( select sum(hash_ing) from
      (select hash(ingredients) as hash_ing
       from smoothies.public.orders
       where order_ts is not null 
       and name_on_order is not null 
       and (name_on_order = 'Kevin' and order_filled = FALSE and hash_ing = 7976616299844859825) 
       or (name_on_order ='Divya' and order_filled = TRUE and hash_ing = -6112358379204300652)
       or (name_on_order ='Xi' and order_filled = TRUE and hash_ing = 1016924841131818535))
     ) as actual 
   , 2881182761772377708 as expected 
   ,'Followed challenge lab directions' as description
); 




