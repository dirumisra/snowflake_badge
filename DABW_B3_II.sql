CREATE or replace DATABASE SMOOTHIES;

CREATE  or replace TABLE FRUIT_OPTIONS (
FRUIT_ID NUMBER,
FRUIT_NAME VARCHAR(25)
);


COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
--FROM '@"SMOOTHIES"."PUBLIC"."%FRUIT_OPTIONS"/__snowflake_temp_import_files__/'
--FROM @smoothies.public.my_internal_stage
FROM 
( SELECT $2 AS FRUIT_ID, $1 AS FRUIT_NAME
FROM @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt )
--FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = (
    FORMAT_NAME= SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM
)
ON_ERROR=ABORT_STATEMENT
--VALIDATION_MODE = RETURN_ERRORS
PURGE=TRUE;

CREATE FILE FORMAT SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM
    TYPE=CSV,
    SKIP_HEADER=2,
    FIELD_DELIMITER='%',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO


CREATE STAGE my_internal_stage 
	DIRECTORY = ( ENABLE = true );

(SELECT $1, $2
FROM @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt)
(file_format => SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM);

drop table SMOOTHIES.PUBLIC.ORDERS;
CREATE  or replace TABLE SMOOTHIES.PUBLIC.ORDERS
(
ingredients varchar(200)
);

insert into smoothies.public.orders(ingredients) values ('Apples Blueberries Figs Jackfruit Lime Kiwi ');

select * from smoothies.public.orders;

--truncate table smoothies.public.orders;



alter table smoothies.public.orders add column name_on_order varchar(100);
alter table smoothies.public.orders add column order_filled boolean default FALSE;
alter table smoothies.public.orders drop column order_filled;
alter table smoothies.public.orders add column order_filled boolean default FALSE;

update smoothies.public.orders
       set order_filled = true
       where name_on_order is null;

       

select count(*) from smoothies.information_schema.columns
    where table_schema = 'PUBLIC' 
    and table_name = 'ORDERS'
    and column_name = 'ORDER_FILLED'
    --and column_default = 'FALSE'
    and data_type = 'BOOLEAN';


alter table smoothies.public.orders
add column order_uid integer default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column;

truncate table smoothies.public.orders;

create or replace table smoothies.public.orders (
       order_uid number(38,0) default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       order_ts timestamp_ltz(9) default current_timestamp(),
       constraint order_uid unique (order_uid)
);


CREATE FUNCTION sum_mystery_bag_vars ( var1 number,var2 number,var3 number)
RETURNS NUMBER as 'SELECT var1+var2+var3';

SELECT sum_mystery_bag_vars(1,2,3);

SELECT INITCAP('str');

CREATE FUNCTION  NEUTRALIZE_WHINING ( str TEXT)
RETURNS TEXT AS 'SELECT INITCAP(str)';

SELECT NEUTRALIZE_WHINING('str');


       
ALTER TABLE FRUIT_OPTIONS  ADD COLUMN SEARCH_ON VARCHAR(25);

-- Apples, Blueberries, Jack Fruit, Raspberries and Strawberries

select * from FRUIT_OPTIONS;
 select * from smoothies.public.orders;
 
--Create an order for a person named Kevin and use the fruits Apples, Lime and Ximenia (in that order). DO NOT mark the order as filled. 
--Create an order for a person named Divya and use the fruits Dragon Fruit, Guava, Figs, Jackfruit and Blueberries (in that order!). Mark the order as FILLED.  
--Create an order for a person named Xi and use the fruits Vanilla Fruit and Nectarine (in that order). Mark the order as FILLED. 


alter table smoothies.public.orders add column order_ts date;

truncate table smoothies.public.orders;
 
insert into smoothies.public.orders(ingredients, name_on_order, order_filled, order_ts) 
values ('Apples Lime Ximenia ','Kevin',false, '2024-05-28');

insert into smoothies.public.orders(ingredients, name_on_order, order_filled, order_ts) 
values ('Dragon Fruit Guava Figs Jackfruit Blueberries ','Divya', true, '2024-05-29');

insert into smoothies.public.orders(ingredients, name_on_order, order_filled, order_ts) 
values ('Vanilla Fruit Nectarine ','Xi',true, '2024-05-30');
--Vanilla Fruit Nectarine
select * from smoothies.public.orders;
 



select sum(hash_ing) from
      (select hash(ingredients) as hash_ing
       from smoothies.public.orders
       where order_ts is not null 
       and name_on_order is not null 
       and (name_on_order = 'Kevin' and order_filled = FALSE and hash_ing = 7976616299844859825) 
       or (name_on_order ='Divya' and order_filled = TRUE and hash_ing = -6112358379204300652)
       or (name_on_order ='Xi' and order_filled = TRUE and hash_ing = 1016924841131818535));


--Create an order for a person named Kevin and use the fruits Apples, Lime and Ximenia (in that order). DO NOT mark the order as filled. 
--Create an order for a person named Divya and use the fruits Dragon Fruit, Guava, Figs, Jackfruit and Blueberries (in that order!). Mark the order as FILLED.  
--Create an order for a person named Xi and use the fruits Vanilla Fruit and Nectarine (in that order). Mark the order as FILLED. 

-- Apples, Blueberries, Jack Fruit, Raspberries and Strawberries
select hash('Apples Lime Ximenia ') as hash_ing;
select hash('Dragon Fruit Guava Figs Jackfruit Blueberries ') as hash_ing;
select hash('Vanilla Fruit Nectarine ') as hash_ing;

