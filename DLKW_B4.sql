-------------------------Data Lake Workshop L4--------------------------------

use role accountadmin;

select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
);

//  Reviewing Data Types

// Try This Fake Table to View Data Type Symbols

create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);


// Create a Database for Zena's Athleisure Idea


CREATE OR REPLACE DATABASE  ZENAS_ATHLEISURE_DB;
CREATE OR REPLACE SCHEMA PRODUCTS;

USE ROLE ACCOUNTADMIN;
USE SCHEMA PUBLIC;
USE DATABASE UTIL_DB;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW01' as step
  ,( select count(*)  
      from ZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.STAGES 
      where stage_schema = 'PRODUCTS'
      and 
      (stage_type = 'Internal Named' 
      and stage_name = ('PRODUCT_METADATA'))
      or stage_name = ('SWEATSUITS')
   ) as actual
, 2 as expected
, 'Zena stages look good' as description
); 

// List Commands Versus Select Statements 

list @product_metadata;

list @zenas_athleisure_db.products.product_metadata;


select $1
from @product_metadata; 

--USE UTIL_DB.PUBLIC;
SELECT $1
From @product_metadata/product_coordination_suggestions.txt

SELECT $1
FROM  @product_metadata/sweatsuit_sizes.txt

SELECT $1
FROM @product_metadata/swt_product_line.txt

// Create an Exploratory File Format

CREATE OR REPLACE FILE FORMAT zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);


// Testing Our Second Theory
create OR REPLACE file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1, $2, $3, $4
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

// A Third Possibility?
CREATE OR REPLACE file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACE  = TRUE; ; 

select $1, $2
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

//Revise zmd_file_format_1/Rewrite zmd_file_format_1

CREATE OR REPLACE FILE FORMAT zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

create or replace view zenas_athleisure_db.products.sweatsuit_sizes as 
SELECT REPLACE($1,CHR(13) || CHR(10)) AS sizes_available
FROM @product_metadata/sweatsuit_sizes.txt 
(FILE_FORMAT => zmd_file_format_1)
WHERE sizes_available <> '';

select * from 
zenas_athleisure_db.products.sweatsuit_sizes

// replace file formate 2

CREATE OR REPLACE file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

CREATE VIEW zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE AS
select REPLACE( $1, CHAR(13)||CHAR(10) ) 
AS PRODUCT_CODE, REPLACE( $2, CHAR(13)||CHAR(10) ) 
AS HEADBAND_DESCRIPTION, REPLACE( $3, CHAR(13)||CHAR(10) ) 
AS WRISTBAND_DESCRIPTION
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2);

SELECT * FROM zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE;

// replace format 3

CREATE OR REPLACE file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACE  = TRUE; ; 

CREATE VIEW  zenas_athleisure_db.products.SWEATBAND_COORDINATION AS
select REPLACE( $1, CHAR(13)||CHAR(10) ) 
AS PRODUCT_CODE, REPLACE( $2, CHAR(13)||CHAR(10) ) 
AS HAS_MATCHING_SWEATSUIT
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

SELECT * FROM zenas_athleisure_db.products.SWEATBAND_COORDINATION


// DORA CHECK

USE ROLE ACCOUNTADMIN;
USE DATABASE UTIL_DB;
USE SCHEMA PUBLIC;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
   'DLKW02' as step
   ,( select sum(tally) from
        (select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATBAND_PRODUCT_LINE
        where length(product_code) > 7 
        union
        select count(*) as tally
        from ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUIT_SIZES
        where LEFT(sizes_available,2) = char(13)||char(10))     
     ) as actual
   ,0 as expected
   ,'Leave data where it lands.' as description
); 

select product_code, has_matching_sweatsuit
from zenas_athleisure_db.products.sweatband_coordination;

select product_code, headband_description, wristband_description
from zenas_athleisure_db.products.sweatband_product_line;

select sizes_available
from zenas_athleisure_db.products.sweatsuit_sizes;

// Non-Loaded Data is Easy, Let's Do Some More!

// Run a List Command On the SWEATSUITS Stage

List @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS

// Let's Query the Unstructured Unloaded Data!

SELECT $1
FROM @PRODUCT_METADATA;

SELECT $1
FROM @PRODUCT_METADATA/product_coordination_suggestions.txt
(file_format => zmd_file_format_1)

CREATE OR REPLACE FILE FORMAT zmd_file_format_2
FIELD_DELIMITER = '^';


list @sweatsuits

select $1
from @sweatsuits

// Try to Query an Unstructured Data File
// Query with 2 Built-In Meta-Data Columns

select $1
from @sweatsuits/purple_sweatsuit.png;

SELECT metadata$filename, COUNT(*) AS NUMBER_OF_ROWS
FROM @sweatsuits
GROUP BY metadata$filename;

// Query the Directory Table of a Stage

select * 
from directory(@sweatsuits)


// Start By Checking Whether Functions will Work on Directory Tables 

select REPLACE(relative_path, '_', ' ') as no_underscores_filename
, REPLACE(no_underscores_filename, '.png') as just_words_filename
, INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);

//  Nest 3 Functions into 1 Statement

SELECT REPLACE(REPLACE(relative_path,'_',' '),'.png') as just_words_filename,
INITCAP(just_words_filename) as product_name
from directory(@sweatsuits);


// Now, can you nest them all into a single column and name it "PRODUCT_NAME"? 

select REPLACE(REPLACE(REPLACE(UPPER(RELATIVE_PATH),'/'),'_',' ') ,'.PNG')as product_name
from directory(@sweatsuits);

// Create an Internal Table in the Zena Database

--create an internal table for some sweatsuit info
create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);


--fill the new table with some data
insert into  zenas_athleisure_db.products.sweatsuits 
          (color_or_style, file_name, price)
values
 ('Burgundy', 'burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'forest_green_sweatsuit.png',64)
,('Navy Blue', 'navy_blue_sweatsuit.png',65)
,('Orange', 'orange_sweatsuit.png',65)
,('Pink', 'pink_sweatsuit.png',63)
,('Purple', 'purple_sweatsuit.png',64)
,('Red', 'red_sweatsuit.png',68)
,('Royal Blue',	'royal_blue_sweatsuit.png',65)
,('Yellow', 'yellow_sweatsuit.png',67);

SELECT * FROM sweatsuits

// Can You Join These?

SELECT * 
FROM DIRECTORY (@sweatsuits)

SELECT INITCAP(REPLACE(REPLACE(relative_path,'_',' '),'.png')) as Product_Name, *
From directory(@sweatsuits) d
join sweatsuits s
on d.relative_path = s.file_name

// create view table

CREATE OR REPLACE VIEW PRODUCT_LIST AS
SELECT INITCAP(REPLACE(REPLACE(relative_path,'_',' '),'.png')) as Product_Name,file_name,color_or_style,price,file_url
From directory(@sweatsuits) d
join sweatsuits s
on d.relative_path = s.file_name

SELECT * FROM PRODUCT_LIST

// Add the CROSS JOIN

CREATE OR REPLACE VIEW CATALOG AS
select * 
from product_list p
cross join sweatsuit_sizes;

select * from catalog

// Run This in Your Worksheet to Send a Report to DORA 

USE ROLE ACCOUNTADMIN;
USE DATABASE UTIL_DB;
USE SCHEMA PUBLIC;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW03' as step
 ,( select count(*) from ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG) as actual
 ,180 as expected
 ,'Cross-joined view exists' as description
); 

// Add the Upsell Table and Populate It

-- Add a table to map the sweatsuits to the sweat band sets
create OR REPLACE table zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style varchar(25)
,upsell_product_code varchar(10)
);

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping
(
sweatsuit_color_or_style
,upsell_product_code 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');



SELECT * FROM ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING


// Zena's View for the Athleisure Web Catalog Prototype

-- Zena needs a single view she can query for her website prototype
create OR REPLACE view catalog_for_website as 
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
-- ,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey --- Sweat Accessories')  as upsell_product_desc,
, 'xyz' as upsell_product_desc
from
(   select color_or_style, price, file_name    
,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code
limit 6;

SELECT * FROM CATALOG_FOR_WEBSITE

---doora not working

// Run This in Your Worksheet to Send a Report to DORA

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW04' as step
 ,( select count(*) 
  from zenas_athleisure_db.products.catalog_for_website 
  where upsell_product_desc not like '%e, Bl%') as actual
 ,6 as expected
 ,'Relentlessly resourceful' as description
); 

---------------------------Lesson 5: Mel's Concept Kickoff --------------------------------------

CREATE OR REPLACE DATABASE MELS_SMOOTHIE_CHALLENGE_DB
DROP SCHEMA PUBLIC
CREATE OR REPLACE SCHEMA TRAILS;

// Create a Very Basic Parquet File Format

-- Create a file format, name it FF_PARQUET and set the Type to PARQUET
-- Make sure it's in the TRAILS schema and are owned by SYSADMIN

// Query Your TRAILS_GEOJSON Stage!

SELECT * 
FROM @trails_geojson (file_format => ff_json)

SELECT * 
FROM @trails_PARQUET (file_format => FF_PARQUET)


// Run This in Your Worksheet to Send a Report to DORA



select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW05' as step
 ,( select sum(tally)
   from
     (select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.stages 
      union all
      select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.file_formats)) as actual
 ,4 as expected
 ,'Camila\'s Trail Data is Ready to Query' as description
 );


// 🥋 Look at the Parquet Data

-- Write a more sophisticated query to parse the data into columns
SELECT 
$1:"sequence_1" AS sequence_1,
$1:"sequence_2" AS sequence_2,
$1:"trail_name"::VARCHAR AS trail_name,
$1:"elevation" AS elevation,
$1:"latitude"::NUMBER(11,8) AS lng,
$1:"longitude"::NUMBER(11,8) AS lat
FROM @TRAILS_PARQUET
(FILE_FORMAT => FF_PARQUET)
ORDER BY sequence_1;


//

--Nicely formatted trail data
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng, --remember we did a gut check on this data
 $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

// Create a View Called CHERRY_CREEK_TRAIL

CREATE OR REPLACE VIEW cherry_creek_trail AS
SELECT 
    $1:sequence_1 AS point_id,
    $1:trail_name::varchar AS trail_name,
    $1:latitude::number(11,8) AS lng,
    $1:longitude::number(11,8) AS lat,
    CONCAT(lng, ' ', lat) AS coord_pair
FROM @trails_parquet
(FILE_FORMAT => ff_parquet)
ORDER BY point_id;

SELECT * FROM CHERRY_CREEK_TRAIL;

--Using concatenate to prepare the data for plotting on a map
select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

//

--To add a column, we have to replace the entire view
--changes to the original are shown in red
create or replace view cherry_creek_trail as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

-- Run this SELECT and Paste the Results into WKT Playground!

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;



// Normalize the Data Without Loading It!

select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);


// Create a View Called DENVER_AREA_TRAILS

CREATE OR REPLACE VIEW DENVER_AREA_TRAILS
AS
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json)


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DLKW06' as step
 ,( select count(*) as tally
      from mels_smoothie_challenge_db.information_schema.views 
      where table_name in ('CHERRY_CREEK_TRAIL','DENVER_AREA_TRAILS')) as actual
 ,2 as expected
 ,'Mel\'s views on the geospatial data from Camila' as description
 );


-----------------------Lesson 7: Exploring GeoSpatial Functions -----------------------------------

-- Re-Using Earlier Code (with a Small Addition)

--Remember this code? 
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,st_length(TO_GEOGRAPHY(my_linestring)) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail
group by trail_name;

// Calculate the Lengths for the Other Trails

create or replace view DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    trail_length,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,st_length(TO_GEOGRAPHY(geometry)) as trail_length 
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

SELECT * FROM DENVER_AREA_TRAILS;

SELECT * FROM CHERRY_CREEK_TRAIL


// Create a View on Cherry Creek Data to Mimic the Other Trail Data
--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry))  as trail_length
from cherry_creek_trail
group by trail_name;


// Use A Union All to Bring the Rows Into a Single Result Set
--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;


SELECT FEATURE_NAME, GEOMETRY, TRAIL_LENGTH
FROM DENVER_AREA_TRAILS
UNION ALL
SELECT FEATURE_NAME,GEOMETRY,TRAIL_LENGTH
FROM DENVER_AREA_TRAILS_2

--Add more GeoSpatial Calculations to get more GeoSpecial Information! 

select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

// Make it a View
CREATE OR REPLACE VIEW TRAILS_AND_BOUNDARIES AS 
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from DENVER_AREA_TRAILS_2;

select round(max(max_northsouth))
      from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES;

ALTER DATABASE OPENSTREETMAP_DENVER RENAME TO SONRA_DENVER_CO_USA_FREE;


//  A Polygon Can be Used to Create a Bounding Box

select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from trails_and_boundaries;


// Run This in Your Worksheet to Send a Report to DORA


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
  'DLKW07' as step
   ,( select round(max(max_northsouth))
      from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_AND_BOUNDARIES)
      as actual
 ,40 as expected
 ,'Trails Northern Extent' as description
 ); 

 

 --- Lesson 8: Supercharging Development with Marketplace Data----------------
 
// Using Variables in Snowflake Worksheets 

{
      "type": "Feature",
     "properties": {
        "marker-color": "#ee9bdc",
       "marker-size": "medium",
        "marker-symbol": "cafe",
        "name": "Melanie's Cafe"
     },
     "geometry": {
        "type": "Point",
       "coordinates": [
          -104.97300870716572,
          39.76469906695095
        ]
      }
    }



-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat)
        ,st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;

// Variables are Cool, But Constants Aren't So Bad!

select st_distance(
    st_makepoint($mc_lng,$mc_lat),
    st_makepoint($loc_lng,$loc_lat)) as mc_to_cp;

select st_distance(
    st_makepoint('-104.97300245114094','39.76471253574085'),
    st_makepoint($loc_lng,$loc_lat)) as mc_to_cp;


//Create a second Schema in Mel's Database
CREATE OR REPLACE SCHEMA LOCATIONS

    
// Let's Create a UDF for Measuring Distance from Melanie's Café
CREATE OR REPLACE FUNCTION distance_to_mc(loc_lat number(38,32), loc_lng number(38,32))
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,st_makepoint(loc_lat,loc_lng)
        )
  $$
  ;

// Test the New Function!
--Tivoli Center into the variables 
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

select distance_to_mc($tc_lng,$tc_lat);
  

// Create a List of Competing Juice Bars in the Area

CREATE OR REPLACE VIEW COMPETITION AS
select * 
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');


SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;


//  Changing the Function to Accept a GEOGRAPHY Argument 

CREATE OR REPLACE FUNCTION distance_to_mc(lng_and_lat GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lng_and_lat
        )
  $$;
  
SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

// Now We Can Use it In Our Sonra Select

SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_to_melanies
 ,*
FROM  competition
ORDER by distance_to_melanies;


// Different Options, Same Outcome!

-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lng,$tcb_lat);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lng,$tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

CREATE OR REPLACE VIEW DENVER_BIKE_SHOPS 
AS
SELECT name
, distance_to_mc(coordinates) as DISTANCE_TO_MELANIES  
, ST_ASWKT(coordinates) AS COORDINATES
FROM OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where shop='bicycle';

SELECT * FROM DENVER_BIKE_SHOPS WHERE DISTANCE_TO_MELANIES LIKE '2490%';

// Run This in Your Worksheet to Send a Report to DORA

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW08' as step
  ,( select truncate(distance_to_melanies)
      from mels_smoothie_challenge_db.locations.denver_bike_shops
      where name like '%Mojo%') as actual
  ,14084 as expected
  ,'Bike Shop View Distance Calc works' as description
 ); 

-------------------------Lesson 9: Lions & Tigers & Bears, Oh My!!! ------------------------------

//  Let's TRY TO CREATE a Super-Simple, Stripped Down External Table
-- create or replace external table T_CHERRY_CREEK_TRAIL(
-- 	my_filename varchar(100) as (metadata$filename::varchar(100))
-- ) 
-- location= @trails_parquet
-- auto_refresh = true
-- file_format = (type = parquet);

//  Let's TRY AGAIN to Create a Super-Simple, Stripped Down External Table

CREATE OR REPLACE EXTERNAL TABLE T_CHERRY_CREEK_TRAIL(
my_file_name varchar(100) as (metadata$filename :: varchar(100)))

location  = @external_aws_dlkw
auto_refresh = true
file_format = (type=parquet);

SELECT * FROM T_CHERRY_CREEK_TRAIL

//  Create a Materialized View Version of Our New External Table

CREATE SECURE MATERIALIZED VIEW MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
    POINT_ID,
    TRAIL_NAME,
    LNG,
    LAT,
    COORD_PAIR,
    DISTANCE_TO_MELANIES
) AS
SELECT 
    VALUE:sequence_1 AS point_id,
    VALUE:trail_name::varchar AS trail_name,
    VALUE:longitude::number(11,8) AS lng,
    VALUE:latitude::number(11,8) AS lat,
    VALUE:longitude::varchar || ' ' || VALUE:latitude::varchar AS coord_pair,
    locations.distance_to_mc(VALUE:longitude::number(11,8), VALUE:latitude::number(11,8)) AS distance_to_melanies
FROM t_cherry_creek_trail;


---------------------------------

create or replace secure materialized view mels_smoothie_challenge_db.trails.SMV_CHERRY_CREEK_TRAIL
    comment = 'My 1st secure view'
    as 
    SELECT * FROM  mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL;

SELECT * FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL
----------------------------------

// Run This in Your Worksheet to Send a Report to DORA

USE ROLE ACCOUNTADMIN;
USE DATABASE UTIL_DB;
USE SCHEMA PUBLIC;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
  'DLKW09' as step
  ,( select row_count
     from mels_smoothie_challenge_db.information_schema.tables
     where table_schema = 'TRAILS'
    and table_name = 'SMV_CHERRY_CREEK_TRAIL')   
   as actual
  ,3526 as expected
  ,'Secure Materialized View Created' as description
 ); 


 // Create an External Volume

USE ROLE ACCOUNTADMIN

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );
 
// Check Your Volume (And Get the User Info for Us)

DESC EXTERNAL VOLUME iceberg_external_volume;


//  Create an Iceberg Database
create database my_iceberg_db
 catalog = 'SNOWFLAKE'
 external_volume = 'iceberg_external_volume';

// Create a Table 
set table_name = 'CCT_'||current_account();

create iceberg table identifier($table_name) (
    point_id number(10,0)
    , trail_name string
    , coord_pair string
    , distance_to_melanies decimal(20,10)
    , user_name string
)
  BASE_LOCATION = $table_name
  AS SELECT top 100
    point_id
    , trail_name
    , coord_pair
    , distance_to_melanies
    , current_user()
  FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;
 