use role sysadmin;

CREATE DATABASE ZENAS_ATHLEISURE_DB;

DROP SCHEMA ZENAS_ATHLEISURE_DB.PUBLIC;

CREATE SCHEMA ZENAS_ATHLEISURE_DB.PRODUCTS;

LIST @uni_klaus_clothing;

LIST @uni_klaus_zmd;

list @uni_klaus_sneakers;


select $1,$2 from @uni_klaus_zmd;

create stage temp_check
url = 's3://uni-klaus/zenas_metadata/product_coordination_suggestions.txt';

select $1,$2 from @temp_check;

select $1
from @uni_klaus_zmd; 

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt; 

create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACES = TRUE; 

select $1, $2
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);



create or replace file format zmd_file_format_4
RECORD_DELIMITER = ';'
TRIM_SPACE=True;

select $1 as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_4 );


create or replace file format zmd_file_format_5
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE=True; 

select $1, $2, $3 from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_5);


select replace($1, chr(13)||chr(10)) as sizes_available 
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_4)
where sizes_available <> '';

create or replace view zenas_athleisure_db.products.sweatsuit_sizes as 
select replace($1, chr(13)||chr(10)) as sizes_available 
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_4)
where sizes_available <> '';

create or replace view zenas_athleisure_db.products.sweatband_product_line as
select 
replace($1, chr(13)||chr(10)) as product_code,
replace($2, chr(13)||chr(10)) as headband_description,
replace($3, chr(13)||chr(10)) as wristband_description
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_5);

select * from zenas_athleisure_db.products.sweatband_product_line;

create or replace view zenas_athleisure_db.products.sweatband_coordination as
select 
replace($1, chr(13)||chr(10)) as product_code,
replace($2, chr(13)||chr(10)) as has_matching_sweatsuit
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

select * from zenas_athleisure_db.products.sweatband_coordination;


