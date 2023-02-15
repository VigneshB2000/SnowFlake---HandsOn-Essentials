 create stage garden_plants.veggies.like_a_window_into_an_s3_bucket 
 url = 's3://uni-lab-files';
 
 show stages in account;
 
 list @garden_plants.veggies.like_a_window_into_an_s3_bucket;
 
 create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

copy into vegetable_details_soil_type
from @like_a_window_into_an_s3_bucket
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=PIPECOLSEP_ONEHEADROW );


--The data in the file, with no FILE FORMAT specified
select $1
from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier  
select $1, $2, $3
from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3
from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

create file format garden_plants.veggies.L8_CHALLENGE_FF 
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t' 
    SKIP_HEADER = 1 
    ;
    
    create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
 );
 
 copy into LU_SOIL_TYPE
from @like_a_window_into_an_s3_bucket
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=L8_CHALLENGE_FF );

create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT(
plant_name varchar(75),	
UOM varchar(1),
Low_End_of_Range number, 
High_End_of_Range number
 );
 
 copy into VEGETABLE_DETAILS_PLANT_HEIGHT 
from @like_a_window_into_an_s3_bucket
files = ( 'veg_plant_height.csv')
file_format = ( format_name=COMMASEP_DBLQUOT_ONEHEADROW );
