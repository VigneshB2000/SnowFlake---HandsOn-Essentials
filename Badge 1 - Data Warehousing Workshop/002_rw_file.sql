select * from garden_plants.veggies.root_depth;

create table vegetable_details
(
plant_name varchar(25)
, root_depth_code varchar(1)    
);
-- File uploaded using snowloader streamlit app
select * from GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS;

create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '|' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;
    
create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;
    
select * from vegetable_details;

select * from vegetable_details where plant_name = 'Spinach';

delete from vegetable_details where plant_name = 'Spinach' and root_depth_code = 'D';