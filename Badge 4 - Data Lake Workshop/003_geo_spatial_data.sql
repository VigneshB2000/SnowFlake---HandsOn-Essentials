create database MELS_SMOOTHIE_CHALLENGE_DB;

drop schema MELS_SMOOTHIE_CHALLENGE_DB.PUBLIC;

create schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;

create or replace stage trails_parquet
url = 's3://uni-lab-files-more/dlkw/trails/trails_parquet';

create or replace stage trails_geojson
url = 's3://uni-lab-files-more/dlkw/trails/trails_geojson';

list @trails_geojson;

list @trails_parquet;

create file format MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON
type = 'JSON'
COMPRESSION = 'AUTO'
STRIP_OUTER_ARRAY = TRUE
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE;

create file format MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET
type = 'PARQUET';

select $1
from @trails_geojson
(file_format => ff_json);

select $1
from @trails_parquet
(file_format => ff_parquet);

select
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude:: number (11,8) as Ing,
 $1:longitude:: number (11,8) as lat
from @trails_parquet
 (file_format => ff_parquet)
order by point_id;


create or replace view CHERRY_CREEK_TRAIL as 
select
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude:: number (11,8) as lng,
 $1:longitude:: number (11,8) as lat
from @trails_parquet
 (file_format => ff_parquet)
order by point_id;

--Using concatenate to prepare the data for plotting on a map
select top 100 
 lng||' '||lat as coord_pair
,'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

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

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
group by trail_name;


select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

create or replace view DENVER_AREA_TRAILS as
select
$1:features[0]:properties:Name::string as feature_name
,$1:features[0]:geometry:coordinates::string as feature_coordinates
,$1:features[0]:geometry::string as geometry
,$1:features[0]:properties::string as feature_properties
,$1:crs:properties:name::string as specs
,$1 as whole_object
from @trails_geojson (file_format => ff_json);

select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
,st_length(to_geography(my_linestring)) as length_of_trail --this line is new! but it won't work!
from cherry_creek_trail;

select 
feature_name,
st_length(to_geography(geometry)) as trail_length 
from denver_area_trails;

create or replace view DENVER_AREA_TRAILS(
FEATURE_NAME,
FEATURE_COORDINATES,
GEOMETRY,
TRAIL_LENGTH,
FEATURE_PROPERTIES,
SPECS,
WHOLE_OBJECT
) as
select
$1:features[0]:properties:Name::string as feature_name,
$1:features[0]:geometry:coordinates::string as feature_coordinates,
$1:features[0]:geometry::string as geometry,
st_length(to_geography(geometry)) as trail_length,
$1:features[0]:properties::string as feature_properties,
$1:crs:properties:name::string as specs
 ,$1 as whole_object
from @trails_geojson (file_format => ff_json);


select * from denver_area_trails;

select * from cherry_creek_trail;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
--Even though this data started out as Parquet, and we're joining it with geoJSON data
--So let's make it look like geoJSON instead.
create view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;

--Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;

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

create or replace view TRAILS_AND_BOUNDARIES as
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

select * from trails_and_boundaries;

select min (min_eastwest) as western_edge,
 min (min_northsouth) as southern_edge
 ,max (max_eastwest) as eastern_edge
 , max (max_northsouth) as northern_edge
 from trails_and_boundaries;
 
 
 select'POLYGON(('||
     min(min_eastwest)||' '||max(max_northsouth) ||'.'||
     max(min_eastwest)||' '||max(max_northsouth) ||'.'||
     max(min_eastwest)||' '||min(max_northsouth) ||'.'||
     min(min_eastwest)||' '||min(max_northsouth) ||'))'
 as my_polygon
 from trails_and_boundaries;


