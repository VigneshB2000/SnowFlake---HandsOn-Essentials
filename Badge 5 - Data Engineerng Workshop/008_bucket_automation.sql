create stage uni_kishore_pipeline
url = 's3://uni-kishore-pipeline';

list @uni_kishore_pipeline;

select current_timestamp();

create or replace TABLE AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS (
	RAW_LOG VARIANT
);

copy into ags_game_audience.raw.pipeline_logs
from @uni_kishore_pipeline
file_format = (format_name = ags_game_audience.raw.FF_JSON_LOGS);

select count(*) from pipeline_logs;


select * from pipeline_logs;


create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	IP_ADDRESS,
	RAW_LOG
) as
select 
-- $1:agent::text as AGENT,
$1:user_event::text as USER_EVENT,
$1:user_login::text as USER_LOGIN,
$1:datetime_iso8601::Timestamp_ntz as DATETIME_ISO8601,
$1:ip_address::text as IP_ADDRESS,
$1
from @uni_kishore_pipeline(file_format => ff_json_logs)
 where ip_address is not null;
 
 select * from pl_logs;
 
 
 create or replace task load_logs_enhanced
 warehouse = 'COMPUTE_WH'
 schedule = '5 minute'
 as 
 MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e 
USING (
   SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.PL_LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601)) = tod.hour 
) AS r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
WHEN NOT MATCHED THEN
INSERT
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, 
 GAME_EVENT_UTC, CITY, REGION, COUNTRY, 
 GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
VALUES
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, 
 GAME_EVENT_UTC, CITY, REGION, COUNTRY, 
 GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);
 
 create task get_new_files
 warehouse = 'COMPUTE_WH'
 schedule = '5 minute'
 as 
copy into ags_game_audience.raw.pipeline_logs
from @uni_kishore_pipeline
file_format = (format_name = ags_game_audience.raw.FF_JSON_LOGS);

execute task get_new_files;

execute task load_logs_enhanced;


 
 
 
 