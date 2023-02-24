create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH
after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
as MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e 
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
