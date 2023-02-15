create database AGS_GAME_AUDIENCE;

drop schema AGS_GAME_AUDIENCE.PUBLIC;

create schema AGS_GAME_AUDIENCE.RAW;

create or replace table ags_game_audience.raw.game_logs(
raw_log variant
);

create stage uni_kishore
url = 's3://uni-kishore';

list @uni_kishore/kickoff;

create file format AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS
type = 'JSON'
COMPRESSION = 'AUTO'
STRIP_OUTER_ARRAY = TRUE
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE;

select $1
from @uni_kishore/kickoff
(file_format => ff_json_logs);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);

select * from game_logs;

select RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT
,*
from game_logs;

select 
RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT,
RAW_LOG:user_login::text as USER_LOGIN,
RAW_LOG:datetime_iso8601::Timestamp_ntz as DATETIME_ISO8601,
RAW_LOG
from game_logs;

create or replace view ags_game_audience.raw.logs as
select 
RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT,
RAW_LOG:user_login::text as USER_LOGIN,
RAW_LOG:datetime_iso8601::Timestamp_ntz as DATETIME_ISO8601,
RAW_LOG
from game_logs;


