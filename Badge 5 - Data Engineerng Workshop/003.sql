select $1
from @uni_kishore/
(file_format => ff_json_logs);

select * from game_logs;

copy into ags_game_audience.raw.game_logs
from @uni_kishore
file_format = (format_name = ff_json_logs);

select * from game_logs;

select * from LOGS;

create or replace view ags_game_audience.raw.logs as
select 
-- $1:agent::text as AGENT,
$1:user_event::text as USER_EVENT,
$1:user_login::text as USER_LOGIN,
$1:datetime_iso8601::Timestamp_ntz as DATETIME_ISO8601,
$1:ip_address::text as IP_ADDRESS,
$1
from @uni_kishore(file_format => ff_json_logs)
 where ip_address is not null;

select * from Logs;

select * from logs where user_login like '%prajina%';


