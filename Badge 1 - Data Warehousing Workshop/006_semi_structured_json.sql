CREATE TABLE "LIBRARY_CARD_CATALOG"."PUBLIC"."AUTHOR_INGEST_JSON" 
(
  "RAW_AUTHOR" Variant
);

//Create File Format for JSON Data
CREATE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE; 

COPY INTO 
LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON
FROM @garden_plants.veggies.like_a_window_into_an_s3_bucket
FILES = ('author_with_header.json')
FILE_FORMAT = (FORMAT_NAME = JSON_FILE_FORMAT);

SELECT *  FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON;


SELECT 
raw_author:"@AUTHOR_UID" as AUTHOR_ID
,XMLGET(raw_author, 'FIRST_NAME'):"$"::STRING as FIRST_NAME
,XMLGET(raw_author, 'MIDDLE_NAME'):"$"::STRING as MIDDLE_NAME
,XMLGET(raw_author, 'LAST_NAME'):"$"::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;

//returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

//returns the data in a way that makes it look like a normalized table
SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;