CREATE TABLE LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML 
(
  "RAW_AUTHOR" VARIANT
);

CREATE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.XML_FILE_FORMAT 
TYPE = 'XML' 
STRIP_OUTER_ELEMENT = FALSE 
; 

COPY INTO LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML
FROM @garden_plants.veggies.like_a_window_into_an_s3_bucket
FILES = ('author_with_header.xml')
FILE_FORMAT = (FORMAT_NAME = XML_FILE_FORMAT);

SELECT * FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML;

COPY INTO LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML
FROM @garden_plants.veggies.like_a_window_into_an_s3_bucket
FILES = ('author_no_header.xml')
FILE_FORMAT = (FORMAT_NAME = XML_FILE_FORMAT);

SELECT * FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML;


CREATE OR REPLACE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.XML_FILE_FORMAT 
TYPE = 'XML' 
COMPRESSION = 'AUTO' 
PRESERVE_SPACE = FALSE 
STRIP_OUTER_ELEMENT = TRUE 
DISABLE_SNOWFLAKE_DATA = FALSE 
DISABLE_AUTO_CONVERT = FALSE 
IGNORE_UTF8_ERRORS = FALSE; 

TRUNCATE LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML;

COPY INTO LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML
FROM @garden_plants.veggies.like_a_window_into_an_s3_bucket
FILES = ('author_with_header.xml')
FILE_FORMAT = (FORMAT_NAME = XML_FILE_FORMAT);

SELECT * FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_XML;

//Returns entire record
SELECT raw_author 
FROM author_ingest_xml;

// Meta data view of data
SELECT raw_author:"$" 
FROM author_ingest_xml;

//shows the root or top-level object name of each row
SELECT raw_author:"@" 
FROM author_ingest_xml;

//returns AUTHOR_UID value from top-level object's attribute
SELECT raw_author:"@AUTHOR_UID"
FROM author_ingest_xml;

//returns value of NESTED OBJECT called FIRST_NAME
SELECT XMLGET(raw_author, 'FIRST_NAME'):"$"
FROM author_ingest_xml;

//returns the data in a way that makes it look like a normalized table
SELECT 
raw_author:"@AUTHOR_UID" as AUTHOR_ID
,XMLGET(raw_author, 'FIRST_NAME'):"$" as FIRST_NAME
,XMLGET(raw_author, 'MIDDLE_NAME'):"$" as MIDDLE_NAME
,XMLGET(raw_author, 'LAST_NAME'):"$" as LAST_NAME
FROM AUTHOR_INGEST_XML;

//add ::STRING to cast the values into strings and get rid of the quotes
SELECT 
raw_author:"@AUTHOR_UID" as AUTHOR_ID
,XMLGET(raw_author, 'FIRST_NAME'):"$"::STRING as FIRST_NAME
,XMLGET(raw_author, 'MIDDLE_NAME'):"$"::STRING as MIDDLE_NAME
,XMLGET(raw_author, 'LAST_NAME'):"$"::STRING as LAST_NAME
FROM AUTHOR_INGEST_XML; 