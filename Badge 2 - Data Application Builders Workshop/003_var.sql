use role pc_rivery_role;
use database pc_rivery_db;
use schema public;

create table L7_end_fdc_food_ingest
clone fdc_food_ingest;

truncate table FDC_FOOD_INGEST;

create table L8_fdc_food_ingest
clone fdc_food_ingest;

create or replace table FRUIT_LOAD_LIST(FRUIT_NAME VARCHAR(25));

insert into pc_rivery_db.public.fruit_load_list
values 
('banana')
,('cherry')
,('strawberry')
,('pineapple')
,('apple')
,('mango')
,('coconut')
,('plum')
,('avocado')
,('starfruit');