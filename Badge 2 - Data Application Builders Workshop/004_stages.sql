show stages in account;

list @my_internal_named_stage;

select $1 from @my_internal_named_stage/my_file.txt.gz;
