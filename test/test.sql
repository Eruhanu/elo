CREATE USER source_test IDENTIFIED BY source_test;
GRANT CONNECT, resource TO source_test;


CREATE USER target_test IDENTIFIED BY target_test;
GRANT CONNECT, resource TO target_test;


drop  DATABASE LINK local;

CREATE DATABASE LINK local 
CONNECT TO source_test IDENTIFIED BY source_test
USING 
'(DESCRIPTION=
(ADDRESS=
(PROTOCOL=TCP)
(HOST=localhost)
(PORT=1521))
(CONNECT_DATA=
(SID=orcl)))';


drop table  source_test.ALL_TABLES;
CREATE TABLE source_test.ALL_TABLES
AS 
SELECT * 
FROM ALL_TABLES;

drop table  target_test.ALL_TABLES;
CREATE TABLE target_test.ALL_TABLES
AS 
SELECT * 
FROM ALL_TABLES where 1=2;


truncate table util.ELO_TABLES; 

truncate table util.ELO_COLUMNS; 

insert into util.ELO_TABLES 
(name,db_link,source,target,filter)
values
('UTIL.ALL_TABLES@local','local', 'source_test.ALL_TABLES','target_test.ALL_TABLES', 'NAME like ''A%'' ');


insert into util.ELO_COLUMNS
(name,source_col,target_col)
values
('UTIL.ALL_TABLES@local','name','name');


insert into util.ELO_COLUMNS
(name,source_col,target_col)
values
('UTIL.ALL_TABLES@local','owner','owner');



commit;