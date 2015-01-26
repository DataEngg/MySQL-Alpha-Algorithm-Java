use bpi_2013;
create table connected(setB varchar(255));
Insert INTO connected(setB)
SELECT CONCAT(eventA,',',eventB) FROM nconnected;
SELECT setA,setB from new_xw as A
join 
connected
using(setB);
create table new_connected(setA varchar(255));
Insert INTO new_connected(setA)
SELECT CONCAT(eventA,',',eventB) FROM nconnected;
SELECT setA,setB from new_xw as A
join 
new_connected
using(setA);
Select concat_ws('|','KUNAL','Gupta');
select * from bpi_2013.casulaity;
select eventA,group_concat(eventB) as eventB from bpi_2013.casulaity group by eventA;
select group_concat(eventA),eventB as eventB from bpi_2013.casulaity group by eventB;
use bpi_2013;
create table compare(event varchar(255));
truncate table compare;
LOAD DATA LOCAL INFILE '/Users/Datamaster/Desktop/nc.txt' INTO TABLE compare;
Create table dummy(setA varchar(200),setB varchar(255));
insert into dummy
values
('A','B');
insert into dummy
values
('C','D');
insert into dummy
values
('E','F');
insert into dummy
values
('K,L','M');
insert into dummy
values
('F','G,H');
select * from dummy;

call dummy_table(',');
call dummy_table1(',');
create table dummylast2(id varchar(200),value varchar(200));
insert into dummylast2
select * from dummylast;
create table dummylast3(id varchar(200),value varchar(200));
insert into dummylast3
select * from dummylast1;

create table dummylast4(id varchar(200),value varchar(200));
insert into dummylast4
select id,value from dummylast2
where id in (select distinct id from dummylast3) and value in(select distinct value from dummylast3);

create table dummylast5(id varchar(200),value varchar(200));
insert into dummylast5
select id,value from dummylast
where id not in (select distinct id from dummylast4) and value not in(select distinct value from dummylast4);

create table dummylast6(id varchar(200),value varchar(200));
insert into dummylast6
select id,value from dummylast1
where id not in (select distinct id from dummylast4) and value not in(select distinct value from dummylast4);

select concat_ws(' & ',dummylast5.id,dummylast6.value)as id,dummylast5.value from dummylast5 JOIN dummylast6 where dummylast5.id=dummylast6.id;

select dummylast6.id,concat_ws(' & ',dummylast6.value,dummylast5.id) from dummylast5 JOIN dummylast6 where dummylast5.value=dummylast6.value;


##### NEW Way of Doing FW
create table dummylast9(id varchar(200),value varchar(200));
insert into dummylast9i
select dummy.setA,concat_ws(' & ',dummy.setA,dummy.setB) from dummy;

create table dummylast10(id varchar(200),value varchar(200));
insert into dummylast10
select concat_ws(' & ',dummy.setA,dummy.setB),dummy.setB from dummy;

call dummy_table3(',');
call dummy_table4(',');
use dummy;
SELECT @@profiling;
SET profiling=1;
DROP TABLE if exists numbers;
call explode_tableYW(',');
Show profiles;
SHOW PROFILE CPU FOR QUERY 1;

SELECT table_name,Engine,Version,Row_format,table_rows,Avg_row_length,
Data_length,Max_data_length,Index_length,Data_free,Auto_increment,
Create_time,Update_time,Check_time,table_collation,Checksum,
Create_options,table_comment FROM information_schema.tables
WHERE table_schema = 'bpi_2014';

Create Schema bpi_2014;
use bpi_2014;

DROP TABLE eventlog;
CREATE TABLE eventlog (caseid varchar(255) ,timestamp DATETIME ,activity varchar(255),actor varchar(255));
LOAD DATA LOCAL INFILE 'C:\\Users\\Datamaster\\Documents\\Mysql\\400000.csv'     
INTO TABLE eventlog
FIELDS TERMINATED BY ';'
(caseid,timestamp,@id1,activity,actor,@id2,@id3);
Select * from eventlog;
DROP TABLE totalevents;
CREATE TABLE TotalEvents(events varchar(50) PRIMARY KEY);
INSERT INTO TotalEvents(events)
          (SELECT DISTINCT eventlog.activity 
           FROM eventlog);
DROP TABLE initialevents;
CREATE TABLE InitialEvents(InitialEvents varchar(50));
INSERT INTO InitialEvents(InitialEvents)
SELECT distinct derived.activity 
FROM 
(SELECT eventlog.caseid, min(eventlog.timestamp),eventlog.activity 
FROM eventlog GROUP BY eventlog.caseid) AS derived;
DROP TABLE finalevents;
CREATE TABLE FinalEvents(FinalEvents varchar(50));
INSERT INTO FinalEvents(FinalEvents)
SELECT DISTINCT A.activity 
           FROM eventlog AS A
           WHERE A.timestamp IN 
                                (SELECT MAX(derived.timestamp) AS timestamp
                                  FROM eventlog As derived
                                  GROUP BY derived.caseid);
SELECT DISTINCT initialevents from initialevents;
SELECT DISTINCT finalevents from finalevents;
SELECT DISTINCT caseid FROM eventlog LIMIT 50000;
SELECT * FROM parallel;
SELECT * FROM casulaity;
SELEct * from not_connected;
use bpi_2014;
SELECT * FROM safeeventa;
SELECT * from safeeventb;
LOAD DATA LOCAL INFILE 'C:\\Users\\Datamaster\\Documents\\Mysql\\safeeventa.csv'     
INTO TABLE safeeventa
FIELDS TERMINATED BY ';'
(setA,setB);
LOAD DATA LOCAL INFILE 'C:\\Users\\Datamaster\\Documents\\Mysql\\safeeventb.csv'     
INTO TABLE safeeventb
FIELDS TERMINATED BY ';'
(setA,setB);

Select * from xw;
Select * from yw;
Select * from pw;
Select * from fw;

select table_name, (((data_length + index_length))) from information_schema.tables where table_schema = "bpi_2014";

SELECT TABLE_NAME, table_rows, data_length, index_length, 
round(((data_length + index_length) / 1024 / 1024),2) "Size in MB"
FROM information_schema.TABLES WHERE table_schema = "bpi_2014";

SELECT (data_length+index_length) tablesize
FROM information_schema.tables
WHERE table_schema='bpi_2014' and table_name='fw';