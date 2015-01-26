# please Don't remove casulaity and not_connected file and safeeventA and safeeventB

use bpi_2014;

DROP TABLE IF EXISTS eventlog;
CREATE TABLE eventlog (caseid varchar(255) ,timestamp DATETIME ,status varchar(255),activity varchar(255),actor varchar(255),PRIMARY KEY(caseid,timestamp,status));
LOAD DATA LOCAL INFILE 'C:\\Users\\Datamaster\\Documents\\Mysql\\dataset.csv'     
INTO TABLE eventlog
FIELDS TERMINATED BY ','
(caseid,timestamp,status,activity,actor,@id2,@id3);

DROP TABLE IF EXISTS TotalEvents;
CREATE TABLE TotalEvents(events varchar(50) PRIMARY KEY);
INSERT INTO TotalEvents(events)
          (SELECT DISTINCT eventlog.activity 
           FROM eventlog);

DROP TABLE IF EXISTS InitialEvents;
CREATE TABLE InitialEvents(InitialEvents varchar(50));
INSERT INTO InitialEvents(InitialEvents)
SELECT DISTINCT A.activity 
           FROM eventlog AS A
           WHERE A.timestamp IN 
                                (SELECT MIN(derived.timestamp) AS timestamp
                                  FROM eventlog As derived
                                  GROUP BY derived.caseid);

DROP TABLE IF EXISTS FinalEvents;
CREATE TABLE FinalEvents(FinalEvents varchar(50));
INSERT INTO FinalEvents(FinalEvents)
SELECT DISTINCT A.activity 
           FROM eventlog AS A
           WHERE A.timestamp IN 
                                (SELECT MAX(derived.timestamp) AS timestamp
                                  FROM eventlog As derived
                                  GROUP BY derived.caseid);


DROP TABLE IF EXISTS eventA;
DROP TABLE IF EXISTS eventB;
DROP TABLE IF EXISTS xw;
CREATE TABLE eventA (setA varchar(255),setB varchar(255));
CREATE TABLE eventB (setA varchar(255),setB varchar(255));
CREATE TABLE xw (setA varchar(255),setB varchar(255));
INSERT INTO eventA (setA,setB)
SELECT eventA , GROUP_CONCAT(eventB) 
FROM casulaity 
GROUP BY eventA;
INSERT INTO eventB (setA,setB)
SELECT GROUP_CONCAT(eventA) , eventB 
FROM casulaity 
GROUP BY eventB;
INSERT INTO xw (setA,setB)
SELECT eventA,eventB 
FROM casulaity;
INSERT INTO xw (setA,setB)
SELECT setA,setB 
FROM safeEventA;
INSERT INTO xw (setA,setB)
SELECT setA,setB 
FROM safeEventB;

DROP TABLE IF EXISTS yw;
DROP TABLE IF EXISTS eventAsafe;
DROP TABLE IF EXISTS eventBsafe;
DROP TABLE IF EXISTS temporary_table;
CALL explode_tableYW1(',');
CALL explode_tableYW(',');
CREATE TABLE eventASafe(setA varchar(255),setB varchar (255));
INSERT INTO eventASafe (setA,setB)
SELECT setA,setB from safeA;
CREATE TABLE eventBSafe(setA varchar(255),setB varchar (255));
INSERT INTO eventBSafe (setA,setB)
SELECT setA,setB FROM safeB;
CREATE TABLE yw(setA varchar(255) ,setB varchar (255), primary key(setA,setB));
CREATE TABLE temporary_table(setA varchar(255) ,setB varchar (255),primary key(setA,setB));
INSERT INTO temporary_table(setA,setB)
SELECT setA,setB FROM eventBsafe;
INSERT IGNORE INTO temporary_table(setA,setB)
SELECT setA,setB FROM eventAsafe;
INSERT IGNORE INTO yw (setA,setB)
SELECT eventA ,eventB 
FROM bpi_2014.casulaity
WHERE eventB  NOT IN(SELECT distinct setB FROM temporary_table) 
AND eventA  NOT IN (SELECT distinct  setA FROM temporary_table);
INSERT INTO yw (setA,setB)
SELECT setA ,setB 
FROM bpi_2014.safeEventA;
INSERT INTO yw (setA,setB)
SELECT setA ,setB 
FROM bpi_2014.safeEventB;

DROP TABLE IF EXISTS pw;
DROP TABLE IF EXISTS start;
CREATE TABLE pw(setA varchar(255));
CREATE TABLE start(event varchar(255));
INSERT INTO start Values ('i');
INSERT INTO start Values ('o');
INSERT INTO pw(setA)
SELECT CONCAT_WS(' & ',setA,SetB) AS place 
From yw;
INSERT INTO pw(setA)
SELECT event FROM start;

DROP TABLE IF EXISTS place1;
DROP TABLE IF EXISTS place2;
DROP TABLE IF EXISTS fw;
CREATE TABLE place1 (id varchar(200),value varchar(200));
INSERT INTO place1
SELECT yw.setA, CONCAT_WS(' & ',yw.setA,yw.setB) 
FROM yw;
CREATE TABLE place2(id varchar(200),value varchar(200));
INSERT INTO place2
SELECT CONCAT_WS(' & ',yw.setA,yw.setB),yw.setB 
FROM yw;
CALL explode_table_for_place2(',');
CALL explode_table_for_place1(',');
CREATE TABLE fw(setA varchar(200),setB varchar(200));
INSERT INTO fw
SELECT * FROM temp_place2;
INSERT INTO fw
SELECT * FROM temp_place1;
INSERT INTO fw(setA,setB)
SELECT S.event , I.InitialEvents 
FROM start AS S, InitialEvents AS I 
WHERE S.event='i';
INSERT INTO fw(setA,setB)
SELECT F.finalevents , S.event 
FROM start AS S, FinalEvents AS F 
WHERE S.event='o';

SELECT distinct * FROM fw;