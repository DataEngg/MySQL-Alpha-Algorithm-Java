use dummy;
create table dum(A varchar(255),B varchar(255),C varchar(255));
insert into dum values ('A,B,C','D','A');
insert into dum values ('A,B,C','D','B');
insert into dum values ('A,B,C','D','C');
insert into dum values ('K,M','N','K');
insert into dum values ('K,M','N','M');
insert into dum values ('S','o,p,q',null);
insert into dum values ('R','M','R');
select C,concat_ws('|',A,B) from dum;
create table dumA(A varchar(255),B varchar(255));
insert into dumA values ('A,B,C','D');
insert into dumA values ('A,B,C','D');
insert into dumA values ('A,B,C','D');
insert into dumA values ('K,M','N');
insert into dumA values ('K,M','N');
insert into dumA values ('S','o,p,q');
insert into dumA values ('R','M');
use bpi_2013;
DROP table temp1;
DROP table temp2;
DROP table temp3;
DROP table temp4;
DROP table temp;
DROP table eventA;
DROP table eventB;
create table eventA(setA varchar(255),setB varchar(255));
create table eventB(setA varchar(255),setB varchar(255));
insert into eventA(setA,setB)
select eventA,group_concat(eventB) from casulaity group by eventA;
insert into eventB(setA,setB)
select group_concat(eventA),eventB from casulaity group by eventB;
select * from eventA;
select * from eventB;
create table temp(setA varchar(255),setB varchar (255));
Insert into temp (setA,setB)
select setA ,setB from eventA 
where setB  in(select distinct setB from eventB) and setA  in (select distinct setA from eventB);
create table temp1(setA varchar(255),setB varchar (255));
Insert into temp1 (setA,setB)
select setA ,setB from eventA 
where setB  not in(select distinct setB from temp) and setA not in (select distinct setA from temp);
select * from temp1;
select * from eventB;
create table temp2(setA varchar(255),setB varchar (255));
Insert into temp2 (setA,setB)
select temp1.setA,temp1.setB from temp1
join
eventB
where eventB.setB=temp1.setB;
create table temp3(setA varchar(255),setB varchar (255));
Insert into temp3 (setA,setB)
select eventB.setA,eventB.setB from eventB
join
temp1
where eventB.setA=temp1.setA;
create table temp4(setA varchar(255),setB varchar (255));
Insert into temp4 (setA,setB)
select setA ,setB from temp1 
where setB  not in(select distinct setB from temp2) and setA not in (select distinct setA from temp2);
insert into temp4(setA,setB)
select distinct setA ,setB from eventB 
where setB  not in(select distinct setB from temp3) and setA not in (select distinct setA from temp3);
select * from temp4;