
CALL explode_table(',');
CALL explode_table1(',');

drop table if exists bpi_2013.temp6;
drop table if exists bpi_2013.temp7;
drop table if exists bpi_2013.temp8;
drop table if exists bpi_2013.temp5;
drop table if exists bpi_2013.matched;
drop table if exists bpi_2013.temp9;
drop table if exists bpi_2013.temp10;
drop table if exists bpi_2013.matched1;
drop table if exists bpi_2013.temp11;
drop table if exists bpi_2013.temp12;
drop table if exists bpi_2013.temp13;

create table bpi_2013.matched1(setA varchar (255),setB varchar(255));
select * from tablelast;
select * from tablelast1;
Insert into bpi_2013.matched1(setA,setB)
select tablelast.id,tablelast.value from tablelast join tablelast1 where tablelast.id=tablelast1.id and tablelast.value=tablelast1.value;

create table bpi_2013.matched(setA varchar (255),setB varchar(255));
Insert into bpi_2013.matched(setA,setB)
select matched1.setA,matched1.setB from matched1 join eventB where eventB.setA=matched1.setA and eventB.setB=matched1.setB;

create table bpi_2013.temp5(setA varchar (255),setB varchar(255));

Insert into bpi_2013.temp5(setA,setB)
select distinct tablelast.id,tablelast.value from tablelast join temp4 where tablelast.id= temp4.setA and tablelast.value=temp4.setB;

create table bpi_2013.temp9(setA varchar (255),setB varchar(255));

Insert into bpi_2013.temp9(setA,setB)
select tablelast.id,tablelast.value from tablelast where tablelast.id not in(select setA from temp5);

create table bpi_2013.temp7(setA varchar (255),setB varchar(255));

Insert into bpi_2013.temp7(setA,setB)
select tablelast.id,tablelast.value from tablelast where tablelast.value not in(select setB from temp5);

Insert into temp9(setA,setB)
select * from matched; 

select * from temp9;

create table bpi_2013.temp6(setA varchar (255),setB varchar(255));

Insert into bpi_2013.temp6(setA,setB)
select distinct tablelast1.id,tablelast1.value from tablelast1 join temp4 where tablelast1.id= temp4.setA and tablelast1.value=temp4.setB;

create table bpi_2013.temp10(setA varchar (255),setB varchar(255));
Insert into bpi_2013.temp10(setA,setB)
select tablelast1.id,tablelast1.value from tablelast1 where tablelast1.value not in(select setB from temp6);
create table bpi_2013.temp8(setA varchar (255),setB varchar(255));
Insert into bpi_2013.temp8(setA,setB)
select tablelast1.id,tablelast1.value from tablelast1 where tablelast1.id not in(select setA from temp6);
insert into temp10(setA,setB)
select * from matched; 

create table temp11(setA varchar(255),setB varchar(255));
insert into temp11(setA,setB)
select setA ,setB from temp9
union 
select setA,setB from temp10;

create table temp12(setA varchar(255),setB varchar(255));
insert into temp12(setA,setB)
select temp11.setA,concat_ws(' & ',temp4.setA,temp4.setB) as setB from temp4 join temp11 where temp4.setB=temp11.setB
UNION 
select concat_ws(' & ',temp4.setA,temp4.setB) as setA,temp11.setB from temp4 join temp11 where temp4.setA=temp11.setA;

create table temp13(setA varchar(255),setB varchar(255));
insert into temp13(setA,setB)
select setA ,concat_ws(' & ',setA,setB) as setB from temp1 
where setB  not in(select distinct setB from temp2) and setA not in (select distinct setA from temp2)
UNION 
select concat_ws(' & ',setA,setB),setB from eventB 
where setB  not in(select distinct setB from temp3) and setA not in (select distinct setA from temp3); 

insert into temp12(setA,setB)
select setA,setB 
from temp13;

insert into temp12(setA,setB)
select event,initial from start,initial where start.event='i';

insert into temp12(setA,setB)
select final,event from start,final where start.event='o';

select * from temp12;