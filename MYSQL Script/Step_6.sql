use bpi_2013;

create table pw(setA varchar(255));

create table start(event varchar(255));

insert into start values ('i');
insert into start values ('o');

insert into pw(setA)
select concat_ws(' & ',setA,SetB) as place from temp;

insert into pw(setA)
select * from start;
