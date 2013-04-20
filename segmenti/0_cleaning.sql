DROP DATABASE IF EXISTS work ;
CREATE DATABASE work Default character set utf8 default collate utf8_unicode_ci;
use work;

source procedure.sql;

CALL MAKE_SUPPORT_TABLES();

drop tables if exists base;
create table base like support; 
insert into base(id1,      id2,      val1,     val2,        val3) 
          select person_id,doc_sn_id,last_name,doc_std_name,address from itaappdb.TLS206_PERSON_FULL left join itaappdb.`TLS208_DOC_STD_NMS` on doc_sn_id=doc_std_name_id where ctry_code='IT';
alter table base add primary key person_id (id1);
select count(*) as base from base ;
-- erano 204154 sono 175107

drop tables if exists input;
insert into support(id1,val1,val2,val3) select id1,val1,val2,val3 from base ;
alter table support add primary key person_id (id1);
rename table support TO input;
select count(*) as input from base ;


create table names as select * from  otherdb.names;
create table surnames as select  * from  otherdb.surnames;


drop table if exists cebi_names;
create table cebi_names
select distinct NOME from otherdb.cebi where DATA_NASCITA!=0;
drop table if exists cebi_surnames;
create table cebi_surnames
select distinct COGNOME from otherdb.cebi where DATA_NASCITA!=0;

-- truncate names;
insert into names(name)
select * from cebi_names;

drop table if exists temp;
create table temp as select distinct name from names order by length(name) desc,name asc;
drop table names;
rename table temp to names;
drop table if exists temp;
create table temp as select distinct length(name),name from names order by length(name) desc,name asc;
drop table names;
rename table temp to names;
alter table names add primary key (name);

-- truncate surnames;
insert into surnames(name)
select * from cebi_surnames;

drop table if exists temp;
create table temp as select distinct name from surnames order by length(name) desc, name asc;
drop table surnames;
rename table temp to surnames;
drop table if exists temp;
create table temp as select distinct length(name),name from surnames  where length(name)<12 order by length(name) desc, name asc;
drop table surnames;
rename table temp to surnames;
alter table surnames add primary key (name);


-- select val1,count(id1) from 04_persone_residui_parole group by val1 order by count(ID1) desc limit 100;
