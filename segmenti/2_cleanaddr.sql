DROP DATABASE IF EXISTS workaddr ;
CREATE DATABASE workaddr Default character set utf8 default collate utf8_unicode_ci;
use workaddr;

source procedure.sql;

CALL MAKE_SUPPORT_TABLES();
drop tables if exists base;
create table base like support; 
insert into base(id1,      id2,      val1,     val2,        val3, val4,val5) 
          select person_id,doc_sn_id,last_name,doc_std_name,city,address,city from itaappdb.TLS206_PERSON_FULL left join itaappdb.`TLS208_DOC_STD_NMS` on doc_sn_id=doc_std_name_id where ctry_code='IT';
alter table base add primary key person_id (id1);
select count(*) as base from base ;
-- erano 204154 sono 175107

drop tables if exists input;
insert into support(id1,val1,val2,val3) select id1,val1,val2,val3 from base ;
alter table support add primary key person_id (id1);
rename table support TO input;
select count(*) as input from base ;


create table comuni as select * from  ISTAT.COMUNI;

-- select val1,count(id1) from 04_persone_residui_parole group by val1 order by count(ID1) desc limit 100;
