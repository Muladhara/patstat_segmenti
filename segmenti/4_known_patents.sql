
DROP DATABASE IF EXISTS patstat_segmenti2012 ;
CREATE DATABASE patstat_segmenti2012 default character set utf8 default collate utf8_unicode_ci;

use patstat_segmenti2012;


drop table if exists segmenti1;
create table segmenti1
select a.id1 as person_id_k, b.person_id as person_id_l, a.id3 as _kind, b.location as prov from otherdb.patstat_names a left join otherdb.location b on a.id1 = b.person_id order by a.id1;
-- Query OK, 175104 rows affected (1.47 sec)
-- Records: 175104  Duplicates: 0  Warnings: 0

alter table segmenti1 add key p_id (person_id_k);
alter table segmenti1 add index p_id_l (person_id_l);


-- applicants italiani
drop table if exists ITA_APPLICANTS;
create table ITA_APPLICANTS as
select person_id from itaappdb.TLS206_PERSON_FULL where person_id in (select distinct(person_id) from itaappdb.TLS207_PERS_APPLN where applt_seq_nr>0) and ctry_code='IT';
ALTER TABLE `ITA_APPLICANTS` 
ADD PRIMARY KEY (`person_id`) ;
-- mysql>     -> Query OK, 53755 rows affected (1.92 sec)
-- Records: 53755  Duplicates: 0  Warnings: 0
-- mysql>     -> Query OK, 53755 rows affected (0.10 sec)
-- Records: 53755  Duplicates: 0  Warnings: 0


drop table if exists ITA_KNOWN_APPLICANTS;
create table ITA_KNOWN_APPLICANTS as
select person_id from patstat_segmenti.segmenti1 join ITA_APPLICANTS on person_id = person_id_k;
ALTER TABLE `ITA_KNOWN_APPLICANTS` 
ADD PRIMARY KEY (`person_id`) ;
-- Query OK, 53752 rows affected (0.14 sec)
-- Records: 53752  Duplicates: 0  Warnings: 0
-- mysql>     -> Query OK, 53752 rows affected (0.10 sec)
-- Records: 53752  Duplicates: 0  Warnings: 0




