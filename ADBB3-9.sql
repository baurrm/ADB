--ASSIGNMENT 9 (Partitions)

--1 Partition Hash
create table schools_hash (
    school_id number(38) not null,
    max_number_of_students number(38) not null,
    school_name varchar2 (128 byte),
    primary key (school_id)
)
partition by hash (school_id)
partitions 8;

drop table schools_hash;

select count(*) from schools_hash;

Insert into schools_hash select * from schools;
commit;

begin
DBMS_STATS.gather_table_stats('ADBB3', 'schools_hash');
end;

--2 Partition Range
create table housings_range (
    house_id number(38) not null,
    size_in_m2 number(38) not null,
    condition number(38,2) not null,
    age number(38) not null,
    village_id number(38) not null,
    primary key (house_id),
    foreign key (village_id) REFERENCES VILLAGES(village_id)
)
partition by range (condition)
(PARTITION p1 VALUES LESS THAN (0.25),
 PARTITION p2 VALUES LESS THAN (0.50),
 PARTITION p3 VALUES LESS THAN (0.75),
 PARTITION p4 VALUES LESS THAN (maxvalue));

drop table housings_range;

select count(*) from housings_range;

Insert into housings_range select * from housings;
commit;

begin
DBMS_STATS.gather_table_stats('ADBB3', 'housings_range');
end;

--3 Partition List
CREATE TABLE villagers_list
( VILLAGER_ID NUMBER(38,0) NOT NULL, 
	FIRST_NAME VARCHAR2(26 BYTE), 
	LAST_NAME VARCHAR2(26 BYTE), 
	GENDER VARCHAR2(26 BYTE), 
	AGE NUMBER(38,0), 
	COMPETENCE NUMBER(38,2), 
	HAPINESS NUMBER(38,2), 
	DISEASE_ID NUMBER(38,0), 
	ANIMAL_ID NUMBER(38,0), 
	FAMILY_ID NUMBER(38,0), 
	SCHOOL_ID NUMBER(38,0),
    PRIMARY KEY (VILLAGER_ID),
    FOREIGN KEY (DISEASE_ID) REFERENCES DISEASES(DISEASE_ID), 
    FOREIGN KEY (ANIMAL_ID) REFERENCES ANIMALS(ANIMAL_ID), 
    FOREIGN KEY (FAMILY_ID) REFERENCES FAMILIES(FAMILY_ID), 
    FOREIGN KEY (SCHOOL_ID) REFERENCES SCHOOLS(SCHOOL_ID)
)
PARTITION BY LIST (GENDER)
(   PARTITION MEN VALUES ('Male'), 
    PARTITION WOMEN VALUES ('Female')
);

drop table villagers_list;

select count(*) from villagers_list;

Insert into villagers_list select * from villagers;
commit;

begin
DBMS_STATS.gather_table_stats('ADBB3', 'villagers_list');
end;
--1 query 1 sec Partitioned
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for
    
select animals.animal_name, animals.age as animal_age, animals.domesticated, villagers_list.first_name as villager_name, 
villagers_list.animal_id, villagers_list.age as villager_age from animals
    full outer join villagers_list on animals.animal_id = villagers_list.villager_id
    full outer join families on villagers_list.family_id = families.family_id
    full outer join housings_range on families.house_id = housings_range.house_id
        where competence_bonus > 0.5
        or villagers_list.school_id in (select villagers_list.competence from villagers_list join schools_hash on villagers_list.school_id = schools_hash.school_id);
        
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
--1 query 1.5 Simple
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for
    
select animals.animal_name, animals.age as animal_age, animals.domesticated, villagers.first_name as villager_name, 
villagers.animal_id, villagers.age as villager_age from animals
    full outer join villagers on animals.animal_id = villagers.villager_id
    full outer join families on villagers.family_id = families.family_id
    full outer join housings on families.house_id = housings.house_id
        where competence_bonus > 0.5
        or villagers.school_id in (select villagers.competence from villagers join schools on villagers.school_id = schools.school_id);
        
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

--2 query 10 > 0.2 Indexes > 9.5 Partitioned
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

select count(*) from (
select villagers_list.first_name, villagers_list.last_name, villagers_list.age as villagers_age, villagers_list.school_id,
    families.family_happiness, villagers_list.gender,
    housings_range.condition, cast ((housings_range.size_in_m2 / 100) as INT) as string_size from villagers_list
        full outer join families on villagers_list.family_id = families.family_id
        full outer join housings_range on families.house_id = housings_range.house_id
            where housings_range.village_id in (select villages.village_id from villages join housings_range on housings_range.village_id = villages.village_id)
            or housings_range.condition > 0.8);
            
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

--2 query 10.8 simple
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

select count(*) from (
select villagers.first_name, villagers.last_name, villagers.age as villagers_age, villagers.school_id,
    families.family_happiness, villagers.gender,
    housings.condition, cast ((housings.size_in_m2 / 100) as INT) as string_size from villagers
        full outer join families on villagers.family_id = families.family_id
        full outer join housings on families.house_id = housings.house_id
            where housings.village_id in (select villages.village_id from villages join housings on housings.village_id = villages.village_id)
            or housings.condition > 0.8);
            
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
--3 query 1.2 Partitioned 
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for 

select count(*) from (
select * from villagers_list
    full outer join animals on animals.animal_id = villagers_list.animal_id
    full outer join families on villagers_list.family_id = families.family_id
    full outer join housings_range on housings_range.house_id = families.house_id
    full outer join villages on villages.village_id = housings_range.village_id
    full outer join locations on locations.location_id = villages.location_id
        where animals.animal_id in 
            (select animals.animal_id from animals where animals.animal_id in
                (select animal_id from animals where animal_name like 'Gol%' and (substr(animal_name, 7,2) like 'ey')))
                or villagers_list.school_id is not null 
                or villages.popularity > 0.5
                or locations.condition >= (select avg(condition) from locations));
                
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

--3 query 1.6 simple > 1,168 Indexes
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for 

select count(*) from (
select * from villagers
    full outer join animals on animals.animal_id = villagers.animal_id
    full outer join families on villagers.family_id = families.family_id
    full outer join housings on housings.house_id = families.house_id
    full outer join villages on villages.village_id = housings.village_id
    full outer join locations on locations.location_id = villages.location_id
        where animals.animal_id in 
            (select animals.animal_id from animals where animals.animal_id in
                (select animal_id from animals where animal_name like 'Gol%' and (substr(animal_name, 7,2) like 'ey')))
                or villagers.school_id is not null 
                or villages.popularity > 0.5
                or locations.condition >= (select avg(condition) from locations));
                
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- 4 transaction 8,472 > 3 Indexes > 
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

update jobs
    set wage = 17
        where risk_factor > (select avg(risk_factor) from jobs) 
        or round(required_competence) >= (select round(avg(required_competence)) from jobs)
        or substr(job_name, 2,3) like '%ata%' and end > '04/20/1948'
        or job_id in (select job_id from locations
        where number_of_trees > (select avg(number_of_trees) from locations));
        
rollback;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- 5 transaction Partitioned 2.1
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

delete from villagers_list
    where first_name = 'Burnard' and gender = 'female'
 or substr(last_name, 2,3) not like '%a' and hapiness < (select avg(hapiness) from villagers_list);

rollback;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- 5 transaction Simple 4,25  
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

delete from VILLAGERS
    where first_name = 'Burnard' and gender = 'female'
 or substr(last_name, 2,3) not like '%a' and hapiness < (select avg(hapiness) from villagers);

rollback;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

-- 6 transaction Partitioned 2.9
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

update villages
    set popularity = (select avg(popularity) from villages
        where villages.village_id in (select villages.village_id from housings_range join villages on housings_range.village_id = villages.village_id)
        and substr(village_name, 2,3) like '%aji%' and hospital <> 'false'
        or location_id in (select location_id from locations
        where number_of_trees > (select avg(number_of_trees) from locations)));
        
rollback;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
    
-- 6 transaction Simple 3,2 
alter system flush buffer_cache;
alter system flush shared_pool;

explain plan for

update villages
    set popularity = (select avg(popularity) from villages
        where villages.village_id in (select villages.village_id from housings join villages on housings.village_id = villages.village_id)
        and substr(village_name, 2,3) like '%aji%' and hospital <> 'false'
        or location_id in (select location_id from locations
        where number_of_trees > (select avg(number_of_trees) from locations)));
        
rollback;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);