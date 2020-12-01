--ASIGNMENT 4 (DATA WORKLOAD)

--1 query 2.8
alter system flush buffer_cache;
alter system flush shared_pool;
    
    select animals.animal_name, animals.age as animal_age, animals.domesticated, villagers.first_name as villager_name, 
    villagers.animal_id, villagers.age as villager_age from animals
        full outer join villagers on animals.animal_id = villagers.villager_id
        full outer join families on villagers.family_id = families.family_id
        full outer join housings on families.house_id = housings.house_id
            where competence_bonus > 0.5
            or villagers.school_id in (select villagers.competence from villagers join schools on villagers.school_id = schools.school_id);
 
--2 query  10
alter system flush buffer_cache;
alter system flush shared_pool;

select count(*) from (
select villagers.first_name, villagers.last_name, villagers.age as villagers_age, villagers.school_id,
    families.family_happiness, villagers.gender,
    housings.condition, cast ((housings.size_in_m2 / 100) as INT) as string_size from villagers
        full outer join families on villagers.family_id = families.family_id
        full outer join housings on families.house_id = housings.house_id
            where housings.village_id in (select villages.village_id from villages join housings on housings.village_id = villages.village_id)
            or housings.condition > 0.8);

--3 query 1.6
alter system flush buffer_cache;
alter system flush shared_pool;

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

-- 4 transaction 8,472
alter system flush buffer_cache;
alter system flush shared_pool;

update jobs
    set wage = 17
        where risk_factor > (select avg(risk_factor) from jobs) 
        or round(required_competence) >= (select round(avg(required_competence)) from jobs)
        or substr(job_name, 2,3) like '%ata%' and end > '04/20/1948'
        or job_id in (select job_id from locations
        where number_of_trees > (select avg(number_of_trees) from locations));
rollback;

-- 5 transaction 4,25
alter system flush buffer_cache;
alter system flush shared_pool;

delete from VILLAGERS
    where first_name = 'Burnard' and gender = 'female'
 or substr(last_name, 2,3) not like '%a' and hapiness < (select avg(hapiness) from villagers);

rollback;
    
-- 6 transaction 3,2
alter system flush buffer_cache;
alter system flush shared_pool;

update villages
    set popularity = (select avg(popularity) from villages
        where villages.village_id in (select villages.village_id from housings join villages on housings.village_id = villages.village_id)
        and substr(village_name, 2,3) like '%aji%' and hospital <> 'false'
        or location_id in (select location_id from locations
        where number_of_trees > (select avg(number_of_trees) from locations)));
        
rollback;



