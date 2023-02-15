-- schema
drop table employee;
create table employee(
    id          serial primary key,
    manager_id  int,
    name        text
);

copy employee(id, manager_id, name)
from '/home/nick/Projects/databases/shad/homework/graph.csv'
delimiter ',';

-- 1
select max(id) from employee; -- to get current max id

insert into employee(id, manager_id, name) values
    (1240604, -11, 'Nick Berezikov');

-- 2
update employee
set manager_id = 1
where id = 12;

-- 3
with manager as (select 1)
select * from employee where id = (select * from manager)
union all
select * from employee where manager_id = (select * from manager);

-- 4
select id from employee
except
select manager_id from employee;

-- 5
with recursive subordination_list as (
    select * from employee where id = 1000
    union all
    select e.id, e.manager_id, e.name from employee e, subordination_list sl
    where e.id = sl.manager_id
)
select * from subordination_list;

-- 6
with recursive subordination_list as (
    select * from employee where id = 1
    union all
    select e.id, e.manager_id, e.name from employee e, subordination_list sl
    where e.manager_id = sl.id
)
select count(id) as dep_size from subordination_list;

-- 7
create view subordinate_manager as
select e1.id as id, e2.id as manager_id
from employee e1 left outer join employee e2 on e1.manager_id = e2.id;

-- absence of a manager
select * from subordinate_manager
where manager_id is null;

-- absence of multiple managers
select id from subordinate_manager
group by id
having count(manager_id) > 2;


-- 8
with recursive subordination_list as (
    select * from employee where id = 19550
    union all
    select e.id, e.manager_id, e.name from employee e, subordination_list sl
    where e.id = sl.manager_id
)
select count(id) - 1 as rank from subordination_list;


-- 10
with recursive emps as (select 1000 as emp1, 1023 as emp2),
subordination_list1 as (
    select * from employee where id = (select emp1 from emps)
    union all
    select e.id, e.manager_id, e.name from employee e, subordination_list1 sl
    where e.id = sl.manager_id
), subordination_list2 as (
    select * from employee where id = (select emp2 from emps)
    union all
    select e.id, e.manager_id, e.name from employee e, subordination_list2 sl
    where e.id = sl.manager_id
)
select distinct employee.id, employee.name from(
    (select * from subordination_list1
     except
     select * from subordination_list2)
    union
    (select * from subordination_list2
     except
     select * from subordination_list1)
) path join employee on path.manager_id = employee.id
where employee.id != (select emp1 from emps) and
      employee.id != (select emp2 from emps);





