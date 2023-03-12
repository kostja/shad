drop table if exists employees cascade;

create table if not exists employees( 
	id serial not null primary key, 
	supervisor_id int not null,
	name text not null
);

create or replace procedure load_data()
as 
$$
	begin
		copy employees FROM '/Users/Shared/graph.csv' delimiter ',' csv;
		select setval(pg_get_serial_sequence('employees', 'id'), coalesce((select max(id)+1 FROM employees), 1), false);
	end;
$$
language plpgsql;

-- task 1
create or replace procedure add_employee(supervisor_id int, name text)
as
$$
	begin 
		insert into employees(supervisor_id, name) values ($1, $2);
	end;
$$
language plpgsql;

create or replace procedure add_employee_with_id(employee_id int, supervisor_id int, name text)
as
$$
	begin 
		insert into employees(id, supervisor_id, name) values ($1, $2, $3);
	end;
$$
language plpgsql;


-- task 2
create or replace procedure move_employee(employee_id int, new_supervisor_id int) 
as 
$$
	begin 
		update employees
		set supervisor_id = $2
		where id = $1;
	end;
$$
language plpgsql;

-- task 3 
create or replace function get_department(employee_id int) 
returns setof employees 
as 
$$
declare 
	r employees%rowtype;
begin 
	for r in 
		select * from employees where id = employee_id or supervisor_id = employee_id
	loop 
		return next r;
	end loop;
end;
$$
language plpgsql;


-- task 4
create or replace function get_leaf_employees() 
returns setof employees 
as 
$$
	declare
    	r employees%rowtype;
	begin
		for r in 
			select e1.id, e1.supervisor_id, e1.name
			from 
				employees as e1 
				left join 
				employees as e2
				on e1.id = e2.supervisor_id
			where e2.id is null
		loop 
			return next r;
		end loop;
	end;
$$
language plpgsql;


-- task 5
create or replace function get_supervisor_chain(employee_id int)
returns setof employees
as 
$$
declare
    r employees%rowtype;
begin 
	for r in 
		with recursive supervisor_list(e_id) as (
			select employee_id as e_id
			union all 
			select * from (
				with prev_supervisor as (
					select * from supervisor_list
				)
				select supervisor_id as e_id from employees where id = (select * from prev_supervisor) and supervisor_id != -1
			)e_id
		)
		select e.id, e.supervisor_id, e.name
		from 
			employees as e 
			join 
			supervisor_list 
			on e.id = supervisor_list.e_id
	loop 
		return next r;
	end loop;
end;
$$
language plpgsql;

-- task 6 
create or replace function get_full_department_num(employee_id int) 
returns int
as
$$
begin 
	return (
		with recursive department_id(e_id) as (
			select employee_id as e_id 
			union all 
			select * from (
				with previous_layer as (
					select * from department_id
				)
				select e.id as e_id 
				from 
					employees as e 
					join 
					previous_layer as pl 
					on e.supervisor_id = pl.e_id
			)e_id
		) 
		select count(e_id) from department_id
		);
end;
$$
language plpgsql;


-- task 7 
create or replace function check_table() 
returns bool 
as 
$$
begin 
	return (
		with recursive one_boss_check(res) as (
			select count(id) = 1 from employees where supervisor_id = -1
		), correct_references(res) as (
			select count(e1.id) = (select count(id) - 1 from employees) 
			from 
			employees as e1 
			inner join 
			employees as e2 
			on e1.id = e2.supervisor_id
		), reachable_employees(id) as (
			select e.id from employees as e where supervisor_id = -1
			union all 
			select e.id 
			from 
				employees as e 
				inner join 
				reachable_employees as re 
				on e.supervisor_id = re.id
        -- in our case cycles are possible if and only if there are employees that cannot be reached from employyes who do not have supervisor
		), no_cycle_check as ( 
			select count(e.id) = (select count(id) from employees) 
			from 
				employees as e 
				inner join 
				reachable_employees as re 
				on e.id = re.id
		)
		select 
			(select * from one_boss_check) and 
			(select * from correct_references) and 
			(select * from no_cycle_check)
	);
end;
$$
language plpgsql;


-- task 8
create or replace function get_employee_rank(employee_id int) 
returns int 
as 
$$
begin 
	return (select count(id) from get_supervisor_chain(employee_id));
end;
$$
language plpgsql;


-- task 9 
create or replace function pretty_view() 
returns text
as 
$$ 
begin 
	return (
		with recursive employee_with_rank(id, name, trace, rank) as (
			select e.id, e.name, e.id::text, 1 from employees as e where supervisor_id = -1
			union all 
			select e.id, e.name, concat(ewr.trace, '.', e.id::text), ewr.rank + 1 
			from 
				employees as e 
				inner join 
				employee_with_rank as ewr 
				on e.supervisor_id = ewr.id
		)
 		select string_agg(concat(repeat(' ', ewr.rank - 1), ewr.name), '\n' order by ewr.trace) 
 		from employee_with_rank as ewr
	);
end;
$$
language plpgsql;


-- task 10
create or replace function get_route(employee_id_1 int, employee_id_2 int)
returns setof employees 
as 
$$ 
declare 
	r employees%rowtype;
begin 
	for r in 
		with route_e_1 as (
			select * from get_supervisor_chain(employee_id_1)
		), route_e_2 as (
			select * from get_supervisor_chain(employee_id_2)
		), route_without_lca as (
			(select * from route_e_1 
			except 
			select * from route_e_2)
			union 
			(select * from route_e_2
			except 
			select * from route_e_1)
		), lca as (
			select e.id, e.supervisor_id, e.name 
			from 
				employees as e 
				join 
				route_without_lca as rwl 
				on e.id = rwl.supervisor_id
		)
		select * from route_without_lca
		union 
		select * from lca
	loop 
		return next r;
	end loop;
end;
$$
language plpgsql;