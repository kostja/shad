CREATE TABLE staff (
    employee_id integer primary key,
    manager_id integer not null,
    employee_name varchar(100)
);

-- \copy staff FROM '/Users/sikalov/study/postgres-hw/graph.csv' WITH (FORMAT csv);
