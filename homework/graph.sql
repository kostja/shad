-- Homework by Dmitry Gusev
-- Gitlab login: pitamakan
-- PostgreSQL

DROP TABLE IF EXISTS graph;

CREATE TABLE graph
(
    idx       int UNIQUE,
    boss      int,
    full_name varchar
);

COPY graph FROM '/Users/pitamakan/mipt/shad/homework/graph.csv' DELIMITER ',' CSV;

-- 1 --
INSERT INTO graph (idx, boss, full_name)
VALUES ((SELECT MAX(idx) + 1 FROM graph), 1, 'Leeroy Jenkins');


-- 2 --
UPDATE graph SET boss = 23
WHERE idx = 52;


-- 3 --
(SELECT full_name,
        TRUE AS is_boss
 FROM graph
 WHERE idx = 218
 UNION
 SELECT full_name,
        FALSE AS is_boss
 FROM graph
 WHERE boss = 218)
    ORDER BY is_boss DESC;


-- 4 --
SELECT *
FROM (
    SELECT idx FROM graph
    EXCEPT
    SELECT boss FROM graph) AS a
    INNER JOIN graph AS b
    USING (idx);


-----
-- function used in many task
------

DROP FUNCTION IF EXISTS get_hierarchy(integer);

create or replace function get_hierarchy(person int)
  returns table (idx int, boss int, full_name text, subordinate int, depth int)
as
$body$
    SELECT
        idx,
        boss,
        full_name,
        subordinate,
        max_depth - depth + 1 as depth
    FROM (
        WITH RECURSIVE hierarchy AS (
     SELECT idx, boss, full_name, -1 as subordinate, 0 as depth
     FROM graph
     WHERE idx = person
     UNION ALL
     SELECT g.idx, g.boss, g.full_name, h.idx as subordinate, h.depth + 1 as depth
     FROM graph AS g
              JOIN hierarchy AS h ON g.idx = h.boss)
    SELECT *, max(depth) OVER () AS max_depth
    FROM hierarchy
    ) as a;
$body$
language sql;

-- 5 --
SELECT idx, boss, full_name
FROM get_hierarchy(3135);


-- 6 --
WITH RECURSIVE department AS (
     SELECT idx, boss
     FROM graph
     WHERE idx = 346
     UNION ALL
     SELECT g.idx, g.boss
     FROM graph AS g
              JOIN department AS h ON g.boss = h.idx)
SELECT count(idx)
FROM department;


-- 7 --

-- check none boss --
SELECT * FROM graph
WHERE boss is NULL;

-- check cycles --
DROP TABLE IF EXISTS cyclic;

CREATE TABLE cyclic (LIKE graph);
INSERT INTO cyclic
VALUES
    (1, 2, 'a'),
    (2, 3, 'b'),
    (3, 1, 'c')
;

WITH RECURSIVE cycles(idx, boss, full_name, path, cycle) AS (
     SELECT g.idx,
            g.boss,
            g.full_name,
            ARRAY [g.idx] AS path,
            FALSE
     FROM cyclic g
     UNION ALL
     SELECT g.idx,
            g.boss,
            g.full_name,
            path || g.idx,
            g.idx = ANY (path)
     FROM cyclic g,
          cycles sg
     WHERE g.idx = sg.boss
       AND NOT cycle)
SELECT *
FROM cycles
WHERE cycle;


-- 8 --
SELECT count(idx)
FROM get_hierarchy(3135);


-- 9 --
WITH RECURSIVE hierarchy AS (
     SELECT idx, boss, full_name, 0 as depth, '' as path
     FROM graph
     WHERE idx = 21411
UNION
     SELECT g.idx, g.boss, g.full_name, h.depth + 1 as depth, h.path || g.boss as path
     FROM graph AS g
              JOIN hierarchy AS h ON g.boss = h.idx)
SELECT  repeat('  ', depth) || full_name as full_name
FROM hierarchy
ORDER BY path;


-- 10 --
create or replace function get_connection(person1 int, person2 int)
    RETURNS TABLE(connection text)
as
$body$
    select (case when coalesce(a.full_name, '') != coalesce(b.full_name, '')
        then format('%25s | %25s', coalesce(a.full_name),  coalesce(b.full_name))
        else  format('            %25s              ', a.full_name)
        end)
    from get_hierarchy(person1) as a
    FULL OUTER JOIN
    get_hierarchy(person2) as b
    USING (depth)
    WHERE NOT (coalesce(a.idx, 0) = coalesce(b.idx, 0) and coalesce(a.subordinate) =  coalesce(b.subordinate));
$body$
language sql;

select * from get_connection(21411, 1241);

select * from get_connection(18943, 2);
