CREATE TABLE employees
(
    id           serial PRIMARY KEY,
    "supervisor" INT     NOT NULL,
    "name"       varchar NOT NULL
);

COPY employees
    FROM
    PROGRAM 'curl "https://raw.githubusercontent.com/kostja/shad/main/homework/graph.csv"' DELIMITER ',' CSV;

SELECT *
FROM employees
LIMIT 10;

SELECT setval('employees_id_seq', COALESCE((SELECT MAX(id) + 1
                                            FROM employees), 1), FALSE);

-- Problem 1--
INSERT INTO employees
VALUES (DEFAULT, 1, 'John Allen');


/* id 1240604 assigned*/
-- Problem 2 (Update a department) --
CREATE PROCEDURE change_supervisor(id integer, new_supervisor integer)
    LANGUAGE sql
AS
$$
UPDATE
    employees
SET supervisor = $2
WHERE id = $1
$$;

-- CALL change_supervisor(1240604, 2);

-- SELECT * FROM employees where id = 1240604;
-- Problem 3 (Print a department) --
CREATE OR REPLACE FUNCTION select_department(super int)
    RETURNS SETOF employees
    LANGUAGE SQL
AS
$$
SELECT *
FROM employees
WHERE supervisor = super
   OR id = super;
$$;

SELECT *
FROM
    select_department(1);

-- Problem 4 (Print Leafs)
CREATE OR REPLACE FUNCTION find_leafs()
    RETURNS SETOF employees
    LANGUAGE sql
AS
$$
SELECT e.*
FROM employees AS e
         LEFT JOIN employees e2 ON e.id = e2.supervisor
WHERE e2.supervisor IS NULL
$$;

SELECT *
FROM
    find_leafs();

-- Problem 5 (Print bossess)
CREATE OR REPLACE FUNCTION get_bosses(employee_id int)
    RETURNS TABLE
            (
                supervisor_id INT,
                supervisor_name VARCHAR
            )
    LANGUAGE SQL
AS
$$
WITH RECURSIVE supervisors (
                            eid,
                            height,
                            supervisor
    ) AS (SELECT id,
                 0,
                 supervisor
          FROM employees
          WHERE id = employee_id
          UNION ALL
          SELECT e.id,
                 height + 1,
                 e.supervisor
          FROM supervisors
                   JOIN employees AS e ON supervisors.supervisor = e.id)
SELECT id, name
FROM supervisors,employees WHERE supervisors.eid = id AND height > 0 ORDER BY height;
$$;

DROP function get_bosses(employee_id int);

SELECT *
FROM
    get_bosses(14);

-- Problem 6 --
WITH RECURSIVE indirect_supervisors (
                                     eid,
                                     supervisor
    ) AS (SELECT id,
                 supervisor
          FROM employees
          UNION ALL
          SELECT i.eid,
                 e.supervisor
          FROM indirect_supervisors AS i
                   JOIN employees AS e ON i.supervisor = e.id)
SELECT sq.supervisor,
       count(sq.eid) AS subordinates
FROM indirect_supervisors AS sq
GROUP BY supervisor
ORDER BY subordinates DESC;

-- Problem 7 --
/* Let's verify, that we have only one CEO node with -1 supervisor,
 check that we have no loops, and that BFS from CEO
 reaches all employees once
 */
WITH RECURSIVE bfs_from_ceo (
                             id
    ) AS (SELECT id
          FROM employees
          WHERE supervisor = - 1
          UNION ALL
          SELECT e.id
          FROM employees AS e
                   JOIN bfs_from_ceo AS bfs ON e.supervisor = bfs.id)
                   CYCLE id SET is_cycle USING cycle_path
SELECT CASE
           WHEN (SELECT COUNT(DISTINCT id)
                 FROM bfs_from_ceo) = (SELECT COUNT(id)
                                       FROM employees) AND (SELECT COUNT(id)
                                                            FROM employees
                                                            WHERE supervisor = - 1) = 1 AND (SELECT COUNT(id)
                                                                                             FROM bfs_from_ceo
                                                                                             WHERE is_cycle) = 0 THEN
               'valid'
           ELSE
               'invalid'
           END;

-- Problem 8 (employee ranks) --
WITH RECURSIVE dfs_from_ceo (
                             id,
                             depth
    ) AS (SELECT id,
                 0
          FROM employees
          WHERE supervisor = - 1
          UNION ALL
          SELECT e.id,
                 bfs.depth + 1
          FROM employees AS e
                   JOIN dfs_from_ceo AS bfs ON e.supervisor = bfs.id)
SELECT *
FROM dfs_from_ceo
ORDER BY (depth,
          id);

-- Problem 9 (Print hierarchy)
WITH RECURSIVE dfs_from_ceo (
                             id,
                             depth,
                             path
    ) AS (SELECT id,
                 0,
                 ARRAY [id]
          FROM employees
          WHERE supervisor = - 1
          UNION ALL
          SELECT e.id,
                 bfs.depth + 1,
                 bfs.path || e.id
          FROM employees AS e
                   JOIN dfs_from_ceo AS bfs ON e.supervisor = bfs.id)
SELECT concat(repeat(text ' ', g.depth), e.name)
    /*, g.depth, path --For debug--*/
FROM dfs_from_ceo AS g,
     employees AS e
WHERE g.id = e.id
ORDER BY path;


-- Problem 10 (Construct a path ) --
CREATE OR REPLACE FUNCTION find_path(from_id INT, to_id int) RETURNS varchar
    LANGUAGE sql
AS
$$
WITH RECURSIVE edges (
                      id,
                      parent,
                      origin
    ) AS (SELECT id,
                 supervisor,
                 id
          FROM employees
          WHERE id = from_id
             OR id = to_id
          UNION ALL
          SELECT e.id, e.supervisor, edges.origin
          FROM employees as e,
               edges
          WHERE edges.parent = e.id)
   , uniq (id, parent) AS (SELECT id, parent
                           from edges
                           GROUP BY (id, parent)
                           HAVING COUNT(id) = 1)
   , unique_with_origin (id, parent, origin) as (SELECT edges.*
                                                 from edges
                                                          RIGHT JOIN uniq on edges.id = uniq.id AND edges.parent = uniq.parent)
   , unique_right_order(from_, to_) AS (SELECT id, parent
                                        FROM unique_with_origin
                                        WHERE origin = from_id
                                        UNION ALL
                                        SELECT parent, id
                                        FROM unique_with_origin
                                        WHERE origin = to_id)
   , count_steps(from_, to_, length_, full_path) AS (SELECT from_, to_, 0, concat(e1.name, concat('->', e2.name))
                                                     FROM unique_right_order,
                                                          employees as e1, employees as e2
                                                     WHERE from_ = from_id
                                                       AND e1.id = from_id
                                                       AND e2.id = e1.supervisor

                                                     UNION ALL
                                                     SELECT u.from_,
                                                            u.to_,
                                                            c.length_ + 1,
                                                            concat(c.full_path, concat('->', name))
                                                     FROM count_steps as c,
                                                          unique_right_order as u,
                                                          employees
                                                     WHERE c.to_ = u.from_
                                                       AND employees.id = u.to_)
SELECT full_path
FROM count_steps
ORDER BY length_ DESC
LIMIT 1;
$$;



SELECT * FROM find_path(14, 15);

