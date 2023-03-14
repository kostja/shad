-- 0. Create table and fill with data

CREATE TABLE employees (
    id SERIAL PRIMARY KEY NOT NULL,
    boss_id INT NOT NULL,
    name VARCHAR(50) NOT NULL
);

COPY employees (id, boss_id, name) FROM '<path-to-file>/graph.csv' DELIMITER ',' CSV;
SELECT setval('employees_id_seq', (SELECT MAX(id) FROM employees));


-- 1. Add employee
-- Vasya Pupkin with boss id = 10

INSERT INTO employees (name, boss_id) VALUES ('Vasya Pupkin', 10);


-- 2. Transfer employee
-- employee with id = 121 to boss with id = 20

UPDATE employees SET boss_id = 20 WHERE id = 121;


-- 3. Get department
-- department with boss id = 20
-- use 'ORDER BY' only if we want get the department boss first in list (could also be used UNION with two 'select where')

SELECT id, name FROM employees WHERE id = 20 OR boss_id = 20 ORDER BY boss_id = 20;


-- 4. Get all 'leaf' employees

SELECT boss.id, boss.boss_id, boss.name FROM employees AS boss LEFT JOIN employees AS sub ON sub.boss_id = boss.id WHERE sub.id is null;


-- 5. Show employees hierarchy
-- bosses hierarchy from employee with id = 20

WITH RECURSIVE sub AS (
    SELECT * FROM employees WHERE id = 20
  UNION ALL
    SELECT bosses.id, bosses.boss_id, bosses.name FROM sub JOIN employees AS bosses ON bosses.id = sub.boss_id
)
SELECT * FROM sub;


-- 6. Show employees count in department
-- department with boss id = 20

WITH RECURSIVE bosses AS (
    SELECT id FROM employees WHERE id = 20
  UNION ALL
    SELECT sub.id FROM bosses JOIN employees AS sub ON sub.boss_id = bosses.id
)
SELECT COUNT(id) FROM bosses;


-- 7. Validate employees records graph

-- Check that there is only one big boss

SELECT * FROM employees WHERE boss_id = -1;

-- Show not valid employees records

WITH RECURSIVE tree AS (
    SELECT * FROM employees WHERE boss_id = -1
  UNION ALL
    SELECT sub.id, sub.boss_id, sub.name FROM tree JOIN employees AS sub ON sub.boss_id = tree.id
)
SELECT * FROM employees
EXCEPT
SELECT * FROM tree;


-- 8. Get employee rank (the depth of subordination)
-- employee with id = 20

WITH RECURSIVE sub AS (
    SELECT id, boss_id FROM employees WHERE id = 20
  UNION ALL
    SELECT bosses.id, bosses.boss_id FROM sub JOIN employees AS bosses ON bosses.id = sub.boss_id
)
SELECT COUNT(id) - 1 FROM sub;


-- 9. Show employees hierarchy

WITH RECURSIVE bosses(id, name, boss_id, depth, bosses_id) AS (
    SELECT id, name, boss_id, 0, ARRAY[id]
    FROM employees
    WHERE boss_id = -1
  UNION ALL
    SELECT sub.id, sub.name, sub.boss_id, bosses.depth + 1, bosses.bosses_id || sub.id
    FROM bosses JOIN employees AS sub ON sub.boss_id = bosses.id
)
SELECT REPEAT(' ', depth) || id || ' ' || name || ' (both_id = ' || boss_id || ')'
FROM bosses
ORDER BY bosses_id;


-- 10. Get intermediary employees between two emloyees
-- employees between employee with id = 25 and employee with id = 20

WITH RECURSIVE
sub2(id, name, boss_id, step) AS (
    SELECT id, name, boss_id, 0
    FROM employees
    WHERE id = 20
  UNION ALL
    SELECT bosses.id, bosses.name, bosses.boss_id, sub2.step + 1
    FROM sub2 JOIN employees AS bosses ON bosses.id = sub2.boss_id
),
sub1(id, name, boss_id) AS (
    SELECT id, name, boss_id FROM employees WHERE id = 25
  UNION ALL
    SELECT bosses.id, bosses.name, bosses.boss_id
    FROM sub1 JOIN employees AS bosses ON bosses.id = sub1.boss_id
    WHERE NOT EXISTS (SELECT 1 FROM sub2 WHERE sub2.id = sub1.id)
)
SELECT * FROM sub1
UNION ALL
(
    SELECT id, name, boss_id
    FROM sub2
    WHERE step < (SELECT step FROM sub2 WHERE EXISTS (SELECT 1 FROM sub1 WHERE sub1.id = sub2.id)) ORDER BY step DESC
);
