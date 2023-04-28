--Create db
--Usage CALL create_table()
CREATE OR REPLACE PROCEDURE create_table()
LANGUAGE SQL
AS $$
create table workers(id int, prev int, name varchar(100), PRIMARY KEY (id));
$$;

--Load from csv
CREATE OR REPLACE PROCEDURE load_table()
LANGUAGE SQL AS $$
COPY workers(id, prev, name)
FROM 'graph.csv'
DELIMITER ',';
$$;

--First task
--Usage CALL add_workers
CREATE OR REPLACE PROCEDURE add_workers(id int, prev int, name varchar(100))
 AS $$

BEGIN
    INSERT INTO workers VAlUES (id, prev, name);
END;
$$
LANGUAGE plpgsql;

--Second task
--Usage CALL reset_boss
CREATE OR REPLACE PROCEDURE reset_boss(worker_id int, new_boss_id int)
 AS $$
DECLARE
    worker_name varchar(100);
BEGIN	
    worker_name := (SELECT name FROM workers WHERE id = worker_id);
    DELETE FROM workers WHERE id = worker_id;
    INSERT INTO workers VAlUES (worker_id, new_boss_id, worker_name);
END;
$$
LANGUAGE plpgsql;

--Third task
--Usage select * from  view_command(1);

CREATE OR REPLACE FUNCTION view_command(boss_id int) 
RETURNS table(worker_name varchar(100), worker_status varchar(100)) 
 AS $$
      SELECT name, CASE WHEN id=boss_id THEN 'BOSS'
                 		     ELSE 'WORKER'
                                END AS status
      FROM workers
      WHERE id = boss_id OR prev = boss_id          
      ORDER BY status;
$$ LANGUAGE sql;

--Four task
--Usage select * from view_empty();
CREATE OR REPLACE FUNCTION view_empty()
RETURNS table(worker_id int, worker_name varchar(100))
 AS $$
      WITH not_list_nodes AS (
	SELECT prev FROM workers
        GROUP BY prev 
      )
      SELECT id, name FROM workers LEFT JOIN not_list_nodes ON id = not_list_nodes.prev
      WHERE not_list_nodes.prev IS NULL;
$$ LANGUAGE sql;

--Five task
--Usage select * from get_bosses();
CREATE OR REPLACE FUNCTION get_bosses_alternative(boss_id int)
RETURNS table(worker_id int, worker_name varchar(100))
 AS $$
WITH RECURSIVE bosses AS (
  SELECT
      id, prev, name, 0 AS level
  FROM workers 
  WHERE id = (SELECT prev FROM workers WHERE id = boss_id)
  UNION
      SELECT
         w.id, w.prev, w.name, level + 1
      FROM workers as w  
      INNER JOIN bosses as b
      ON w.id = b.prev
)
SELECT id, name FROM bosses
ORDER BY level;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_bosses(boss_id int)
RETURNS table(worker_id int, worker_name varchar(100))
 AS $$
WITH RECURSIVE bosses AS (
  SELECT
      id, prev, name, 0 AS level
  FROM workers
  WHERE id = boss_id
  UNION
      SELECT
         w.id, w.prev, w.name, level + 1
      FROM workers as w
      INNER JOIN bosses as b
      ON w.id = b.prev
)
SELECT id, name FROM bosses
ORDER BY level;
$$ LANGUAGE sql;

--Six task
CREATE OR REPLACE FUNCTION get_workers_count(boss_id int)
RETURNS table(workers_count int)
 AS $$
WITH RECURSIVE boss_workers AS (
  SELECT boss_id as id
  UNION
      SELECT
         w.id
      FROM workers as w
      INNER JOIN boss_workers as b
      ON w.prev = b.id
)
SELECT COUNT(*) FROM boss_workers;
$$ LANGUAGE sql;

--Seven task
CREATE OR REPLACE PROCEDURE check_workers()
 AS $$
BEGIN
   IF ((SELECT COUNT(*) FROM workers WHERE prev = -1) > 1 OR (((SELECT COUNT(*) FROM workers WHERE prev = -1) = 0) AND ((SELECT COUNT(*) FROM workers) > 0))) 
   THEN
      RAISE EXCEPTION 'cannot have a multiple or zero count of start vertex';
   END IF;
   IF (WITH RECURSIVE counter(id) AS (
   	SELECT id  FROM workers 
        WHERE prev = -1
  	UNION
	   SELECT w.id
           FROM counter as c INNER JOIN workers as w 
	   ON c.id = w.prev
   )
   SELECT COUNT(id) FROM counter) !=
   (
      SELECT COUNT(id) FROM workers
   ) THEN
	RAISE EXCEPTION 'Not connected graph';
   END IF;
   RAISE NOTICE 'Good graph!';
END;
$$
LANGUAGE plpgsql;

--Eight task
CREATE OR REPLACE FUNCTION get_rank(boss_id int)
RETURNS table(worker_rank int)
 AS $$
WITH RECURSIVE bosses AS (
  SELECT
      id, prev, name, 1 AS level
  FROM workers
  WHERE id = boss_id
  UNION
      SELECT
         w.id, w.prev, w.name, level + 1
      FROM workers as w
      INNER JOIN bosses as b
      ON w.id = b.prev
)
SELECT MAX(level) FROM bosses;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_deep_rank(boss_id int)
RETURNS table(worker_rank int)
 AS $$
WITH RECURSIVE bosses AS (
  SELECT
      boss_id as id, -1, 1 AS level
  UNION
      SELECT
         w.id, w.prev, level + 1
      FROM workers as w
      INNER JOIN bosses as b
      ON w.prev = b.id
)
SELECT MAX(level) FROM bosses;
$$ LANGUAGE sql;

--Nine task

CREATE OR REPLACE FUNCTION get_grpaph_view()
RETURNS table(string_format varchar(400)) AS $$
WITH RECURSIVE id_depth AS (
   SELECT id, name, 0 as depth, '' as step FROM workers
   WHERE prev = -1
   UNION
      SELECT w.id, w.name, i.depth + 1, step || '    ' FROM 
      workers as w
      INNER JOIN id_depth as i
      ON w.prev = i.id
)
SELECT step || name from id_depth
ORDER BY depth;
$$ LANGUAGE sql;

--Ten task
CREATE OR REPLACE FUNCTION get_path(from_id int, to_id int)
RETURNS table(paht_worker_id int, path_worker_name varchar(100))
 AS $$
WITH RECURSIVE from_path AS (
  SELECT
      id, prev, name, 0 AS level
  FROM workers
  WHERE id = from_id
  UNION
      SELECT
         w.id, w.prev, w.name, level + 1
      FROM workers as w
      INNER JOIN from_path as b
      ON w.id = b.prev
),
to_path AS (
  SELECT
      id, prev, name, 0 AS level
  FROM workers
  WHERE id = to_id
  UNION
      SELECT
         w.id, w.prev, w.name, level + 1
      FROM workers as w
      INNER JOIN to_path as b
      ON w.id = b.prev
),
first_parent AS (
  SELECT f.id, f.name FROM from_path AS f INNER JOIN to_path AS t ON f.id = t.id
  ORDER BY f.level
  LIMIT 1
),
from_without_to AS (
   SELECT t.id, t.name FROM to_path AS t LEFT JOIN from_path AS f ON t.id = f.id
   WHERE f.id IS NULL
   ORDER BY t.level DESC
),
to_without_from AS (
   SELECT f.id, f.name FROM to_path AS t RIGHT JOIN from_path AS f ON t.id = f.id
   WHERE t.id IS NULL
   ORDER BY f.level
)
SELECT id, name FROM to_without_from
UNION ALL 
SELECT id, name FROM first_parent
UNION ALL
SELECT id, name FROM from_without_to;
$$ LANGUAGE sql;
