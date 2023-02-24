CREATE TABLE workers (
    id         SERIAL PRIMARY KEY,
    parent_id  INT,
    "name"     TEXT
);

COPY workers (id, parent_id, "name") from '/var/lib/postgresql/data/graph/graph.csv' CSV;

-- Correcting current value of the primary key sequence.
SELECT setval('workers_id_seq', (SELECT MAX(id) FROM workers));

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

-- 1) Procedure for worker addition.
CREATE OR REPLACE FUNCTION add_worker(parent_id INT, "name" TEXT)
RETURNS void AS
$BODY$
BEGIN
    INSERT INTO workers VALUES (nextval('workers_id_seq'), add_worker.parent_id, add_worker.name);
    RAISE NOTICE 'Added worker % with id % and parent_id %', "name", currval('workers_id_seq'), parent_id;
END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION sql_add_worker(parent_id INT, "name" TEXT)
RETURNS void AS
$BODY$
    INSERT INTO workers VALUES (nextval('workers_id_seq'), sql_add_worker.parent_id, sql_add_worker.name);
$BODY$
LANGUAGE SQL;
    

-- 2) Procedure for changing the "department" of the worker. Since we store
-- everything in one table this translates to simply changing the parent of the
-- worker.
CREATE FUNCTION change_worker_parent(worker_id INT, new_parent_id INT)
RETURNS void AS
$BODY$
BEGIN
    UPDATE workers SET workers.parent_id = new_parent_id WHERE workers.id = worker_id;
    RAISE NOTICE 'Changed parent to % for worker %', new_parent_id, worker_id;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 3) Printing out workers from the "department", which is determined by head's id.
CREATE FUNCTION print_dpt_workers(dpt_id INT)
RETURNS void AS 
$BODY$
DECLARE
    parent_name TEXT;
    parent_id INT;
    len INT;
    worker_names TEXT[];
    worker_ids INT[];
BEGIN
    SELECT workers.name, workers.id INTO parent_name, parent_id FROM workers WHERE workers.id = dpt_id;
    RAISE INFO 'Department head is %(%)', parent_name, parent_id;

    worker_names := ARRAY(SELECT workers.name FROM workers WHERE workers.parent_id = dpt_id);
    worker_ids := ARRAY(SELECT workers.id FROM workers WHERE workers.parent_id = dpt_id);
    len := array_length(worker_names, 1);
    RAISE INFO 'Workers are:';
    WHILE len > 0 LOOP
        RAISE INFO '%(%)', worker_names[len], worker_ids[len];
        len := len - 1;
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 4) Returning a table with all workers lacking subordinates. Here it makes sense to specifically return a table
-- instead of printing since there is quite a number of leaf workers.
CREATE FUNCTION leaf_workers()
RETURNS TABLE(id INT, "name" TEXT) AS
$BODY$
BEGIN
    RETURN QUERY
    WITH worker_ids_with_subs AS (
        SELECT DISTINCT ON (workers.parent_id)
            workers.parent_id AS id 
        FROM 
            workers
    )
    SELECT 
        workers.id, 
        workers.name 
    FROM 
        workers 
    WHERE NOT EXISTS (
        SELECT 
            * 
        FROM 
            worker_ids_with_subs 
        WHERE 
            workers.id = worker_ids_with_subs.id
    );
END;
$BODY$
LANGUAGE 'plpgsql';

-- 5) Finding a subordination chain for an worker - a sequence of bosses all the way up to -1.
-- Simple recursive approach with exit condition on parent_id == -1.
CREATE FUNCTION print_command_chain(worker_id INT)
RETURNS void AS
$BODY$
DECLARE
    current_id INT;
    parent_id INT;
    worker_name TEXT;
BEGIN
    SELECT 
        workers.id, 
        workers.parent_id, 
        workers.name 
    INTO 
        current_id, 
        parent_id, 
        worker_name 
    FROM 
        workers 
    WHERE 
        workers.id = worker_id;

    RAISE INFO '%(%)', worker_name, worker_id;
    IF parent_id <> -1 THEN
        PERFORM print_command_chain(parent_id); -- PERFORM is needed because of the void return type.
    END IF;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 6.a) Printing out the amount of people in the department + all subordinate departments. Simply calling 
-- the function recursively for each subordinate worker in a department and aggregating the result.
CREATE FUNCTION dpt_size(dpt_id INT)
RETURNS INT AS
$BODY$
DECLARE
    amount INT;
    len INT;
    sub_worker_ids INT[];
BEGIN
    amount := 1;
    sub_worker_ids := ARRAY(SELECT workers.id FROM workers WHERE workers.parent_id = dpt_id);
    len := array_length(sub_worker_ids, 1);
    WHILE len > 0 LOOP
        amount := amount + dpt_size(sub_worker_ids[len]);
        len := len - 1;
    END LOOP;
    RETURN amount;
END;
$BODY$
LANGUAGE 'plpgsql';


-- 6.b) Returning a table with people in the department + all subordinate departments. Recursive CTE solution here.
CREATE FUNCTION dpt_workers_rec(dpt_id INT)
RETURNS TABLE(id INT, parent_id INT, "name" TEXT) AS
$BODY$
BEGIN
    RETURN QUERY
    WITH RECURSIVE dpt_workers AS (
	SELECT
		workers.id,
		workers.parent_id,
		workers.name
	FROM
		workers
	WHERE
		workers.id = dpt_id
	UNION
        SELECT
            workers.id,
            workers.parent_id,
            workers.name
        FROM
            workers
        INNER JOIN dpt_workers ON dpt_workers.id = workers.parent_id
    ) SELECT
        *
    FROM
        dpt_workers;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 7) Checking for 2 anomalies - invalid parent and several trees. No check for cycles :(
CREATE FUNCTION check_for_anomalies()
RETURNS void AS
$BODY$
DECLARE
    num_workers INT;
    invalid_parent_ids INT[];
    num_workers_reached INT;    
BEGIN
    SELECT count(*)
    INTO num_workers
    FROM workers;

    -- Checking invalid parent relations with SELECT *** WHERE NOT EXISTS (select for parent in workers).
    invalid_parent_ids := ARRAY(
        SELECT 
            l_workers.parent_id
        FROM 
            workers AS l_workers
        LEFT JOIN 
            workers AS r_workers ON l_workers.parent_id = r_workers.id
        WHERE 
            r_workers.id IS NULL
    );

    IF array_length(invalid_parent_ids, 1) <> 1 THEN -- one person has parent_id == -1
        RAISE INFO 'ANOMALY - there are bad parent ids: %', invalid_parent_ids;
    ELSE
        RAISE INFo 'OK - parent ids';
    END IF;

    -- Checking for several trees by seeing how many workers we can reach from worker with id 1.
    WITH RECURSIVE reached_workers AS (
        SELECT
            workers.id,
            workers.parent_id
        FROM
            workers
        WHERE
            workers.id = 1
        UNION
            SELECT
                workers.id,
                workers.parent_id
            FROM
                workers
            INNER JOIN reached_workers ON reached_workers.id = workers.parent_id
    ) SELECT
        count(*)
    INTO
        num_workers_reached
    FROM
        reached_workers;

    IF num_workers <> num_workers_reached THEN
        RAISE INFO 'ANOMALY - there are several trees';
    ELSE
        RAISE INFO 'OK - single tree';
    END IF;
END;
$BODY$
LANGUAGE 'plpgsql';


-- 8) Print out the rank of the worker - number of levels below the worker, including his own.
-- Calculating the rank by iteratively trying to add all of the subordinates to temp table, so on
-- the first iteration we add people directly subordinate to the worker and then we start to add people,
-- which are subordinate to the added ones as well. When table stops changing, it means that we reached
-- all the leafs and rank then is the amount of iteration done.
-- PS: number of bosses above the worker can be obtained from 5) with minimal changes.
CREATE FUNCTION print_rank(worker_id INT)
RETURNS void AS
$BODY$
DECLARE
    rank INT;
    prev_count INT;
    cur_count INT;
    count_changed BOOLEAN;
BEGIN
    rank := 1;    

    CREATE TEMP TABLE IF NOT EXISTS agg_subs (id SERIAL PRIMARY KEY);
    TRUNCATE TABLE agg_subs; -- cleaning the table between invocations
    -- "INSERT INTO" specifically so agg_subs is correctly processed in function scope.
    INSERT INTO agg_subs VALUES (worker_id); 
    prev_count := 1;
    cur_count := 1;
    count_changed := true;

    -- While loop instead of recursive CTE, because I couldn't think of a way to increment rank and define a
    -- stopping condition in a CTE.
    WHILE count_changed LOOP
        RAISE INFO 'Rank is at least %', rank; -- left in to provide at least some feel for the speed of iteration.
        -- SELECT INTO has different semantics in plpgsql, so using INSERT INTO.
        INSERT INTO agg_subs
        SELECT
            workers.id
        FROM 
            agg_subs
        INNER JOIN 
            workers
        ON 
            agg_subs.id = workers.parent_id OR agg_subs.id = workers.id
        ON CONFLICT
            DO NOTHING;

        SELECT count(*) INTO cur_count FROM agg_subs;

        IF cur_count = prev_count THEN
            count_changed := false; -- exiting the loop
        ELSE
            rank := rank + 1; -- increasing the rank
            prev_count := cur_count;
        END IF;
    END LOOP;

    RAISE INFO 'Rank of worker % is %', worker_id, rank;    
END;
$BODY$
LANGUAGE 'plpgsql';

/*
-- 9) 
CREATE FUNCTION fn()
RETURNS *** AS
$BODY$
BEGIN
    
END;
$BODY$
LANGUAGE 'plpgsql';
*/

CREATE OR REPLACE FUNCTION paths(src_id INT, dst_id INT)
RETURNS void AS
$BODY$
DECLARE
dst_reached BOOLEAN;
len INT;
path_ids INT[];
BEGIN
    dst_reached := false;
    path_ids := {dst_id};

    CREATE TEMP TABLE IF NOT EXISTS reached_workers (prev_id INT, id SERIAL PRIMARY KEY);
    TRUNCATE TABLE reached_workers;
    INSERT INTO reached_workers VALUES (-1, src_id);

    WHILE NOT dst_reached LOOP
        INSERT INTO reached_workers
        SELECT
            (CASE WHEN reached_workers.id = workers.parent_id
                THEN reached_workers.id ELSE reached_workers.id END) AS prev_id,
            (CASE WHEN reached_workers.id = workers.parent_id
                THEN workers.id ELSE workers.parent_id END) AS id
        FROM
            reached_workers
        INNER JOIN workers 
        ON 
            reached_workers.id = workers.parent_id -- reaching child
            OR 
            reached_workers.id = workers.id -- reaching parent
        ON CONFLICT
            DO NOTHING;

        IF exists (select * from reached_workers where reached_workers.id = dst_id) THEN
            dst_reached := true;
        END IF;
    END LOOP;

    -- Using arrays allows to get a deteminitsin ordering in contrast to joins in recursive
    -- CTE, where ordering is not enforced(?)
    WHILE path_ids[array_length(path_ids, 1)] <> src_id LOOP
        array_append(
            path_ids, 
            SELECT 
                reached_workers.prev_id 
            FROM 
                reached_workers 
            WHERE 
                reached_workers.id = path_ids[array_length(path_ids, 1)]
        );
    END LOOP;

    len := array_length(path_ids, 1);
    WHILE len > 0 LOOP
        RAISE INFO '%', path_ids[i];
        len := len - 1;
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 10) Print out the path between 2 workers, defined by their ids.
-- To do this we effectively treat our tree as bidirected graph and doing breadth first search by iteratively
-- appending ids, which we can reach to the ones that we reached until we meet dst_id in a way similar to 7).
CREATE FUNCTION print_shortest_path(src_id INT, dst_id INT)
RETURNS void AS
$BODY$
DECLARE
len INT;
path_id_arr INT[];
BEGIN
    path_ids := ARRAY(
        WITH RECURSIVE reached_workers AS (
            SELECT
                -1 AS prev_id,
                workers.id AS id
            FROM
                workers
            WHERE
                workers.id = src_id
            UNION
                SELECT 
                    (CASE WHEN reached_workers.id = workers.parent_id
                        THEN workers.parent_id ELSE reached_workers.id END) AS prev_id,
                    (CASE WHEN reached_workers.id = workers.parent_id
                        THEN workers.id ELSE workers.parent_id END) AS id
                FROM
                    workers
                INNER JOIN reached_workers 
                ON 
                    (reached_workers.id = workers.parent_id OR reached_workers.id = workers.id) 
                    AND 
                    (reached_workers.prev_id != workers.id)
                WHERE dst_id NOT IN (reached_workers.id)
        ), path_ids AS (
            SELECT
                dst_id as id
            FROM 
                reached_workers
            UNION
                SELECT 
                    reached_workers.prev_id AS id
                FROM 
                    reached_workers
                INNER JOIN path_ids ON path_ids.id = reached_workers.id
                WHERE src_id NOT IN (path_ids.id)
        ) SELECT * FROM path_ids
    );

    len := array_length(path_id_arr, 1);
    WHILE len > 0 LOOP
        RAISE INFO '%', path_id_arr[len];
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';

WITH RECURSIVE reached_workers AS (
    SELECT
        -1 AS prev_id,
        workers.id AS id
    FROM
        workers
    WHERE
        workers.id = 11
    UNION
        SELECT
            (CASE WHEN reached_workers.id = workers.parent_id
                THEN reached_workers.id ELSE reached_workers.id END) AS prev_id,
            (CASE WHEN reached_workers.id = workers.parent_id
                THEN workers.id ELSE workers.parent_id END) AS id
        FROM
            reached_workers
        INNER JOIN workers 
        ON 
            reached_workers.id = workers.parent_id -- reaching child
            OR 
            reached_workers.id = workers.id -- reaching parent
        WHERE 9 NOT IN (reached_workers.id)
) select * from reached_workers;