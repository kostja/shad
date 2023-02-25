CREATE TABLE workers (
    id         SERIAL PRIMARY KEY,
    parent_id  INT,
    "name"     TEXT
);

COPY workers (id, parent_id, "name") from '/var/lib/postgresql/data/graph/graph.csv' CSV;

SELECT setval('workers_id_seq', (SELECT MAX(id) FROM workers));

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

-- 1) Добавляем работника.
CREATE OR REPLACE FUNCTION add_worker(parent_id INT, "name" TEXT)
RETURNS void AS
$BODY$
    INSERT INTO workers VALUES (nextval('workers_id_seq'), sql_add_worker.parent_id, sql_add_worker.name);
$BODY$
LANGUAGE SQL;
    

-- 2) Меняем отдел работника, что в данной структуре соответствует просто смене родителя.
CREATE OR REPLACE FUNCTION change_worker_parent(worker_id INT, new_parent_id INT)
RETURNS void AS
$BODY$
    UPDATE workers SET workers.parent_id = new_parent_id WHERE workers.id = worker_id;
$BODY$
LANGUAGE SQL;

-- 3) Собираем работников отдела, определяющегося айдишником руководителя.
CREATE OR REPLACE FUNCTION dpt_workers(dpt_id INT)
RETURNS TABLE("name" TEXT, id INT) AS 
$BODY$
    SELECT 
        workers.name, workers.id 
    FROM 
        workers 
    WHERE 
        workers.id = dpt_id OR workers.parent_id = dpt_id;
$BODY$
LANGUAGE SQL;

-- 4) Возвращаем табличку с работниками без подчиненных.
CREATE OR REPLACE FUNCTION leaf_workers()
RETURNS TABLE(id INT, "name" TEXT) AS
$BODY$
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
$BODY$
LANGUAGE SQL;

-- 5) Печатаем цепочку подчинения.
CREATE OR REPLACE FUNCTION print_command_chain(worker_id INT)
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
        PERFORM print_command_chain(parent_id); -- PERFORM потому что void.
    END IF;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 6) Возвращаем табличку со всеми работниками отдела, включая подотделы.
CREATE OR REPLACE FUNCTION dpt_workers_rec(dpt_id INT)
RETURNS TABLE(id INT, parent_id INT, "name" TEXT) AS
$BODY$
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
$BODY$
LANGUAGE SQL;

-- 7) Проверяем 2 аномалии - плохие родители и больше чем одну компоненту. Циклы не осилил, хотя
-- успешная проверка на компоненту в том виде, в котором она здесь сделана говорит, что и циклов нет.
CREATE OR REPLACE FUNCTION check_for_anomalies()
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

    -- Проверяем плохих родителей с помощью SELECT *** WHERE NOT EXISTS (select for parent in workers).
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

    IF array_length(invalid_parent_ids, 1) <> 1 THEN -- один работник имеет -1 родителя
        RAISE INFO 'ANOMALY - there are bad parent ids: %', invalid_parent_ids;
    ELSE
        RAISE INFo 'OK - parent ids';
    END IF;

    -- Проверяем на компоненту, считая сколько людей можно достичь из работника с id = 1.
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


-- 8) Печатаем ранг работника - итеративно добавляем подвластных работников в табличку пока
-- не обнаруживаем, что никого больше не добавили, после чего выводим ранг как кол-во итераций.
CREATE OR REPLACE FUNCTION print_rank(worker_id INT)
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
    TRUNCATE TABLE agg_subs; -- чистим между вызовами
    -- "INSERT INTO" для того, чтобы не было жалоб из-за скоупа.
    INSERT INTO agg_subs VALUES (worker_id); 
    prev_count := 1;
    cur_count := 1;
    count_changed := true;

    -- Итеарация вместо рекурсивного CTE, потому что не осилил как вставить нормальное условие
    -- на остановку.
    WHILE count_changed LOOP
        RAISE INFO 'Rank is at least %', rank; -- оставил, чтобы можно было хоть немного понимать работает или нет.
        -- SELECT INTO отличается по функционалу в plpgsql, так что INSERT INTO.
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
            count_changed := false; -- выходим из цикла
        ELSE
            rank := rank + 1;
            prev_count := cur_count;
        END IF;
    END LOOP;

    RAISE INFO 'Rank of worker % is %', worker_id, rank;    
END;
$BODY$
LANGUAGE 'plpgsql';

-- 9) Печатаем иерархию отдела. Здесь концептульано даже не понимаю, как сделать это на
-- чистом SQL, потому что надо что-то содержательное печатать. Но выполняется, конечно, миллиард лет :(
CREATE OR REPLACE FUNCTION print_hierarchy(dpt_id INT, depth INT DEFAULT 0)
RETURNS void AS
$BODY$
DECLARE
prefix TEXT;
len INT;
cur_name TEXT;
cur_worker_ids INT[];
i INT;
BEGIN
    prefix := '';
    i := 0;
    WHILE i < depth LOOP
        prefix := prefix || '  ';
        i := i + 1;
    END LOOP;

    SELECT "name" INTO cur_name FROM workers WHERE workers.id = dpt_id;

    RAISE INFO '% %', prefix, cur_name;

    cur_worker_ids := ARRAY(
        SELECT workers.id FROM workers WHERE workers.parent_id = dpt_id
    );

    len := array_length(cur_worker_ids, 1);
    WHILE len > 0 AND len IS NOT NULL LOOP
        PERFORM print_hierarchy(cur_worker_ids[len], depth + 1);
        len := len - 1;
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';

-- 10) Печатаем наикратчайший путь между двумя работниками.
CREATE OR REPLACE FUNCTION print_shortest_path(src_id INT, dst_id INT)
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

    -- Это просто поиск в ширину.
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
            reached_workers.id = workers.parent_id -- можем достичь ребенка
            OR 
            reached_workers.id = workers.id -- можем достичь предка
        ON CONFLICT
            DO NOTHING;

        IF exists (select * from reached_workers where reached_workers.id = dst_id) THEN
            dst_reached := true;
        END IF;
    END LOOP;

    -- Здесь массив, чтобы обеспечить правильный порядок. Джойны вроде не обязаны соблюдать
    -- какие-либо порядки строк(?).
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
