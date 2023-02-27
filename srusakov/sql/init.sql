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
    INSERT INTO workers VALUES (nextval('workers_id_seq'), add_worker.parent_id, add_worker.name);
$BODY$
LANGUAGE SQL;
    

-- 2) Меняем отдел работника, что в данной структуре соответствует просто смене родителя.
CREATE OR REPLACE FUNCTION change_worker_parent(worker_id INT, new_parent_id INT)
RETURNS void AS
$BODY$
    UPDATE workers SET parent_id = new_parent_id WHERE id = worker_id;
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

-- 5.a) Печатаем цепочку подчинения.
CREATE OR REPLACE FUNCTION command_chain(worker_id INT)
RETURNS TABLE(chain TEXT) AS
$BODY$
    WITH RECURSIVE chain_agg AS (
        SELECT 
            workers.id AS id, workers.parent_id AS parent_id, CAST(worker_id AS TEXT) AS chain
        FROM 
            workers
        WHERE 
            workers.id = worker_id
        UNION
            SELECT
                workers.id, 
                workers.parent_id, 
                (
                    CASE WHEN workers.parent_id <> -1 THEN 
                        chain || ' -> ' || CAST(workers.parent_id AS TEXT)
                    ELSE 
                        chain
                    END
                )
            FROM
                chain_agg
            INNER JOIN
                workers
            ON
                chain_agg.parent_id = workers.id AND workers.id <> -1

    ) SELECT chain FROM chain_agg WHERE parent_id = -1;
$BODY$
LANGUAGE SQL;

-- 5.b) Печатаем цепочку подчинения. PL/pgSQL версия.
CREATE OR REPLACE FUNCTION command_chain_cheat(worker_id INT)
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
            INNER JOIN 
                dpt_workers 
            ON 
                dpt_workers.id = workers.parent_id
    ) SELECT
        *
    FROM
        dpt_workers;
$BODY$
LANGUAGE SQL;

-- Проверяем 2 аномалии - плохие родители и больше чем одну компоненту. Выводим плохих родителей,
-- если есть и кол-во достигнутых людей по сравнению с общим. Сделано через 2 функции.
-- Нет явной проверки на циклы.
-- 7.a.1) Выводит -1, если все хорошо.
CREATE OR REPLACE FUNCTION check_for_anomalies_parent()
RETURNS TABLE(id INT) AS
$BODY$
    -- Проверяем плохих родителей с помощью SELECT *** WHERE NOT EXISTS (select for parent in workers).
    SELECT 
        l_workers.parent_id
    FROM 
        workers AS l_workers
    LEFT JOIN 
        workers AS r_workers ON l_workers.parent_id = r_workers.id
    WHERE 
        r_workers.id IS NULL;
$BODY$
LANGUAGE SQL;

-- 7.a.2) Выводит 2 одинаковых числа, если все хорошо.
CREATE OR REPLACE FUNCTION check_for_anomalies_component()
RETURNS TABLE(r_count INT, i_count INT) AS
$BODY$
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
        count(reached_workers.id), count(workers.id)
    FROM
        reached_workers
    RIGHT JOIN
        workers
    ON reached_workers.id = workers.id;
$BODY$
LANGUAGE SQL;

-- 7.b) Проверяем 2 аномалии - плохие родители и больше чем одну компоненту. Циклы не осилил, хотя
-- успешная проверка на компоненту в том виде, в котором она здесь сделана говорит, что и циклов нет.
-- PL/pgSQL версия.
CREATE OR REPLACE FUNCTION check_for_anomalies_cheat()
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


-- 8.a) Печатаем ранг работника - набираем в рекурсивном CTE подвластных работников до момента, когда
-- не можем, на каждом юнионе запоминаем, на каком именно новый был добавлен + 1. В итоге ранг - это
-- номер последнего юниона.
CREATE OR REPLACE FUNCTION rank(worker_id INT)
RETURNS TABLE(rank INT) AS
$BODY$
    WITH RECURSIVE rank_agg AS (
        SELECT worker_id AS id, 1 AS rank
        UNION
            SELECT 
                workers.id, rank + 1
            FROM
                rank_agg
            INNER JOIN
                workers
            ON
                rank_agg.id = workers.parent_id
    ) SELECT max(rank) FROM rank_agg;
$BODY$
LANGUAGE SQL;

-- 8.b) Печатаем ранг работника - итеративно добавляем подвластных работников в табличку пока
-- не обнаруживаем, что никого больше не добавили, после чего выводим ранг как кол-во итераций.
-- PL/pgSQL версия.
CREATE OR REPLACE FUNCTION rank_cheat(worker_id INT)
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

-- 9.a) Печатаем иерархию отдела. Добавляем работников рекурсивным CTE и при добавлении проставляем им
-- некоторый сортировочный ключ, первая часть которого наследуется от начальника. Таким образом при конечной
-- сортировке они будут идти сразу после своего начальника. Величина отступа, в свою очередь, определяется
-- длиной этого ключа.
CREATE OR REPLACE FUNCTION hierarchy(dpt_id INT)
RETURNS TABLE(name_line TEXT) AS
$BODY$
    WITH RECURSIVE name_agg AS (
        SELECT
            workers.id AS id,
            workers.name AS name_line,
            '' AS sort_str
        FROM
            workers
        WHERE
            workers.id = dpt_id
        UNION
            SELECT
                num_workers.id,
                repeat('   ', length(name_agg.sort_str) + 1) || num_workers.name,
                name_agg.sort_str || chr(cast(num_workers.num % 256 AS INT))
            FROM (
                SELECT 
                    workers.id, 
                    workers.parent_id, 
                    workers.name, 
                    ROW_NUMBER () OVER () AS num
                FROM workers
            ) AS num_workers
            INNER JOIN
                name_agg 
            ON name_agg.id = num_workers.parent_id
    ) SELECT name_line FROM name_agg ORDER BY sort_str;
$BODY$
LANGUAGE SQL;

-- 9.b) Печатаем иерархию отдела. Dыполняется, конечно, миллиард лет :(
-- PL/pgSQL версия.
CREATE OR REPLACE FUNCTION hierarchy_cheat(dpt_id INT, depth INT DEFAULT 0)
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

-- 10.a) Печатаем наикратчайший путь между двумя работниками. Работаем в предположении, что у вершин
-- есть общий родитель и в графе нет циклов.
-- Работаем следующим образом - строим две таблички со всеми путями от источника и цели до их родителей,
-- записывая длину и путь. После этого делаем джойн на них, пересекая по айдишнику концов путей и сортируем
-- по сумме путей до этого айдишника. Путь через наименьшего общего руководителя тогда - это комбинация 
-- путей до этого айдишника от источника + путь от айдишника до таргета(разница в направлениях отражается
-- в CTE, где добавляем к пути слева или справа).
-- Звездочка для супер явного указания, что является наименьшим руководителем, но можно легко убрать.
CREATE OR REPLACE FUNCTION shortest_path(src_id INT, dst_id INT)
RETURNS TABLE(shortest_path TEXT) AS
$BODY$
    WITH RECURSIVE src_half_path AS (
        SELECT
            src_id AS id,
            CAST(src_id AS TEXT) AS "path",
            0 AS len
        UNION
            SELECT 
                workers.parent_id,
                src_half_path.path || '->' || CAST(workers.parent_id AS TEXT),
                len + 1
            FROM 
                src_half_path
            INNER JOIN
                workers
            ON
                src_half_path.id = workers.id
    ), dst_half_path AS (
        SELECT
            dst_id AS id,
            CAST(dst_id AS TEXT) AS "path",
            0 AS len
        UNION
            SELECT 
                workers.parent_id,
                CAST(workers.parent_id AS TEXT) || '->' || dst_half_path.path,
                len + 1
            FROM 
                dst_half_path
            INNER JOIN
                workers
            ON
                dst_half_path.id = workers.id
    ) SELECT
            src_half_path.path || ' * ' || dst_half_path.path
        FROM 
            src_half_path 
        INNER JOIN 
            dst_half_path
        ON
            src_half_path.id = dst_half_path.id
        ORDER BY 
            src_half_path.len + dst_half_path.len
        LIMIT 1;
$BODY$
LANGUAGE SQL;

-- 10.b) Печатаем наикратчайший путь между двумя работниками.
-- PL/pgSQL версия.
CREATE OR REPLACE FUNCTION shortest_path_cheat(src_id INT, dst_id INT)
RETURNS void AS
$BODY$
DECLARE
dst_reached BOOLEAN;
temp_id INT;
len INT;
path_ids INT[];
BEGIN
    dst_reached := false;
    path_ids := ARRAY[dst_id];

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

    -- Здесь массив, чтобы обеспечить правильный порядок, поскольку джойны вроде не обязаны 
    -- соблюдать какие-либо порядки строк(?).
    WHILE path_ids[array_length(path_ids, 1)] <> src_id LOOP
        SELECT 
            reached_workers.prev_id 
        FROM 
            reached_workers
        INTO 
            temp_id
        WHERE 
            reached_workers.id = path_ids[array_length(path_ids, 1)];

        path_ids = array_append(path_ids, temp_id);
    END LOOP;

    len := array_length(path_ids, 1);
    WHILE len > 0 LOOP
        RAISE INFO '%', path_ids[len];
        len := len - 1;
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';
