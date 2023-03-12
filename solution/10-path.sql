-- Если кратко описывать алгоритм, то происходит примерно следующее:
-- 1) выделяем пути до корня от обоих концов путей;
-- 2) выделяем между ними общую часть с помощью пересечения;
-- 3) объединяем строки двух путей до корня между собой и удаляем общую часть до корня;
-- 4) меняем местами eployee_id и manager_id во второй части пути (нисходящий путь);
-- 5) с помощью рекурсии упорядочиваем путь от начальной вершины до конечной, используя все ребра пути.
CREATE VIEW path_unordered AS (
    WITH RECURSIVE r1(employee_id, manager_id, employee_name) AS (
            SELECT *
            FROM staff
            WHERE employee_id = 770 --Начало пути
        UNION ALL
            SELECT staff.employee_id, staff.manager_id, staff.employee_name
            FROM r1 INNER JOIN staff ON r1.manager_id = staff.employee_id
            WHERE staff.employee_id != -1
    ), r2(employee_id, manager_id, employee_name) AS (
            SELECT *
            FROM staff
            WHERE employee_id = 990 --Конец пути
        UNION ALL
            SELECT staff.employee_id, staff.manager_id, staff.employee_name
            FROM r2 INNER JOIN staff ON r2.manager_id = staff.employee_id
            WHERE staff.employee_id != -1
    ), common_path AS (
        SELECT * FROM r1
        INTERSECT
        SELECT* FROM r2
    ), both_paths AS (
        SELECT * FROM r1
        UNION
        SELECT* FROM r2
    ), path_prepare AS (
        SELECT * FROM both_paths
        EXCEPT
        SELECT * FROM common_path
    )
    SELECT path_prepare.employee_id AS from_, path_prepare.manager_id AS to_
    FROM path_prepare INNER JOIN r1 ON path_prepare.employee_id = r1.employee_id
    UNION ALL
    SELECT path_prepare.manager_id AS from_, path_prepare.employee_id AS to_
    FROM path_prepare INNER JOIN r2 ON path_prepare.employee_id = r2.employee_id
);

WITH RECURSIVE r(from_, to_) AS (
        SELECT *
        FROM path_unordered
        WHERE from_ = 770 --Начало пути
    UNION ALL
        SELECT path_unordered.from_, path_unordered.to_
        FROM r INNER JOIN path_unordered ON r.to_ = path_unordered.from_
)
SELECT * FROM r;


