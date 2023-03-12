-- Аналогично запросу 5-managers-list.sql и 8-rank.sql для каждого сотрудника
-- вычисляем путь до корня. Корень -- это самый верхнеуровневый руководитель,
-- то есть у которого manager_id == -1. Условием валидности нашей иерархии будет
-- в том, что для каждого сотрудника должен найтись такой путь, причем единственный.
-- Соотвественно в конце мы считаем, сколько образовалось валидных путей (= #строк с employee_id = 1).
-- Сравнием получившееся число с количество всех уникалных сотрудников.
-- Иерархия валидна, если эти числа совпадают.
WITH RECURSIVE r(employee_id, manager_id, employee_name) AS (
        SELECT *
        FROM staff

    UNION ALL

        SELECT staff.employee_id, staff.manager_id, staff.employee_name
        FROM r INNER JOIN staff ON r.manager_id = staff.employee_id
        WHERE staff.employee_id != -1
),
all_staff AS (SELECT COUNT(DISTINCT staff.employee_id) as all FROM staff),
all_correct AS (
    SELECT COUNT(*) AS correct_count
    FROM r, all_staff
    WHERE r.employee_id = 1
)
SELECT (CASE WHEN all_staff.all = all_correct.correct_count THEN 'true' ELSE 'false' END) AS is_valid
FROM all_staff, all_correct;

-- Анамалию на отсутствие руководителя можно запретить при правильном указании схемы таблицы,
-- то есть в нашем случае значение в колонке manager_id не может быть null.

-- Двойные подчинение можно проверить так (отсуствие строк == отсутсвию двойных подчинений)
SELECT COUNT(staff.manager_id) AS managers_count
FROM staff
GROUP BY staff.employee_id
HAVING COUNT(staff.manager_id) > 1;
