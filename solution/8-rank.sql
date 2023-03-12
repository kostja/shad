WITH RECURSIVE r(employee_id, manager_id, employee_name, rank) AS (
        SELECT *, 1
        FROM staff
        WHERE employee_id = 770

    UNION ALL

        SELECT staff.employee_id, staff.manager_id, staff.employee_name, r.rank + 1
        FROM r INNER JOIN staff ON r.manager_id = staff.employee_id
        WHERE staff.employee_id != -1
), max_rank AS (SELECT max(rank) AS mr FROM r)
SELECT r.employee_id, r.manager_id, r.employee_name, (max_rank.mr - r.rank) AS rank
FROM r, max_rank
WHERE r.employee_id = 770
