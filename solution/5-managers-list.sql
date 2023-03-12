WITH RECURSIVE r(employee_id, manager_id, employee_name) AS (
        SELECT *
        FROM staff
        WHERE employee_id = 770 --choose employee id for showing managers list

    UNION ALL

        SELECT staff.employee_id, staff.manager_id, staff.employee_name
        FROM r INNER JOIN staff ON r.manager_id = staff.employee_id
        WHERE staff.employee_id != -1
)
SELECT * FROM r;
