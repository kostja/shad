SELECT A.employee_id, A.employee_name
FROM staff AS A LEFT JOIN staff AS B ON A.employee_id = B.manager_id
WHERE B.employee_id is NULL
ORDER BY A.employee_id asc;
