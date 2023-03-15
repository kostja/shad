-- Для решения был выбран postgres

-- 0. Создать таблицу

CREATE TABLE "staff" (
  "id" integer NOT NULL,
  "parent_id" integer NOT NULL,
  "name" text NOT NULL
);

-- 1. Добавить сотрудника.
-- INSERT INTO staff VALUES(<employee_id>, <chief_id>, <name>);
-- Например
INSERT INTO staff VALUES(3,2,'Julie Mcduffy');

-- 2. Перевести сотрудника из отдела в отдел. В случае перевода руководителя,
-- переводятся все его подчинённые.
-- UPDATE staff SET parent_id = <new_chief_id> WHERE id = <employee_id>
-- Например
UPDATE staff SET parent_id = 3 WHERE id = 5


-- 3. Вывести отдел - начальник, все непосредственные подчинённые
-- SELECT * FROM staff WHERE id = <chief_id> OR parent_id = <chief_id>;
-- Например
SELECT * FROM staff WHERE id = 4 OR parent_id = 4;

-- 4. Вывести список всех "листовых" узлов дерева (сотрудники не имеющие
-- подчинённых)
SELECT * FROM staff WHERE id NOT IN (SELECT parent_id FROM staff);


-- 5. Вывести список подчинения - руководитель, руководитель руководителя,
-- и т.д. до вершины иерархии
-- WITH RECURSIVE r AS (
--     SELECT * FROM staff WHERE id = <employee_id>
--     UNION
--     SELECT staff.id, staff.parent_id, staff.name 
--         FROM staff JOIN r ON r.parent_id = staff.id)
-- SELECT * FROM r;
-- Например
WITH RECURSIVE r AS (
    SELECT * FROM staff WHERE id = 5
    UNION
    SELECT staff.id, staff.parent_id, staff.name 
        FROM staff JOIN r ON r.parent_id = staff.id)
SELECT * FROM r;
