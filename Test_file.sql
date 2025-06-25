
use query_logger;

CREATE TABLE IF NOT EXISTS `employees` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NOT NULL,
  `department_id` INT UNSIGNED NOT NULL,
  `salary` DECIMAL(10,2) NOT NULL,
  `performance_rating` CHAR(1) NOT NULL COMMENT 'A, B, C, etc.',
  `last_login` DATETIME NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Basic employees table for examples';

-- Sample data
INSERT INTO `employees` (name, department_id, salary, performance_rating, last_login) VALUES
  ('Alice Johnson',    1,  72000.00, 'A', '2025-06-20 08:15:00'),
  ('Bob Smith',        2,  58000.00, 'B', '2025-06-22 16:45:00'),
  ('Carol Williams',   1,  81000.00, 'A', '2025-06-21 12:30:00'),
  ('David Lee',        3,  49000.00, 'C', NULL),
  ('Eva Martinez',     2,  63000.00, 'B', '2025-06-19 09:00:00');
SELECT * FROM employees;

-- 1) SELECT … FROM
SELECT * FROM employees WHERE department_id = 3;

-- 2) SELECT … JOIN (requires a departments table)
--    you can create a minimal departments table too if you like:
CREATE TABLE IF NOT EXISTS `departments` (
  `id`   INT UNSIGNED NOT NULL,
  `name` VARCHAR(50) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT e.name, d.name AS dept_name
  FROM employees e
  JOIN departments d ON e.department_id = d.id
 WHERE d.name = 'HR';


-- 4) INSERT … INTO (requires an employees_history table)
CREATE TABLE IF NOT EXISTS `employees_history` (
  `emp_id`      INT UNSIGNED NOT NULL,
  `old_salary`  DECIMAL(10,2) NOT NULL,
  `changed_at`  DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO employees_history (emp_id, old_salary, changed_at)
  VALUES ( (SELECT id FROM employees LIMIT 1),
           (SELECT salary FROM employees LIMIT 1),
           NOW() );
select sleep(5);