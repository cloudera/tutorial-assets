-- -------------------------------------------------------------
-- QUERY 1: FACTORY PERFORMANCE
-- Take a peek into how many units each factory is making
-- -------------------------------------------------------------

-- Factory Unit Production
DROP VIEW IF EXISTS factory.v_units_prod;
CREATE VIEW factory.v_units_prod AS
    SELECT factory_id, machine_id, ROUND(AVG(daily_units_produced),0) AS avg_units_produced
      FROM factory.machine_throughput
      GROUP BY factory_id, machine_ID
      ORDER BY factory_id, machine_ID LIMIT 100;
SELECT * FROM factory.v_units_prod;


-- Factory Revenue
DROP VIEW IF EXISTS factory.v_revenue;
CREATE VIEW factory.v_revenue AS
    SELECT factory_id, sum(revenue) AS total_revenue
      FROM (  SELECT factory_id, (avg_units_produced * revenue_per_unit) AS revenue
                FROM factory.v_units_prod mt JOIN factory.machine_revenue mr
                  ON mt.machine_id = mr.machine_id
           ) AS v_rev
    GROUP by factory_id
    ORDER BY factory_id ASC LIMIT 100;
SELECT * FROM factory.v_revenue;





-- -------------------------------------------------------------
-- QUERY 2: MACHINE UPTIME FOR FACTORY 2
-- Could there be a specific machine that's down more often than others?
-- -------------------------------------------------------------
SELECT machine_id, ROUND(AVG(hours_operational),0) AS avg_hours_operational
    FROM factory.machine_uptime
    WHERE factory_id = 2
    GROUP BY machine_id
    ORDER BY machine_id ASC;





-- -------------------------------------------------------------
-- QUERY 3: NUMBER OF HOURS WORKED PER FACTORY
-- -------------------------------------------------------------
SELECT factory_id, AVG(time_worked)
    FROM hr.timesheet
    GROUP BY factory_id
    ORDER BY factory_id ASC;





-- -------------------------------------------------------------
-- QUERY 4: EMPLOYEE LEAVE TIME DISTRIBUTION PER FACTORY
-- -------------------------------------------------------------
DROP VIEW IF EXISTS hr.v_employee_leave_time;

CREATE VIEW hr.v_employee_leave_time AS
    SELECT factory_id, leave_type, COUNT(leave_type)/COUNT(DISTINCT employee_id) AS avg_time
      FROM hr.leave_time
      GROUP BY factory_id, leave_type
      ORDER BY factory_id ASC, leave_type DESC LIMIT 100;

SELECT * FROM hr.v_employee_leave_time;
