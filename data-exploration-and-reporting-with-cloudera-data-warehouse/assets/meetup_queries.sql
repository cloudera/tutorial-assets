
    
--Take a peek into how many units each factory is making
    SELECT factory_id,machine_id,round(avg(daily_units_produced),0) as avg_units_produced
    from factory.machine_throughput
    group by factory_id,machine_ID;

-- Looks like Factory 2 has some low throughput, lets check out the specific machines at that factory.
Select machine_id, round(avg(daily_units_produced),0) as avg_units_produced from factory.machine_throughput
    where factory_id = 2
    group by machine_id
    order by machine_id asc

-- Could there be a specific machine that's down more often than others?
  Select machine_id, round(avg(hours_operational),0) as avg_hours_operational from factory.machine_uptime
    where factory_id = 2
    group by machine_id
    order by machine_id asc

-- Are machines down more often in factory 2?
    Select factory_id, round(avg(hours_operational),0) as avg_hours_operational from factory.machine_uptime
    group by factory_id,machine_id
  order by factory_id asc

--Yes, it does seem to be lower than other factories. Okay lets go see if we can find something in the HR data 
-- that could explain why these machines are down more often.
select factory_id, count(leave_type)/count(distinct employee_id) as avg_worker_vacation from hr.leave_time
where leave_type = 'vacation'
group by factory_id
order by factory_id asc

-- Huh interesting, factory 2 actually takes LESS vacation time than any other. What about sick time?
select factory_id, count(leave_type)/count(distinct employee_id) as avg_worker_vacation from hr.leave_time
where leave_type = 'sick'
group by factory_id
order by factory_id asc

-- Wow okay, so factory 2 takes way more sick time. Satisfied with having something to show, lets see how to present these insights.

-- Create Dashboard view for units produced by factory
DROP VIEW IF EXISTS factory.v_units_prod;
CREATE VIEW factory.v_units_prod as
    SELECT factory_id,machine_id,round(avg(daily_units_produced),0) as avg_units_produced
    from factory.machine_throughput
    group by factory_id,machine_ID;

--Revenue reporting
DROP VIEW IF EXISTS factory.v_revenue;
CREATE VIEW factory.v_revenue as
select factory_id,sum(revenue) as total_revenue from
(
    select factory_id, (avg_units_produced * revenue_per_unit) as revenue from factory.v_units_prod mt
    join factory.machine_revenue mr
on mt.machine_id = mr.machine_id) as v_rev
group by factory_id
order by factory_id asc;


-- Create Dashboard view for employee leave time
DROP VIEW IF EXISTS hr.v_employee_leave_time;
CREATE VIEW hr.v_employee_leave_time as
select factory_id,leave_type, count(leave_type)/count(distinct employee_id) as avg_worker_sickdays
  from hr.leave_time
  group by factory_id,leave_type;
