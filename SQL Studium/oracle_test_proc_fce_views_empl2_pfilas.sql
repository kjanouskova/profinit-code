clear screen;
SET SERVEROUTPUT ON;


/* 1 Procedura, která všem zamìstnancùm, kteøí nastoupili pøed rokem 2005 zvýší plat o X % (vstupní parametr) 
a kteøí nejsou PRES nebo VP (jen bìžným zamìstnancùm). 
Pokud se nìkterému zamìstnanci zvýší plat mimo povolené rozmezí (v JOBS), tak se vyhodí výjimka (a provede se rollback). */

CREATE or replace PROCEDURE narust_mzdy(narust in number) AS
max_plat number;
plat number;
too_much exception;
begin
  update employee
     set salary = salary*narust
     where job_name<>'PRES' AND job_name<>'VP' and extract(year from hire_date) < 2005 ;
END narust_mzdy;


CREATE OR REPLACE PROCEDURE narust_platu2
(narust in number)
IS
  e_same EXCEPTION;
  cursor platy is
    select job_name, employee_id, hire_date, salary, max_salary
    FROM  employee join jobs using(job_id);
    --where job_name <> 'PRES' AND job_name <> 'VP' AND extract(year from hire_date) < 2005;
begin  
  for plat_ind in platy
    loop
      IF plat_ind.max_salary > plat_ind.salary*narust
        THEN 
          UPDATE employee
          set salary = salary*narust
          where employee_id=plat_ind.employee_id and plat_ind.job_name <> 'PRES' AND plat_ind.job_name <> 'VP' AND extract(year from plat_ind.hire_date) < 2005;
      END IF;
    end loop;

  EXCEPTION
   WHEN e_same THEN dbms_output.put_line ('email address exist');  
END;


EXECUTE narust_platu2(1.2);

SELECT * FROM employee;

SELECT job_name, employee_id, hire_date, salary, max_salary
FROM employee JOIN jobs USING(job_id)
WHERE job_name <> 'PRES' AND job_name <> 'VP' AND extract(YEAR FROM hire_date) < 2005;

UPDATE employee SET salary = ROUND(salary);          



select job_id, employee_id, first_name, surname, hire_date, salary, max_salary, salary*1.1 narust
from employee join jobs using(job_id)
where job_name <> 'PRES' AND job_name <> 'VP' AND extract(year from hire_date) < 2005;












/* 2 Funkce, která bude mít na vstupu EMPLOYEE_ID a vrátí jméno nadøízeného (managera oddìlení). */

CREATE OR REPLACE FUNCTION jmeno_manazera(eid IN NUMBER)
/* dostane employee_id jako cislo, vrati retezec znaku "Jmeno Prijmeni" */
  RETURN VARCHAR2
IS
  jmeno VARCHAR2(20);
BEGIN
  SELECT e2.first_name || ' ' || e2.surname INTO jmeno
  FROM employee e1, employee e2
  WHERE e1.employee_id <> e2.employee_id AND e1.manager_id = e2.employee_id AND e1.employee_id = eid;
  /* vytvoreni paru zamestnanec - jeho manazer */
  RETURN jmeno;
END;

SELECT employee_id, first_name || ' ' || surname jmeno_zamestnance, manager_id, jmeno_manazera(employee_id) jmeno_manazera
FROM employee;



/* 3 Vytvoøení view, ve kterém bude: Název oddìlení, Jméno managera oddìlení, poèet zamìstnancù daného oddìlení, prùmìrný plat zamìstnancù oddìlení */
CREATE OR REPLACE FUNCTION jmeno_manazera2(eid IN NUMBER)
/* dostane employee_id jako cislo, vrati retezec znaku "Jmeno Prijmeni" */
/* vrati jmeno manazera oddeleni - tedy ne vedouciho, ale manazera (manazer oddeleni muze byt sam zamestnanec) */
  RETURN VARCHAR2
IS
  jmeno VARCHAR2(20);
BEGIN
  select e2.first_name || ' ' || e2.surname INTO jmeno
--d.department_id, d.department_name, d.manager_id --e2.first_name || ' ' || e2.surname 
  FROM employee e1, employee e2, departments d
  WHERE d.manager_id = e2.employee_id AND e1.department_id = d.department_id AND e1.employee_id = eid;
    /* vytvoreni paru zamestnanec - jeho manazer - tentokrat muze byt manager i samotny zamestannec*/
  RETURN jmeno;
END;

SELECT employee_id, first_name || ' ' || surname jmeno_zamestnance, manager_id, jmeno_manazera2(employee_id) jmeno_manazera
FROM employee;

CREATE OR REPLACE VIEW oddeleni
AS
  SELECT department_name, jmeno_manazera2(d.manager_id) jmeno_man, COUNT(employee_id) pocet_zam, ROUND(AVG(salary),3) AS prum_plat
  FROM departments d
  JOIN employee e USING(department_id)
  GROUP BY department_id, department_name, d.manager_id, jmeno_manazera2(d.manager_id)
  ORDER BY department_id;

