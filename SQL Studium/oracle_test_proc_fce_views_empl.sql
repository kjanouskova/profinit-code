clear screen;
SET SERVEROUTPUT ON;


/* 1 Procedura, která všem zamìstnancùm, kteøí nastoupili pøed rokem 2005 zvýší plat o X % (vstupní parametr) 
a kteøí nejsou PRES nebo VP (jen bìžným zamìstnancùm). 
Pokud se nìkterému zamìstnanci zvýší plat mimo povolené rozmezí (v JOBS), tak se vyhodí výjimka (a provede se rollback). */

CREATE or replace PROCEDURE narust_mzdy(narust in number) AS
begin
  update employee
     set salary = salary*narust
     where job_name<>'PRES' AND job_name<>'VP' and extract(year from hire_date) < 2005 ;
END narust_mzdy;

execute narust_mzdy(1.2);
select * from employee;





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

