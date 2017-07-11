clear screen;
SET SERVEROUTPUT ON;


/* 1 Procedura, kter� v�em zam�stnanc�m, kte�� nastoupili p�ed rokem 2005 zv��� plat o X % (vstupn� parametr) 
a kte�� nejsou PRES nebo VP (jen b�n�m zam�stnanc�m). 
Pokud se n�kter�mu zam�stnanci zv��� plat mimo povolen� rozmez� (v JOBS), tak se vyhod� v�jimka (a provede se rollback). */

CREATE or replace PROCEDURE narust_mzdy(narust in number) AS
begin
  update employee
     set salary = salary*narust
     where job_name<>'PRES' AND job_name<>'VP' and extract(year from hire_date) < 2005 ;
END narust_mzdy;

execute narust_mzdy(1.2);
select * from employee;





/* 2 Funkce, kter� bude m�t na vstupu EMPLOYEE_ID a vr�t� jm�no nad��zen�ho (managera odd�len�). */

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



/* 3 Vytvo�en� view, ve kter�m bude: N�zev odd�len�, Jm�no managera odd�len�, po�et zam�stnanc� dan�ho odd�len�, pr�m�rn� plat zam�stnanc� odd�len� */
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

