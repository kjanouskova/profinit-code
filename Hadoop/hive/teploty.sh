#=============#
# (2) Teploty #
#=============#

# spojit (na lokálním FS /home/_data) teplota1-6.csv, teplota7-12.csv do jednoho teploty.csv 
# tak, aby obsahoval jen 1 hlavičku na začátku souboru
# prenest na HDFS

# vytvorit tabulku a prevest teplotu na °C
# vzorec ((teplota / 10) - 32) * 5/9

# Ukol 1: Najdete stat s nejvyssi prumernou teplotou v lete (mesice 6, 7, 8)

# Ukol 2: Najdete pro kazdy mesic stat s nejvyssi prumernou teplotou 

# Ukol 3: Najdete pro kazdou sezonu stat s nejvyssi prumernou teplotou 

# Ukol 4: Najdete nejvyssi hodinovy pokles/stoupani teplot v ramci jedne stanice 
# Vystup ve formatu stat | stanice | max_delta

# Ukol 5: Najdete nejvyssi hodinovy pokles/stoupani teplot v ramci jedne stanice
# Vystup ve formatu stat | stanice | delta | mesic | den | hodina

#================#
# Uprava souboru #
#================#

# zkopirovat soubory k sobe
cp /home/_data/teplota*.csv .

# spojime soubory 1-6 a 7-12 do jednoho
head -1 teplota1-6.csv >> teplota.csv
tail -n+2 teplota1-6.csv >> teplota.csv
tail -n+2 teplota7-12.csv >> teplota.csv


#===================#
# Preneseme na HDFS #
#===================#

# slozka se MUSI jmenovat exttab, jinde nejsou prava
hdfs dfs -mkdir -p exttab/teplota
# ulozime na HDFS jako teplota.csv
hdfs dfs -put teplota.csv exttab/teplota/teplota.csv


#===================#
# Dostat se do hive #
#===================#

# viz confluence
beeline -u "jdbc:hive2://hadoop-mn.profinit.lan:10000/default;principal=hive/hadoop-mn.profinit.lan@PROFINIT.HAD"

#======#
# HIVE #
#======#

SHOW DATABASES;
CREATE DATABASE IF NOT EXISTS kjanouskova;

# !!!!!!
# pouzivat jen svoji databazi, jinde stejne nejsou prava
USE kjanouskova;

SHOW TABLES;

# DROP TABLE teplota_ext;

CREATE TABLE teplota_ext ( 
    stanice string, mesic string, den string, hodina string, 
    teplota string, flag string, latitude string, longitude string, 
    vyska string, stat string, nazev string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
'separatorChar' = ',',
'quoteChar'     = '"',
'escapeChar'    = '\\'
)  
STORED AS TEXTFILE
LOCATION '/user/kjanouskova/exttab/teplota'
tblproperties ("skip.header.line.count"="1");

DROP TABLE teplota;

CREATE TABLE IF NOT EXISTS
teplota (
stanice string,
mesic int,
den int,
hodina int,
teplota double,
flag string,
latitude double,
longitude double,
vyska double,
stat string,
nazev string
)
STORED AS parquet;

# SELECT musi byt ve stejnem poradi jako definice tabulky do ktere se importuji data

INSERT OVERWRITE TABLE teplota
SELECT stanice, mesic, den, hodina,
        ( (teplota / 10) - 32) * 5/9,
        flag, latitude, longitude, vyska, stat, nazev
  FROM teplota_ext
 WHERE mesic IS NOT NULL;

SELECT * FROM teplota LIMIT 10;
SELECT * FROM teplota_ext LIMIT 10;

# stat s nejvyssi prumernou teplotou v lete (mesice 6, 7, 8)
SELECT stat, AVG(teplota) AS prumer_stat 
  FROM teplota 
 WHERE mesic BETWEEN 6 AND 8 
 GROUP BY stat 
 ORDER BY prumer_stat DESC;
/* LIMIT 1 */

# pro kazdy mesic stat s nejvyssi prumernou teplotou 
SELECT stat, mesic, avg_teplota
  FROM (SELECT stat, mesic, avg_teplota, 
                  RANK() OVER (PARTITION BY mesic 
                                   ORDER BY avg_teplota DESC) AS poradi
          FROM (SELECT stat, mesic, AVG(teplota) avg_teplota 
                  FROM teplota 
                 GROUP BY stat, mesic
                ) prumery
        ) max_prum
 WHERE poradi = 1;


# pro kazdou sezonu stat s nejvyssi prumernou teplotou 
SELECT stat, sezona, avg_teplota
  FROM (SELECT stat, sezona, avg_teplota, 
                  RANK() OVER (PARTITION BY sezona 
                                   ORDER BY avg_teplota DESC) AS poradi
          FROM (SELECT stat, sezona, AVG(teplota) avg_teplota 
                  FROM (SELECT stat
                                ,CASE  
                                    WHEN mesic BETWEEN 3 AND 5 THEN 'jaro'
                                    WHEN mesic BETWEEN 6 AND 8 THEN 'leto'
                                    WHEN mesic BETWEEN 9 AND 11 THEN 'podzim'
                                    WHEN mesic in (12, 1, 2) THEN 'zima'
                                END AS sezona
                                , teplota    
                            FROM teplota
                          ) tepl_sez  
                 GROUP BY stat, sezona
                ) prumery
        ) max_prum
 WHERE poradi = 1;

# nejvyssi hodinovy pokles/stoupani teplot v ramci jedne stanice 
# Vystup ve formatu stat | stanice | max_delta
SELECT stat, stanice, MAX(teplota_1-teplota) AS max_delta, MIN(teplota_1-teplota) AS min_delta
  FROM (SELECT stat, stanice, teplota, 
                LAG(teplota, 1, null) OVER (PARTITION BY stanice ORDER BY stanice, mesic, den, hodina) AS teplota_1 
        FROM teplota
       ) tepl_lag
 GROUP BY stat, stanice
 ORDER BY max_delta DESC

# nejvyssi hodinovy pokles/stoupani teplot v ramci jedne stanice
# Vystup ve formatu stat | stanice | delta | mesic | den | hodina
