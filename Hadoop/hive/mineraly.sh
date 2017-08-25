#==============#
# (1) Mineraly #
#==============#

# /home/_data soubor deposit.csv
# prenest soubor na HDFS 
# vytvorit externi tabulku - serializace csv (FORMAT SERDE)
# vytvorit managed tabulku formatu ORC s kompresi SNAPPY 
# insert dat z externi tabulky - vhodne pozmenit typy

# Ukol 1: zjistit zeme a seznam mineralu v zemi bez duplicit 
# Vytvorit view 
# Ukol 2: zjistit zeme, kde se tezi diamant

#================#
# Uprava souboru #
#================#
# jen id, zeme a mineraly
# bez prvniho radku
csv 1 3 7 </home/_data/deposit.csv | tail -n +2 > deposit2.csv

# ale mame duplicity - nektere kombinace jsou vicekrat
diff -ay deposit.csv deposit2.csv | less

# bez 1. sloupce a pak znovu indexujeme
# sort -u == sort unique
# awk prikaz prida cisla radek na zacatek
csv 3 7 </home/_data/deposits/deposit.csv | tail -n +2 | sort -u | awk '{printf("%d,%s\n", NR, $0)}' >deposit3.csv
diff -ay deposit.csv deposit3.csv | less

#===================#
# Preneseme na HDFS #
#===================#

# slozka se MUSI jmenovat exttab, jinde nejsou prava
hdfs dfs -mkdir -p exttab/deposit
# ulozime na HDFS jako deposit.csv
hdfs dfs -put deposit3.csv exttab/deposit_ext/deposit.csv


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

# DROP TABLE deposit_ext;

CREATE TABLE deposit_ext (
id BIGINT, 
country VARCHAR(255), 
minerals STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
'separatorChar' = ',',
'quoteChar'     = '"',
'escapeChar'    = '\\'
)  
STORED AS TEXTFILE
LOCATION '/user/kjanouskova/exttab/deposit_ext';

DROP TABLE deposit_orc;

# codec snappy musi byt upercase, jinak hodi chybu ...
CREATE TABLE IF NOT EXISTS deposit_orc (
id BIGINT, 
minerals STRING
)
PARTITIONED BY (country STRING)
STORED AS ORC tblproperties ("orc.compress"="SNAPPY");

# pokud mame pouzity partitioning, musi se uvest tento partition
# SELECT musi byt ve stejnem poradi jako definice tabulky do ktere se importuji data

INSERT OVERWRITE TABLE deposit_orc
PARTITION (country)
SELECT id, minerals, country
FROM deposit_ext;

SELECT * FROM deposit_orc LIMIT 10;
SELECT * FROM deposit_ext LIMIT 10;

SELECT country, collect_set(minerals) FROM deposit_orc GROUP BY country;

# | Afghanistan  | ["Aluminum","Barite","Beryllium-niobium,tin","Chromium","Copper","Copper-molybdenum-gold","Emerald","Gold","Iron","Kunzite","Lapis Lazuli","Lithium-tantalum","Magnesite-talc","Phosphorous-rare earths","Ruby","Tourmaline"]                                                                                                                                                                                                                               |
# | Albania      | ["Chromium","Copper","Nickel"]                                                                                                                                                                                                                                                                                                                                                                                                                              |
# | Algeria      | ["Barite","Clay","Clay (Bentonite)","Clay (Kaolin)","Copper","Copper, Iron","Copper, Lead","Copper, Zinc","Gypsum","Halite","Iron","Iron, Copper","Iron, Lead","Iron, Manganese","Lead","Lead, Zinc","Limestone","Limestone (marble)","Mercury","Phosphate","Sodium sulfate","Strontium","Tungsten, Tin","Zinc","Zinc, Lead, Copper, Silver, Cobalt, Gold"]                                                                                                 |
# | Angola       | ["Copper","Diamond","Iron","Vanadium, Lead"]                                                                                                                                                                                                                                                                                                                                                                                                                |
# | Argentina    | ["Boron","Copper","Copper, gold","Copper, lead","Copper, molybdenum","Gold","Gold, silver","Gypsum","Halite","Iron","Limestone","Lithium, boron","Potash, halite","Rhodochrosite","Silver, gold","Silver, lead","Silver, tin"]                                    

# ulozime vystup tohoto SELECTu jako jinou tabulku
CREATE TABLE deposit_list_min
AS SELECT country, collect_set(minerals) AS min_list
FROM deposit_orc GROUP BY country;

# zeme, kde se tezi diamant
SELECT country
FROM deposit_list_min 
WHERE array_contains(`_c1`,'Diamond') = TRUE;