#!/bin/sh
echo "Write system"
read s
echo "Write env"
read e

PRD_FILE="C:/Users/janouskk/Repositories/mantis-misc/analysis/${s}/PRD/${s}_PRD_table_catalog.sql"
OTHER_FILE="C:/Users/janouskk/Repositories/mantis-misc/analysis/${s}/${e}/${s}_${e}_table_catalog.sql"

# vytvorit tabulku

sqlite3 temp.db << EOF
CREATE TABLE META_SRC_TABLE_CATALOG (
        TABLE_NAME            VARCHAR2(255) NOT NULL,
        COLUMN_NAME           VARCHAR2(255) NOT NULL,
        DATA_TYPE             VARCHAR2(255) NOT NULL,
        DATA_LENGTH           NUMBER(15,0) NOT NULL,
        COLUMN_ORDER          NUMBER,
        IS_PK                 CHAR(1),
        PRECISION           NUMBER,
        SCALE               NUMBER,
        MODEL_DATA_TYPE     VARCHAR2(30),
        TABLE_COMMENT VARCHAR2(4000),
        COLUMN_COMMENT VARCHAR2(4000),
  CONSTRAINT META_SRC_TABLE_CATALOG
        );
EOF


# 2) nacpat do ni data

# 2.2) insert metadata z PRD

sed -e '1s/^.*$/BEGIN TRANSACTION;/' ${PRD_FILE} | sqlite3 temp.db

# # 2.3) rename tabulky

sqlite3 temp.db << EOF
alter table META_SRC_TABLE_CATALOG rename to META_SRC_TABLE_CATALOG_PRD;
EOF

# 2.4) vytvorit tabulku

sqlite3 temp.db << EOF
CREATE TABLE META_SRC_TABLE_CATALOG (
        TABLE_NAME            VARCHAR2(255) NOT NULL,
        COLUMN_NAME           VARCHAR2(255) NOT NULL,
        DATA_TYPE             VARCHAR2(255) NOT NULL,
        DATA_LENGTH           NUMBER(15,0) NOT NULL,
        COLUMN_ORDER          NUMBER,
        IS_PK                 CHAR(1),
        PRECISION           NUMBER,
        SCALE               NUMBER,
        MODEL_DATA_TYPE     VARCHAR2(30),
        TABLE_COMMENT VARCHAR2(4000),
        COLUMN_COMMENT VARCHAR2(4000),
  CONSTRAINT META_SRC_TABLE_CATALOG
        );
EOF

# 2.5) insert metadata z porovnavaneho prostredi

sed -e '1s/^.*$/BEGIN TRANSACTION;/' ${OTHER_FILE} | sqlite3 temp.db

# 3) porovnani
# 3.1) extra tabulky na PRD


sqlite3 temp.db -column -header << EOF
SELECT DISTINCT table_name
  FROM META_SRC_TABLE_CATALOG_PRD
EXCEPT
SELECT DISTINCT table_name
  FROM META_SRC_TABLE_CATALOG;
EOF


# 3.2) sloupce s jinym schematem

sqlite3 temp.db -column -header << EOF
SELECT p.table_name AS TABLE_NAME_PROD, p.column_name AS COLUMN_NAME_PROD,  p.data_type AS DATA_TYPE_PROD, 
            p.data_length AS DATA_LENGTH_PROD, p.is_pk AS IS_PK_PROD, p.precision AS precision_PROD, 
            p.scale AS scale_PROD, o.table_name AS TABLE_NAME_OTHER, o.column_name AS COLUMN_NAME_OTHER, 
            o.data_type AS DATA_TYPE_OTHER, o.data_length AS DATA_LENGTH_OTHER, o.is_pk AS IS_PK_OTHER, 
            o.precision AS PRECISION_OTHER, o.scale AS SCALE_OTHER
  FROM META_SRC_TABLE_CATALOG_PRD p 
        JOIN META_SRC_TABLE_CATALOG o 
        ON (p.table_name = o.table_name AND p.column_name = o.column_name) 
 WHERE
    p.data_type <> o.data_type OR
    p.data_length <> o.data_length OR
    (p.is_pk <> o.is_pk) OR (p.is_pk IS NULL AND o.is_pk IS NOT NULL) 
            OR (p.is_pk IS NOT NULL AND o.is_pk IS NULL) OR
    (p.scale <> o.scale) OR (p.scale IS NULL AND o.scale IS NOT NULL) 
            OR (p.scale IS NOT NULL AND o.scale IS NULL) OR
    (p.precision <> o.precision) OR (p.precision IS NULL AND o.precision IS NOT NULL) 
            OR (p.precision IS NOT NULL AND o.precision IS NULL);
EOF

# 3.3) extra sloupce na PRD

sqlite3 temp.db -column -header << EOF
  WITH different_columns AS 
        (SELECT DISTINCT TABLE_NAME, COLUMN_NAME
           FROM META_SRC_TABLE_CATALOG_PRD
         EXCEPT
         SELECT DISTINCT TABLE_NAME, COLUMN_NAME
           FROM META_SRC_TABLE_CATALOG)
SELECT TABLE_NAME, COLUMN_NAME 
  FROM different_columns 
 WHERE TABLE_NAME IN (
          SELECT DISTINCT TABLE_NAME
            FROM META_SRC_TABLE_CATALOG);
EOF

# 4. uklid
rm temp.db
