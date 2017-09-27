#!/bin/sh

####
#### PURPOSE:
#### Compare specified DB metadata with production version
####
#### REQUIREMENTS:
#### - sqlite3
####
#### USAGE:
#### $ db_compare.sh -e <environment> -s <site> -p <path>
####
#### EXAMPLE:
#### $ ./db_compare.sh -e DEV -s ORIGL
####

####
#### VARIABLES
####
alias sqlite3_cmd=/opt/sqlite3/sqlite3
alias sqldiff_cmd=/opt/sqlite3/sqldiff
alias sqlite3_analyzer_cmd=/opt/sqlite3/sqlite3_analyzer

#### FUNCTIONS
####
. ../infa/bash_functions

usage() {
  echo "Usage:"
  echo "$0 -e <environment> -s <site>"
  echo "<environment> - possible vaules: DEV|SIT|UAT|PRD"
  echo "<site> - site (source system) name - 5 characters"
  1>&2;
}

####
#### INPUT PARAMETERS CHECKING
####
if [  ! ${#} -gt 0 ]; then
  err "Illegal number of parameters. Aborting.";
  usage
  exit 1
fi

while getopts ":e:s:" o; do
  case "${o}" in
    e)
      e=${OPTARG}
      if [ -z "${e}" ] || [ ! "${e,,}" == "dev" ] && [ ! "${e,,}" == "sit" ] && [ ! "${e,,}" == "uat" ] && [ ! "${e,,}" == "prd" ]; then
        err "Environment name \"${e}\" is in incorrect format or empty."
        usage
        exit 1
      fi
    ;;
    s)
      s=${OPTARG}
      if [ -z "${s}" ] || [[ ! "${s,,}" =~ ^[a-z0-9]{5}$ ]]; then
        err "Site name \"${s}\" is in incorrect format or empty."
        usage
        exit 1
      fi
    ;;
    *)
      usage
    ;;
  esac
done

####
#### CHECKING REQUIREMENTS
####
command -v sqlite3_cmd >/dev/null 2>&1 || { echo >&2 "|ERROR|$0 requires sqlite3 but it's not installed."; exit 1; }

PRD_FILE="../../../git-misc/analysis/${s}/PRD/${s}_PRD_table_catalog.sql"
OTHER_FILE="../../../git-misc/analysis/${s}/${e}/${s}_${e}_table_catalog.sql"

# vytvorit tabulku
log "Creating table temp.db for ${s}/PRD metadata..."
sqlite3_cmd temp.db << EOF
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
  CONSTRAINT META_SRC_TABLE_CATALOG_PK PRIMARY KEY (TABLE_NAME, COLUMN_NAME)
        );
EOF

if [ $? -eq 0 ]; then
  log "Table temp.db created successfully."
else
  err "Couldn't create table temp.db."
  exit 1
fi

# 2) nacpat do ni data

# 2.2) insert metadata z PRD
log "Populating table temp.db with ${s}/PRD metadata..."
sed -e '1s/^.*$/BEGIN TRANSACTION;/' ${PRD_FILE} | sqlite3_cmd temp.db
if [ $? -eq 0 ]; then
  log "Table temp.db populated successfully."
else
  err "Couldn't populate table temp.db."
  exit 1
fi
# # 2.3) rename tabulky
log "Renaming table temp.db to META_SRC_TABLE_CATALOG_PRD..."
sqlite3_cmd temp.db << EOF
alter table META_SRC_TABLE_CATALOG rename to META_SRC_TABLE_CATALOG_PRD;
EOF
if [ $? -eq 0 ]; then
  log "Table renamed successfully."
else
  err "Couldn't rename table."
  exit 1
fi

# 2.4) vytvorit tabulku
log "Creating table temp.db for ${s}/${e} metadata..."
sqlite3_cmd temp.db << EOF
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
  CONSTRAINT META_SRC_TABLE_CATALOG_PK PRIMARY KEY (TABLE_NAME, COLUMN_NAME)
        );
EOF
if [ $? -eq 0 ]; then
  log "Table temp.db created successfully."
else
  err "Couldn't create table temp.db."
  exit 1
fi

# 2.5) insert metadata z porovnavaneho prostredi
log "Populating table temp.db with ${s}/${e} metadata..."
sed -e '1s/^.*$/BEGIN TRANSACTION;/' ${OTHER_FILE} | sqlite3_cmd temp.db
if [ $? -eq 0 ]; then
  log "Table temp.db populated successfully."
else
  err "Couldn't populate table temp.db."
  exit 1
fi

# 3) porovnani
# 3.1) extra tabulky na PRD

log ""
log "### Extra tables on PRD - START ###"
log ""
sqlite3_cmd temp.db -column -header << EOF
SELECT DISTINCT table_name
  FROM META_SRC_TABLE_CATALOG_PRD
EXCEPT
SELECT DISTINCT table_name
  FROM META_SRC_TABLE_CATALOG;
EOF
if [ $? -ne 0 ]; then
  err "Couldn't compare tables (3.1)."
  exit 1
fi
log ""
log "### Extra tables on PRD - END ###"
log ""

# 3.2) sloupce s jinym schematem

log ""
log "### Columns with different schema - START ###"
log ""
sqlite3_cmd temp.db -column -header << EOF
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
if [ $? -ne 0 ]; then
  err "Couldn't compare tables (3.2)."
  exit 1
fi
log ""
log "### Columns with different schema - END ###"
log ""

# 3.3) extra sloupce na PRD
log ""
log "### Extra columns on PRD - START ###"
log ""
sqlite3_cmd temp.db -column -header << EOF
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
if [ $? -ne 0 ]; then
  err "Couldn't compare tables (3.3)."
  exit 1
fi
log ""
log "### Extra columns on PRD - END ###"
log ""

# 4. uklid
if [ -e temp.db ]; then
  rm temp.db
fi

exit 0
