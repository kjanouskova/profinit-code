# #!/bin/sh

echo Write environment:
read e

PRD_FILE="H:/mantis-misc/analysis/${e}/PRD/${e}_PRD_table_catalog.sql"

# # vytvorit tabulku

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


# # 2) nacpat do ni data

# # 2.2) insert metadata z PRD

sed -e '1s/^.*$/BEGIN TRANSACTION;/' ${PRD_FILE} | sqlite3 temp.db


# how many tables are there
echo "How many tables are there:"

sqlite3 temp.db -column << EOF 
SELECT count(distinct table_name)
  FROM META_SRC_TABLE_CATALOG;
EOF

# how many columns for each table
sqlite3 temp.db -column -header -csv << EOF > nr_of_cols.csv
SELECT table_name, count(column_name)
  FROM META_SRC_TABLE_CATALOG
 GROUP BY table_name; 
EOF

# which data types are there
sqlite3 temp.db -column -header -csv << EOF > data_types.csv
SELECT data_type, count(1)
  FROM META_SRC_TABLE_CATALOG
 GROUP BY data_type;
sum

# EOF of lengths for all data types but VARBINARY and BLOB
sqlite3 temp.db -column -header -csv << EOF > sum_data_length.csv
SELECT table_name, sum(data_length)+ifnull(precision,0) AS sum_of_lengths
  FROM META_SRC_TABLE_CATALOG
 WHERE data_type not IN ('VARBINARY', 'BLOB')
 GROUP BY table_name
 ORDER BY 2 desc;
EOF

echo "How many tables have a primary key:"
# how many tables have a primary key
sqlite3 temp.db -column << EOF 
SELECT count(distinct table_name)
  FROM META_SRC_TABLE_CATALOG
 WHERE IS_PK = 'Y';
EOF


# how many columns are part of a primary key
sqlite3 temp.db -column << EOF > count_pk.csv
SELECT table_name, sum(CASE is_pk  
         WHEN 'Y' THEN 1
         ELSE 0
      END)
  FROM META_SRC_TABLE_CATALOG
 GROUP BY table_name;
EOF


sqlite3 temp.db -column -header -csv << EOF 
SELECT table_name, column_name, data_type
  FROM META_SRC_TABLE_CATALOG
 WHERE data_type IN ('BLOB', 'VARBINARY');
EOF

# sqlite3 temp.db -column -header << EOF 
# SELECT table_name, count(distinct data_type)
#   FROM META_SRC_TABLE_CATALOG
#  WHERE table_name IN (SELECT table_name FROM META_SRC_TABLE_CATALOG WHERE data_type = 'TIMESTAMP')
#  GROUP BY table_name
#  ORDER BY 2;
# EOF

# sqlite3 temp.db -column -header << EOF 
# select * 
# from META_SRC_TABLE_CATALOG
# where table_name in (RS_STATUS, RS_MESSAGE);
# EOF


sqlite3 temp.db -column -header  << EOF 
SELECT precision, scale, count(1)
  FROM META_SRC_TABLE_CATALOG
 WHERE data_type IN ('DECIMAL', 'DOUBLE', 'INTEGER', 'SMALLINT')
 GROUP BY precision, scale;
EOF


# 4. uklid
rm temp.db
