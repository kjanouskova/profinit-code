#===================#
#=== (2) Teploty ===#
#===================#

#=== Zadani ===#

# (a) Napiste script v PySparku, ktery najde stat s nejvyssi prumernou teplotou v lete.

# (b) Napiste script v PySparku, ktery najde pro kazdy mesic stat s nejvyssi prumernou teplotou.

#=== Reseni ===#

pyspark --packages com.databricks:spark-csv_2.11:1.5.0
sc.setLogLevel("WARN")

#===========================================#
#=== (a) Stat s nejvyssi teplotou v lete ===#
#===========================================#

# I. moznost - DataFrame
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# nacteni csv
df_a = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('teplota.csv')

# filter na leto, prepocet na stupne Celsia
df_leto = df_a.filter((df["mesic"]>=6) & (df["mesic"] <= 8))\
.withColumn('teplota', ((df.teplota / 10) - 32) * 5/9)

# prumer teplot podle statu, pojmenovani sloupce, serazeni podle teploty
df_leto_prum = df_leto.groupBy("stat").agg({'teplota': 'mean'})\
.withColumnRenamed("avg(teplota)","prum_tepl").sort("avg(teplota)", ascending=False)

df_leto_prum.show()

# II. moznost - DataFrame + select
# musime prvne pojmenovat tabulku teplota a pak jednoduse vlozit hotovy select
df_a.registerTempTable('teplota')
sqlContext.sql("SELECT stat, AVG(((teplota/10)-32)*5/9) AS prumer_stat FROM teplota WHERE mesic BETWEEN 6 AND 8  GROUP BY stat ORDER BY prumer_stat DESC").show()


# III. moznost - RDD

# z DataFrame do RDD
rdd_a = df_a.map(lambda row: row.asDict())

# filtrujeme mesic a nezname hodnoty teploty vyradime
rdd_leto = rdd_a.filter(lambda x: (x['mesic'] == 6 or x['mesic'] == 7 or x['mesic'] == 8) and x['teplota'] != None)

# zobrazime jen par (klic, hodnota), kde klic je stat a hodnota je teplota. prevedeme na stupne celsia
rdd_leto_stat = rdd_leto.map(lambda x: (x['stat'], ((float(x['teplota'])/10)-32)*5/9))

# vytvorime trojici (stat, soucet teplot, pocet zaznamu)
rdd_leto_soucet = rdd_leto_stat.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))

# vydelime soucet teplot poctem zaznamu a dostaneme prumer
rdd_leto_prumer = rdd_leto_soucet.map(lambda (label, (value_sum, count)): (label, value_sum / count))\
.sortBy(lambda x: x[1], ascending=False)

#====================================================#
#=== (b) Stat s nejvyssi teplotou pro kazdy mesic ===#
#====================================================#

# prepnout do HIVE contextu
hc = HiveContext(sc)
# nacteni csv
df_b = hc.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('teplota.csv')


# I. moznost - DataFrame

# prevedeni na stupne Celsia
df_cels = df_b.withColumn('teplota', ((df_b.teplota / 10) - 32) * 5/9)

# spocteni prumeru podle statu a mesice
df_prum = df_cels.groupBy("stat", "mesic").agg({'teplota': 'mean'})\
.withColumnRenamed("avg(teplota)","prum_tepl")

# pro pouziti funkce rank over je treba nacist Window funkce
from pyspark.sql.window import Window
from pyspark.sql import functions as F
windowSpec = Window.partitionBy(df_prum['mesic'])\
.orderBy(df_prum['prum_tepl'].desc())

# vytvoreni poradi pro kazdy mesic
df_poradi = df_prum.select('stat','mesic','prum_tepl', 
    F.rank().over(windowSpec).alias("poradi"))

# nakonec filtrovat poradi 1
df_max_tepl = df_poradi.filter(df_poradi["poradi"]==1).select('stat','mesic','prum_tepl')
df_max_tepl.show()

# II. moznost - DataFrame + select
df_cels.registerTempTable('teplota')
hc.sql("SELECT stat, mesic, avg_teplota FROM (SELECT stat, mesic, avg_teplota, RANK() OVER (PARTITION BY mesic ORDER BY avg_teplota DESC) AS poradi FROM (SELECT stat, mesic, AVG(teplota) avg_teplota FROM teplota GROUP BY stat, mesic) prumery) max_prum WHERE poradi = 1")\
.show()

# III. moznost - RDD

# z DataFrame do RDD
rdd_b = df_b.map(lambda row: row.asDict())

# filtrujeme mesic a nezname hodnoty teploty vyradime
rdd_cels = rdd_b.filter(lambda x: x['teplota'] != None)

# zobrazime jen par (klic, hodnota), kde klic je stat a hodnota je teplota. prevedeme na stupne celsia
rdd_sm = rdd_cels.map(lambda x: ((x['stat'], x['mesic']), ((float(x['teplota'])/10)-32)*5/9))

# vytvorime trojici (stat, soucet teplot, pocet zaznamu)
rdd_sm_soucet = rdd_sm.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))

# vydelime soucet teplot poctem zaznamu a dostaneme prumer
rdd_sm_prumer = rdd_sm_soucet.map(lambda ((label1, label2), (value_sum, count)): ((label1, label2), value_sum/count))

# spocte maximum pro kazdy mesic a seradi podle mesice
rdd_sm_max = rdd_sm_prumer.map(lambda x: (x[0][1], (x[0][0], x[1])))\
.reduceByKey(lambda x1, x2: max(x1, x2, key = lambda x: x[-1])).sortByKey()

rdd_sm_max.collect()
