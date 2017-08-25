#=================#
#=== (1) Bible ===#
#=================#

#=== Zadani ===#

# V /home/_data je soubor bible.txt. Přeneste soubor do home adresáře na HDFS (/user/...) a napište program v PySparku, který:

# (a) Spočítá histogram výskytu jednotlivých slov, vypište prvních 10 nejpoužívanějších slov a jejich počet výskytů.
# Nerozlišujte slova s malými nebo velkými písmeny a interpunkcí (čárka, tečka, vykřičník, závorky, apod.)

# (b) Modifikujte minulý příklad a zjistěte jaký verš obsahuje nejvíce slov a kolik.
# (c) Modifikujte minulý příklad a zjistěte jaký verš obsahuje nejvíce unikátních slov a kolik.


#=== Reseni ===#

hdfs dfs -put /home/_data/bible.txt .
pyspark --master yarn-client --num-executors 2
sc = SparkContext()
sc.setLogLevel("WARN")

# (a)
# sc = spark context
rdd = sc.textFile('/user/kjanouskova/bible.txt')

# split dokumentu podle tabulatoru, pak vezme druhy sloupec, zmensi na mala pismena a rozdeli dle mezer
# tim vytvorime slova
words = rdd.flatMap(lambda document: document.split('\t')[1].lower().split())

# ze slov musime odstranit interpunkci
words2 = words.flatMap(lambda x: x.strip("!#$%&'()*+,-./:;<=>?@[\]^_`{|}~").split())

# vytvori par klic, hodnota, kde klic je slovo a hodnota je pocet 
pairs = words2.map(lambda w: (w,1))

# secte hodnoty podle klice
words_count = pairs.reduceByKey(lambda x, y: x + y)

# vypise serazene podle hodnoty, vezme prvnich deset
words_count.sortBy(lambda x: x[1], ascending=False).take(10)

# (u'the', 63924)
# (u'and', 51696)
# (u'of', 34617)
# (u'to', 13561)
# (u'that', 12913)
# (u'in', 12667)
# (u'he', 10420)
# (u'shall', 9838)
# (u'unto', 8997)
# (u'for', 8971)


# (b)

rdd = sc.textFile('/user/kjanouskova/bible.txt')
# (klic, hodnota), kde klic je nazev verse a hodnota je delka retezce - neboli pocet slov
rows = rdd.map(lambda document: 
    (document.split('\t')[0].lower(), len(document.split('\t')[1].lower().split())))
# seradit podle druheho sloupce
rows.sortBy(lambda x: x[1], ascending=False).take(1)

# (u'esther 8:9', 86)

# (c) 

rdd2 = rdd.map(lambda x: x.replace(',','').replace('.','').replace(';','').replace('?','').replace('!','').replace('-',''))
rows = rdd2.map(lambda document: 
    (document.split('\t')[0].lower(), len(set(document.split('\t')[1].lower().split()))))

# seradit podle druheho sloupce
rows.sortBy(lambda x: x[1], ascending=False).take(1)

# (u'esther 4:11', 53)

