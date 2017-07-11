pyspark --master yarn-client --num-executors 2

# sc = spark context
rdd = sc.textFile('/user/sstamenov/bible.txt')

words = rdd.flatMap(lambda document: document.split('\t')[1].lower().split())

# creates pairs key, value 
pairs = words.map(lambda w: (w,1))

words_count = pairs.reduceByKey(lambda x, y: x + y)

words_count.sortBy(lambda x: x[1], ascending=False).take(10)


# number of words for each verse - verses split by \n, name of the verse and the verse split by \t
# first split takes first element of split document by \t, second split calculates number of words - second element after \t
rows = rdd.map(lambda document: (document.split('\t')[0].lower(), len(document.split('\t')[1].lower().split())))
# sort by the count of words - longest verse to shortest
rows.sortBy(lambda x: x[1], ascending=False).take(10)


# calculate histogram - 
# number of words modulo 10 -> then sum it
freq = rows.map(lambda x: x[1]/10)
# 
# assign to each key value 10
freq_pairs = freq.map(lambda w: (w*10,1))

freq_count = freq_pairs.reduceByKey(lambda x, y: x + y)

freq_count.sortBy(lambda x: x[0]).take(10)

