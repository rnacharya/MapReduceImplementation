from pyspark import SparkContext    
sc = SparkContext()

path = 'public/shakespeare-scenes.json'
rdd = sc.wholeTextFiles(path)

output = rdd.flatMap(lambda (file,contents):[(file, word) for word in contents.lower().split()]).map(lambda (file, word): (word,[file])).reduceByKey(lambda a,b: a+b)