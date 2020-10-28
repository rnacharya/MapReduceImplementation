val data=sc.textFile("sparkdata.txt");
val data2 = data.map(x => x.toLowerCase);
data2.collect;
val data3 = data2.map(x => x.replaceAll("[^\\w\\s]"," ") )
data3.collect;
val splitdata = data3.flatMap(line => line.split("\\s+"));
splitdata.collect;
val mapdata = splitdata.map(word => (word,1));
mapdata.collect;
val reducedata = mapdata.reduceByKey(_+_);
reducedata.collect;
reducedata.saveAsTextFile("out2");