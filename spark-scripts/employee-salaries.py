# Coded on google colab
import pandas as pd

df = sqlContext.read.csv("public/employee-salaries.csv")
tempList = [] 
for col in df.columns:
    new_name = col.strip()
    new_name = "".join(new_name.split())
    new_name = new_name.replace('.','') 
    tempList.append(new_name) 
df = df.withColumn("_c4", df["_c4"].cast("double"))

df = df.toDF(*tempList) 
x=df.groupBy("_c1").mean("_c4")
x.show()
x.toPandas().to_csv('salary.csv')