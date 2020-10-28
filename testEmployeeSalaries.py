import pandas as pd
x=pd.read_csv('spark-scripts/salary.csv')

x=zip(x['_c1'],x['avg(_c4)'])
salary_python={}
# print(list(x))
for a,b in x:
	# print(a, str(b))
	salary_python[a]=str(round(b,2))

# print(salary_python[55])
salary_java={}
for i in range(5):
    f = open('public/outputFile-employee-salaries-'+str(i)+'.txt', "r")
    for x in f:
        if len(x.split(" "))==2:
            salary_java[int(x.split(" ")[0])]=str(round(float(x.split(" ")[1][2:-4]),2))

print('-------------------------------------------------------')
if (salary_python==salary_java):
	print("The average salaries matches to the spark output")
else:
	print("The average salaries does not match to the spark output")
print('-------------------------------------------------------')