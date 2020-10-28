file = open('public/configFile.txt', "r")
read = file.read()

N=1
application="word-count"
for line in read.splitlines():
    if 'N=' in line:
        N = int(line.split('=',1)[1])


dic_f1={}

for i in range(N):
    fileName = "public/outputFile-"+application+"-"+str(i)+".txt"
    f = open(fileName, "r")
    for i in f:
        x=i.lower().split(' ')
        if len(x)==2:
            dic_f1[x[0].lower()]=int(x[1][x[1].find("[")+1:x[1].find("]")])

dic_f2={}        
f = open("spark-scripts/part-00000", "r")
for i in f:
    x=i[1:-2].split(',')
    dic_f2[x[0].lower()]=int(x[-1])
f = open("spark-scripts/part-00001", "r")
for i in f:
    x=i[1:-2].split(',')
    if x[0].lower() in dic_f2:
        dic_f2[x[0].lower()]+=int(x[-1])
    else:
        dic_f2[x[0].lower()]=int(x[-1])

dic_f1.pop('project')   
dic_f2.pop('project')
dic_f1.pop('')   
dic_f2.pop('')
match = True
for i in dic_f2:
    if i in dic_f1:
        if not dic_f2[i]==dic_f1[i]:
            print(i, dic_f1[i], dic_f2[i])
            match = False
            # print("Not equal")
            break
if match:
    print("The output file and the mapreduce output matches")
else:
    print("Not equal")