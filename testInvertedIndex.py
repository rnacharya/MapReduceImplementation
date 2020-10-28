def check_inverted():
    import pandas as pd
    dict_java={}
    for i in range(5):
        f = open('docs/outputFile-inverted-index-'+str(i)+'.txt', "r")
        for x in f:
            if len(x.split(" "))>2:
                dict_java[x.split(" ")[0]]=[]
                for i in x.split(" ")[1:][1:-1]:
                    dict_java[x.split(" ")[0]].append((i[:-1]))
    for i in dict_java:
        if i:
            dict_java[i]=[" ".join(dict_java[i])]

    x=pd.read_csv('spark-scripts/inverted_index.csv').fillna('')
    dic_python=x.to_dict('split')
    check={}
    for i in dic_python['data']:
        if i:
            check[i[0]]=i[1:]
    for i in check:
        if i!='':
            if not (dict_java[i]==check[i]):
                print('The inverted index output does not match with the spark output')
                return
    print('-------------------------------------------------------')
    print('The inverted index output matches with the spark output')
    print('-------------------------------------------------------')
    
check_inverted()