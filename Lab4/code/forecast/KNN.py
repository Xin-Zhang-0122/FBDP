name=[]
index=[]
neu=[]
neg=[]
pos=[]
i=0
for line in open("E:/南大文档/课件/大三上/大数据处理与编程/Lab4/neg_tfidf.txt",encoding='utf-8'):
    neg.append(line.split(" "))
    neg[i].pop(-1)
    for j in range(0,2585):
        neg[i][j]=float(neg[i][j])
    i+=1
i=0
for line in open("E:/南大文档/课件/大三上/大数据处理与编程/Lab4/neu_tfidf.txt",encoding='utf-8'):
    neu.append(line.split(" "))
    neu[i].pop(-1)
    for j in range(0,2585):
        neu[i][j]=float(neu[i][j])
    i+=1
i=0
for line in open("E:/南大文档/课件/大三上/大数据处理与编程/Lab4/pos_tfidf.txt",encoding='utf-8'):
    pos.append(line.split(" "))
    pos[i].pop(-1)
    for j in range(0,2585):
        pos[i][j]=float(pos[i][j])
    i+=1
for t in range(0,100871):
    a=[]
    for line in open("E:/南大文档/课件/大三上/大数据处理与编程/Lab4/file/"+str(t)+".txt",encoding='utf-8'):
        name.append(line.split("%%")[0])
        a=line.split("%%")[1].split(" ")
        a.pop(-1)
        if(a[0][0]=='%'):
            a[0]=a[0][1:]
        for i in range(0,2585):
            a[i]=float(a[i])
    result=[]
    for i in range(0,470):
        Sum=0
        for j in range(0,2585):
            Sum+=neg[i][j]*a[j]
        result.append([Sum,-1])
    for i in range(0,512):
        Sum=0
        for j in range(0,2585):
            Sum+=neu[i][j]*a[j]
        result.append([Sum,0])
    for i in range(0,516):
        Sum=0
        for j in range(0,2585):
            Sum+=pos[i][j]*a[j]
        result.append([Sum,1])
    result=sorted(result,reverse=True)
    Sum=0
    for i in range(0,4):
        Sum+=result[i][0]*result[i][1]
    if Sum>16:
        index.append(1)
    elif Sum<-16:
        index.append(-1)
    else:
        index.append(0)
file=open("E:/User/KNN.txt",'a',encoding='utf-8')
for i in range(0,100871):
    if(index[i]==-1):
        sign="neg"
    elif(index[i]==0):
        sign="neu"
    else:
        sign="pos"
    file.write(name[i]+"   "+sign+"\n")
file.close()