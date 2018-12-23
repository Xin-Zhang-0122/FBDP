bayes=[]
index=[]
name=[]
for line in open("E:/南大文档/课件/大三上/大数据处理与编程/Lab4/bayes_data.txt",encoding='utf-8'):
    bayes.append([float(line.split("  ")[0]),float(line.split("  ")[1]),float(line.split("  ")[2][:-1])])
for temp in range(0,100871):
    for line in open("E:/南大文档/课件/大三上/大数据处理与编程/Lab4/bayes_file/"+str(temp)+".txt",encoding='utf-8'):
        b_neg=0;b_neu=0;b_pos=0;
        name.append(line.split("%%")[0])
        a=line.split("%%")[1].split(" ")
        a.pop(-1)
        if(a[0][0]=='%'):
            a[0]=a[0][1:]
        for i in range(0,2585):
            a[i]=float(a[i])
        for i in range(0,2585):
            b_neg+=a[i]*bayes[i][0]
            b_neu+=a[i]*bayes[i][1]
            b_pos+=a[i]*bayes[i][2]
        if(b_neg>b_neu and b_neg>=b_pos):
            index.append(-1)
        elif(b_neu>b_neg and b_neu>=b_pos):
            index.append(0)
        elif(b_pos>=b_neg and b_pos>=b_neu):
            index.append(1)
file=open("E:/User/Bayes.txt",'w',encoding='utf-8')
for i in range(0,100871):
    if(index[i]==-1):
        sign="neg"
    elif(index[i]==0):
        sign="neu"
    else:
        sign="pos"
    file.write(name[i]+"   "+sign+"\n")