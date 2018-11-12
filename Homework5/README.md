实现过程：

    类名：cluster      记录cluster中心的信息
          point        记录文件中每个点      其中函数 choose_id(cluster) 的作用是找到距离point最近的cluster的id
     
   首先对整个程序初始化，选择Instance文件中前k个点作为初始的cluster，k为聚类数；
   随后给定迭代次数times，执行times次mapreduce；
   最后通过result()函数生成聚类文件。
   
    其中，map函数调用choose_id()选出最近的cluster，将该id作为键，坐标(x, y)作为值传入reduce
    reduce根据结果计算出新的cluster中心坐标并生成新的cluster文件。
