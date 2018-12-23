knn.java 通过修改读入的文件，统计了pos、neg、neu以及fulldata.txt文件中出现在chi_words.txt中的词频

tf_idf.java 计算了每个pos、neg、neu中的文件的 tf_idf 值，为 1*2585 维向量，即 chi_words 中2585个特征

tf_idf_train.java 计算了fulldata.txt中每条新闻的 tf_idf 值，同样为 1*2585 维向量
