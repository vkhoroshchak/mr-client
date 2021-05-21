from dask import dataframe

a = dataframe.read_csv("/home/ad1/PycharmProjects/MapReduce/SQL-manager/A.csv", sep=",")
b = dataframe.read_csv("/home/ad1/PycharmProjects/MapReduce/SQL-manager/B.csv")
a = a.drop()

a.merge(b)
