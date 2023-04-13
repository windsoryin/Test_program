import pandas as pd
a=range(5)
b=range(5,10)
c=range(10,15)
data=pd.DataFrame([a,b,c]).T
data.columns=["a","b","c"]
print(data)
data["a"]=data[["b"]].apply(lambda x:x["b"]+1,axis=1)
print(data)