import pandas as pd
data = pd.DataFrame(None,index=['A','B','C'])
print(data)
data2 = pd.DataFrame([[1, 2], [3, 4],[5,6]],index=['A','B','C'])
print(data2)
# data3 = real_data.append(data2,ignore_index=True)
data3=pd.concat([data,data2],axis=1)
data3=pd.concat([data3,data2],axis=1,ignore_index=True)

print(data3)
