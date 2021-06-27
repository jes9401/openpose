import glob
import pandas as pd
import os
local_path = './'
file_list = glob.glob(os.path.join(local_path, '*.txt'))
file_list = list(map(lambda x:x.split('\\')[-1],file_list))

for i in file_list:
    file = pd.read_csv(i)
    name = i[:-3]+"csv"
    file.to_csv(name,index=False)
