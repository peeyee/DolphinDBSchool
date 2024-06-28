import dolphindb as ddb
import numpy as np
import pandas as pd
import time
import math
from tqdm import tqdm

s = ddb.session()
conn = s.connect(host="1.1.1.1", port=8911, userid="admin", password="DolphinDB@123")
# 替换自己的 ip/port

if conn:
    print("successfully connected!")

data_path = "C:\\Users\dolphin\Desktop\\4.16高金培训\homework\\20210901tick\\20210901tick.csv"
df = pd.read_csv(data_path)
df["SecurityID"] = df["SecurityID"].astype('str') + ".SH"
df["TradeTime"] = pd.to_datetime(df["TradeTime"], format='%Y.%m.%dT%H:%M:%S.%f')  # convert str to datetime
begin = time.time()
tbAppender = ddb.tableAppender(dbPath="dfs://demo_tick", tableName="tick", ddbSession=s)
# 数据量： 47723974 时间：113 s


k = 50000
total_iter = math.floor(len(df) / k)
progress_bar = tqdm(total=total_iter, desc="Writing to DDB", unit=" 50K")
for i in range(0, math.ceil(len(df) / k) * k, k):
    if i + k > len(df):
        end = len(df)
    else:
        end = i + k
        tbAppender.append(df.iloc[i:end, ])

    progress_bar.update(1)

progress_bar.close()
print(time.time() - begin)
