import dolphindb as ddb

s = ddb.session()
conn = s.connect(host="1.1.1.1", port=8911)
# 替换自己的 ip/port


if conn:
    print("successfully connected!")

# 若失败，会抛出异常：Failed to connect to host = 1.1.1.1 port = 8911 with error code 10061

s.close()
