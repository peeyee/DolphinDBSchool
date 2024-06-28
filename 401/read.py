import dolphindb as ddb

s = ddb.session(enableSSL=False)
conn = s.connect(host="1.1.1.1", port=8911, userid="admin", password="123456")
# 替换自己的 ip/port

if conn:
    print("successfully connected!")
# case1 use run sql-string
sql = """
    select first(TradePrice) as open
        , max(TradePrice) as high
        , min(TradePrice) as low
        , last(TradePrice) as close
        , wavg(TradePrice, TradeQty) as vwap 
        , sum(TradeQty) as totalQty
        , sum(TradePrice * TradeQty) as totalAmount
    from loadTable("%s", "%s") 
    where date(tradetime)=2021.09.01
    group by SecurityID, interval(TradeTime, 10m, 'prev') as TradeTime
""" % ("dfs://demo_tick", "tick")
print(s.run(sql))


# case2 use sql method

tick = s.loadTable(tableName="tick", dbPath="dfs://demo_tick")
data = tick.select([
    "first(TradePrice) as open"
    ,"max(TradePrice) as high"
    ,"min(TradePrice) as low"
    ,"last(TradePrice) as close"
    ,"wavg(TradePrice, TradeQty) as vwap"
    ,"sum(TradeQty) as totalQty"
    ,"sum(TradePrice * TradeQty) as totalAmount"
     ]).where("date(tradetime)=2021.09.01")\
    .groupby(["SecurityID", "interval(TradeTime, 10m, 'prev') as TradeTime"])\
    .toDF()

print(data)

# case2.2
factor = s.loadTable(dbPath="dfs://level2FactorDB", tableName="level2FactorTB")
data2 = factor.select("bfill(value)")\
    .where("tradetime >= 2021.01.06T10:09:30.000")\
    .where("tradetime <= 2021.01.06T10:09:35.000")\
    .pivotby("tradetime", "securityid").toDF()

print(data2)

s.close()