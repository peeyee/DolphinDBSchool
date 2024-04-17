import dolphindb as ddb

s = ddb.session(enableSSL=False)
conn = s.connect(host="1.1.1.1", port=8911, userid="admin", password="123456")

"""
login("admin", "DolphinDB@123")
try{
    deleteUser("u1")
    dropFunctionView("getFactor")
}catch(ex){print ex}

createUser("u1", "u1@1234")
def getFactor(factorStr, n){
    t=select * from loadTable("dfs://level2FactorDB","level2FactorTB") 
      where factorName=factorStr
      limit n
    return t
}

addFunctionView(getFactor)
grant("u1", VIEW_EXEC, "getFactor")

login("u1", "u1@1234")
undef("getFactor", DEF)
getUserAccess()
"""





#case1: call functionView
factor1 = s.run("getFactor", "flow", 1000)
print(factor1)

#case2: call module
"""
see https://github.com/dolphindb/DolphinDBModules
alpha101 alpha191 mytt ta
"""




prepareData = """
use wq101alpha
data = loadTable("dfs://k_day_level", "k_day")
input1 = exec close from data where tradetime between %s : %s pivot by tradetime, securityid
""" % ("2010.01.01T00:00:00.000", "2010.01.29T00:00:00.000")
s.run(prepareData)
panelAlpha101 = s.run("WQAlpha1(input1).setIndexedMatrix!()")
print(panelAlpha101)
