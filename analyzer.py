# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
from processor_db import *
from net import *
import getjson
import aiomysql
import asyncio
# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    dbconfig = {'sourcetable': 'ethereum', 'database': 'topo_p2p', 'databaseip': 'localhost',
                'databaseport': 3306, 'databaseuser': 'root', 'databasepassword': 'hggforget'}
    db=Db(dbconfig)
    db.connect()
    total=db.execute("SELECT DISTINCT nodeid,ip FROM ethereum")
    id2ip=dict()
    total_tmp=set()
    for i in total:
        total_tmp.add(i[0])
        id2ip.update({i[0]:i[1]})
    active = db.execute("SELECT DISTINCT nodeid FROM ethereum_active_nodes")
    nodes=set()
    for i in active:
        nodes.add(i[0])
    print(len(total).__str__()+"  探测到的所有节点")
    route_node = db.execute("SELECT DISTINCT nodeid1  FROM ethereum_neighbours")
    print(len(route_node).__str__() + "  有路由表的节点")
    route=set()
    for i in route_node:
        route.add(i[0])
    dis = db.execute("SELECT DISTINCT nodeid2  FROM ethereum_neighbours")
    print(len(dis).__str__() + " distinct connected nodes")
    all = db.execute("SELECT  nodeid2  FROM ethereum_neighbours")
    print(len(all).__str__() + "  connected nodes")
    new_dis = set()
    diss=set()
    new_all=set()
    for i in dis:
        new_dis.add(i[0])
        diss.add(i[0])
    new_all = set()
    for i in all:
        if {i[0]} & new_dis:
            new_dis.remove(i[0])
        else:
            new_all.add(i[0])
    print(len(new_all).__str__() + "  Nodes with degrees greater than 1")
    conns = db.execute("SELECT nodeid1,nodeid2  FROM ethereum_neighbours")
    num=list()
    G=buildnet(nodes,conns)
    G2=buildnet((new_all|route),conns)
    G3=buildnet((new_all|route)&nodes,conns)
    G4=buildnet(total_tmp,conns)
    print("________________________________")
    print("所有节点组成的网络")
    #net_analyzer(G4)
    print("--------------------------------")
    print("活跃节点（有pong回应的节点）组成的网络")
    #net_analyzer(G)
    print("--------------------------------")
    print("度大于1的节点 | 有路由表的节点  组成的网络")
    #net_analyzer(G2)
    print("--------------------------------")
    print("(度大于1的节点 | 有路由表的节点) & 活跃节点  组成的网络")
    net.net_analyzer(G3)
    getjson.createjson('G3',G3,id2ip)
    print("--------------------------------")
    for i in route_node:
        sql="SELECT nodeid2 from ethereum_neighbours where nodeid1='%s'" % (i[0])
        nodes =db.execute(sql)
        num.append(len(nodes))
        bucket = set()
        for node in nodes:
            if int(node[0], 16) ^ int(i[0], 16) == 0 or int(math.log2(int(node[0], 16) ^ int(i[0], 16)) - 239) < 0:
                bucket.add(0)
                continue
            bucket.add(int(math.log2(int(node[0], 16) ^ int(i[0], 16)) - 239))
    # print(bucket)
    # print(i[0])
    conns_num=db.execute("SELECT COUNT(*)AS connections  FROM ethereum_neighbours")
    print("平均路由表大小: "+(conns_num[0][0] / len(route_node)).__str__())
    plt.hist(num, bins=10)
    plt.xlabel("connections")
    plt.ylabel("nodes")
    plt.title("nodes in routing table distribution")
    plt.show()
    db.close()
