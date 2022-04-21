# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
from processor_db import *
from net import *
import getjson
import aiomysql
import asyncio
# 按间距中的绿色按钮以运行脚本。
CYCLE_TIME=0
if __name__ == '__main__':
    with open('CYCLE_TIME.txt', 'r', encoding='utf-8') as file_obj:
        data=file_obj.read()
        CYCLE_TIME=data
        print(CYCLE_TIME)
        file_obj.close()
    dbconfig = {'sourcetable': 'ethereum', 'database': 'topo_p2p5', 'databaseip': 'localhost',
                'databaseport': 3306, 'databaseuser': 'root', 'databasepassword': 'hggforget'}
    db=Db(dbconfig)
    db.connect()
    total=db.execute("SELECT DISTINCT nodeid,ip FROM ethereum")
    id2ip=dict()
    total_tmp=set()
    ips=dict()
    ids=dict()
    for i in total:
        total_tmp.add(i[0])
        id2ip.update({i[0]:i[1]})
        if ips.get(i[0]):
            tmp=ips.get(i[0])
            tmp.add(i[1])
            ips.update({i[0]:tmp})
        else:
            ips.update({i[0]:{i[1]}})
        if ids.get(i[1]):
            tmp=ids.get(i[1])
            tmp.add(i[0])
            ids.update({i[1]:tmp})
        else:
            ids.update({i[1]:{i[0]}})
    id2ips=list()
    ip2ids=list()
    for i in ips:
        try:
            if len(ips.get(i))>1:
                id2ips.append(len(ips.get(i)))
        except:
            print(i)
    for i in ids:
        try:
            if len(ids.get(i)) > 1:
                ip2ids.append(len(ids.get(i)))
        except:
            print(i)
    total = db.execute("SELECT DISTINCT nodeid FROM ethereum")
    print("一个nodeid有多个ip")
    print(len(id2ips).__str__() + " : " +len(total).__str__())
    print("一个ip有多个nodeid")
    print(len(ip2ids).__str__() + " : " +len(total).__str__())
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
    conns = db.execute("SELECT DISTINCT nodeid1,nodeid2  FROM ethereum_neighbours")
    num=list()
    G=buildnet(nodes,conns)
    G2=buildnet((new_all|route),conns)
    G3=buildnet((new_all|route)&nodes,conns)
    G4=buildnet(total_tmp,conns)
    print("________________________________")
    print("所有节点组成的网络")
    net_analyzer(G4)
    print("--------------------------------")
    print("活跃节点（有pong回应的节点）组成的网络")
    net_analyzer(G)
    print("--------------------------------")
    print("度大于1的节点 | 有路由表的节点  组成的网络")
    net_analyzer(G2)
    print("--------------------------------")
    print("(度大于1的节点 | 有路由表的节点) & 活跃节点  组成的网络")
    #pltnet(G3)
    net.net_analyzer(G3)
    #getjson.createjson('init_data',G3,id2ip)
    print("--------------------------------")
    for i in route_node:
        sql="SELECT DISTINCT nodeid2 from ethereum_neighbours where nodeid1='%s'" % (i[0])
        nodes =db.execute(sql)
        num.append(len(nodes))
        bucket = set()
        for node in nodes:
            if int(node[0], 16) ^ int(i[0], 16) == 0 or int(math.log2(int(node[0], 16) ^ int(i[0], 16)) - 239) < 0:
                bucket.add(0)
                continue
            bucket.add(int(math.log2(int(node[0], 16) ^ int(i[0], 16)) - 239))
        #print(bucket)
        #print(i[0])
        #print(len(nodes))
    print("平均路由表大小: "+(len(conns) / len(route_node)).__str__())
    plt.hist(num, bins=10)
    plt.xlabel("connections")
    plt.ylabel("nodes")
    plt.title("nodes in routing table distribution")
    plt.show()
    db.close()
