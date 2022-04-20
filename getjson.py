import json
from processor_db import *
import requests
requests.adapters.DEFAULT_RETRIES = 5
def ask4region(ip):
    s = requests.session()
    s.keep_alive = False
    str='http://ip.taobao.com/outGetIpInfo?ip='+ip+'&accessKey=alibaba-inc'
    response=requests.get(str)
    response_obj=response.json()
    node={'country':response_obj['data']['country'],'ip':response_obj['data']['queryIp']}
    return node

def createjson(name,G,id2ip):
    filename=name+'.json'
    content=dict()
    nodes=list(G.nodes())
    Num=0
    for i in nodes:
        ip=id2ip[i]
        try:
            region = ask4region(ip)
            region.update({'nodeid': i})
            Num += 1
            region.update({'Num': Num})
            content.update({'nodeNum_' + Num.__str__(): region})
        except Exception as e:
            print(e)
    connections=list(G.edges())
    content.update({'connections':connections})
    tojson=json.dumps(content,ensure_ascii=False)
    #print(tojson)
    with open(filename,'w',encoding='utf-8') as file_obj:
        file_obj.seek(0)
        file_obj.truncate()  # 清空文件
        file_obj.write(tojson)
        file_obj.close()