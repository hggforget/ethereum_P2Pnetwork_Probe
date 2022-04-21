import json
import xlrd
import openpyxl
import pandas as pd
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
    hashrate_file = 'hashret.xlsx'
    hashrate = pd.read_excel(hashrate_file, header=0)
    print(hashrate.columns)
    regions = hashrate['country']
    regions = set(regions.values)
    Num=0
    map_id2Num=dict()
    for node in nodes:
        Num+=1
        map_id2Num.update({node:Num})
    for node in nodes:
        ip=id2ip[node]
        try:
            region = ask4region(ip)
            region.update({'nodeid': node})
            region.update({'Num': map_id2Num.get(node)})
            if {region.get('country')}&regions:
                region.update({'region':region.get('country')})
            else:
                region.update({'region':'其他'})
            region.update({'NumConnections':G.degree(node)})
            neighbors=list()
            for neighbor in G.edges(node):
                neighbors.append(map_id2Num.get(neighbor[1]))
            region.update({'neighbors':neighbors})
            content.update({'nodeNum_' + map_id2Num.get(node).__str__(): region})
            print(region)
        except Exception as e:
            print(e)
    content.update({'Num':Num})
    tojson=json.dumps(content,ensure_ascii=False)
    #print(tojson)
    with open(filename,'w',encoding='utf-8') as file_obj:
        file_obj.seek(0)
        file_obj.truncate()  # 清空文件
        file_obj.write(tojson)
        file_obj.close()
