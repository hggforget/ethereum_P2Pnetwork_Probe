#pip install pysha3
#pip install eciespy
#pip install netifaces
#pip install rlp
#pip install aioredis
import asyncio
import math
import socket
import os
import secrets

import pandas as pd

from boostrapnodes import BOOTNODES
import urllib.parse
import binascii
import sha3
import rlp
import struct
from ecies.utils import generate_eth_key
import time
from typing import Any,List
from typing import NewType
Hash32 = NewType('Hash32', bytes)
from eth_keys.datatypes import PrivateKey,PublicKey
from eth_keys import keys
import ipaddress
import netifaces
import warnings
warnings.filterwarnings("ignore")
CMD_PING=1
CMD_PONG = 2
CMD_FIND_NODE = 3
CMD_NEIGHBOURS =4
CMD_ENR_REQUEST = 5
CMD_ENR_RESPONSE = 6
MAC_SIZE = 256 // 8
SIG_SIZE = 520 // 8  # 65
MODE=True
HEAD_SIZE = MAC_SIZE + SIG_SIZE
CYCLE_TIME=int(60*60)
BEGIN_TIME=int(time.time())
sem = asyncio.Semaphore(100) #一次最多同时连接查询100个节点

'''
    获取外部ip地址  算是一种内网转外网
'''
def write_log(str):
    with open('data.txt', 'a+') as f:
        f.write(str)  # 文件的写操作
        f.write('\n')
def get_external_ipaddress() -> ipaddress.IPv4Address:
    for iface in netifaces.interfaces():
        for family, addresses in netifaces.ifaddresses(iface).items():
            if family != netifaces.AF_INET:
                continue
            for item in addresses:
                iface_addr = ipaddress.ip_address(item['addr'])
                if iface_addr.is_global:
                    return iface_addr
    return ipaddress.ip_address('127.0.0.1')

'''
    获得包的过期时间
'''
def _get_msg_expiration() -> bytes:
    return rlp.sedes.big_endian_int.serialize(int(time.time() + 300))

'''
    载入公钥 若不存在key文件 则自动创建
'''
def init():
    if os.path.exists('key'):
        try:

            with open('key','rb') as f:
                eth_k=PrivateKey(f.read())
                return eth_k
        except Exception as e:
            write_log('55 '+e.__str__())
            print('55',e)
            pass
    eth_k = generate_eth_key()
    with open('key','wb') as f:
       f.write(eth_k.to_bytes())
    return eth_k
eth_k=init()
'''
    加密
'''

def keccak256(s):
    k = sha3.keccak_256()
    k.update(s)
    return k.digest()
myid= keccak256(eth_k.public_key.to_bytes()).hex()


sequence_number=111
def get_local_enr_seq():
    global sequence_number
    sequence_number+=1
    return sequence_number
'''
    int转大端排列 （符合网络传输的格式要求
'''
def int_to_big_endian4(integer: int) -> bytes:
    return struct.pack('>I', integer)
def int_to_big_endian(value: int) -> bytes:
    return value.to_bytes((value.bit_length() + 7) // 8 or 1, "big")
def big_endian_to_int(value: bytes) -> int:
    return int.from_bytes(value, "big")
def enc_port(p: int) -> bytes:
    return int_to_big_endian4(p)[-2:]
'''
    判断消息是否过期
'''
def _is_msg_expired(rlp_expiration: bytes) -> bool:
    expiration = rlp.sedes.big_endian_int.deserialize(rlp_expiration)
    if time.time() > expiration:
        return True
    return False

class Address():
    def __init__(self, ip: str, udp_port: int, tcp_port: int=0) -> None:
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self._ip = ipaddress.ip_address(ip)

    @property
    def is_loopback(self) -> bool:
        return self._ip.is_loopback

    @property
    def is_unspecified(self) -> bool:
        return self._ip.is_unspecified

    @property
    def is_reserved(self) -> bool:
        return self._ip.is_reserved

    @property
    def is_private(self) -> bool:
        return self._ip.is_private

    @property
    def ip(self) -> str:
        return str(self._ip)
    def ip_packed(self) -> str:
        """The binary representation of this IP address."""
        return self._ip.packed

    def __eq__(self, other: Any) -> bool:
        return (self.ip, self.udp_port) == (other.ip, other.udp_port)

    def __repr__(self) -> str:
        return 'Address(%s:udp:%s|tcp:%s)' % (self.ip, self.udp_port, self.tcp_port)

    def to_endpoint(self) -> List[bytes]:

        return [self._ip.packed, enc_port(self.udp_port), enc_port(self.tcp_port)]

def _pack_v4(cmd_id, payload, privkey) -> bytes:
    cmd_id_bytes = int(cmd_id).to_bytes(1,byteorder='big')
    encoded_data = cmd_id_bytes + rlp.encode(payload)
    signature = privkey.sign_msg(encoded_data)
    message_hash = keccak256(signature.to_bytes() + encoded_data)
    return message_hash + signature.to_bytes() + encoded_data

def _unpack_v4(message: bytes):
    message_hash = Hash32(message[:MAC_SIZE])
    if message_hash !=keccak256(message[MAC_SIZE:]):
        raise Exception("Wrong msg mac")
    signature = keys.Signature(message[MAC_SIZE:HEAD_SIZE])
    signed_data = message[HEAD_SIZE:]
    remote_pubkey = signature.recover_public_key_from_msg(signed_data)
    cmd_id = message[HEAD_SIZE]
    payload = tuple(rlp.decode(message[HEAD_SIZE + 1:], strict=False))
    return remote_pubkey, cmd_id, payload, message_hash
async def sendlookuptonode(remote_publickey,remote_address):
    global Kbucket
    targetKeys = Kbucket.get(remote_publickey)
    if not targetKeys:
        targetKeys=cal_Kbucket(remote_publickey)
    tasks=[]
    for value in targetKeys:
        expiration=_get_msg_expiration()
        tasks.append(send(remote_address, CMD_FIND_NODE, (value, expiration)))
    await asyncio.wait(tasks)
async def addtodb_active(db,arr):
    '''
        将活跃节点加入数据库
    '''
    if arr:
        sql = "insert ignore into ethereum_active_nodes (nodeid,ip,port,publickey,pongtime) values (%s,%s,%s,%s,%s)"
        await db.executemany(sql,arr)

async def recv_pong_v4(remote_publickey,remote_address, payload, _: Hash32,db) -> None:
    # The pong payload should have at least 3 elements: to, token, expiration
    nowtime = int(time.time())
    '''
        若己方收到PONG报文：说明我们PING对方的时候，对方有回应，则该节点为活跃节点，加入数据库
    '''
    if nowtime<=BEGIN_TIME+CYCLE_TIME and MODE:
        await addtodb_active(db, [[keccak256(remote_publickey.to_bytes()).hex(), remote_address[0], remote_address[1], remote_publickey.to_bytes().hex(),nowtime]])
        sql = f"update ethereum set pingtime='%d' where publickey= '%s'" %(nowtime,remote_publickey.to_bytes().hex())
        await redis.execute(sql)
    await sendlookuptonode(remote_publickey,remote_address)



async def addtodb(db,arr):
    if arr:
        sql = "insert ignore into ethereum (nodeid,ip,port,publickey) values (%s,%s,%s,%s)"
        await db.executemany(sql,arr)

import datetime
RECV_NUM=1
async def recv_neighbours_v4(remote_publickey,remote_address, payload, _: Hash32,db) -> None:

    # The neighbours payload should have 2 elements: nodes, expiration
    if len(payload) < 2:
        write_log('neighbors wrong')
        print('neighbors wrong')
        return
    write_log("nerighbor!")
    print('nerighbor!')
    nodes, expiration = payload[:2]
    arr=[]
    update_arr=[]
    nodeid1=keccak256(remote_publickey.to_bytes()).hex()
    tm = datetime.datetime.now()
    for item in nodes:
        try:
            ip, udp_port, tcp_port, publickey = item
            ip=ipaddress.ip_address(ip)
            udp_port=big_endian_to_int(udp_port)
            node_id=keccak256(publickey).hex()
            update_arr.append([nodeid1,node_id,tm,int(time.time()),RECV_NUM])
            arr.append([node_id,str(ip),udp_port,publickey.hex()])
        except Exception as e:
            write_log(e.__str__())
            print(e)
            continue
    if arr and int(time.time())<=BEGIN_TIME+CYCLE_TIME and MODE:
        await addtodb(db,arr)
    if update_arr:

        sql="insert into ethereum_neighbours (nodeid1,nodeid2,update_time,intTIME,RECV_NUM) values (%s,%s,%s,%s,%s)"
        await db.executemany(sql,update_arr)


async def recv_ping_v4(
        remotepk,remote_address, payload, message_hash: Hash32,db) -> None:
    targetnodeid = keccak256(remotepk.to_bytes())
    if targetnodeid.hex()  == myid:
        return
    if int(time.time())<=(BEGIN_TIME+CYCLE_TIME) and MODE:
        write_log(int(time.time()).__str__()+"   "+(BEGIN_TIME+CYCLE_TIME).__str__())
        print(int(time.time()).__str__()+"   "+(BEGIN_TIME+CYCLE_TIME).__str__())
        write_log('ping insert '+[targetnodeid.hex(),remote_address[0],remote_address[1],remotepk.to_bytes().hex(),0].__str__())
        print('ping insert',[targetnodeid.hex(),remote_address[0],remote_address[1],remotepk.to_bytes().hex(),0].__str__())
        await addtodb(db,[[targetnodeid.hex(),remote_address[0],remote_address[1],remotepk.to_bytes().hex()]])

    if len(payload) < 4:
        write_log('error ping')
        print('error ping')
        return
    elif len(payload) == 4:
        _, _, _, expiration = payload[:4]
        enr_seq = None
    else:
        _, _, _, expiration, enr_seq = payload[:5]
        enr_seq = big_endian_to_int(enr_seq)
    if _is_msg_expired(expiration):
        #print('msg ping timeout')
        return
    expiration = _get_msg_expiration()
    local_enr_seq = get_local_enr_seq()
    payload = (Address(remote_address[0],remote_address[1],remote_address[1]).to_endpoint(),message_hash, expiration, int_to_big_endian(local_enr_seq))
    await send(remote_address, CMD_PONG, payload)



def _get_handler(cmd):
    if cmd == CMD_PING:
        return recv_ping_v4
    elif cmd == CMD_PONG:
        return recv_pong_v4
    elif cmd == CMD_FIND_NODE:
        return None
    elif cmd == CMD_NEIGHBOURS:
        return recv_neighbours_v4
    elif cmd == CMD_ENR_REQUEST:
        return None
    elif cmd == CMD_ENR_RESPONSE:
        return None

def _onrecv(sock,db):
    try:
        data,remoteaddr=sock.recvfrom(1280*2)
    except Exception as e:
        return
    try:
        remote_pubkey, cmd_id, payload, message_hash = _unpack_v4(data)



    except Exception as e:
        write_log(e.__str__())
        print(e)
        return
    if cmd_id not in [CMD_PING,CMD_PONG,CMD_NEIGHBOURS]:
        return
    handler = _get_handler(cmd_id)
   # asyncio.create_task(handler(remote_pubkey,remoteaddr,payload, message_hash,db))
    return handler(remote_pubkey,remoteaddr,payload, message_hash,db)

async def send_ping_v4(hostname,port):

    version = rlp.sedes.big_endian_int.serialize(4)
    expiration = rlp.sedes.big_endian_int.serialize(int(time.time() + 300))
    local_enr_seq = get_local_enr_seq()
    payload = (version, Address('127.0.0.1',30303,30303).to_endpoint(), Address(hostname,port,port).to_endpoint(),
               expiration, int_to_big_endian(local_enr_seq))
    await send((hostname,port),1,payload)


async def inibootstrapnode(redis):
    ids=[]
    arr=[]
    for nodeurl in BOOTNODES:
        node_parsed = urllib.parse.urlparse(nodeurl)
        raw_pubkey = binascii.unhexlify(node_parsed.username)
        hostname=node_parsed.hostname
        strid=keccak256(raw_pubkey).hex()
        hid='hid:'+strid
        arr.append([strid,raw_pubkey.hex(),hostname,node_parsed.port,0])
    if arr:
        sql="insert ignore into ethereum (nodeid,publickey,ip,port,pingtime) values (%s,%s,%s,%s,%s)"
        await redis.executemany(sql,arr)

async def find_node_to_lookup(redis):
    sql = f"select id,nodeid,ip,port,publickey from ethereum_active_nodes"
    result = await redis.execute(sql, 1)
    write_log(len(result).__str__()+'  len')
    tasks=[]
    for row in result:
        nodeid = row[1]
        ip= row[2]
        port=row[3]
        try:
            pk=PublicKey(int_to_big_endian(int(row[4],16)).rjust(512 // 8, b'\x00'))
        except Exception as e:
            write_log(e.__str__()+row[4])
            print(e,row[4])
        if myid != nodeid:
            tasks.append(sendlookuptonode(pk,(ip, int(port))))
            print("lookup ",pk)
    if tasks:
        await asyncio.wait(tasks)

async def find_node_to_ping(redis):
    nowtime=int(time.time())
    sql = f"select id,nodeid,ip,port,publickey from ethereum where pingtime='%d'" %(0)
    result = await redis.execute(sql, 1)
    '''
        tmpid = [str(row[0]) for row in result]
        if tmpid:
        sql = f"update ethereum set pingtime={nowtime + 300} where id in ({','.join(tmpid)})"
        await redis.execute(sql)
    
    '''
    tasks = []
    for row in result:
        nodeid = row[1]
        ip= row[2]
        port=row[3]
        if myid != nodeid:
            tasks.append(asyncio.create_task(send_ping_v4(ip, int(port))))
    if tasks:
        await asyncio.wait(tasks)
Kbucket=dict()
def cal_Kbucket(remote_publickey):
    global Kbucket
    '''
            此处为感知路由表的算法
      count:用位来表示某个K桶是否已获得，第i位为1代表第i个K桶已获得，第i位为0代表第i个K桶未获得
    '''
    count = (1 << 17) - 1
    now = 1
    nowid = keccak256(remote_publickey.to_bytes())
    value = set()
    value.add(remote_publickey.to_bytes())
    while now != count:
        index = 0
        target_key = int_to_big_endian(secrets.randbits(512)).rjust(512 // 8, b'\x00')
        target_id = keccak256(target_key) # 随机生成target_id
        dis = (big_endian_to_int(target_id) ^ big_endian_to_int(nowid))
        '''
                计算target_id与要感知路由表的节点的id 之间的异或距离
        '''
        for i in range(1, 17):
            if (1 << (239 + i)) & dis:
                index = i
        '''
                推测该target_id应该落入哪个桶
        '''
        if not (1 << index) & now:
            '''
                若该target_id所在的桶尚未请求过，则发送FIND_NODE报文
                并记录已被请求的桶
            '''
            now = now | (1 << index)
            value.add(target_key)
    Kbucket.update({remote_publickey:value})
    return value
async def inical(redis):
    global Kbucket
    sql = f"select DISTINCT nodeid,publickey from ethereum_active_nodes"
    result = await redis.execute(sql, 1)
    for row in result:
        remote_publickey=PublicKey(int_to_big_endian(int(row[1],16)).rjust(512 // 8, b'\x00'))
        Kbucket.update({remote_publickey:cal_Kbucket(remote_publickey)})

async def main(db):
    global RECV_NUM
    if MODE:
        await inibootstrapnode(redis)
    else:
        await inical(redis)
    while 1:
        if int(time.time()) <= BEGIN_TIME+CYCLE_TIME and MODE:
            await find_node_to_ping(redis)
            await find_node_to_lookup(redis)
            # await asyncio.sleep(180)
        else:
            RECV_NUM+=1
            for i in range(20):
                await find_node_to_lookup(redis)
                # await asyncio.sleep(180)
from db import Db
async def getredis():
    dbconfig = {'sourcetable': 'ethereum', 'database': 'p2p', 'databaseip': 'localhost',
                'databaseport': 3306, 'databaseuser': 'root', 'databasepassword': '', 'condition': '',
                'conditionarr': []}
    db=await Db(dbconfig)

    return db
async def send(ipport, cmd_id, payload) -> bytes:
    global eth_k,sock,sem
    async with sem:
        message = _pack_v4(cmd_id, payload, eth_k)
        try:
            sock.sendto(message, ipport)
        except Exception as e:
            write_log(ipport.__str__())
            print(ipport)
    return message
if __name__=='__main__':
    with open('data.txt', "a") as f:
        f.seek(0)
        f.truncate()   #清空文件
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0',30314))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    redis=loop.run_until_complete(getredis())
    loop.add_reader(sock, _onrecv,sock,redis)
    loop.create_task(main(redis))
    loop.run_forever()
