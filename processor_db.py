import asyncio
import datetime
import math
import pymysql
import aiomysql
import matplotlib.pyplot as plt
import sha3
import net
import networkx as nx
def keccak256(s):
    k = sha3.keccak_256()
    k.update(s)
    return k.digest()
def big_endian_to_int(value: bytes) -> int:
    return int.from_bytes(value, "big")
class Db:
    def __init__(self,dbconfig):
        self.dbconfig=dbconfig
    def connect(self):
        self.conn=pymysql.connect(host=self.dbconfig['databaseip'], port=self.dbconfig['databaseport'],
                                user=self.dbconfig['databaseuser'], password=self.dbconfig['databasepassword'], db=self.dbconfig['database'])
    def execute(self,sql,hasret=1):
        cursor=self.conn.cursor()
        cursor.execute(sql)
        if hasret:
            return cursor.fetchall()
    def close(self):
        self.conn.close()



