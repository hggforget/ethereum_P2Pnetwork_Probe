import asyncio
import aiomysql
class Db(object):
    def __init__(self,dbconfig, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbconfig=dbconfig
        self.closed=0
    async def executemany(self,sql,data,ignoreerror=False):
        while data:
            try:
                async with self.db.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.executemany(sql, data)
                        return 1

            except Exception as e:
                print(e)
                await asyncio.sleep(1)
                if ignoreerror:
                    return 1

    async def execute(self,sql,hasret=0):
        while 1:
            try:
                async with self.db.acquire() as conn:
                    async with conn.cursor() as cur:
                        result = 0
                        await cur.execute(sql)
                        if hasret:
                            result = await cur.fetchall()
                        return result
            except Exception as e:
                print(e)
                await asyncio.sleep(1)
    async def connect(self):
        if 1:
            while 1:
                try:
                    loop=asyncio.get_event_loop()
                    self.db = await aiomysql.create_pool(host=self.dbconfig['databaseip'], port=self.dbconfig['databaseport'], user=self.dbconfig['databaseuser'], password=self.dbconfig['databasepassword'],
                                              db=self.dbconfig['database'], charset="utf8", autocommit=True, loop=loop)
                    return self
                except Exception as e:

                    print(42,e)
        return self
    def __await__(self):
        return self.connect().__await__()
    async def close(self):
        if self.closed==0:
            self.closed=1
            self.db.close()
            await self.db.wait_closed()
    def __del__(self):
        if self.closed==0:
            asyncio.get_event_loop().run_until_complete(self.close())
