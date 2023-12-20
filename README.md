analyzer.py #数据分析器本体
net.py  #网络相关处理
processor_db.py #连接数据库

以上为数据分析的py文件

main.py为网络探测相关代码

使用poetry 安装环境
poetry install

使用ethereum.sql创建数据库
在main/get_redis中填入username、password、table

python3 main.py
开始网络探测

