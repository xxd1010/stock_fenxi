import pandas as pd
import baostock as bs
import datetime
import sqlite_db_manager as spl


# 新建数据库
# 检测是否数据库是否已存在
if not os.path.exists('stock_data.db'):
    # 新建数据库
    db_manager = spl.SQLiteDBManager('stock_data.db')
    # 链接数据库
    db_manager.connect()
    # 创建表
    db_manager.create_table('history_k_data', {
        'date': 'TEXT',
        'code': 'TEXT',
        'open': 'REAL',
        'high': 'REAL',
        'low': 'REAL',
        'close': 'REAL',
        'preclose': 'REAL',
        'volume': 'REAL',
        'amount': 'REAL',
        'adjustflag': 'TEXT',
        'turn': 'REAL',
        'tradestatus': 'TEXT',
        'pctChg': 'REAL',
        'peTTM': 'REAL',
        'pbMRQ': 'REAL',
        'psTTM': 'REAL',
        'pcfNcfTTM': 'REAL',
        'isST': 'TEXT'
    })
    

#### 登陆系统 ####
lg = bs.login()
# 打印登录返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

#### 获取历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节
rs = bs.query_history_k_data_plus("sh.600000",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
    start_date='2017-06-01', end_date=datetime.date.today().strftime('%Y-%m-%d'), 
    frequency="d", adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权
print('query_history_k_data_plus respond error_code:'+rs.error_code)
print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

#### 打印结果集 ####
data_list = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)
#### 结果集输出到csv文件 ####
result.to_csv("D:/history_k_data.csv", encoding="gbk", index=False)
print(result)

#### 数据写入数据库 ####

db_manager.write_dataframe(result, 'history_k_data', if_exists='append')

#### 登出系统 ####
bs.logout()