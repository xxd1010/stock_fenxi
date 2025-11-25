import os
import pandas as pd
import baostock as bs
import datetime
import sqlite_db_manager as spl
# 导入日志工具
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/stock_fetcher.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('stock_fetcher')


def init_database():
    """
    初始化数据库，创建表结构
    """
    # 检测是否数据库是否已存在
    if not os.path.exists('stock_data.db'):
        logger.info('创建新数据库: stock_data.db')
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
        logger.info('数据库初始化完成')
        return db_manager
    else:
        logger.info('数据库已存在，直接连接: stock_data.db')
        db_manager = spl.SQLiteDBManager('stock_data.db')
        db_manager.connect()
        return db_manager


def fetch_stock_data(code, start_date, end_date):
    """
    获取股票历史K线数据
    
    Args:
        code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        pd.DataFrame: 股票历史数据
    """
    logger.info(f'开始获取股票 {code} 历史数据，日期范围: {start_date} 至 {end_date}')
    
    # 登陆 baostock 系统
    lg = bs.login()
    logger.debug(f'baostock 登录结果: error_code={lg.error_code}, error_msg={lg.error_msg}')
    
    if lg.error_code != '0':
        logger.error(f'baostock 登录失败: {lg.error_msg}')
        return None
    
    try:
        # 获取历史K线数据
        rs = bs.query_history_k_data_plus(code,
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
            start_date=start_date, end_date=end_date, 
            frequency="d", adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权
        
        logger.debug(f'获取历史K线数据结果: error_code={rs.error_code}, error_msg={rs.error_msg}')
        
        if rs.error_code != '0':
            logger.error(f'获取股票 {code} 历史数据失败: {rs.error_msg}')
            return None
        
        # 解析结果集
        data_list = []
        while rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        
        result = pd.DataFrame(data_list, columns=rs.fields)
        logger.info(f'成功获取股票 {code} 历史数据，共 {len(result)} 条记录')
        
        return result
    finally:
        # 登出系统
        bs.logout()
        logger.debug('baostock 已登出')


def save_to_csv(df, file_path):
    """
    将数据保存到CSV文件
    
    Args:
        df: 要保存的数据
        file_path: CSV文件路径
    """
    try:
        logger.info(f'将数据保存到CSV文件: {file_path}')
        df.to_csv(file_path, encoding="gbk", index=False)
        logger.info(f'CSV文件保存成功: {file_path}')
    except Exception as e:
        logger.error(f'保存CSV文件失败: {str(e)}')


def main():
    """
    主函数
    """
    logger.info('===== 股票数据获取程序启动 =====')
    
    # 初始化数据库
    db_manager = init_database()
    
    # 配置参数
    stock_code = "sh.600000"
    start_date = '2017-06-01'
    end_date = datetime.date.today().strftime('%Y-%m-%d')
    csv_file = "D:/history_k_data.csv"
    
    try:
        # 获取股票数据
        stock_data = fetch_stock_data(stock_code, start_date, end_date)
        
        if stock_data is not None and not stock_data.empty:
            # 保存到CSV文件
            save_to_csv(stock_data, csv_file)
            
            # 数据写入数据库
            logger.info('开始将数据写入数据库')
            db_manager.write_dataframe(stock_data, 'history_k_data', if_exists='append')
            logger.info('数据写入数据库成功')
            
            # 显示部分数据
            logger.info(f'数据示例: \n{stock_data.head()}')
        else:
            logger.warning('未获取到有效股票数据')
    except Exception as e:
        logger.error(f'程序执行出错: {str(e)}', exc_info=True)
    finally:
        # 关闭数据库连接
        try:
            db_manager.disconnect()
            logger.info('数据库连接已关闭')
        except Exception as e:
            logger.error(f'关闭数据库连接失败: {str(e)}')
    
    logger.info('===== 股票数据获取程序结束 =====')


if __name__ == '__main__':
    main()