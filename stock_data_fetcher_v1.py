import os
import json
import time
import pandas as pd
import baostock as bs
import datetime
import sqlite_db_manager as spl
# 导入日志工具
from log_utils import setup_logger, get_logger
# 导入数据校验器
from data_validator import DataValidator
# 导入更新状态管理器
from update_status import UpdateStatusManager

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


def load_config(config_path='config.json'):
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        dict: 配置字典
    """
    try:
        logger.info(f'加载配置文件: {config_path}')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 处理特殊值，如"today"
        if config['data_fetch']['end_date'] == 'today':
            config['data_fetch']['end_date'] = datetime.date.today().strftime('%Y-%m-%d')
        
        logger.info('配置文件加载成功')
        return config
    except FileNotFoundError:
        logger.error(f'配置文件不存在: {config_path}')
        raise
    except json.JSONDecodeError:
        logger.error(f'配置文件格式错误: {config_path}')
        raise
    except Exception as e:
        logger.error(f'加载配置文件失败: {str(e)}')
        raise


def init_database(config=None):
    """
    初始化数据库，创建表结构
    
    Args:
        config: 配置字典
    """
    db_path = config['output']['database_path'] if config else 'stock_data.db'
    # 检测是否数据库是否已存在
    if not os.path.exists(db_path):
        logger.info(f'创建新数据库: {db_path}')
        # 新建数据库
        db_manager = spl.SQLiteDBManager(db_path)
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
        logger.info(f'数据库已存在，直接连接: {db_path}')
        db_manager = spl.SQLiteDBManager(db_path)
        db_manager.connect()
        return db_manager


def fetch_all_stock_codes(date=None):
    """
    获取所有A股股票代码和名称
    
    Args:
        date: 查询日期，格式为YYYY-MM-DD，默认为今天
        
    Returns:
        pd.DataFrame: 包含股票代码、名称等信息的DataFrame
    """
    if date is None:
        date = datetime.date.today().strftime('%Y-%m-%d')
    
    logger.info(f'开始获取所有A股股票代码，查询日期: {date}')
    
    # 登陆 baostock 系统
    lg = bs.login()
    logger.info(f'baostock 登录结果: error_code={lg.error_code}, error_msg={lg.error_msg}')
    
    if lg.error_code != '0':
        logger.error(f'baostock 登录失败: {lg.error_msg}')
        return None
    
    try:
        # 获取所有股票代码
        rs = bs.query_all_stock(date)
        logger.info(f'获取所有股票代码结果: error_code={rs.error_code}, error_msg={rs.error_msg}')
        
        if rs.error_code != '0':
            logger.error(f'获取所有股票代码失败: {rs.error_msg}')
            return None
        
        # 解析结果集
        stock_list = []
        all_stocks = []
        
        while rs.next():
            stock_info = rs.get_row_data()
            stock_code = stock_info[0]
            all_stocks.append(stock_code)
            
            # 过滤出A股股票（上海和深圳）
            if stock_code.startswith('sh.') or stock_code.startswith('sz.'):
                # 简化处理，先只保存股票代码和默认名称
                stock_list.append({
                    'code': stock_code,
                    'name': stock_code,  # 先使用股票代码作为名称
                    'ipoDate': '',
                    'outDate': '',
                    'type': '',
                    'status': ''
                })
        
        logger.info(f'总共获取到 {len(all_stocks)} 只股票，其中A股股票 {len(stock_list)} 只')
        
        # 如果没有获取到A股股票，尝试使用另一种方式
        if len(stock_list) == 0:
            logger.info('尝试使用另一种方式获取A股股票代码')
            # 使用query_stock_basic获取所有股票基本信息
            rs_basic = bs.query_stock_basic()
            logger.info(f'获取股票基本信息结果: error_code={rs_basic.error_code}, error_msg={rs_basic.error_msg}')
            
            if rs_basic.error_code == '0':
                while rs_basic.next():
                    basic_info = rs_basic.get_row_data()
                    stock_code = basic_info[0]
                    stock_name = basic_info[1]
                    
                    if stock_code.startswith('sh.') or stock_code.startswith('sz.'):
                        stock_list.append({
                            'code': stock_code,
                            'name': stock_name,
                            'ipoDate': basic_info[2],
                            'outDate': basic_info[3],
                            'type': basic_info[4],
                            'status': basic_info[5]
                        })
            
            logger.info(f'通过基本信息获取到 {len(stock_list)} 只A股股票')
        
        result = pd.DataFrame(stock_list)
        logger.info(f'最终成功获取 {len(result)} 只A股股票代码')
        return result
    finally:
        # 登出系统
        bs.logout()
        logger.info('baostock 已登出')


def fetch_stock_data(code, start_date, end_date, config=None):
    """
    获取股票历史K线数据
    
    Args:
        code: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        config: 配置字典
        
    Returns:
        pd.DataFrame: 股票历史数据
    """
    retry_count = config['data_fetch']['retry_count'] if config else 3
    retry_interval = config['data_fetch']['retry_interval'] if config else 5
    frequency = config['data_fetch']['frequency'] if config else 'd'
    adjustflag = config['data_fetch']['adjustflag'] if config else '3'
    validation_enabled = config['update']['validation_enabled'] if config and 'update' in config else False
    
    # 创建数据校验器
    validator = DataValidator()
    
    for attempt in range(retry_count):
        try:
            logger.info(f'开始获取股票 {code} 历史数据，日期范围: {start_date} 至 {end_date} (尝试 {attempt+1}/{retry_count})')
            
            # 登陆 baostock 系统
            lg = bs.login()
            logger.debug(f'baostock 登录结果: error_code={lg.error_code}, error_msg={lg.error_msg}')
            
            if lg.error_code != '0':
                logger.error(f'baostock 登录失败: {lg.error_msg}')
                if attempt < retry_count - 1:
                    logger.info(f'将在 {retry_interval} 秒后重试...')
                    time.sleep(retry_interval)
                    continue
                else:
                    return None
            
            # 获取历史K线数据
            rs = bs.query_history_k_data_plus(code,
                "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                start_date=start_date, end_date=end_date, 
                frequency=frequency, adjustflag=adjustflag)
            
            logger.debug(f'获取历史K线数据结果: error_code={rs.error_code}, error_msg={rs.error_msg}')
            
            if rs.error_code != '0':
                logger.error(f'获取股票 {code} 历史数据失败: {rs.error_msg}')
                if attempt < retry_count - 1:
                    logger.info(f'将在 {retry_interval} 秒后重试...')
                    time.sleep(retry_interval)
                    continue
                else:
                    return None
            
            # 解析结果集
            data_list = []
            while rs.next():
                # 获取一条记录，将记录合并在一起
                data_list.append(rs.get_row_data())
            
            result = pd.DataFrame(data_list, columns=rs.fields)
            logger.info(f'成功获取股票 {code} 历史数据，共 {len(result)} 条记录')
            
            # 数据校验
            if validation_enabled:
                logger.info(f'开始验证股票 {code} 的数据')
                validation_result = validator.validate_dataframe(result, code)
                
                if not validation_result['is_valid']:
                    # 尝试清理数据
                    logger.info(f'股票 {code} 数据验证失败，尝试清理数据')
                    result = validator.clean_dataframe(result)
                    
                    # 再次验证
                    validation_result = validator.validate_dataframe(result, code)
                    if not validation_result['is_valid']:
                        logger.error(f'股票 {code} 数据验证失败，已清理后仍无效，跳过该股票')
                        return None
                
                logger.info(f'股票 {code} 数据验证通过')
            
            return result
        except Exception as e:
            logger.error(f'获取股票 {code} 历史数据时发生异常: {str(e)}')
            if attempt < retry_count - 1:
                logger.info(f'将在 {retry_interval} 秒后重试...')
                time.sleep(retry_interval)
                continue
            else:
                return None
        finally:
            # 登出系统
            bs.logout()
            logger.debug('baostock 已登出')


def read_stock_config(file_path):
    """
    读取股票配置文件，每行一个股票代码
    
    Args:
        file_path: 配置文件路径
        
    Returns:
        list: 股票代码列表
    """
    stock_codes = []
    try:
        logger.info(f'读取股票配置文件: {file_path}')
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip()
                if code:  # 跳过空行
                    stock_codes.append(code)
        logger.info(f'成功读取 {len(stock_codes)} 个股票代码')
    except FileNotFoundError:
        logger.error(f'股票配置文件不存在: {file_path}')
    except Exception as e:
        logger.error(f'读取股票配置文件失败: {str(e)}')
    return stock_codes


def save_stock_codes(stock_df, config):
    """
    将股票代码保存为结构化文件
    
    Args:
        stock_df: 包含股票代码信息的DataFrame
        config: 配置字典
    """
    format_type = config['output']['stock_list_format']
    save_path = config['output']['stock_list_path']
    
    # 确保保存目录存在
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    # 根据格式保存文件
    if format_type == 'csv':
        file_path = os.path.join(save_path, 'stock_list.csv')
        try:
            logger.info(f'将股票代码保存为CSV文件: {file_path}')
            stock_df.to_csv(file_path, encoding="gbk", index=False)
            logger.info(f'股票代码CSV文件保存成功: {file_path}')
        except Exception as e:
            logger.error(f'保存股票代码CSV文件失败: {str(e)}')
    elif format_type == 'json':
        file_path = os.path.join(save_path, 'stock_list.json')
        try:
            logger.info(f'将股票代码保存为JSON文件: {file_path}')
            stock_df.to_json(file_path, orient='records', force_ascii=False, indent=2)
            logger.info(f'股票代码JSON文件保存成功: {file_path}')
        except Exception as e:
            logger.error(f'保存股票代码JSON文件失败: {str(e)}')
    elif format_type == 'txt':
        file_path = os.path.join(save_path, 'stock_list.txt')
        try:
            logger.info(f'将股票代码保存为TXT文件: {file_path}')
            with open(file_path, 'w', encoding='utf-8') as f:
                for code in stock_df['code']:
                    f.write(f'{code}\n')
            logger.info(f'股票代码TXT文件保存成功: {file_path}')
        except Exception as e:
            logger.error(f'保存股票代码TXT文件失败: {str(e)}')
    else:
        logger.error(f'不支持的文件格式: {format_type}')


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
    主函数，实现自动化股票数据获取流程
    """
    logger.info('===== 股票数据获取程序启动 =====')
    start_time = datetime.datetime.now()
    
    # 创建更新状态管理器
    status_manager = UpdateStatusManager()
    
    try:
        # 1. 加载配置文件
        config = load_config()
        
        # 2. 初始化数据库
        db_manager = init_database(config)
        
        # 3. 获取所有A股股票代码
        logger.info('开始获取所有A股股票代码')
        stock_df = fetch_all_stock_codes()
        
        if stock_df is None or stock_df.empty:
            logger.error('未获取到任何股票代码，程序退出')
            return
        
        # 4. 保存股票代码到文件
        save_stock_codes(stock_df, config)
        
        # 5. 准备获取K线数据
        start_date = config['data_fetch']['start_date']
        end_date = config['data_fetch']['end_date']
        csv_dir = config['output']['kline_data_path']
        
        # 确保保存目录存在
        if not os.path.exists(csv_dir):
            os.makedirs(csv_dir)
        
        # 6. 遍历所有股票代码，获取K线数据
        total_stocks = len(stock_df)
        success_count = 0
        failure_count = 0
        
        # 检查是否启用测试模式
        test_mode = config.get('test_mode', {})
        is_test_mode = test_mode.get('enabled', False)
        max_stocks = test_mode.get('max_stocks', 5)
        
        # 如果是测试模式，只处理前max_stocks只股票
        if is_test_mode:
            logger.info(f'测试模式已启用，只处理前 {max_stocks} 只股票')
            stock_df = stock_df.head(max_stocks)
            total_stocks = len(stock_df)
        
        logger.info(f'开始批量获取K线数据，共 {total_stocks} 只股票，日期范围: {start_date} 至 {end_date}')
        
        # 记录更新开始状态
        status_manager.start_update(
            update_type=config['update']['update_type'],
            start_date=start_date,
            end_date=end_date,
            stock_codes=stock_df['code'].tolist()
        )
        
        for index, stock in stock_df.iterrows():
            stock_code = stock['code']
            stock_name = stock['name']
            
            logger.info(f'处理进度: {index+1}/{total_stocks} - 股票: {stock_code}({stock_name})')
            
            try:
                # 获取股票K线数据
                stock_data = fetch_stock_data(stock_code, start_date, end_date, config)
                
                if stock_data is not None and not stock_data.empty:
                    # 保存到CSV文件
                    csv_file = f"{csv_dir}{stock_code}_history_k_data.csv"
                    save_to_csv(stock_data, csv_file)
                    
                    # 数据写入数据库
                    logger.debug(f'开始将股票 {stock_code} 数据写入数据库')
                    db_manager.write_dataframe(stock_data, 'history_k_data', if_exists='append')
                    logger.debug(f'股票 {stock_code} 数据写入数据库成功')
                    
                    success_count += 1
                else:
                    logger.warning(f'未获取到股票 {stock_code}({stock_name}) 的有效数据')
                    failure_count += 1
            except Exception as e:
                logger.error(f'处理股票 {stock_code}({stock_name}) 时发生异常: {str(e)}', exc_info=True)
                failure_count += 1
            
            # 更新进度
            progress = (index + 1) / total_stocks * 100
            status_manager.update_progress(progress, total_stocks, success_count, failure_count)
            
            # 添加请求间隔，避免请求过于频繁
            time.sleep(config['concurrency']['request_interval'])
        
        # 7. 输出统计信息
        logger.info('===== 数据获取统计 =====')
        logger.info(f'总股票数: {total_stocks}')
        logger.info(f'成功获取: {success_count}')
        logger.info(f'获取失败: {failure_count}')
        logger.info(f'成功率: {success_count/total_stocks*100:.2f}%')
        
        # 记录更新完成状态
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        status_manager.finish_update(
            status='success',
            message='数据获取完成',
            duration=str(duration)
        )
        
    except Exception as e:
        logger.error(f'程序执行出错: {str(e)}', exc_info=True)
        # 记录更新失败状态
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        status_manager.finish_update(
            status='failed',
            message=str(e),
            duration=str(duration)
        )
    finally:
        # 关闭数据库连接
        try:
            if 'db_manager' in locals():
                db_manager.disconnect()
                logger.info('数据库连接已关闭')
        except Exception as e:
            logger.error(f'关闭数据库连接失败: {str(e)}')
    
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    logger.info(f'===== 股票数据获取程序结束 =====')
    logger.info(f'程序运行时间: {duration}')


if __name__ == '__main__':
    main()