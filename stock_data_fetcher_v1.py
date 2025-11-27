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
# 导入akshare数据获取器
from akshare_data_fetcher import AkShareDataFetcher

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


def fetch_all_stock_codes(date=None, config=None):
    """
    获取所有A股股票代码和名称
    
    Args:
        date: 查询日期，格式为YYYY-MM-DD，默认为今天
        config: 配置字典，用于指定数据源类型
        
    Returns:
        pd.DataFrame: 包含股票代码、名称等信息的DataFrame
    """
    if date is None:
        date = datetime.date.today().strftime('%Y-%m-%d')
    
    data_source_type = config['data_source']['type'] if config and 'data_source' in config else 'baostock'
    
    logger.info(f'开始获取所有A股股票代码，查询日期: {date}，数据源: {data_source_type}')
    
    if data_source_type == 'baostock':
        # 使用baostock获取股票代码
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
    else:
        # 使用akshare获取股票代码
        try:
            logger.info(f'使用akshare获取股票代码')
            
            # 导入akshare（仅在需要时导入，避免不必要的依赖）
            import akshare as ak
            
            # 使用akshare获取A股股票列表
            stock_zh_a_spot_df = ak.stock_zh_a_spot_em()
            logger.debug(f'akshare返回的股票列表包含 {len(stock_zh_a_spot_df)} 条记录')
            
            # 处理股票代码，添加前缀
            stock_list = []
            for _, row in stock_zh_a_spot_df.iterrows():
                stock_code = row['代码']
                stock_name = row['名称']
                
                # 添加前缀，转换为与baostock兼容的格式
                if stock_code.startswith('6'):
                    # 上海股票
                    full_code = f'sh.{stock_code}'
                else:
                    # 深圳股票
                    full_code = f'sz.{stock_code}'
                
                stock_list.append({
                    'code': full_code,
                    'name': stock_name,
                    'ipoDate': '',
                    'outDate': '',
                    'type': '',
                    'status': ''
                })
            
            result = pd.DataFrame(stock_list)
            logger.info(f'最终成功获取 {len(result)} 只A股股票代码')
            return result
        except Exception as e:
            logger.error(f'使用akshare获取股票代码失败: {str(e)}')
            return None


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
    validation_enabled = config['update']['validation_enabled'] if config and 'update' in config else False
    data_source_type = config['data_source']['type'] if config and 'data_source' in config else 'baostock'
    
    # 创建数据校验器
    validator = DataValidator()
    
    for attempt in range(retry_count):
        try:
            logger.debug(f'开始获取股票 {code} 历史数据，日期范围: {start_date} 至 {end_date} (尝试 {attempt+1}/{retry_count})')
            
            if data_source_type == 'baostock':
                # 使用baostock获取数据
                frequency = config['data_fetch']['frequency'] if config else 'd'
                adjustflag = config['data_fetch']['adjustflag'] if config else '3'
                
                # 获取历史K线数据（每个线程独立登录，此处不需要再次登录）
                rs = bs.query_history_k_data_plus(code,
                    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                    start_date=start_date, end_date=end_date, 
                    frequency=frequency, adjustflag=adjustflag)
                
                logger.debug(f'获取历史K线数据结果: error_code={rs.error_code}, error_msg={rs.error_msg}')
                
                # 检查是否需要重新登录
                if rs.error_code == '10001001' and '未登录' in rs.error_msg:
                    logger.warning(f'股票 {code} 数据获取失败: {rs.error_msg}，尝试重新登录...')
                    # 重新登录（由调用者处理，此处直接返回失败，让调用者重试）
                    return None
                
                if rs.error_code != '0':
                    logger.error(f'获取股票 {code} 历史数据失败: {rs.error_msg}')
                    if attempt < retry_count - 1:
                        logger.debug(f'将在 {retry_interval} 秒后重试...')
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
                logger.debug(f'成功获取股票 {code} 历史数据，共 {len(result)} 条记录')
            else:
                # 使用akshare获取数据
                # 创建akshare数据获取器
                akshare_fetcher = AkShareDataFetcher(config)
                
                # 处理股票代码格式，akshare不需要前缀
                if code.startswith('sh.'):
                    ak_code = code[3:]
                elif code.startswith('sz.'):
                    ak_code = code[3:]
                else:
                    ak_code = code
                
                # 获取股票数据
                result = akshare_fetcher.fetch_stock_data(ak_code, start_date, end_date)
                logger.debug(f'成功获取股票 {code} 历史数据，共 {len(result)} 条记录')
            
            # 数据校验
            if validation_enabled:
                logger.debug(f'开始验证股票 {code} 的数据')
                validation_result = validator.validate_dataframe(result, code)
                
                if not validation_result['is_valid']:
                    # 尝试清理数据
                    logger.debug(f'股票 {code} 数据验证失败，尝试清理数据')
                    result = validator.clean_dataframe(result)
                    
                    # 再次验证
                    validation_result = validator.validate_dataframe(result, code)
                    if not validation_result['is_valid']:
                        logger.error(f'股票 {code} 数据验证失败，已清理后仍无效，跳过该股票')
                        return None
                
                logger.debug(f'股票 {code} 数据验证通过')
            
            return result
        except Exception as e:
            logger.error(f'获取股票 {code} 历史数据时发生异常: {str(e)}')
            if attempt < retry_count - 1:
                logger.debug(f'将在 {retry_interval} 秒后重试...')
                time.sleep(retry_interval)
                continue
            else:
                return None


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


def test_network_connection():
    """
    测试网络连接和baostock API的可用性
    
    Returns:
        bool: 网络连接是否正常
    """
    logger.info('开始测试网络连接...')
    
    # 简化测试，只测试登录功能，不测试数据获取
    try:
        # 测试baostock登录
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f'baostock API登录测试失败: {lg.error_msg}')
            return False
        
        # 登出
        bs.logout()
        logger.info('网络连接和baostock API登录测试通过')
        return True
    except Exception as e:
        logger.error(f'网络连接测试发生异常: {str(e)}')
        try:
            bs.logout()
        except:
            pass
        return False


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


def process_single_stock(stock_code, stock_name, start_date, end_date, config, csv_dir):
    """
    处理单个股票数据的函数，串行执行
    
    Args:
        stock_code: 股票代码
        stock_name: 股票名称
        start_date: 开始日期
        end_date: 结束日期
        config: 配置字典
        csv_dir: CSV文件保存目录
        
    Returns:
        tuple: (stock_code, success, data)
    """
    retry_count = config['data_fetch']['retry_count'] if config else 3
    request_interval = config['concurrency']['request_interval'] if config and 'concurrency' in config else 1
    data_source_type = config['data_source']['type'] if config and 'data_source' in config else 'baostock'
    
    for attempt in range(retry_count):
        try:
            # 添加请求间隔
            time.sleep(request_interval)
            
            # 获取股票K线数据
            stock_data = fetch_stock_data(stock_code, start_date, end_date, config)
            
            if stock_data is not None and not stock_data.empty:
                # 保存到CSV文件
                csv_file = f"{csv_dir}{stock_code}_history_k_data.csv"
                save_to_csv(stock_data, csv_file)
                
                logger.info(f'股票 {stock_code}({stock_name}) 数据获取成功，共 {len(stock_data)} 条记录')
                return (stock_code, True, stock_data)
            else:
                logger.warning(f'未获取到股票 {stock_code}({stock_name}) 的有效数据')
                return (stock_code, False, None)
        except Exception as e:
            logger.error(f'处理股票 {stock_code}({stock_name}) 时发生异常: {str(e)}，尝试 {attempt+1}/{retry_count}', exc_info=True)
            if attempt < retry_count - 1:
                time.sleep(request_interval)
                continue
            else:
                return (stock_code, False, None)


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
        stock_df = fetch_all_stock_codes(config=config)
        
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
        
        # 7. 根据数据源类型执行不同的处理逻辑
        data_source_type = config['data_source']['type']
        
        # 8. 测试网络连接（仅对baostock需要）
        if data_source_type == 'baostock':
            if not test_network_connection():
                logger.error('网络连接测试失败，程序退出')
                return
        
        # 9. 串行处理股票数据
        logger.info('使用串行方式处理股票数据')
        
        # 用于批量写入数据库的数据列表
        batch_data = []
        batch_size = config['update']['batch_size']
        
        # 如果使用baostock，全局登录一次
        data_source_type = config['data_source']['type']
        if data_source_type == 'baostock':
            logger.info('全局登录baostock系统')
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f'baostock登录失败: {lg.error_msg}')
                return
        
        # 遍历所有股票，串行处理
        for index, stock in stock_df.iterrows():
            stock_code = stock['code']
            stock_name = stock['name']
            
            try:
                # 直接调用处理函数
                result = process_single_stock(
                    stock_code,
                    stock_name,
                    start_date,
                    end_date,
                    config,
                    csv_dir
                )
                
                if result[1]:
                    success_count += 1
                    # 将成功获取的数据添加到批量列表
                    batch_data.append(result[2])
                    
                    # 当批量数据达到指定大小时，写入数据库
                    if len(batch_data) >= batch_size:
                        logger.info(f'批量写入数据库，共 {len(batch_data)} 个股票数据')
                        # 合并所有数据
                        combined_df = pd.concat(batch_data, ignore_index=True)
                        db_manager.write_dataframe(combined_df, 'history_k_data', if_exists='append')
                        # 清空批量数据列表
                        batch_data = []
                else:
                    failure_count += 1
                
                # 更新进度
                progress = (success_count + failure_count) / total_stocks * 100
                status_manager.update_progress(progress, total_stocks, success_count, failure_count)
            except Exception as e:
                logger.error(f'处理股票 {stock_code}({stock_name}) 时发生异常: {str(e)}', exc_info=True)
                failure_count += 1
                
                # 更新进度
                progress = (success_count + failure_count) / total_stocks * 100
                status_manager.update_progress(progress, total_stocks, success_count, failure_count)
        
        # 如果使用baostock，全局登出一次
        if data_source_type == 'baostock':
            logger.info('全局登出baostock系统')
            bs.logout()
        
        # 处理剩余的批量数据
        if batch_data:
            logger.info(f'批量写入剩余数据，共 {len(batch_data)} 个股票数据')
            combined_df = pd.concat(batch_data, ignore_index=True)
            db_manager.write_dataframe(combined_df, 'history_k_data', if_exists='append')
        
        # 10. 输出统计信息
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
        # 确保登出baostock系统
        try:
            bs.logout()
        except:
            pass
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