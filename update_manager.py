import os
import sys
import time
import datetime
import schedule
from log_utils import setup_logger, get_logger

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock_data_fetcher_v1 import load_config, init_database, fetch_all_stock_codes, fetch_stock_data, save_stock_codes, save_to_csv

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/update_manager.log",
    "max_bytes": 5 * 1024 * 1024,
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('update_manager')


class UpdateManager:
    """
    更新管理器，负责股票数据的定期自动更新和手动触发更新
    """
    
    def __init__(self, config_path='config.json'):
        """
        初始化更新管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config = load_config(config_path)
        self.db_manager = None
        self.update_status = {
            'last_update': None,
            'last_update_status': 'idle',  # idle, running, success, failed
            'last_update_time': None,
            'current_update': None,
            'progress': 0,
            'total_stocks': 0,
            'updated_stocks': 0,
            'failed_stocks': 0
        }
        self.is_running = False
    
    def init_database(self):
        """
        初始化数据库连接
        """
        self.db_manager = init_database(self.config)
    
    def close_database(self):
        """
        关闭数据库连接
        """
        if self.db_manager:
            try:
                self.db_manager.disconnect()
                logger.info('数据库连接已关闭')
            except Exception as e:
                logger.error(f'关闭数据库连接失败: {str(e)}')
    
    def get_last_update_date(self):
        """
        获取上次更新日期
        
        Returns:
            str: 上次更新日期，格式YYYY-MM-DD
        """
        if self.update_status['last_update']:
            return self.update_status['last_update']
        
        # 从数据库中查询上次更新日期
        try:
            if not self.db_manager:
                self.init_database()
            
            # 查询history_k_data表中最新的日期
            query = "SELECT MAX(date) FROM history_k_data"
            cursor = self.db_manager.execute_query(query)
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
        except Exception as e:
            logger.error(f'查询上次更新日期失败: {str(e)}')
        
        # 默认返回7天前的日期
        default_date = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        return default_date
    
    def update_stock_data(self, update_type='incremental', stock_codes=None, start_date=None, end_date=None):
        """
        更新股票数据
        
        Args:
            update_type: 更新类型，full=全量更新, incremental=增量更新, partial=部分更新
            stock_codes: 要更新的股票代码列表，None表示所有股票
            start_date: 开始日期，None表示使用默认值
            end_date: 结束日期，None表示使用默认值
        
        Returns:
            dict: 更新结果
        """
        logger.info(f'开始更新股票数据，更新类型: {update_type}')
        
        # 更新状态
        self.update_status['current_update'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.update_status['last_update_status'] = 'running'
        self.update_status['progress'] = 0
        self.update_status['updated_stocks'] = 0
        self.update_status['failed_stocks'] = 0
        
        start_time = datetime.datetime.now()
        
        try:
            # 初始化数据库
            if not self.db_manager:
                self.init_database()
            
            # 获取配置
            end_date = end_date or self.config['data_fetch']['end_date']
            if end_date == 'today':
                end_date = datetime.date.today().strftime('%Y-%m-%d')
            
            # 根据更新类型确定开始日期
            if update_type == 'incremental':
                # 增量更新，从上次更新日期的下一天开始
                last_update = self.get_last_update_date()
                start_date = start_date or last_update
                logger.info(f'增量更新，从 {start_date} 到 {end_date}')
            elif update_type == 'full':
                # 全量更新，使用配置中的开始日期
                start_date = start_date or self.config['data_fetch']['start_date']
                logger.info(f'全量更新，从 {start_date} 到 {end_date}')
            else:  # partial
                # 部分更新，使用指定的开始日期
                start_date = start_date or (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
                logger.info(f'部分更新，从 {start_date} 到 {end_date}')
            
            # 获取股票代码列表
            if stock_codes is None:
                # 获取所有A股股票代码
                stock_df = fetch_all_stock_codes()
                if stock_df is None or stock_df.empty:
                    logger.error('未获取到任何股票代码')
                    self.update_status['last_update_status'] = 'failed'
                    return {'status': 'failed', 'message': '未获取到任何股票代码'}
                
                # 保存股票代码到文件
                save_stock_codes(stock_df, self.config)
                
                # 转换为股票代码列表
                stock_codes = stock_df['code'].tolist()
            
            self.update_status['total_stocks'] = len(stock_codes)
            
            # 确保保存目录存在
            csv_dir = self.config['output']['kline_data_path']
            if not os.path.exists(csv_dir):
                os.makedirs(csv_dir)
            
            # 遍历所有股票代码，获取K线数据
            success_count = 0
            failure_count = 0
            
            for index, stock_code in enumerate(stock_codes):
                # 更新进度
                self.update_status['progress'] = (index + 1) / len(stock_codes) * 100
                
                try:
                    # 获取股票K线数据
                    stock_data = fetch_stock_data(stock_code, start_date, end_date, self.config)
                    
                    if stock_data is not None and not stock_data.empty:
                        # 保存到CSV文件
                        csv_file = f"{csv_dir}{stock_code}_history_k_data.csv"
                        save_to_csv(stock_data, csv_file)
                        
                        # 数据写入数据库
                        self.db_manager.write_dataframe(stock_data, 'history_k_data', if_exists='append')
                        
                        success_count += 1
                        self.update_status['updated_stocks'] = success_count
                    else:
                        logger.warning(f'未获取到股票 {stock_code} 的有效数据')
                        failure_count += 1
                        self.update_status['failed_stocks'] = failure_count
                except Exception as e:
                    logger.error(f'处理股票 {stock_code} 时发生异常: {str(e)}', exc_info=True)
                    failure_count += 1
                    self.update_status['failed_stocks'] = failure_count
                
                # 添加请求间隔，避免请求过于频繁
                time.sleep(self.config['concurrency']['request_interval'])
            
            # 更新状态
            self.update_status['last_update'] = end_date
            self.update_status['last_update_status'] = 'success'
            self.update_status['last_update_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.update_status['current_update'] = None
            
            end_time = datetime.datetime.now()
            duration = end_time - start_time
            
            logger.info('===== 数据更新统计 =====')
            logger.info(f'总股票数: {len(stock_codes)}')
            logger.info(f'成功更新: {success_count}')
            logger.info(f'更新失败: {failure_count}')
            logger.info(f'成功率: {success_count/len(stock_codes)*100:.2f}%')
            logger.info(f'更新时间: {duration}')
            
            return {
                'status': 'success',
                'message': '数据更新完成',
                'total_stocks': len(stock_codes),
                'success_count': success_count,
                'failure_count': failure_count,
                'duration': str(duration)
            }
        
        except Exception as e:
            logger.error(f'更新股票数据时发生异常: {str(e)}', exc_info=True)
            self.update_status['last_update_status'] = 'failed'
            self.update_status['current_update'] = None
            return {
                'status': 'failed',
                'message': f'更新失败: {str(e)}'
            }
    
    def manual_update(self, update_type='incremental', stock_codes=None, start_date=None, end_date=None):
        """
        手动触发更新
        
        Args:
            update_type: 更新类型
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            dict: 更新结果
        """
        logger.info('手动触发更新')
        return self.update_stock_data(update_type, stock_codes, start_date, end_date)
    
    def run_scheduled_update(self):
        """
        执行定时更新
        """
        logger.info('执行定时更新')
        update_type = self.config['update']['update_type']
        return self.update_stock_data(update_type)
    
    def start_scheduler(self):
        """
        启动定时任务调度器
        """
        if not self.config['update']['auto_update_enabled']:
            logger.info('自动更新已禁用')
            return
        
        logger.info('启动定时更新任务')
        
        # 获取更新频率和时间
        frequency = self.config['update']['frequency']
        update_time = self.config['update']['update_time']
        
        # 设置定时任务
        if frequency == 'daily':
            # 每天指定时间更新
            schedule.every().day.at(update_time).do(self.run_scheduled_update)
            logger.info(f'设置每天 {update_time} 执行更新')
        elif frequency == 'weekly':
            # 每周一指定时间更新
            schedule.every().monday.at(update_time).do(self.run_scheduled_update)
            logger.info(f'设置每周一 {update_time} 执行更新')
        elif frequency == 'monthly':
            # 每月1号指定时间更新
            schedule.every().month.at(update_time).do(self.run_scheduled_update)
            logger.info(f'设置每月1号 {update_time} 执行更新')
        
        # 启动调度器
        self.is_running = True
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    
    def stop_scheduler(self):
        """
        停止定时任务调度器
        """
        logger.info('停止定时更新任务')
        self.is_running = False
    
    def get_update_status(self):
        """
        获取更新状态
        
        Returns:
            dict: 更新状态
        """
        return self.update_status


# 主函数，用于手动触发更新
if __name__ == '__main__':
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='股票数据更新管理器')
    parser.add_argument('--update-type', type=str, default='incremental', help='更新类型: full, incremental, partial')
    parser.add_argument('--start-date', type=str, help='开始日期，格式YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, help='结束日期，格式YYYY-MM-DD')
    parser.add_argument('--stock-codes', type=str, help='股票代码列表，用逗号分隔')
    parser.add_argument('--auto', action='store_true', help='启动自动更新服务')
    
    args = parser.parse_args()
    
    # 创建更新管理器实例
    update_manager = UpdateManager()
    
    try:
        update_manager.init_database()
        
        if args.auto:
            # 启动自动更新服务
            logger.info('启动自动更新服务')
            update_manager.start_scheduler()
        else:
            # 手动触发更新
            stock_codes = None
            if args.stock_codes:
                stock_codes = args.stock_codes.split(',')
            
            result = update_manager.manual_update(
                update_type=args.update_type,
                stock_codes=stock_codes,
                start_date=args.start_date,
                end_date=args.end_date
            )
            
            logger.info(f'更新结果: {result}')
    except Exception as e:
        logger.error(f'程序执行出错: {str(e)}', exc_info=True)
    finally:
        update_manager.close_database()
