import pandas as pd
from sqlite_db_manager import SQLiteDBManager
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/stock_data_reader.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('stock_data_reader')


class StockDataReader:
    """
    股票数据读取类
    基于现有的SQLiteDBManager类，提供便捷的股票数据读取接口
    """
    
    def __init__(self, db_path: str = 'stock_data.db'):
        """
        初始化股票数据读取器
        
        Args:
            db_path: 数据库文件路径，默认'stock_data.db'
        """
        self.db_path = db_path
        self.db_manager = SQLiteDBManager(db_path)
        self.logger = logger
        self.logger.info(f"股票数据读取器已初始化，数据库路径: {db_path}")
    
    def connect(self):
        """
        连接数据库
        """
        try:
            self.db_manager.connect()
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise
    
    def disconnect(self):
        """
        断开数据库连接
        """
        try:
            self.db_manager.disconnect()
            self.logger.info("数据库连接已断开")
        except Exception as e:
            self.logger.error(f"断开数据库连接失败: {str(e)}")
            raise
    
    def get_stock_codes(self) -> list:
        """
        获取所有股票代码列表
        
        Returns:
            股票代码列表
        """
        try:
            self.connect()
            query = "SELECT DISTINCT code FROM history_k_data ORDER BY code"
            result = self.db_manager.fetch_all(query)
            stock_codes = [item['code'] for item in result]
            self.logger.info(f"成功获取 {len(stock_codes)} 个股票代码")
            return stock_codes
        except Exception as e:
            self.logger.error(f"获取股票代码列表失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_stock_data(self, stock_code: str, start_date: str = None, end_date: str = None, 
                      limit: int = None, columns: list = None) -> pd.DataFrame:
        """
        获取单只股票的历史数据
        
        Args:
            stock_code: 股票代码，如'sh.600000'
            start_date: 开始日期，格式'YYYY-MM-DD'，默认为None（不限制）
            end_date: 结束日期，格式'YYYY-MM-DD'，默认为None（不限制）
            limit: 返回数据条数限制，默认为None（不限制）
            columns: 要返回的列列表，默认为None（返回所有列）
            
        Returns:
            股票历史数据的DataFrame
        """
        try:
            self.connect()
            
            # 构建查询语句
            if columns:
                columns_str = ', '.join(columns)
            else:
                columns_str = '*'
            
            query = f"SELECT {columns_str} FROM history_k_data WHERE code = ?"
            params = [stock_code]
            
            # 添加日期条件
            if start_date:
                query += " AND date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date <= ?"
                params.append(end_date)
            
            # 添加排序和限制
            query += " ORDER BY date DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            # 执行查询
            df = self.db_manager.read_dataframe(query, params=params)
            
            # 如果有数据，按日期升序排序
            if not df.empty:
                df = df.sort_values('date')
                df.reset_index(drop=True, inplace=True)
            
            self.logger.info(f"成功获取股票 {stock_code} 的历史数据，共 {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.error(f"获取股票 {stock_code} 数据失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_multiple_stocks_data(self, stock_codes: list, start_date: str = None, 
                               end_date: str = None, limit: int = None, 
                               columns: list = None) -> dict:
        """
        获取多只股票的历史数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式'YYYY-MM-DD'，默认为None（不限制）
            end_date: 结束日期，格式'YYYY-MM-DD'，默认为None（不限制）
            limit: 返回数据条数限制，默认为None（不限制）
            columns: 要返回的列列表，默认为None（返回所有列）
            
        Returns:
            以股票代码为键，DataFrame为值的字典
        """
        result = {}
        
        for stock_code in stock_codes:
            try:
                df = self.get_stock_data(stock_code, start_date, end_date, limit, columns)
                result[stock_code] = df
            except Exception as e:
                self.logger.error(f"获取股票 {stock_code} 数据失败，跳过该股票: {str(e)}")
                continue
        
        self.logger.info(f"成功获取 {len(result)} 只股票的历史数据")
        return result
    
    def get_latest_data(self, stock_code: str, days: int = 100) -> pd.DataFrame:
        """
        获取股票的最新数据
        
        Args:
            stock_code: 股票代码
            days: 获取最近多少天的数据，默认100天
            
        Returns:
            股票最新数据的DataFrame
        """
        try:
            self.connect()
            query = f"SELECT * FROM history_k_data WHERE code = ? ORDER BY date DESC LIMIT ?"
            params = [stock_code, days]
            df = self.db_manager.read_dataframe(query, params=params)
            
            if not df.empty:
                df = df.sort_values('date')
                df.reset_index(drop=True, inplace=True)
            
            self.logger.info(f"成功获取股票 {stock_code} 的最近 {days} 天数据，共 {len(df)} 条记录")
            return df
        except Exception as e:
            self.logger.error(f"获取股票 {stock_code} 最新数据失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_trading_dates(self, start_date: str = None, end_date: str = None) -> list:
        """
        获取交易日期列表
        
        Args:
            start_date: 开始日期，格式'YYYY-MM-DD'，默认为None（不限制）
            end_date: 结束日期，格式'YYYY-MM-DD'，默认为None（不限制）
            
        Returns:
            交易日期列表
        """
        try:
            self.connect()
            query = "SELECT DISTINCT date FROM history_k_data"
            params = []
            
            if start_date:
                query += " WHERE date >= ?"
                params.append(start_date)
            
            if end_date:
                if start_date:
                    query += " AND date <= ?"
                else:
                    query += " WHERE date <= ?"
                params.append(end_date)
            
            query += " ORDER BY date"
            
            result = self.db_manager.fetch_all(query, tuple(params))
            trading_dates = [item['date'] for item in result]
            self.logger.info(f"成功获取 {len(trading_dates)} 个交易日期")
            return trading_dates
        except Exception as e:
            self.logger.error(f"获取交易日期列表失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_stock_count(self) -> int:
        """
        获取股票数量
        
        Returns:
            股票数量
        """
        try:
            self.connect()
            query = "SELECT COUNT(DISTINCT code) as count FROM history_k_data"
            result = self.db_manager.fetch_one(query)
            count = result['count'] if result else 0
            self.logger.info(f"当前数据库中有 {count} 只股票")
            return count
        except Exception as e:
            self.logger.error(f"获取股票数量失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_data_count(self, stock_code: str = None) -> int:
        """
        获取数据记录数量
        
        Args:
            stock_code: 股票代码，默认为None（所有股票）
            
        Returns:
            数据记录数量
        """
        try:
            self.connect()
            if stock_code:
                query = "SELECT COUNT(*) as count FROM history_k_data WHERE code = ?"
                params = [stock_code]
            else:
                query = "SELECT COUNT(*) as count FROM history_k_data"
                params = None
            
            result = self.db_manager.fetch_one(query, tuple(params) if params else None)
            count = result['count'] if result else 0
            
            if stock_code:
                self.logger.info(f"股票 {stock_code} 共有 {count} 条数据记录")
            else:
                self.logger.info(f"数据库中共有 {count} 条数据记录")
            
            return count
        except Exception as e:
            self.logger.error(f"获取数据记录数量失败: {str(e)}")
            raise
        finally:
            self.disconnect()


# 使用示例
if __name__ == '__main__':
    # 创建股票数据读取器实例
    reader = StockDataReader()
    
    # 获取所有股票代码
    stock_codes = reader.get_stock_codes()
    print(f"共有 {len(stock_codes)} 只股票")
    print(f"前5只股票代码: {stock_codes[:5]}")
    
    # 获取单只股票数据
    if stock_codes:
        stock_code = stock_codes[0]
        df = reader.get_stock_data(stock_code, limit=10)
        print(f"\n股票 {stock_code} 的最近10条数据:")
        print(df)
    
    # 获取股票数量
    stock_count = reader.get_stock_count()
    print(f"\n股票数量: {stock_count}")
    
    # 获取数据记录数量
    data_count = reader.get_data_count()
    print(f"数据记录数量: {data_count}")