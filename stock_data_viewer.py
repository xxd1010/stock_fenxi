import sqlite3
import pandas as pd
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/stock_viewer.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('stock_viewer')


class StockDataViewer:
    """
    股票数据查看器，用于查看和分析stock_data.db数据库
    """
    
    def __init__(self, db_path):
        """
        初始化数据库查看器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        logger.info(f'初始化股票数据查看器，数据库路径: {db_path}')
    
    def connect(self):
        """
        连接到数据库
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info('成功连接到数据库')
            return True
        except Exception as e:
            logger.error(f'连接数据库失败: {str(e)}')
            return False
    
    def get_tables(self):
        """
        获取数据库中的所有表名
        
        Returns:
            list: 表名列表
        """
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [table[0] for table in self.cursor.fetchall()]
            logger.info(f'获取到数据库表: {tables}')
            return tables
        except Exception as e:
            logger.error(f'获取表列表失败: {str(e)}')
            return []
    
    def get_table_schema(self, table_name):
        """
        获取指定表的结构
        
        Args:
            table_name: 表名
            
        Returns:
            list: 表结构信息
        """
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            schema = self.cursor.fetchall()
            logger.info(f'获取表 {table_name} 的结构成功')
            return schema
        except Exception as e:
            logger.error(f'获取表 {table_name} 结构失败: {str(e)}')
            return []
    
    def view_table_data(self, table_name, limit=10):
        """
        查看表中的数据
        
        Args:
            table_name: 表名
            limit: 显示的行数，默认10行
            
        Returns:
            pd.DataFrame: 表数据
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT ?"
            df = pd.read_sql_query(query, self.conn, params=(limit,))
            logger.info(f'成功获取表 {table_name} 的 {limit} 行数据')
            return df
        except Exception as e:
            logger.error(f'获取表 {table_name} 数据失败: {str(e)}')
            return pd.DataFrame()
    
    def query_data(self, query, params=None):
        """
        执行自定义查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            pd.DataFrame: 查询结果
        """
        try:
            df = pd.read_sql_query(query, self.conn, params=params)
            logger.info(f'成功执行查询: {query[:50]}...')
            return df
        except Exception as e:
            logger.error(f'执行查询失败: {str(e)}')
            return pd.DataFrame()
    
    def get_table_count(self, table_name):
        """
        获取表中的记录数
        
        Args:
            table_name: 表名
            
        Returns:
            int: 记录数
        """
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            logger.info(f'表 {table_name} 记录数: {count}')
            return count
        except Exception as e:
            logger.error(f'获取表 {table_name} 记录数失败: {str(e)}')
            return 0
    
    def get_summary(self):
        """
        获取数据库概览信息
        
        Returns:
            dict: 数据库概览信息
        """
        try:
            summary = {
                'database_path': self.db_path,
                'tables': []
            }
            
            tables = self.get_tables()
            for table in tables:
                count = self.get_table_count(table)
                schema = self.get_table_schema(table)
                summary['tables'].append({
                    'name': table,
                    'record_count': count,
                    'column_count': len(schema),
                    'columns': [col[1] for col in schema]
                })
            
            logger.info('获取数据库概览成功')
            return summary
        except Exception as e:
            logger.error(f'获取数据库概览失败: {str(e)}')
            return {}
    
    def disconnect(self):
        """
        关闭数据库连接
        """
        try:
            if self.conn:
                self.conn.close()
                logger.info('数据库连接已关闭')
        except Exception as e:
            logger.error(f'关闭数据库连接失败: {str(e)}')


def main():
    """
    主函数，演示股票数据查看器的使用
    """
    logger.info('===== 股票数据查看器启动 =====')
    
    # 数据库路径
    db_path = 'stock_data.db'
    
    # 创建查看器实例
    viewer = StockDataViewer(db_path)
    
    # 连接数据库
    if viewer.connect():
        try:
            # 获取数据库概览
            logger.info('\n=== 数据库概览 ===')
            summary = viewer.get_summary()
            print(f"数据库路径: {summary['database_path']}")
            print(f"表数量: {len(summary['tables'])}")
            
            for table in summary['tables']:
                print(f"\n表名: {table['name']}")
                print(f"记录数: {table['record_count']}")
                print(f"列数量: {table['column_count']}")
                print(f"列名: {', '.join(table['columns'])}")
            
            # 查看历史K线数据表结构
            logger.info('\n=== 表结构详情 ===')
            table_name = 'history_k_data'
            schema = viewer.get_table_schema(table_name)
            print(f"\n{table_name} 表结构:")
            print("ID | 列名 | 类型 | 是否可为空 | 默认值 | 主键")
            print("---|------|------|------------|--------|------")
            for col in schema:
                print(f"{col[0]} | {col[1]} | {col[2]} | {col[3]} | {col[4]} | {col[5]}")
            
            # 查看表数据
            logger.info('\n=== 表数据预览 ===')
            df = viewer.view_table_data(table_name, limit=5)
            print(f"\n{table_name} 表前5行数据:")
            print(df)
            
            # 执行自定义查询 - 查看最近10条记录
            logger.info('\n=== 自定义查询 ===')
            query = "SELECT * FROM history_k_data ORDER BY date DESC LIMIT 10"
            recent_data = viewer.query_data(query)
            print("\n最近10条股票数据:")
            print(recent_data)
            
            # 执行自定义查询 - 查看特定日期范围的数据
            logger.info('\n=== 日期范围查询 ===')
            query = "SELECT * FROM history_k_data WHERE date BETWEEN ? AND ?"
            params = ('2017-06-01', '2017-06-10')
            date_range_data = viewer.query_data(query, params)
            print(f"\n2017-06-01 至 2017-06-10 期间的股票数据 ({len(date_range_data)} 条):")
            print(date_range_data)
            
        except Exception as e:
            logger.error(f'程序执行出错: {str(e)}', exc_info=True)
        finally:
            # 关闭连接
            viewer.disconnect()
    
    logger.info('===== 股票数据查看器结束 =====')


if __name__ == '__main__':
    main()