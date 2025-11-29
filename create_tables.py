from sqlite_db_manager import SQLiteDBManager
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/create_tables.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('create_tables')


def create_tables(db_path: str = 'stock_data.db'):
    """
    创建数据库表结构
    
    Args:
        db_path: 数据库文件路径，默认'stock_data.db'
    """
    try:
        # 创建数据库管理器实例
        db_manager = SQLiteDBManager(db_path)
        db_manager.connect()
        logger.info(f"成功连接到数据库: {db_path}")
        
        # 创建history_k_data表
        logger.info("开始创建history_k_data表...")
        db_manager.create_table(
            'history_k_data',
            {
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
            }
        )
        # 添加索引
        db_manager.execute_update(
            "CREATE INDEX IF NOT EXISTS idx_history_k_data_code ON history_k_data(code)"
        )
        db_manager.execute_update(
            "CREATE INDEX IF NOT EXISTS idx_history_k_data_date ON history_k_data(date)"
        )
        logger.info("history_k_data表创建完成")
        
        # 创建technical_indicators表
        logger.info("开始创建technical_indicators表...")
        db_manager.create_table(
            'technical_indicators',
            {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'date': 'TEXT NOT NULL',
                'code': 'TEXT NOT NULL',
                'MA5': 'REAL',
                'MA10': 'REAL',
                'MA20': 'REAL',
                'MA60': 'REAL',
                'MA120': 'REAL',
                'MA250': 'REAL',
                'MACD_DIF': 'REAL',
                'MACD_DEA': 'REAL',
                'MACD_HIST': 'REAL',
                'RSI6': 'REAL',
                'RSI12': 'REAL',
                'RSI24': 'REAL',
                'KDJ_K': 'REAL',
                'KDJ_D': 'REAL',
                'KDJ_J': 'REAL',
                'BOLL_UPPER': 'REAL',
                'BOLL_MIDDLE': 'REAL',
                'BOLL_LOWER': 'REAL',
                'VOL5': 'REAL',
                'VOL10': 'REAL',
                'VOL20': 'REAL',
                'created_at': 'TEXT DEFAULT CURRENT_TIMESTAMP'
            }
        )
        # 添加唯一约束
        db_manager.execute_update(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_technical_indicators_date_code ON technical_indicators(date, code)"
        )
        logger.info("technical_indicators表创建完成")
        
        # 创建analysis_results表
        logger.info("开始创建analysis_results表...")
        db_manager.create_table(
            'analysis_results',
            {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'stock_code': 'TEXT NOT NULL',
                'analysis_date': 'TEXT NOT NULL',
                'strategy': 'TEXT NOT NULL',
                'rating': 'TEXT NOT NULL',
                'score': 'REAL',
                'macd_signal': 'TEXT',
                'rsi_signal': 'TEXT',
                'kdj_signal': 'TEXT',
                'boll_signal': 'TEXT',
                'ma_signal': 'TEXT',
                'llm_analysis': 'TEXT',
                'llm_provider': 'TEXT',
                'llm_model': 'TEXT',
                'risk_level': 'TEXT',
                'expected_return': 'REAL',
                'created_at': 'TEXT DEFAULT CURRENT_TIMESTAMP'
            }
        )
        # 添加唯一约束
        db_manager.execute_update(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_results_stock_date_strategy ON analysis_results(stock_code, analysis_date, strategy)"
        )
        logger.info("analysis_results表创建完成")
        
        # 创建analysis_reports表
        logger.info("开始创建analysis_reports表...")
        db_manager.create_table(
            'analysis_reports',
            {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'stock_code': 'TEXT NOT NULL',
                'analysis_date': 'TEXT NOT NULL',
                'report_type': 'TEXT NOT NULL',
                'report_content': 'TEXT NOT NULL',
                'created_at': 'TEXT DEFAULT CURRENT_TIMESTAMP'
            }
        )
        # 添加唯一约束
        db_manager.execute_update(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_analysis_reports_stock_date_type ON analysis_reports(stock_code, analysis_date, report_type)"
        )
        logger.info("analysis_reports表创建完成")
        
        # 创建strategy_performance表
        logger.info("开始创建strategy_performance表...")
        db_manager.create_table(
            'strategy_performance',
            {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'strategy_name': 'TEXT NOT NULL',
                'start_date': 'TEXT NOT NULL',
                'end_date': 'TEXT NOT NULL',
                'total_return': 'REAL',
                'annual_return': 'REAL',
                'max_drawdown': 'REAL',
                'sharpe_ratio': 'REAL',
                'win_rate': 'REAL',
                'profit_loss_ratio': 'REAL',
                'trades_count': 'INTEGER',
                'created_at': 'TEXT DEFAULT CURRENT_TIMESTAMP'
            }
        )
        # 添加唯一约束
        db_manager.execute_update(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_performance_name_date ON strategy_performance(strategy_name, start_date, end_date)"
        )
        logger.info("strategy_performance表创建完成")
        
        logger.info("所有表创建完成")
    except Exception as e:
        logger.error(f"创建表结构失败: {str(e)}")
        raise
    finally:
        # 关闭数据库连接
        try:
            db_manager.disconnect()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {str(e)}")


if __name__ == '__main__':
    create_tables()
    print("数据库表结构创建完成")