import pandas as pd
from sqlite_db_manager import SQLiteDBManager
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/result_storage.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('result_storage')


class ResultStorage:
    """
    结果存储模块类
    负责将分析结果存储到数据库中
    """
    
    def __init__(self, db_path: str = 'stock_data.db'):
        """
        初始化结果存储模块
        
        Args:
            db_path: 数据库文件路径，默认'stock_data.db'
        """
        self.db_path = db_path
        self.db_manager = SQLiteDBManager(db_path)
        self.logger = logger
        self.logger.info(f"结果存储模块已初始化，数据库路径: {db_path}")
    
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
    
    def save_technical_indicators(self, df: pd.DataFrame):
        """
        保存技术指标到数据库
        
        Args:
            df: 包含技术指标的DataFrame
        """
        try:
            self.connect()
            
            # 确保DataFrame包含必要的列
            required_columns = ['date', 'code']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"DataFrame缺少必要的列: {col}")
            
            # 保存到数据库
            self.db_manager.write_dataframe(df, 'technical_indicators', if_exists='append', index=False)
            self.logger.info(f"成功保存 {len(df)} 条技术指标数据")
        except Exception as e:
            self.logger.error(f"保存技术指标失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def save_analysis_result(self, result: dict):
        """
        保存分析结果到数据库
        
        Args:
            result: 分析结果字典
        """
        try:
            self.connect()
            
            # 确保结果包含必要的字段
            required_fields = ['stock_code', 'analysis_date', 'strategy', 'rating']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"分析结果缺少必要的字段: {field}")
            
            # 提取信号字段
            signals = result.get('signals', {})
            
            # 构建插入数据
            data = {
                'stock_code': result['stock_code'],
                'analysis_date': result['analysis_date'],
                'strategy': result['strategy'],
                'rating': result['rating'],
                'score': result.get('score'),
                'macd_signal': signals.get('macd'),
                'rsi_signal': signals.get('rsi'),
                'kdj_signal': signals.get('kdj'),
                'boll_signal': signals.get('bollinger'),
                'ma_signal': signals.get('ma'),
                'llm_analysis': result.get('llm_analysis'),
                'llm_provider': result.get('llm_provider'),
                'llm_model': result.get('llm_model'),
                'risk_level': result.get('risk_level'),
                'expected_return': result.get('expected_return')
            }
            
            # 插入数据
            self.db_manager.insert('analysis_results', data)
            self.logger.info(f"成功保存股票 {result['stock_code']} 的分析结果")
        except Exception as e:
            self.logger.error(f"保存分析结果失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def save_batch_analysis_results(self, results: dict):
        """
        批量保存分析结果
        
        Args:
            results: 以股票代码为键，分析结果为值的字典
        """
        try:
            self.connect()
            
            # 开始事务
            self.db_manager.begin_transaction()
            
            count = 0
            for stock_code, result in results.items():
                try:
                    # 确保结果包含必要的字段
                    required_fields = ['stock_code', 'analysis_date', 'strategy', 'rating']
                    for field in required_fields:
                        if field not in result:
                            self.logger.warning(f"股票 {stock_code} 的分析结果缺少必要字段 {field}，跳过")
                            continue
                    
                    # 提取信号字段
                    signals = result.get('signals', {})
                    
                    # 构建插入数据
                    data = {
                        'stock_code': result['stock_code'],
                        'analysis_date': result['analysis_date'],
                        'strategy': result['strategy'],
                        'rating': result['rating'],
                        'score': result.get('score'),
                        'macd_signal': signals.get('macd'),
                        'rsi_signal': signals.get('rsi'),
                        'kdj_signal': signals.get('kdj'),
                        'boll_signal': signals.get('bollinger'),
                        'ma_signal': signals.get('ma'),
                        'llm_analysis': result.get('llm_analysis'),
                        'llm_provider': result.get('llm_provider'),
                        'llm_model': result.get('llm_model'),
                        'risk_level': result.get('risk_level'),
                        'expected_return': result.get('expected_return')
                    }
                    
                    # 插入数据
                    self.db_manager.insert('analysis_results', data)
                    count += 1
                except Exception as e:
                    self.logger.error(f"保存股票 {stock_code} 的分析结果失败: {str(e)}")
                    continue
            
            # 提交事务
            self.db_manager.commit_transaction()
            self.logger.info(f"批量保存完成，成功保存 {count} 条分析结果")
        except Exception as e:
            # 回滚事务
            self.db_manager.rollback_transaction()
            self.logger.error(f"批量保存分析结果失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def save_analysis_report(self, stock_code: str, analysis_date: str, report_type: str, report_content: str):
        """
        保存分析报告到数据库
        
        Args:
            stock_code: 股票代码
            analysis_date: 分析日期
            report_type: 报告类型
            report_content: 报告内容
        """
        try:
            self.connect()
            
            # 构建插入数据
            data = {
                'stock_code': stock_code,
                'analysis_date': analysis_date,
                'report_type': report_type,
                'report_content': report_content
            }
            
            # 插入数据
            self.db_manager.insert('analysis_reports', data)
            self.logger.info(f"成功保存股票 {stock_code} 的分析报告")
        except Exception as e:
            self.logger.error(f"保存分析报告失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def save_strategy_performance(self, performance: dict):
        """
        保存策略绩效到数据库
        
        Args:
            performance: 策略绩效字典
        """
        try:
            self.connect()
            
            # 确保绩效包含必要的字段
            required_fields = ['strategy_name', 'start_date', 'end_date']
            for field in required_fields:
                if field not in performance:
                    raise ValueError(f"策略绩效缺少必要的字段: {field}")
            
            # 插入数据
            self.db_manager.insert('strategy_performance', performance)
            self.logger.info(f"成功保存策略 {performance['strategy_name']} 的绩效数据")
        except Exception as e:
            self.logger.error(f"保存策略绩效失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_analysis_results(self, stock_code: str = None, start_date: str = None, end_date: str = None, 
                            strategy: str = None) -> pd.DataFrame:
        """
        获取分析结果
        
        Args:
            stock_code: 股票代码，默认None（所有股票）
            start_date: 开始日期，默认None（不限制）
            end_date: 结束日期，默认None（不限制）
            strategy: 分析策略，默认None（所有策略）
            
        Returns:
            分析结果的DataFrame
        """
        try:
            self.connect()
            
            # 构建查询条件
            where_clause = []
            params = []
            
            if stock_code:
                where_clause.append("stock_code = ?")
                params.append(stock_code)
            
            if start_date:
                where_clause.append("analysis_date >= ?")
                params.append(start_date)
            
            if end_date:
                where_clause.append("analysis_date <= ?")
                params.append(end_date)
            
            if strategy:
                where_clause.append("strategy = ?")
                params.append(strategy)
            
            # 构建完整查询
            query = "SELECT * FROM analysis_results"
            if where_clause:
                query += " WHERE " + " AND ".join(where_clause)
            
            query += " ORDER BY analysis_date DESC"
            
            # 执行查询
            df = self.db_manager.read_dataframe(query, params=tuple(params) if params else None)
            self.logger.info(f"成功获取 {len(df)} 条分析结果")
            return df
        except Exception as e:
            self.logger.error(f"获取分析结果失败: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_technical_indicators(self, stock_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取技术指标数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，默认None（不限制）
            end_date: 结束日期，默认None（不限制）
            
        Returns:
            技术指标数据的DataFrame
        """
        try:
            self.connect()
            
            # 构建查询条件
            where_clause = ["code = ?"]
            params = [stock_code]
            
            if start_date:
                where_clause.append("date >= ?")
                params.append(start_date)
            
            if end_date:
                where_clause.append("date <= ?")
                params.append(end_date)
            
            # 构建完整查询
            query = f"SELECT * FROM technical_indicators WHERE {' AND '.join(where_clause)} ORDER BY date"
            
            # 执行查询
            df = self.db_manager.read_dataframe(query, params=tuple(params))
            self.logger.info(f"成功获取股票 {stock_code} 的 {len(df)} 条技术指标数据")
            return df
        except Exception as e:
            self.logger.error(f"获取技术指标数据失败: {str(e)}")
            raise
        finally:
            self.disconnect()


# 使用示例
if __name__ == '__main__':
    # 创建结果存储实例
    storage = ResultStorage()
    
    # 示例：保存分析结果
    sample_result = {
        'stock_code': 'sh.600000',
        'analysis_date': '2023-10-01',
        'strategy': 'traditional_technical_analysis',
        'rating': 'buy',
        'score': 85,
        'signals': {
            'macd': 'buy',
            'rsi': 'hold',
            'kdj': 'buy',
            'bollinger': 'hold',
            'ma': 'buy'
        },
        'risk_level': 'medium',
        'expected_return': 0.15
    }
    
    try:
        storage.save_analysis_result(sample_result)
        print("分析结果保存成功")
    except Exception as e:
        print(f"分析结果保存失败: {str(e)}")