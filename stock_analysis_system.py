import json
import datetime
from stock_data_reader import StockDataReader
from technical_indicator_calculator import TechnicalIndicatorCalculator
from traditional_analysis_engine import TraditionalAnalysisEngine
from llm_analysis_engine import LLMAnalysisEngine
from sampling_module import SamplingModule
from result_storage import ResultStorage
from report_generator import ReportGenerator
from strategy_evaluator import StrategyEvaluator
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/stock_analysis_system.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('stock_analysis_system')


class StockAnalysisSystem:
    """
    股票分析系统主类
    整合所有模块，提供完整的股票分析功能
    """
    
    def __init__(self, config_path: str = 'config.json'):
        """
        初始化股票分析系统
        
        Args:
            config_path: 配置文件路径，默认'config.json'
        """
        # 初始化logger
        self.logger = logger
        
        # 加载配置
        self.config = self._load_config(config_path)
        
        # 初始化各模块
        self.data_reader = StockDataReader(self.config.get('output', {}).get('database_path', 'stock_data.db'))
        self.indicator_calculator = TechnicalIndicatorCalculator()
        self.traditional_engine = TraditionalAnalysisEngine()
        self.llm_engine = LLMAnalysisEngine(self.config.get('analysis', {}).get('llm', {}))
        self.sampler = SamplingModule()
        self.storage = ResultStorage(self.config.get('output', {}).get('database_path', 'stock_data.db'))
        self.report_generator = ReportGenerator(self.config.get('analysis', {}).get('report', {}).get('save_path', 'reports'))
        self.evaluator = StrategyEvaluator(self.config.get('output', {}).get('database_path', 'stock_data.db'))
        
        self.logger.info("股票分析系统已初始化")
    
    def _load_config(self, config_path: str) -> dict:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            配置字典
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.logger.info(f"成功加载配置文件: {config_path}")
            return config
        except FileNotFoundError:
            self.logger.warning(f"配置文件 {config_path} 不存在，使用默认配置")
            return {}
        except json.JSONDecodeError:
            self.logger.error(f"配置文件 {config_path} 格式错误，使用默认配置")
            return {}
    
    def analyze_stock(self, stock_code: str, use_llm: bool = False, save_report: bool = True) -> dict:
        """
        分析单只股票
        
        Args:
            stock_code: 股票代码
            use_llm: 是否使用大模型分析
            save_report: 是否保存报告
            
        Returns:
            分析结果字典
        """
        try:
            # 获取股票数据
            stock_data = self.data_reader.get_stock_data(stock_code)
            
            if stock_data.empty:
                self.logger.warning(f"未获取到股票 {stock_code} 的数据")
                return {}
            
            # 计算技术指标
            technical_data = self.indicator_calculator.calculate_all_indicators(stock_data)
            
            # 保存技术指标到数据库
            self.storage.save_technical_indicators(technical_data)
            
            # 传统技术分析
            traditional_result = self.traditional_engine.analyze(technical_data)
            
            # 如果使用大模型分析
            if use_llm:
                # 准备大模型分析数据
                latest_data = stock_data.iloc[-1].to_dict()
                latest_indicators = technical_data.iloc[-1].to_dict()
                
                # 大模型分析
                llm_result = self.llm_engine.analyze_stock(stock_code, latest_data, latest_indicators)
                
                # 合并结果
                traditional_result.update(llm_result)
            
            # 保存分析结果
            self.storage.save_analysis_result(traditional_result)
            
            # 生成并保存报告
            if save_report:
                self.report_generator.generate_and_save_report(stock_code, traditional_result, technical_data)
            
            self.logger.info(f"成功分析股票 {stock_code}")
            return traditional_result
        except Exception as e:
            self.logger.error(f"分析股票 {stock_code} 失败: {str(e)}")
            raise
    
    def batch_analyze_stocks(self, stock_codes: list = None, use_llm: bool = False, save_reports: bool = True) -> dict:
        """
        批量分析多只股票
        
        Args:
            stock_codes: 股票代码列表（可选，默认所有股票）
            use_llm: 是否使用大模型分析
            save_reports: 是否保存报告
            
        Returns:
            批量分析结果，以股票代码为键
        """
        try:
            # 如果没有提供股票代码列表，获取所有股票代码
            if not stock_codes:
                stock_codes = self.data_reader.get_stock_codes()
                
            # 测试模式下抽样
            analysis_config = self.config.get('analysis', {})
            sampling_config = analysis_config.get('sampling', {})
            is_test_mode = self.config.get('test_mode', {}).get('enabled', False)
            
            if is_test_mode:
                sample_size = self.config.get('test_mode', {}).get('max_stocks', 10)
                stock_codes = self.sampler.random_sampling(stock_codes, max_samples=sample_size)
            elif sampling_config.get('enabled', False):
                stock_codes = self.sampler.sample(stock_codes, 
                                               mode=sampling_config.get('mode', 'random'),
                                               config=sampling_config)
            
            results = {}
            
            for stock_code in stock_codes:
                try:
                    result = self.analyze_stock(stock_code, use_llm, save_reports)
                    results[stock_code] = result
                except Exception as e:
                    self.logger.error(f"分析股票 {stock_code} 失败: {str(e)}")
                    continue
            
            self.logger.info(f"批量分析完成，成功分析 {len(results)} 只股票")
            return results
        except Exception as e:
            self.logger.error(f"批量分析股票失败: {str(e)}")
            raise
    
    def generate_strategy_comparison(self, strategy_names: list, start_date: str = None, end_date: str = None) -> str:
        """
        生成策略对比报告
        
        Args:
            strategy_names: 策略名称列表
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            
        Returns:
            Markdown格式的策略对比报告
        """
        try:
            # 如果没有提供日期，使用最近一年
            if not start_date:
                end_date = datetime.datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
            elif not end_date:
                end_date = datetime.datetime.now().strftime('%Y-%m-%d')
            
            # 生成对比报告
            report = self.evaluator.generate_comparison_report(strategy_names, start_date, end_date)
            
            # 保存报告到文件
            report_path = f"{self.report_generator.report_dir}/strategy_comparison_{start_date}_{end_date}.md"
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report)
            
            self.logger.info(f"成功生成策略对比报告，保存路径: {report_path}")
            return report
        except Exception as e:
            self.logger.error(f"生成策略对比报告失败: {str(e)}")
            raise
    
    def run_backtest(self, strategy_name: str, start_date: str, end_date: str, stock_codes: list = None) -> dict:
        """
        回测单个策略
        
        Args:
            strategy_name: 策略名称
            start_date: 开始日期
            end_date: 结束日期
            stock_codes: 股票代码列表（可选）
            
        Returns:
            回测结果字典
        """
        try:
            # 计算策略绩效
            performance = self.evaluator.calculate_performance(strategy_name, start_date, end_date, stock_codes)
            
            if performance:
                # 保存绩效数据
                self.evaluator.save_performance(performance)
            
            self.logger.info(f"成功回测策略 {strategy_name}")
            return performance
        except Exception as e:
            self.logger.error(f"回测策略 {strategy_name} 失败: {str(e)}")
            raise
    
    def batch_run_backtest(self, strategy_names: list, start_date: str, end_date: str, stock_codes: list = None) -> list:
        """
        批量回测多个策略
        
        Args:
            strategy_names: 策略名称列表
            start_date: 开始日期
            end_date: 结束日期
            stock_codes: 股票代码列表（可选）
            
        Returns:
            回测结果列表
        """
        try:
            performances = []
            
            for strategy_name in strategy_names:
                try:
                    performance = self.run_backtest(strategy_name, start_date, end_date, stock_codes)
                    if performance:
                        performances.append(performance)
                except Exception as e:
                    self.logger.error(f"回测策略 {strategy_name} 失败: {str(e)}")
                    continue
            
            # 批量保存绩效数据
            if performances:
                self.evaluator.batch_save_performance(performances)
            
            self.logger.info(f"批量回测完成，成功回测 {len(performances)} 个策略")
            return performances
        except Exception as e:
            self.logger.error(f"批量回测策略失败: {str(e)}")
            raise


# 使用示例
if __name__ == '__main__':
    # 创建股票分析系统实例
    system = StockAnalysisSystem()
    
    # 示例1：分析单只股票
    try:
        result = system.analyze_stock('sh.600000', use_llm=False, save_report=True)
        print("股票分析结果:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"分析股票失败: {str(e)}")
    
    # 示例2：批量分析股票
    try:
        # 获取所有股票代码
        stock_codes = system.data_reader.get_stock_codes()
        # 只分析前5只股票
        results = system.batch_analyze_stocks(stock_codes[:5], use_llm=False, save_reports=True)
        print(f"\n批量分析完成，成功分析 {len(results)} 只股票")
    except Exception as e:
        print(f"批量分析股票失败: {str(e)}")
    
    # 示例3：生成策略对比报告
    try:
        strategies = ['traditional_technical_analysis']
        report = system.generate_strategy_comparison(strategies)
        print(f"\n策略对比报告生成完成")
    except Exception as e:
        print(f"生成策略对比报告失败: {str(e)}")