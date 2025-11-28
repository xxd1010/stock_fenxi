import random
import pandas as pd
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/sampling_module.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('sampling_module')


class SamplingModule:
    """
    抽样模块类
    支持随机抽样和分层抽样
    """
    
    def __init__(self):
        """
        初始化抽样模块
        """
        self.logger = logger
        self.logger.info("抽样模块已初始化")
    
    def random_sampling(self, stock_codes: list, ratio: float = 0.1, max_samples: int = None) -> list:
        """
        随机抽样
        
        Args:
            stock_codes: 股票代码列表
            ratio: 抽样比例，默认0.1（10%）
            max_samples: 最大样本数，默认None（不限制）
            
        Returns:
            抽样后的股票代码列表
        """
        try:
            total_stocks = len(stock_codes)
            if total_stocks == 0:
                self.logger.warning("股票代码列表为空，返回空列表")
                return []
            
            # 计算抽样数量
            sample_size = int(total_stocks * ratio)
            
            # 如果设置了最大样本数，取较小值
            if max_samples and sample_size > max_samples:
                sample_size = max_samples
            
            # 确保至少返回1只股票（如果有股票的话）
            sample_size = max(1, sample_size)
            
            # 随机抽样
            sampled_stocks = random.sample(stock_codes, sample_size)
            
            self.logger.info(f"随机抽样完成，从 {total_stocks} 只股票中抽取了 {len(sampled_stocks)} 只")
            return sampled_stocks
        except Exception as e:
            self.logger.error(f"随机抽样失败: {str(e)}")
            raise
    
    def stratified_sampling(self, stock_codes: list, stock_info_dict: dict = None, 
                           stratify_by: str = 'market', ratio: float = 0.1, 
                           max_samples: int = None) -> list:
        """
        分层抽样
        
        Args:
            stock_codes: 股票代码列表
            stock_info_dict: 股票信息字典，包含分层依据的字段
            stratify_by: 分层依据，默认'market'（市场，如上海、深圳）
            ratio: 抽样比例，默认0.1（10%）
            max_samples: 最大样本数，默认None（不限制）
            
        Returns:
            抽样后的股票代码列表
        """
        try:
            total_stocks = len(stock_codes)
            if total_stocks == 0:
                self.logger.warning("股票代码列表为空，返回空列表")
                return []
            
            # 如果没有提供股票信息字典，默认按市场分层
            if not stock_info_dict:
                stock_info_dict = {}
                for code in stock_codes:
                    # 根据股票代码前缀判断市场
                    if code.startswith('sh.'):
                        market = 'shanghai'
                    elif code.startswith('sz.'):
                        market = 'shenzhen'
                    else:
                        market = 'other'
                    stock_info_dict[code] = {'market': market}
            
            # 按分层依据分组
            strata = {}
            for code in stock_codes:
                if code in stock_info_dict:
                    key = stock_info_dict[code].get(stratify_by, 'unknown')
                    if key not in strata:
                        strata[key] = []
                    strata[key].append(code)
            
            sampled_stocks = []
            
            # 对每个分层进行抽样
            for key, codes in strata.items():
                # 计算该分层的抽样数量
                stratum_size = len(codes)
                sample_size = int(stratum_size * ratio)
                sample_size = max(1, sample_size)  # 每层至少抽取1只股票
                
                # 随机抽样
                stratum_samples = random.sample(codes, sample_size)
                sampled_stocks.extend(stratum_samples)
            
            # 如果设置了最大样本数，随机选择指定数量
            if max_samples and len(sampled_stocks) > max_samples:
                sampled_stocks = random.sample(sampled_stocks, max_samples)
            
            self.logger.info(f"分层抽样完成，从 {total_stocks} 只股票中抽取了 {len(sampled_stocks)} 只")
            self.logger.debug(f"分层抽样详情: {strata}")
            return sampled_stocks
        except Exception as e:
            self.logger.error(f"分层抽样失败: {str(e)}")
            raise
    
    def sample(self, stock_codes: list, mode: str = 'random', config: dict = None) -> list:
        """
        统一抽样接口
        
        Args:
            stock_codes: 股票代码列表
            mode: 抽样模式，'random'（随机抽样）或'stratified'（分层抽样），默认'random'
            config: 抽样配置字典
            
        Returns:
            抽样后的股票代码列表
        """
        try:
            # 默认配置
            default_config = {
                'ratio': 0.1,
                'max_samples': None,
                'stratify_by': 'market',
                'stock_info_dict': None
            }
            
            # 合并配置
            if config:
                default_config.update(config)
            
            self.logger.info(f"开始抽样，模式: {mode}，配置: {default_config}")
            
            if mode == 'random':
                return self.random_sampling(stock_codes, default_config['ratio'], default_config['max_samples'])
            elif mode == 'stratified':
                return self.stratified_sampling(stock_codes, default_config['stock_info_dict'], 
                                             default_config['stratify_by'], default_config['ratio'], 
                                             default_config['max_samples'])
            else:
                self.logger.error(f"不支持的抽样模式: {mode}")
                raise ValueError(f"不支持的抽样模式: {mode}")
        except Exception as e:
            self.logger.error(f"抽样失败: {str(e)}")
            raise
    
    def batch_sample(self, stock_data_dict: dict, mode: str = 'random', config: dict = None) -> dict:
        """
        批量抽样
        
        Args:
            stock_data_dict: 以股票代码为键，DataFrame为值的字典
            mode: 抽样模式，'random'（随机抽样）或'stratified'（分层抽样），默认'random'
            config: 抽样配置字典
            
        Returns:
            抽样后的股票数据字典
        """
        try:
            stock_codes = list(stock_data_dict.keys())
            sampled_codes = self.sample(stock_codes, mode, config)
            
            # 构建抽样后的字典
            sampled_data_dict = {code: stock_data_dict[code] for code in sampled_codes}
            
            self.logger.info(f"批量抽样完成，从 {len(stock_codes)} 只股票中抽取了 {len(sampled_data_dict)} 只")
            return sampled_data_dict
        except Exception as e:
            self.logger.error(f"批量抽样失败: {str(e)}")
            raise


# 使用示例
if __name__ == '__main__':
    # 创建示例股票代码列表
    stock_codes = [f'sh.{i}' for i in range(600000, 600100)] + [f'sz.{i}' for i in range(1, 100)]
    
    # 创建抽样模块实例
    sampler = SamplingModule()
    
    # 测试随机抽样
    random_samples = sampler.random_sampling(stock_codes, ratio=0.2, max_samples=20)
    print(f"随机抽样结果: {random_samples}")
    print(f"随机抽样数量: {len(random_samples)}")
    
    # 测试分层抽样
    stratified_samples = sampler.stratified_sampling(stock_codes, ratio=0.2, max_samples=20)
    print(f"\n分层抽样结果: {stratified_samples}")
    print(f"分层抽样数量: {len(stratified_samples)}")
    
    # 测试统一抽样接口
    sample_result = sampler.sample(stock_codes, mode='random', config={'ratio': 0.15})
    print(f"\n统一抽样接口结果: {sample_result}")
    print(f"统一抽样接口数量: {len(sample_result)}")