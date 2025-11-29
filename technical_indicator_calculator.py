import pandas as pd
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/technical_indicator_calculator.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('technical_indicator_calculator')


class TechnicalIndicatorCalculator:
    """
    技术指标计算器类
    用于计算股票的各种技术指标，包括MACD、RSI、均线等
    """
    
    def __init__(self):
        """
        初始化技术指标计算器
        """
        self.logger = logger
        self.logger.info("技术指标计算器已初始化")
    
    def calculate_ma(self, df: pd.DataFrame, periods: list = [5, 10, 20, 60, 120, 250]) -> pd.DataFrame:
        """
        计算均线指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'close'列
            periods: 均线周期列表，默认[5, 10, 20, 60, 120, 250]
            
        Returns:
            添加了均线指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            for period in periods:
                # 使用pandas计算均线
                result_df[f'MA{period}'] = result_df['close'].rolling(window=period).mean()
            
            self.logger.info(f"成功计算均线指标，周期: {periods}")
            return result_df
        except Exception as e:
            self.logger.error(f"计算均线指标失败: {str(e)}")
            raise
    
    def calculate_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """
        计算MACD指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'close'列
            fast: 快线周期，默认12
            slow: 慢线周期，默认26
            signal: 信号线周期，默认9
            
        Returns:
            添加了MACD指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            # 计算EMA
            ema_fast = result_df['close'].ewm(span=fast, adjust=False).mean()
            ema_slow = result_df['close'].ewm(span=slow, adjust=False).mean()
            
            # 计算DIF
            dif = ema_fast - ema_slow
            
            # 计算DEA
            dea = dif.ewm(span=signal, adjust=False).mean()
            
            # 计算柱状图
            hist = 2 * (dif - dea)
            
            # 添加到DataFrame
            result_df['MACD_DIF'] = dif
            result_df['MACD_DEA'] = dea
            result_df['MACD_HIST'] = hist
            
            self.logger.info(f"成功计算MACD指标，参数: fast={fast}, slow={slow}, signal={signal}")
            return result_df
        except Exception as e:
            self.logger.error(f"计算MACD指标失败: {str(e)}")
            raise
    
    def calculate_rsi(self, df: pd.DataFrame, periods: list = [6, 12, 24]) -> pd.DataFrame:
        """
        计算RSI指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'close'列
            periods: RSI周期列表，默认[6, 12, 24]
            
        Returns:
            添加了RSI指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            # 只计算一次价格变化、涨跌
            delta = result_df['close'].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            
            for period in periods:
                # 计算平均涨跌
                avg_gain = gain.rolling(window=period).mean()
                avg_loss = loss.rolling(window=period).mean()
                
                # 计算RSI
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                
                result_df[f'RSI{period}'] = rsi
            
            self.logger.info(f"成功计算RSI指标，周期: {periods}")
            return result_df
        except Exception as e:
            self.logger.error(f"计算RSI指标失败: {str(e)}")
            raise
    
    def calculate_kdj(self, df: pd.DataFrame, length: int = 9, signal: int = 3) -> pd.DataFrame:
        """
        计算KDJ指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'high'、'low'、'close'列
            length: KDJ周期，默认9
            signal: 信号线周期，默认3
            
        Returns:
            添加了KDJ指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            # 计算最高价和最低价的n日移动平均
            high_n = result_df['high'].rolling(window=length).max()
            low_n = result_df['low'].rolling(window=length).min()
            
            # 计算RSV
            rsv = (result_df['close'] - low_n) / (high_n - low_n) * 100
            
            # 计算K值
            k = rsv.ewm(com=signal-1, adjust=False).mean()
            
            # 计算D值
            d = k.ewm(com=signal-1, adjust=False).mean()
            
            # 计算J值
            j = 3 * k - 2 * d
            
            # 添加到DataFrame
            result_df['KDJ_K'] = k
            result_df['KDJ_D'] = d
            result_df['KDJ_J'] = j
            
            self.logger.info(f"成功计算KDJ指标，参数: length={length}, signal={signal}")
            return result_df
        except Exception as e:
            self.logger.error(f"计算KDJ指标失败: {str(e)}")
            raise
    
    def calculate_bollinger(self, df: pd.DataFrame, length: int = 20, std: int = 2) -> pd.DataFrame:
        """
        计算布林带指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'close'列
            length: 布林带周期，默认20
            std: 标准差倍数，默认2
            
        Returns:
            添加了布林带指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            # 计算中轨（20日均线）
            middle = result_df['close'].rolling(window=length).mean()
            
            # 计算标准差
            std_dev = result_df['close'].rolling(window=length).std()
            
            # 计算上轨和下轨
            upper = middle + std * std_dev
            lower = middle - std * std_dev
            
            # 添加到DataFrame
            result_df['BOLL_UPPER'] = upper
            result_df['BOLL_MIDDLE'] = middle
            result_df['BOLL_LOWER'] = lower
            
            self.logger.info(f"成功计算布林带指标，参数: length={length}, std={std}")
            return result_df
        except Exception as e:
            self.logger.error(f"计算布林带指标失败: {str(e)}")
            raise
    
    def calculate_volume_ma(self, df: pd.DataFrame, periods: list = [5, 10, 20]) -> pd.DataFrame:
        """
        计算成交量均线指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'volume'列
            periods: 成交量均线周期列表，默认[5, 10, 20]
            
        Returns:
            添加了成交量均线指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            for period in periods:
                # 计算成交量均线
                vol_ma = result_df['volume'].rolling(window=period).mean()
                result_df[f'VOL{period}'] = vol_ma
            
            self.logger.info(f"成功计算成交量均线指标，周期: {periods}")
            return result_df
        except Exception as e:
            self.logger.error(f"计算成交量均线指标失败: {str(e)}")
            raise
    
    def calculate_all_indicators(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        计算所有技术指标
        
        Args:
            df: 包含股票数据的DataFrame，必须包含'open'、'high'、'low'、'close'、'volume'列
            config: 指标配置字典，包含各指标的参数设置
            
        Returns:
            添加了所有技术指标的DataFrame
        """
        try:
            result_df = df.copy()
            
            # 默认配置
            default_config = {
                'ma_periods': [5, 10, 20, 60, 120, 250],
                'macd': {'fast': 12, 'slow': 26, 'signal': 9},
                'rsi_periods': [6, 12, 24],
                'kdj': {'length': 9, 'signal': 3},
                'bollinger': {'length': 20, 'std': 2},
                'volume_ma_periods': [5, 10, 20]
            }
            
            # 合并配置
            if config:
                default_config.update(config)
            
            self.logger.info(f"开始计算所有技术指标，配置: {default_config}")
            
            # 计算各指标
            result_df = self.calculate_ma(result_df, default_config['ma_periods'])
            result_df = self.calculate_macd(result_df, **default_config['macd'])
            result_df = self.calculate_rsi(result_df, default_config['rsi_periods'])
            result_df = self.calculate_kdj(result_df, **default_config['kdj'])
            result_df = self.calculate_bollinger(result_df, **default_config['bollinger'])
            result_df = self.calculate_volume_ma(result_df, default_config['volume_ma_periods'])
            
            self.logger.info("所有技术指标计算完成")
            return result_df
        except Exception as e:
            self.logger.error(f"计算所有技术指标失败: {str(e)}")
            raise


# 使用示例
if __name__ == '__main__':
    # 创建一个示例DataFrame
    data = {
        'date': pd.date_range(start='2023-01-01', periods=100),
        'open': pd.Series(range(100, 200)),
        'high': pd.Series(range(105, 205)),
        'low': pd.Series(range(95, 195)),
        'close': pd.Series(range(100, 200)),
        'volume': pd.Series(range(1000, 1100))
    }
    df = pd.DataFrame(data)
    
    # 创建技术指标计算器实例
    calculator = TechnicalIndicatorCalculator()
    
    # 计算所有指标
    result_df = calculator.calculate_all_indicators(df)
    
    # 打印结果
    print("计算结果示例:")
    print(result_df.tail())
    
    # 打印指标列名
    print("\n计算的指标列:")
    indicator_columns = [col for col in result_df.columns if col not in data.keys()]
    print(indicator_columns)