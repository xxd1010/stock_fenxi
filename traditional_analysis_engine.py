import pandas as pd
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/traditional_analysis_engine.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('traditional_analysis_engine')


class TraditionalAnalysisEngine:
    """
    传统分析引擎类
    基于技术指标生成投资建议
    """
    
    def __init__(self):
        """
        初始化传统分析引擎
        """
        self.logger = logger
        self.logger.info("传统分析引擎已初始化")
    
    def analyze_macd(self, df: pd.DataFrame) -> str:
        """
        分析MACD指标，生成信号
        
        Args:
            df: 包含MACD指标的DataFrame
            
        Returns:
            MACD信号：'buy'、'sell'或'hold'
        """
        try:
            # 获取最新的MACD数据
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            # MACD金叉：DIF上穿DEA
            if previous['MACD_DIF'] < previous['MACD_DEA'] and latest['MACD_DIF'] > latest['MACD_DEA']:
                return 'buy'
            # MACD死叉：DIF下穿DEA
            elif previous['MACD_DIF'] > previous['MACD_DEA'] and latest['MACD_DIF'] < latest['MACD_DEA']:
                return 'sell'
            else:
                return 'hold'
        except Exception as e:
            self.logger.error(f"分析MACD指标失败: {str(e)}")
            return 'hold'
    
    def analyze_rsi(self, df: pd.DataFrame, period: int = 14) -> str:
        """
        分析RSI指标，生成信号
        
        Args:
            df: 包含RSI指标的DataFrame
            period: RSI周期，默认14
            
        Returns:
            RSI信号：'buy'、'sell'或'hold'
        """
        try:
            # 获取最新的RSI数据
            latest_rsi = df.iloc[-1][f'RSI{period}']
            
            # RSI超卖（<30）为买入信号
            if latest_rsi < 30:
                return 'buy'
            # RSI超买（>70）为卖出信号
            elif latest_rsi > 70:
                return 'sell'
            else:
                return 'hold'
        except Exception as e:
            self.logger.error(f"分析RSI指标失败: {str(e)}")
            return 'hold'
    
    def analyze_kdj(self, df: pd.DataFrame) -> str:
        """
        分析KDJ指标，生成信号
        
        Args:
            df: 包含KDJ指标的DataFrame
            
        Returns:
            KDJ信号：'buy'、'sell'或'hold'
        """
        try:
            # 获取最新的KDJ数据
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            # KDJ金叉：K值上穿D值
            if previous['KDJ_K'] < previous['KDJ_D'] and latest['KDJ_K'] > latest['KDJ_D']:
                return 'buy'
            # KDJ死叉：K值下穿D值
            elif previous['KDJ_K'] > previous['KDJ_D'] and latest['KDJ_K'] < latest['KDJ_D']:
                return 'sell'
            else:
                return 'hold'
        except Exception as e:
            self.logger.error(f"分析KDJ指标失败: {str(e)}")
            return 'hold'
    
    def analyze_bollinger(self, df: pd.DataFrame) -> str:
        """
        分析布林带指标，生成信号
        
        Args:
            df: 包含布林带指标的DataFrame
            
        Returns:
            布林带信号：'buy'、'sell'或'hold'
        """
        try:
            # 获取最新的布林带和收盘价数据
            latest = df.iloc[-1]
            close = latest['close']
            upper = latest['BOLL_UPPER']
            lower = latest['BOLL_LOWER']
            
            # 价格突破上轨为买入信号
            if close > upper:
                return 'buy'
            # 价格跌破下轨为卖出信号
            elif close < lower:
                return 'sell'
            else:
                return 'hold'
        except Exception as e:
            self.logger.error(f"分析布林带指标失败: {str(e)}")
            return 'hold'
    
    def analyze_ma(self, df: pd.DataFrame, short_period: int = 5, long_period: int = 20) -> str:
        """
        分析均线指标，生成信号
        
        Args:
            df: 包含均线指标的DataFrame
            short_period: 短期均线周期，默认5
            long_period: 长期均线周期，默认20
            
        Returns:
            均线信号：'buy'、'sell'或'hold'
        """
        try:
            # 获取最新的均线数据
            latest = df.iloc[-1]
            previous = df.iloc[-2]
            
            # 短期均线上穿长期均线为金叉（买入信号）
            if previous[f'MA{short_period}'] < previous[f'MA{long_period}'] and latest[f'MA{short_period}'] > latest[f'MA{long_period}']:
                return 'buy'
            # 短期均线下穿长期均线为死叉（卖出信号）
            elif previous[f'MA{short_period}'] > previous[f'MA{long_period}'] and latest[f'MA{short_period}'] < latest[f'MA{long_period}']:
                return 'sell'
            else:
                return 'hold'
        except Exception as e:
            self.logger.error(f"分析均线指标失败: {str(e)}")
            return 'hold'
    
    def calculate_score(self, signals: dict) -> int:
        """
        根据信号计算评分
        
        Args:
            signals: 包含各指标信号的字典
            
        Returns:
            评分（0-100）
        """
        # 初始化评分
        score = 50
        
        # 为每个信号分配权重
        weights = {
            'macd': 25,
            'rsi': 20,
            'kdj': 20,
            'bollinger': 15,
            'ma': 20
        }
        
        # 根据信号调整评分
        for indicator, signal in signals.items():
            weight = weights.get(indicator, 10)
            if signal == 'buy':
                score += weight
            elif signal == 'sell':
                score -= weight
        
        # 确保评分在0-100之间
        score = max(0, min(100, score))
        return score
    
    def determine_rating(self, score: int) -> str:
        """
        根据评分确定投资评级
        
        Args:
            score: 评分（0-100）
            
        Returns:
            投资评级：'buy'、'hold'或'sell'
        """
        if score >= 70:
            return 'buy'
        elif score <= 30:
            return 'sell'
        else:
            return 'hold'
    
    def determine_risk_level(self, df: pd.DataFrame) -> str:
        """
        确定风险等级
        
        Args:
            df: 包含股票数据的DataFrame
            
        Returns:
            风险等级：'low'、'medium'或'high'
        """
        try:
            # 计算最近30天的波动率
            returns = df['close'].pct_change().dropna()
            volatility = returns.std() * (252 ** 0.5)  # 年化波动率
            
            if volatility < 0.2:
                return 'low'
            elif volatility < 0.4:
                return 'medium'
            else:
                return 'high'
        except Exception as e:
            self.logger.error(f"确定风险等级失败: {str(e)}")
            return 'medium'
    
    def analyze(self, df: pd.DataFrame) -> dict:
        """
        综合分析所有技术指标，生成投资建议
        
        Args:
            df: 包含股票数据和技术指标的DataFrame
            
        Returns:
            分析结果字典，包含各指标信号、评分、评级、风险等级等
        """
        try:
            # 确保DataFrame有足够的数据
            if len(df) < 26:
                self.logger.warning("数据量不足，无法进行完整分析")
                return {
                    'error': '数据量不足，至少需要26条记录'
                }
            
            # 分析各指标
            signals = {
                'macd': self.analyze_macd(df),
                'rsi': self.analyze_rsi(df),
                'kdj': self.analyze_kdj(df),
                'bollinger': self.analyze_bollinger(df),
                'ma': self.analyze_ma(df)
            }
            
            # 计算评分
            score = self.calculate_score(signals)
            
            # 确定投资评级
            rating = self.determine_rating(score)
            
            # 确定风险等级
            risk_level = self.determine_risk_level(df)
            
            # 计算预期收益率（基于最近30天的平均涨幅）
            try:
                recent_returns = df['close'].pct_change().tail(30).dropna()
                avg_daily_return = recent_returns.mean()
                expected_return = avg_daily_return * 252  # 年化预期收益率
            except Exception as e:
                self.logger.error(f"计算预期收益率失败: {str(e)}")
                expected_return = 0
            
            # 构建分析结果
            result = {
                'signals': signals,
                'score': score,
                'rating': rating,
                'risk_level': risk_level,
                'expected_return': expected_return,
                'analysis_date': df.iloc[-1]['date'],
                'stock_code': df.iloc[-1]['code'],
                'strategy': 'traditional_technical_analysis'
            }
            
            self.logger.info(f"成功分析股票 {result['stock_code']}，评级: {result['rating']}，评分: {result['score']}")
            return result
        except Exception as e:
            self.logger.error(f"综合分析失败: {str(e)}")
            raise
    
    def batch_analyze(self, stock_data_dict: dict) -> dict:
        """
        批量分析多只股票
        
        Args:
            stock_data_dict: 以股票代码为键，DataFrame为值的字典
            
        Returns:
            批量分析结果，以股票代码为键，分析结果为值
        """
        results = {}
        
        for stock_code, df in stock_data_dict.items():
            try:
                result = self.analyze(df)
                results[stock_code] = result
            except Exception as e:
                self.logger.error(f"分析股票 {stock_code} 失败: {str(e)}")
                continue
        
        self.logger.info(f"批量分析完成，成功分析 {len(results)} 只股票")
        return results


# 使用示例
if __name__ == '__main__':
    # 创建一个示例DataFrame
    data = {
        'date': pd.date_range(start='2023-01-01', periods=100),
        'code': ['sh.600000'] * 100,
        'open': pd.Series(range(100, 200)),
        'high': pd.Series(range(105, 205)),
        'low': pd.Series(range(95, 195)),
        'close': pd.Series(range(100, 200)),
        'volume': pd.Series(range(1000, 1100)),
        'MA5': pd.Series(range(100, 200)),
        'MA10': pd.Series(range(100, 200)),
        'MA20': pd.Series(range(100, 200)),
        'MACD_DIF': pd.Series(range(-10, 90)),
        'MACD_DEA': pd.Series(range(-15, 85)),
        'MACD_HIST': pd.Series(range(-5, 5)),
        'RSI6': pd.Series(range(20, 70)),
        'RSI12': pd.Series(range(25, 75)),
        'RSI24': pd.Series(range(30, 80)),
        'KDJ_K': pd.Series(range(30, 80)),
        'KDJ_D': pd.Series(range(25, 75)),
        'KDJ_J': pd.Series(range(20, 90)),
        'BOLL_UPPER': pd.Series(range(110, 210)),
        'BOLL_MIDDLE': pd.Series(range(100, 200)),
        'BOLL_LOWER': pd.Series(range(90, 190)),
        'VOL5': pd.Series(range(1000, 1100)),
        'VOL10': pd.Series(range(1000, 1100)),
        'VOL20': pd.Series(range(1000, 1100))
    }
    df = pd.DataFrame(data)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # 创建传统分析引擎实例
    engine = TraditionalAnalysisEngine()
    
    # 分析单只股票
    result = engine.analyze(df)
    print("分析结果:")
    print(result)
    
    # 批量分析
    batch_result = engine.batch_analyze({'sh.600000': df})
    print("\n批量分析结果:")
    print(batch_result)