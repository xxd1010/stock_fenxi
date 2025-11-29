import cProfile
import pstats
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from technical_indicator_calculator import TechnicalIndicatorCalculator
from traditional_analysis_engine import TraditionalAnalysisEngine

# 创建测试数据
def create_test_data(days=100):
    """
    创建测试数据
    
    Args:
        days: 测试数据的天数
        
    Returns:
        包含测试数据的DataFrame
    """
    # 创建日期序列
    dates = [datetime.now() - timedelta(days=i) for i in range(days)][::-1]
    dates = [date.strftime('%Y-%m-%d') for date in dates]
    
    # 创建随机价格数据
    np.random.seed(42)
    close = np.random.rand(days) * 100 + 100
    open = close * (1 + np.random.randn(days) * 0.01)
    high = np.maximum(open, close) * (1 + np.random.rand(days) * 0.02)
    low = np.minimum(open, close) * (1 - np.random.rand(days) * 0.02)
    volume = np.random.rand(days) * 1000000 + 100000
    
    # 创建DataFrame
    df = pd.DataFrame({
        'date': dates,
        'code': 'sh.600000',
        'open': open,
        'high': high,
        'low': low,
        'close': close,
        'preclose': np.roll(close, 1),
        'volume': volume,
        'amount': close * volume,
        'adjustflag': '1',
        'turn': np.random.rand(days) * 10,
        'tradestatus': '1',
        'pctChg': (close / np.roll(close, 1) - 1) * 100,
        'peTTM': np.random.rand(days) * 50 + 10,
        'pbMRQ': np.random.rand(days) * 5 + 1,
        'psTTM': np.random.rand(days) * 10 + 1,
        'pcfNcfTTM': np.random.rand(days) * 20 + 5,
        'isST': '0'
    })
    
    # 处理第一个preclose值
    df.loc[0, 'preclose'] = df.loc[0, 'close']
    df.loc[0, 'pctChg'] = 0.0
    
    return df

# 创建测试数据
test_data = create_test_data(200)

# 创建技术指标计算器实例
calculator = TechnicalIndicatorCalculator()

# 创建传统分析引擎实例
engine = TraditionalAnalysisEngine()

# 使用cProfile进行性能分析
cProfile.run('calculator.calculate_all_indicators(test_data)', 'profile_stats')

# 打印性能分析结果
p = pstats.Stats('profile_stats')
p.strip_dirs().sort_stats('cumulative').print_stats(20)
