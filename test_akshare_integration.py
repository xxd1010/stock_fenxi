#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare集成单元测试
"""

import unittest
import pandas as pd
import os
import sys
from unittest.mock import patch, MagicMock

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from akshare_data_fetcher import AkShareDataFetcher


class TestAkShareIntegration(unittest.TestCase):
    """
    AkShare集成单元测试类
    """
    
    def setUp(self):
        """
        设置测试环境
        """
        # 创建测试配置
        self.config = {
            'data_source': {
                'akshare': {
                    'source_name': 'stock_zh_a_hist',
                    'frequency': 'daily',
                    'adjust': 'qfq',
                    'rate_limit': 5,
                    'timeout': 30,
                    'cache_enabled': True,
                    'cache_expire_hours': 24
                }
            }
        }
        
        # 创建AkShare数据获取器实例
        self.fetcher = AkShareDataFetcher(self.config)
        
        # 测试日期范围
        self.start_date = '2023-01-01'
        self.end_date = '2023-01-10'
        
    def tearDown(self):
        """
        清理测试环境
        """
        # 清理缓存文件
        cache_dir = './cache/akshare'
        if os.path.exists(cache_dir):
            for file in os.listdir(cache_dir):
                file_path = os.path.join(cache_dir, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
    
    def test_initialization(self):
        """
        测试AkShareDataFetcher初始化
        """
        self.assertIsInstance(self.fetcher, AkShareDataFetcher)
        self.assertEqual(self.fetcher.source_name, 'stock_zh_a_hist')
        self.assertEqual(self.fetcher.frequency, 'daily')
        self.assertEqual(self.fetcher.adjust, 'qfq')
        self.assertEqual(self.fetcher.rate_limit, 5)
        self.assertEqual(self.fetcher.timeout, 30)
        self.assertTrue(self.fetcher.cache_enabled)
        self.assertEqual(self.fetcher.cache_expire_hours, 24)
    
    def test_stock_data_fetch(self):
        """
        测试股票数据获取功能
        """
        # 创建模拟数据
        mock_data = pd.DataFrame({
            '日期': ['2023-01-01', '2023-01-02', '2023-01-03'],
            '股票代码': ['000001', '000001', '000001'],
            '开盘': [10.0, 10.5, 11.0],
            '收盘': [10.5, 11.0, 11.5],
            '最高': [11.0, 11.5, 12.0],
            '最低': [10.0, 10.5, 11.0],
            '成交量': [1000000, 1500000, 2000000],
            '成交额': [10500000.0, 16500000.0, 23000000.0],
            '振幅': [5.0, 4.76, 4.55],
            '涨跌幅': [5.0, 4.76, 4.55],
            '涨跌额': [0.5, 0.5, 0.5],
            '换手率': [1.0, 1.5, 2.0]
        })
        
        # 直接测试数据转换功能
        stock_code = '000001'
        df = self.fetcher._convert_akshare_to_standard(mock_data, stock_code)
        
        # 验证结果
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 3)
        
        # 验证返回的列是否符合标准格式
        required_columns = [
            'date', 'code', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 
            'pctChg', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'isST'
        ]
        
        for col in required_columns:
            self.assertIn(col, df.columns)
        
        # 验证数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_columns:
            self.assertTrue(pd.api.types.is_numeric_dtype(df[col]))
        
        # 验证数据内容
        self.assertEqual(df['code'].iloc[0], stock_code)
        self.assertEqual(df['date'].iloc[0], '2023-01-01')
        self.assertEqual(df['open'].iloc[0], 10.0)
        self.assertEqual(df['close'].iloc[0], 10.5)
    
    def test_rate_limit(self):
        """
        测试速率限制功能
        """
        import time
        
        # 记录开始时间
        start_time = time.time()
        
        # 连续调用两次check_rate_limit
        self.fetcher._check_rate_limit()
        self.fetcher._check_rate_limit()
        
        # 记录结束时间
        end_time = time.time()
        
        # 验证两次调用之间的时间间隔至少为1/rate_limit秒
        elapsed_time = end_time - start_time
        min_interval = 1.0 / self.fetcher.rate_limit
        self.assertGreaterEqual(elapsed_time, min_interval)
    
    def test_cache_mechanism(self):
        """
        测试缓存机制
        """
        # 创建测试数据
        test_data = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'code': ['000001', '000001'],
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [10.0, 10.5],
            'close': [10.5, 11.0],
            'preclose': [10.0, 10.5],
            'volume': [1000000, 1500000],
            'amount': [10500000.0, 16500000.0],
            'adjustflag': ['1', '1'],
            'turn': [1.0, 1.5],
            'tradestatus': ['1', '1'],
            'pctChg': [5.0, 4.76],
            'peTTM': ['', ''],
            'pbMRQ': ['', ''],
            'psTTM': ['', ''],
            'pcfNcfTTM': ['', ''],
            'isST': ['0', '0']
        })
        
        # 保存到缓存
        stock_code = '000001'
        self.fetcher._save_to_cache(stock_code, self.start_date, self.end_date, test_data)
        
        # 从缓存加载
        cached_data = self.fetcher._load_from_cache(stock_code, self.start_date, self.end_date)
        
        # 验证缓存数据
        self.assertIsInstance(cached_data, pd.DataFrame)
        self.assertFalse(cached_data.empty)
        self.assertEqual(len(cached_data), len(test_data))
        
        # 验证缓存文件路径生成
        cache_file = self.fetcher._get_cache_file_path(stock_code, self.start_date, self.end_date)
        self.assertTrue(os.path.exists(cache_file))
    
    def test_data_conversion(self):
        """
        测试数据格式转换功能
        """
        # 创建akshare格式的数据
        akshare_data = pd.DataFrame({
            '日期': ['2023-01-01', '2023-01-02'],
            '股票代码': ['000001', '000001'],
            '开盘': [10.0, 10.5],
            '收盘': [10.5, 11.0],
            '最高': [11.0, 11.5],
            '最低': [10.0, 10.5],
            '成交量': [1000000, 1500000],
            '成交额': [10500000.0, 16500000.0],
            '振幅': [5.0, 4.76],
            '涨跌幅': [5.0, 4.76],
            '涨跌额': [0.5, 0.5],
            '换手率': [1.0, 1.5]
        })
        
        # 调用转换函数
        stock_code = '000001'
        standard_data = self.fetcher._convert_akshare_to_standard(akshare_data, stock_code)
        
        # 验证转换结果
        self.assertIsInstance(standard_data, pd.DataFrame)
        self.assertFalse(standard_data.empty)
        
        # 验证标准格式的列
        required_columns = [
            'date', 'code', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 
            'pctChg', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'isST'
        ]
        
        for col in required_columns:
            self.assertIn(col, standard_data.columns)
        
        # 验证转换后的数据内容
        self.assertEqual(standard_data['code'].iloc[0], stock_code)
        self.assertEqual(standard_data['date'].iloc[0], '2023-01-01')
        self.assertEqual(standard_data['open'].iloc[0], 10.0)
        self.assertEqual(standard_data['close'].iloc[0], 10.5)
        self.assertEqual(standard_data['adjustflag'].iloc[0], '1')  # 前复权
        self.assertEqual(standard_data['tradestatus'].iloc[0], '1')  # 正常交易
        self.assertEqual(standard_data['isST'].iloc[0], '0')  # 非ST股
    
    def test_invalid_stock_code(self):
        """
        测试无效股票代码处理
        """
        with patch('akshare.stock_feature.stock_hist_em.stock_zh_a_hist') as mock_stock_hist:
            # 设置模拟函数返回空DataFrame
            mock_stock_hist.return_value = pd.DataFrame()
            
            # 调用测试函数
            df = self.fetcher.fetch_stock_data('invalid_code', self.start_date, self.end_date)
            
            # 验证结果
            self.assertIsInstance(df, pd.DataFrame)
            self.assertTrue(df.empty)
    
    def test_cache_invalidation(self):
        """
        测试缓存失效机制
        """
        # 创建测试数据
        test_data = pd.DataFrame({
            'date': ['2023-01-01'],
            'code': ['000001'],
            'open': [10.0],
            'high': [11.0],
            'low': [10.0],
            'close': [10.5],
            'preclose': [10.0],
            'volume': [1000000],
            'amount': [10500000.0],
            'adjustflag': ['1'],
            'turn': [1.0],
            'tradestatus': ['1'],
            'pctChg': [5.0],
            'peTTM': [''],
            'pbMRQ': [''],
            'psTTM': [''],
            'pcfNcfTTM': [''],
            'isST': ['0']
        })
        
        # 保存到缓存
        stock_code = '000001'
        self.fetcher._save_to_cache(stock_code, self.start_date, self.end_date, test_data)
        
        # 获取缓存文件路径
        cache_file = self.fetcher._get_cache_file_path(stock_code, self.start_date, self.end_date)
        
        # 修改缓存文件的修改时间，使其过期
        import time
        os.utime(cache_file, (time.time() - 3600 * 25, time.time() - 3600 * 25))
        
        # 尝试从缓存加载
        cached_data = self.fetcher._load_from_cache(stock_code, self.start_date, self.end_date)
        
        # 验证缓存已失效
        self.assertIsNone(cached_data)


if __name__ == '__main__':
    unittest.main()