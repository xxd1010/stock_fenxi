#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
akshare数据获取模块
负责与akshare API交互，获取金融数据并转换为标准格式
"""

import os
import time
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from log_utils import get_logger

# 获取日志记录器
logger = get_logger('akshare_data_fetcher')


class AkShareDataFetcher:
    """
akshare数据获取器
    """
    
    def __init__(self, config=None):
        """
        初始化akshare数据获取器
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.akshare_config = self.config.get('data_source', {}).get('akshare', {})
        
        # 数据源配置
        self.source_name = self.akshare_config.get('source_name', 'stock_zh_a_hist')
        self.frequency = self.akshare_config.get('frequency', 'daily')
        self.adjust = self.akshare_config.get('adjust', 'qfq')
        
        # 速率限制配置
        self.rate_limit = self.akshare_config.get('rate_limit', 5)
        self.last_request_time = 0
        self.min_interval = 1.0 / self.rate_limit
        
        # 超时配置
        self.timeout = self.akshare_config.get('timeout', 30)
        
        # 缓存配置
        self.cache_enabled = self.akshare_config.get('cache_enabled', True)
        self.cache_expire_hours = self.akshare_config.get('cache_expire_hours', 24)
        self.cache_dir = './cache/akshare'
        os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info(f'akshare数据获取器初始化完成，数据源: {self.source_name}，频率: {self.frequency}，复权: {self.adjust}')
    
    def _check_rate_limit(self):
        """
        检查速率限制，确保不超过每秒最大请求数
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_interval:
            # 需要等待
            wait_time = self.min_interval - elapsed
            logger.debug(f'速率限制：等待 {wait_time:.2f} 秒')
            time.sleep(wait_time)
        
        # 更新最后请求时间
        self.last_request_time = time.time()
    
    def _get_cache_file_path(self, code, start_date, end_date):
        """
        获取缓存文件路径
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            str: 缓存文件路径
        """
        cache_filename = f'{self.source_name}_{code}_{start_date}_{end_date}_{self.frequency}_{self.adjust}.csv'
        return os.path.join(self.cache_dir, cache_filename)
    
    def _is_cache_valid(self, cache_file):
        """
        检查缓存是否有效
        
        Args:
            cache_file: 缓存文件路径
            
        Returns:
            bool: 缓存是否有效
        """
        if not os.path.exists(cache_file):
            return False
        
        # 检查缓存文件的修改时间
        mtime = os.path.getmtime(cache_file)
        expire_time = time.time() - self.cache_expire_hours * 3600
        
        return mtime > expire_time
    
    def _load_from_cache(self, code, start_date, end_date):
        """
        从缓存加载数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame or None: 缓存的数据，如果缓存无效则返回None
        """
        if not self.cache_enabled:
            return None
        
        cache_file = self._get_cache_file_path(code, start_date, end_date)
        
        if self._is_cache_valid(cache_file):
            try:
                df = pd.read_csv(cache_file)
                logger.debug(f'从缓存加载股票 {code} 数据成功，缓存文件: {cache_file}')
                return df
            except Exception as e:
                logger.error(f'从缓存加载股票 {code} 数据失败: {str(e)}')
                return None
        
        return None
    
    def _save_to_cache(self, code, start_date, end_date, df):
        """
        将数据保存到缓存
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            df: 要保存的数据
        """
        if not self.cache_enabled or df is None or df.empty:
            return
        
        cache_file = self._get_cache_file_path(code, start_date, end_date)
        
        try:
            df.to_csv(cache_file, index=False, encoding='utf-8')
            logger.debug(f'将股票 {code} 数据保存到缓存成功，缓存文件: {cache_file}')
        except Exception as e:
            logger.error(f'将股票 {code} 数据保存到缓存失败: {str(e)}')
    
    def _convert_akshare_to_standard(self, df, code):
        """
        将akshare数据转换为系统标准格式
        
        Args:
            df: akshare返回的DataFrame
            code: 股票代码
            
        Returns:
            pd.DataFrame: 标准格式的DataFrame
        """
        if df is None or df.empty:
            return df
        
        # 重命名列，转换为系统标准格式
        column_mapping = {
            '日期': 'date',
            '股票代码': 'code',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'pctChg',
            '涨跌额': 'change',
            '换手率': 'turn',
        }
        
        # 选择需要的列并转换
        standard_df = pd.DataFrame()
        
        # 转换日期格式
        if '日期' in df.columns:
            standard_df['date'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        # 转换其他列
        for ak_col, std_col in column_mapping.items():
            if ak_col in df.columns:
                standard_df[std_col] = df[ak_col]
        
        # 设置股票代码
        standard_df['code'] = code
        
        # 添加缺失的标准列
        required_columns = [
            'date', 'code', 'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'adjustflag', 'turn', 'tradestatus', 
            'pctChg', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'isST'
        ]
        
        for col in required_columns:
            if col not in standard_df.columns:
                if col in ['preclose', 'adjustflag', 'tradestatus', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM', 'isST']:
                    standard_df[col] = ''
                else:
                    standard_df[col] = 0.0
        
        # 转换数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
        for col in numeric_columns:
            if col in standard_df.columns:
                standard_df[col] = pd.to_numeric(standard_df[col], errors='coerce')
        
        # 设置preclose（前收盘价）
        if 'close' in standard_df.columns:
            standard_df['preclose'] = standard_df['close'].shift(1)
            # 填充第一个值
            standard_df['preclose'] = standard_df['preclose'].fillna(standard_df['open'])
        
        # 设置adjustflag（复权标志）
        adjustflag_map = {
            'qfq': '1',  # 前复权
            'hfq': '2',  # 后复权
            '': '3'      # 不复权
        }
        standard_df['adjustflag'] = adjustflag_map.get(self.adjust, '3')
        
        # 设置tradestatus（交易状态）
        standard_df['tradestatus'] = '1'  # 默认正常交易
        
        # 设置isST（是否ST股）
        standard_df['isST'] = '0'  # 默认非ST股
        
        logger.debug(f'数据转换完成，原始列: {list(df.columns)}，标准列: {list(standard_df.columns)}')
        
        return standard_df
    
    def fetch_stock_data(self, code, start_date, end_date):
        """
        获取股票历史K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 股票历史数据
        """
        logger.info(f'开始获取股票 {code} 数据，日期范围: {start_date} 至 {end_date}')
        
        # 检查缓存
        cached_data = self._load_from_cache(code, start_date, end_date)
        if cached_data is not None:
            return cached_data
        
        # 检查速率限制
        self._check_rate_limit()
        
        try:
            # 根据数据源名称调用不同的akshare函数
            if self.source_name == 'stock_zh_a_hist':
                # 获取A股历史行情数据
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period=self.frequency,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=self.adjust
                )
                logger.debug(f'成功获取股票 {code} 数据，共 {len(df)} 条记录')
            else:
                # 其他数据源，使用通用调用方式
                logger.warning(f'暂不支持的数据源: {self.source_name}')
                return pd.DataFrame()
            
            # 转换为标准格式
            standard_df = self._convert_akshare_to_standard(df, code)
            
            # 保存到缓存
            self._save_to_cache(code, start_date, end_date, standard_df)
            
            return standard_df
        except Exception as e:
            logger.error(f'获取股票 {code} 数据失败: {str(e)}')
            return pd.DataFrame()
    
    def fetch_index_data(self, code, start_date, end_date):
        """
        获取指数历史数据
        
        Args:
            code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 指数历史数据
        """
        logger.info(f'开始获取指数 {code} 数据，日期范围: {start_date} 至 {end_date}')
        
        # 检查缓存
        cached_data = self._load_from_cache(code, start_date, end_date)
        if cached_data is not None:
            return cached_data
        
        # 检查速率限制
        self._check_rate_limit()
        
        try:
            # 获取指数历史数据
            df = ak.stock_zh_index_daily(symbol=code)
            
            # 过滤日期范围
            df['date'] = pd.to_datetime(df['date'])
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            
            logger.debug(f'成功获取指数 {code} 数据，共 {len(df)} 条记录')
            
            # 转换为标准格式
            standard_df = self._convert_akshare_to_standard(df, code)
            
            # 保存到缓存
            self._save_to_cache(code, start_date, end_date, standard_df)
            
            return standard_df
        except Exception as e:
            logger.error(f'获取指数 {code} 数据失败: {str(e)}')
            return pd.DataFrame()
    
    def fetch_fund_data(self, code, start_date, end_date):
        """
        获取基金历史数据
        
        Args:
            code: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 基金历史数据
        """
        logger.info(f'开始获取基金 {code} 数据，日期范围: {start_date} 至 {end_date}')
        
        # 检查缓存
        cached_data = self._load_from_cache(code, start_date, end_date)
        if cached_data is not None:
            return cached_data
        
        # 检查速率限制
        self._check_rate_limit()
        
        try:
            # 获取基金历史数据
            df = ak.fund_etf_hist_em(
                symbol=code,
                start_date=start_date,
                end_date=end_date
            )
            
            logger.debug(f'成功获取基金 {code} 数据，共 {len(df)} 条记录')
            
            # 转换为标准格式
            standard_df = self._convert_akshare_to_standard(df, code)
            
            # 保存到缓存
            self._save_to_cache(code, start_date, end_date, standard_df)
            
            return standard_df
        except Exception as e:
            logger.error(f'获取基金 {code} 数据失败: {str(e)}')
            return pd.DataFrame()


# 测试代码
if __name__ == '__main__':
    # 创建配置
    config = {
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
    
    # 创建数据获取器
    fetcher = AkShareDataFetcher(config)
    
    # 测试获取股票数据
    stock_code = '000001'
    start_date = '2023-01-01'
    end_date = '2023-01-10'
    
    logger.info(f'测试获取股票 {stock_code} 数据')
    df = fetcher.fetch_stock_data(stock_code, start_date, end_date)
    logger.info(f'获取到 {len(df)} 条记录')
    print(df.head())
    
    # 测试获取指数数据
    index_code = 'sh000001'
    logger.info(f'测试获取指数 {index_code} 数据')
    df = fetcher.fetch_index_data(index_code, start_date, end_date)
    logger.info(f'获取到 {len(df)} 条记录')
    print(df.head())
