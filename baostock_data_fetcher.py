#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Baostock数据获取适配器
将Baostock API封装为统一的数据获取接口
"""

import os
import time
import pandas as pd
import baostock as bs
from typing import Optional, Dict, Any
from data_fetcher_interface import DataFetcherInterface
from log_utils import get_logger


class BaostockDataFetcher(DataFetcherInterface):
    """
    Baostock数据获取适配器
    实现DataFetcherInterface接口，用于获取股票、指数等金融数据
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化Baostock数据获取器
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.baostock_config = self.config.get('data_source', {}).get('baostock', {})
        self.data_fetch_config = self.config.get('data_fetch', {})
        
        # 数据源配置
        self.frequency = self.data_fetch_config.get('frequency', 'd')
        self.adjustflag = self.data_fetch_config.get('adjustflag', '3')
        
        # 重试配置
        self.retry_count = self.data_fetch_config.get('retry_count', 3)
        self.retry_interval = self.data_fetch_config.get('retry_interval', 5)
        
        # 超时配置
        self.timeout = self.baostock_config.get('timeout', 30)
        
        # 状态跟踪
        self.is_logged_in = False
        self.last_health_check = 0
        self.health_status = False
        
        # 获取日志记录器
        self.logger = get_logger('baostock_data_fetcher')
        self.logger.info('Baostock数据获取器初始化完成')
    
    def _login(self) -> bool:
        """
        登录Baostock系统
        
        Returns:
            bool: 登录是否成功
        """
        if self.is_logged_in:
            return True
        
        try:
            lg = bs.login()
            if lg.error_code == '0':
                self.is_logged_in = True
                self.logger.debug('Baostock登录成功')
                return True
            else:
                self.logger.error(f'Baostock登录失败: {lg.error_msg}')
                return False
        except Exception as e:
            self.logger.error(f'Baostock登录发生异常: {str(e)}')
            return False
    
    def _logout(self):
        """
        登出Baostock系统
        """
        if self.is_logged_in:
            try:
                bs.logout()
                self.is_logged_in = False
                self.logger.debug('Baostock登出成功')
            except Exception as e:
                self.logger.error(f'Baostock登出发生异常: {str(e)}')
    
    def fetch_stock_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票历史K线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD
        
        Returns:
            pd.DataFrame: 股票历史数据，标准格式
        """
        self.logger.info(f'开始获取股票 {code} 数据，日期范围: {start_date} 至 {end_date}')
        
        for attempt in range(self.retry_count):
            try:
                # 确保已登录
                if not self._login():
                    self.logger.error(f'股票 {code} 数据获取失败: 登录失败')
                    time.sleep(self.retry_interval)
                    continue
                
                # 获取历史K线数据
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                    start_date=start_date, 
                    end_date=end_date, 
                    frequency=self.frequency, 
                    adjustflag=self.adjustflag
                )
                
                # 检查是否需要重新登录
                if rs.error_code == '10001001' and '未登录' in rs.error_msg:
                    self.logger.warning(f'股票 {code} 数据获取失败: 未登录，尝试重新登录...')
                    self.is_logged_in = False
                    continue
                
                if rs.error_code != '0':
                    self.logger.error(f'获取股票 {code} 历史数据失败: {rs.error_msg}')
                    if attempt < self.retry_count - 1:
                        self.logger.debug(f'将在 {self.retry_interval} 秒后重试...')
                        time.sleep(self.retry_interval)
                        continue
                    else:
                        return pd.DataFrame()
                
                # 解析结果集
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                
                result = pd.DataFrame(data_list, columns=rs.fields)
                self.logger.debug(f'成功获取股票 {code} 历史数据，共 {len(result)} 条记录')
                return result
            except Exception as e:
                self.logger.error(f'获取股票 {code} 历史数据时发生异常: {str(e)}')
                if attempt < self.retry_count - 1:
                    self.logger.debug(f'将在 {self.retry_interval} 秒后重试...')
                    time.sleep(self.retry_interval)
                    continue
                else:
                    return pd.DataFrame()
    
    def fetch_index_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指数历史数据
        
        Args:
            code: 指数代码
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD
        
        Returns:
            pd.DataFrame: 指数历史数据，标准格式
        """
        self.logger.info(f'开始获取指数 {code} 数据，日期范围: {start_date} 至 {end_date}')
        
        # 指数数据获取与股票类似，直接调用股票数据获取方法
        return self.fetch_stock_data(code, start_date, end_date)
    
    def fetch_fund_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取基金历史数据
        
        Args:
            code: 基金代码
            start_date: 开始日期，格式YYYY-MM-DD
            end_date: 结束日期，格式YYYY-MM-DD
        
        Returns:
            pd.DataFrame: 基金历史数据，标准格式
        """
        self.logger.info(f'开始获取基金 {code} 数据，日期范围: {start_date} 至 {end_date}')
        
        # Baostock的基金数据获取与股票类似，直接调用股票数据获取方法
        return self.fetch_stock_data(code, start_date, end_date)
    
    def check_health(self) -> bool:
        """
        检查接口健康状态
        
        Returns:
            bool: 接口是否健康
        """
        current_time = time.time()
        # 避免过于频繁的健康检查，间隔至少5秒
        if current_time - self.last_health_check < 5:
            return self.health_status
        
        self.last_health_check = current_time
        self.logger.debug('开始检查Baostock健康状态')
        
        try:
            # 测试登录功能
            login_success = self._login()
            if login_success:
                # 测试获取少量数据
                test_code = 'sh.000001'  # 上证指数
                test_start_date = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
                test_end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
                
                rs = bs.query_history_k_data_plus(
                    test_code,
                    "date,code,open,high,low,close",
                    start_date=test_start_date, 
                    end_date=test_end_date, 
                    frequency='d', 
                    adjustflag='3'
                )
                
                if rs.error_code == '0':
                    # 读取一条数据
                    if rs.next():
                        self.health_status = True
                        self.logger.debug('Baostock健康检查通过')
                    else:
                        self.health_status = False
                        self.logger.warning('Baostock健康检查失败: 未返回数据')
                else:
                    self.health_status = False
                    self.logger.warning(f'Baostock健康检查失败: {rs.error_msg}')
            else:
                self.health_status = False
                self.logger.warning('Baostock健康检查失败: 登录失败')
        except Exception as e:
            self.health_status = False
            self.logger.error(f'Baostock健康检查发生异常: {str(e)}')
        
        return self.health_status
    
    def get_name(self) -> str:
        """
        获取数据源名称
        
        Returns:
            str: 数据源名称
        """
        return 'baostock'
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取数据源详细状态
        
        Returns:
            Dict[str, Any]: 包含状态信息的字典
        """
        return {
            'name': self.get_name(),
            'is_logged_in': self.is_logged_in,
            'health_status': self.health_status,
            'last_health_check': self.last_health_check,
            'frequency': self.frequency,
            'adjustflag': self.adjustflag
        }
    
    def __del__(self):
        """
        析构函数，确保登出
        """
        self._logout()
