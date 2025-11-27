#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据获取接口抽象层
定义统一的数据获取接口规范，用于实现不同数据源的无缝切换
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import pandas as pd


class DataFetcherInterface(ABC):
    """
    数据获取接口抽象基类
    定义统一的数据获取方法和健康检查方法
    """
    
    @abstractmethod
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化数据获取器
        
        Args:
            config: 配置字典
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    def check_health(self) -> bool:
        """
        检查接口健康状态
        
        Returns:
            bool: 接口是否健康
        """
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """
        获取数据源名称
        
        Returns:
            str: 数据源名称
        """
        pass
    
    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """
        获取数据源详细状态
        
        Returns:
            Dict[str, Any]: 包含状态信息的字典
        """
        pass
