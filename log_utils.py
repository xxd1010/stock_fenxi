#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志处理工具模块
提供统一的日志配置和使用接口，支持文件日志和控制台日志，带日志滚动功能
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any


class LogUtils:
    """
    日志处理工具类
    提供统一的日志配置和获取日志记录器的方法
    """
    
    # 默认日志配置
    DEFAULT_CONFIG = {
        "log_level": "INFO",
        "log_file": "app.log",
        "max_bytes": 10 * 1024 * 1024,  # 10MB
        "backup_count": 5,  # 保留5个备份文件
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S"
    }
    
    # 日志级别映射
    LOG_LEVEL_MAP = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化日志工具
        
        Args:
            config: 日志配置字典，可选。如果不提供，将使用默认配置
        """
        # 合并配置
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}
        # 确保日志目录存在
        self._ensure_log_dir()
        # 配置根日志
        self._configure_root_logger()
    
    def _ensure_log_dir(self):
        """
        确保日志文件所在目录存在
        """
        log_file = self.config["log_file"]
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    
    def _configure_root_logger(self):
        """
        配置根日志记录器
        """
        # 获取根日志记录器
        root_logger = logging.getLogger()
        
        # 先移除现有的处理器，避免重复添加
        if root_logger.handlers:
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
        
        # 设置日志级别
        log_level = self.LOG_LEVEL_MAP.get(
            self.config["log_level"].upper(), 
            logging.INFO
        )
        root_logger.setLevel(log_level)
        
        # 创建日志格式
        formatter = logging.Formatter(
            fmt=self.config["format"],
            datefmt=self.config["datefmt"]
        )
        
        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # 添加文件处理器（带滚动功能）
        file_handler = RotatingFileHandler(
            filename=self.config["log_file"],
            maxBytes=self.config["max_bytes"],
            backupCount=self.config["backup_count"],
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> logging.Logger:
        """
        获取日志记录器实例
        
        Args:
            name: 日志记录器名称，可选。如果不提供，将返回根日志记录器
        
        Returns:
            logging.Logger: 日志记录器实例
        """
        return logging.getLogger(name)
    
    @classmethod
    def setup_logger(cls, config: Optional[Dict[str, Any]] = None) -> "LogUtils":
        """
        便捷方法：创建并返回日志工具实例
        
        Args:
            config: 日志配置字典，可选
        
        Returns:
            LogUtils: 日志工具实例
        """
        return cls(config)


# 创建默认日志工具实例
# 方便直接使用，无需手动初始化
logger = LogUtils.get_logger()


# 模块级别的便捷函数
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    模块级别的便捷函数：获取日志记录器
    
    Args:
        name: 日志记录器名称
    
    Returns:
        logging.Logger: 日志记录器实例
    """
    return LogUtils.get_logger(name)


def setup_logger(config: Optional[Dict[str, Any]] = None) -> LogUtils:
    """
    模块级别的便捷函数：设置日志
    
    Args:
        config: 日志配置字典
    
    Returns:
        LogUtils: 日志工具实例
    """
    return LogUtils.setup_logger(config)


# 使用示例
if __name__ == "__main__":
    # 配置日志
    log_config = {
        "log_level": "DEBUG",
        "log_file": "logs/app.log",
        "max_bytes": 5 * 1024 * 1024,  # 5MB
        "backup_count": 3
    }
    
    # 初始化日志工具
    log_utils = setup_logger(log_config)
    
    # 获取日志记录器
    app_logger = get_logger("app")
    
    # 记录不同级别的日志
    app_logger.debug("这是一条DEBUG级别日志")
    app_logger.info("这是一条INFO级别日志")
    app_logger.warning("这是一条WARNING级别日志")
    app_logger.error("这是一条ERROR级别日志")
    app_logger.critical("这是一条CRITICAL级别日志")
    
    # 记录异常
    try:
        1 / 0
    except Exception as e:
        app_logger.exception("发生异常：%s", str(e))
