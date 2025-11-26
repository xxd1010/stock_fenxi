import json
import os
import datetime
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/update_status.log",
    "max_bytes": 5 * 1024 * 1024,
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('update_status')


class UpdateStatusManager:
    """
    更新状态管理器，负责记录和管理更新状态
    """
    
    def __init__(self, status_file='update_status.json'):
        """
        初始化更新状态管理器
        
        Args:
            status_file: 状态文件路径
        """
        self.status_file = status_file
        self.status = {
            'last_update': None,
            'last_update_time': None,
            'last_update_status': 'idle',  # idle, running, success, failed
            'update_history': [],
            'current_update': None,
            'progress': 0,
            'total_stocks': 0,
            'updated_stocks': 0,
            'failed_stocks': 0,
            'update_config': {
                'update_type': 'incremental',
                'start_date': None,
                'end_date': None,
                'stock_codes': None
            }
        }
        
        # 加载现有状态
        self.load_status()
    
    def load_status(self):
        """
        从文件加载更新状态
        """
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    saved_status = json.load(f)
                    # 更新状态
                    self.status.update(saved_status)
                    logger.info(f'从文件 {self.status_file} 加载更新状态成功')
            except Exception as e:
                logger.error(f'从文件 {self.status_file} 加载更新状态失败: {str(e)}')
        else:
            logger.info(f'状态文件 {self.status_file} 不存在，使用默认状态')
    
    def save_status(self):
        """
        将更新状态保存到文件
        """
        try:
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(self.status, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f'更新状态已保存到文件 {self.status_file}')
        except Exception as e:
            logger.error(f'保存更新状态到文件 {self.status_file} 失败: {str(e)}')
    
    def start_update(self, update_type='incremental', start_date=None, end_date=None, stock_codes=None):
        """
        开始更新，记录更新开始状态
        
        Args:
            update_type: 更新类型
            start_date: 开始日期
            end_date: 结束日期
            stock_codes: 股票代码列表
        """
        logger.info(f'开始更新，更新类型: {update_type}')
        
        # 更新当前状态
        self.status['current_update'] = {
            'start_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'update_type': update_type,
            'start_date': start_date,
            'end_date': end_date,
            'stock_codes': stock_codes
        }
        
        self.status['last_update_status'] = 'running'
        self.status['progress'] = 0
        self.status['updated_stocks'] = 0
        self.status['failed_stocks'] = 0
        
        # 保存状态
        self.save_status()
    
    def update_progress(self, progress, total_stocks, updated_stocks, failed_stocks):
        """
        更新进度
        
        Args:
            progress: 更新进度，0-100
            total_stocks: 总股票数
            updated_stocks: 已更新股票数
            failed_stocks: 更新失败股票数
        """
        self.status['progress'] = progress
        self.status['total_stocks'] = total_stocks
        self.status['updated_stocks'] = updated_stocks
        self.status['failed_stocks'] = failed_stocks
        
        # 保存状态
        self.save_status()
    
    def finish_update(self, status, message=None, duration=None):
        """
        完成更新，记录更新结束状态
        
        Args:
            status: 更新状态，success或failed
            message: 更新消息
            duration: 更新持续时间
        """
        logger.info(f'更新完成，状态: {status}')
        
        # 记录更新历史
        update_history = {
            'start_time': self.status['current_update']['start_time'],
            'end_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'update_type': self.status['current_update']['update_type'],
            'start_date': self.status['current_update']['start_date'],
            'end_date': self.status['current_update']['end_date'],
            'status': status,
            'message': message,
            'duration': duration,
            'total_stocks': self.status['total_stocks'],
            'updated_stocks': self.status['updated_stocks'],
            'failed_stocks': self.status['failed_stocks'],
            'progress': self.status['progress']
        }
        
        # 添加到更新历史
        self.status['update_history'].append(update_history)
        
        # 限制历史记录数量
        if len(self.status['update_history']) > 100:
            self.status['update_history'] = self.status['update_history'][-100:]
        
        # 更新最后更新信息
        self.status['last_update'] = self.status['current_update']['end_date']
        self.status['last_update_time'] = update_history['end_time']
        self.status['last_update_status'] = status
        
        # 清除当前更新信息
        self.status['current_update'] = None
        
        # 保存状态
        self.save_status()
    
    def get_status(self):
        """
        获取当前更新状态
        
        Returns:
            dict: 更新状态
        """
        return self.status
    
    def get_last_update(self):
        """
        获取最后一次更新信息
        
        Returns:
            dict: 最后一次更新信息
        """
        if self.status['update_history']:
            return self.status['update_history'][-1]
        return None
    
    def get_update_history(self, limit=10):
        """
        获取更新历史
        
        Args:
            limit: 返回的历史记录数量
            
        Returns:
            list: 更新历史记录
        """
        return self.status['update_history'][-limit:]
    
    def reset_status(self):
        """
        重置更新状态
        """
        logger.info('重置更新状态')
        
        self.status = {
            'last_update': None,
            'last_update_time': None,
            'last_update_status': 'idle',
            'update_history': [],
            'current_update': None,
            'progress': 0,
            'total_stocks': 0,
            'updated_stocks': 0,
            'failed_stocks': 0,
            'update_config': {
                'update_type': 'incremental',
                'start_date': None,
                'end_date': None,
                'stock_codes': None
            }
        }
        
        # 保存状态
        self.save_status()
    
    def is_updating(self):
        """
        检查是否正在更新
        
        Returns:
            bool: 是否正在更新
        """
        return self.status['last_update_status'] == 'running'
    
    def get_progress(self):
        """
        获取当前更新进度
        
        Returns:
            float: 更新进度，0-100
        """
        return self.status['progress']
    
    def get_statistics(self):
        """
        获取更新统计信息
        
        Returns:
            dict: 更新统计信息
        """
        total_updates = len(self.status['update_history'])
        success_updates = sum(1 for update in self.status['update_history'] if update['status'] == 'success')
        failed_updates = sum(1 for update in self.status['update_history'] if update['status'] == 'failed')
        
        # 计算平均更新时间
        total_duration = 0
        for update in self.status['update_history']:
            if 'duration' in update and update['duration']:
                # 解析持续时间
                try:
                    if isinstance(update['duration'], str):
                        # 处理字符串格式的持续时间
                        if 'days' in update['duration']:
                            # 格式：X days, HH:MM:SS
                            duration_parts = update['duration'].split(', ')
                            days = int(duration_parts[0].split()[0])
                            time_parts = duration_parts[1].split(':')
                            hours = int(time_parts[0])
                            minutes = int(time_parts[1])
                            seconds = float(time_parts[2])
                            total_duration += days * 86400 + hours * 3600 + minutes * 60 + seconds
                        else:
                            # 格式：HH:MM:SS
                            time_parts = update['duration'].split(':')
                            hours = int(time_parts[0])
                            minutes = int(time_parts[1])
                            seconds = float(time_parts[2])
                            total_duration += hours * 3600 + minutes * 60 + seconds
                except Exception as e:
                    logger.error(f'解析持续时间失败: {str(e)}')
        
        avg_duration = total_duration / total_updates if total_updates > 0 else 0
        
        return {
            'total_updates': total_updates,
            'success_updates': success_updates,
            'failed_updates': failed_updates,
            'success_rate': success_updates / total_updates * 100 if total_updates > 0 else 0,
            'avg_duration': avg_duration,
            'last_update': self.status['last_update'],
            'last_update_time': self.status['last_update_time'],
            'last_update_status': self.status['last_update_status']
        }


# 测试代码
if __name__ == '__main__':
    # 创建状态管理器
    status_manager = UpdateStatusManager()
    
    # 测试开始更新
    status_manager.start_update(
        update_type='incremental',
        start_date='2023-01-01',
        end_date='2023-01-31',
        stock_codes=['sh.600000', 'sh.600001']
    )
    
    # 测试更新进度
    status_manager.update_progress(50, 2, 1, 0)
    
    # 测试完成更新
    status_manager.finish_update(
        status='success',
        message='更新完成',
        duration='0:05:30'
    )
    
    # 测试获取状态
    status = status_manager.get_status()
    print(f'当前状态: {status}')
    
    # 测试获取统计信息
    stats = status_manager.get_statistics()
    print(f'统计信息: {stats}')
