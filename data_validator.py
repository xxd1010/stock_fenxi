import pandas as pd
import datetime
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/data_validator.log",
    "max_bytes": 5 * 1024 * 1024,
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('data_validator')


class DataValidator:
    """
    数据校验器，负责验证股票数据的完整性、一致性和有效性
    """
    
    def __init__(self):
        """
        初始化数据校验器
        """
        # 定义必填字段
        self.required_fields = [
            'date', 'code', 'open', 'high', 'low', 'close', 
            'preclose', 'volume', 'amount', 'adjustflag', 
            'turn', 'tradestatus', 'pctChg', 'peTTM', 
            'pbMRQ', 'psTTM', 'pcfNcfTTM', 'isST'
        ]
        
        # 定义数值字段
        self.numeric_fields = [
            'open', 'high', 'low', 'close', 'preclose', 
            'volume', 'amount', 'turn', 'pctChg', 
            'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM'
        ]
        
        # 定义日期字段
        self.date_fields = ['date', 'ipoDate', 'outDate']
        
        # 定义布尔字段
        self.boolean_fields = ['isST']
    
    def validate_dataframe(self, df, stock_code=None):
        """
        验证DataFrame数据的完整性、一致性和有效性
        
        Args:
            df: 要验证的DataFrame
            stock_code: 股票代码，用于日志记录
            
        Returns:
            dict: 校验结果，包含is_valid和errors字段
        """
        errors = []
        is_valid = True
        
        logger.debug(f'开始验证股票 {stock_code} 的数据，共 {len(df)} 条记录')
        
        # 1. 检查数据是否为空
        if df is None or df.empty:
            errors.append('数据为空')
            is_valid = False
            return {
                'is_valid': is_valid,
                'errors': errors,
                'stock_code': stock_code
            }
        
        # 2. 检查必填字段是否完整
        missing_fields = [field for field in self.required_fields if field not in df.columns]
        if missing_fields:
            errors.append(f'缺少必填字段: {missing_fields}')
            is_valid = False
        
        # 3. 检查数据格式
        format_errors = self._check_data_format(df)
        if format_errors:
            errors.extend(format_errors)
            is_valid = False
        
        # 4. 检查数据完整性
        completeness_errors = self._check_data_completeness(df)
        if completeness_errors:
            errors.extend(completeness_errors)
            is_valid = False
        
        # 5. 检查数据一致性
        consistency_errors = self._check_data_consistency(df)
        if consistency_errors:
            errors.extend(consistency_errors)
            is_valid = False
        
        # 6. 检查数据有效性
        validity_errors = self._check_data_validity(df)
        if validity_errors:
            errors.extend(validity_errors)
            is_valid = False
        
        # 7. 检查时间连续性
        continuity_errors = self._check_time_continuity(df)
        if continuity_errors:
            errors.extend(continuity_errors)
            is_valid = False
        
        logger.debug(f'股票 {stock_code} 数据校验完成，结果: {"有效" if is_valid else "无效"}')
        if not is_valid:
            logger.warning(f'股票 {stock_code} 数据校验失败，错误: {errors}')
        
        return {
            'is_valid': is_valid,
            'errors': errors,
            'stock_code': stock_code,
            'total_records': len(df),
            'valid_records': len(df) if is_valid else 0
        }
    
    def _check_data_format(self, df):
        """
        检查数据格式
        
        Args:
            df: 要检查的DataFrame
            
        Returns:
            list: 格式错误列表
        """
        errors = []
        
        # 检查数值字段格式
        for field in self.numeric_fields:
            if field in df.columns:
                try:
                    # 尝试转换为数值类型
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                    # 检查是否有NaN值
                    nan_count = df[field].isna().sum()
                    if nan_count > 0:
                        errors.append(f'字段 {field} 包含 {nan_count} 个无效数值')
                except Exception as e:
                    errors.append(f'字段 {field} 格式错误: {str(e)}')
        
        # 检查日期字段格式
        for field in self.date_fields:
            if field in df.columns:
                try:
                    # 尝试转换为日期类型
                    df[field] = pd.to_datetime(df[field], errors='coerce')
                    # 检查是否有NaN值
                    nan_count = df[field].isna().sum()
                    if nan_count > 0:
                        errors.append(f'字段 {field} 包含 {nan_count} 个无效日期')
                except Exception as e:
                    errors.append(f'字段 {field} 格式错误: {str(e)}')
        
        return errors
    
    def _check_data_completeness(self, df):
        """
        检查数据完整性
        
        Args:
            df: 要检查的DataFrame
            
        Returns:
            list: 完整性错误列表
        """
        errors = []
        
        # 检查每一行是否有缺失值
        total_rows = len(df)
        missing_rows = df.isnull().any(axis=1).sum()
        if missing_rows > 0:
            errors.append(f'数据不完整，共 {missing_rows}/{total_rows} 行包含缺失值')
        
        return errors
    
    def _check_data_consistency(self, df):
        """
        检查数据一致性
        
        Args:
            df: 要检查的DataFrame
            
        Returns:
            list: 一致性错误列表
        """
        errors = []
        
        # 检查最高价是否大于等于最低价
        if 'high' in df.columns and 'low' in df.columns:
            inconsistent_rows = df[df['high'] < df['low']]
            if len(inconsistent_rows) > 0:
                errors.append(f'最高价小于最低价，共 {len(inconsistent_rows)} 行')
        
        # 检查收盘价是否在最高价和最低价之间
        if 'open' in df.columns and 'high' in df.columns and 'low' in df.columns and 'close' in df.columns:
            inconsistent_rows = df[(df['close'] > df['high']) | (df['close'] < df['low'])]
            if len(inconsistent_rows) > 0:
                errors.append(f'收盘价不在最高价和最低价之间，共 {len(inconsistent_rows)} 行')
        
        # 检查开盘价是否在最高价和最低价之间
        if 'open' in df.columns and 'high' in df.columns and 'low' in df.columns:
            inconsistent_rows = df[(df['open'] > df['high']) | (df['open'] < df['low'])]
            if len(inconsistent_rows) > 0:
                errors.append(f'开盘价不在最高价和最低价之间，共 {len(inconsistent_rows)} 行')
        
        return errors
    
    def _check_data_validity(self, df):
        """
        检查数据有效性
        
        Args:
            df: 要检查的DataFrame
            
        Returns:
            list: 有效性错误列表
        """
        errors = []
        
        # 检查数值字段是否为正数
        for field in self.numeric_fields:
            if field in df.columns:
                negative_rows = df[df[field] < 0]
                if len(negative_rows) > 0:
                    # 有些字段可以为负数，如pctChg和pcfNcfTTM
                    if field not in ['pctChg', 'pcfNcfTTM']:
                        errors.append(f'字段 {field} 包含 {len(negative_rows)} 个负值')
        
        # 检查成交量是否为正数
        if 'volume' in df.columns:
            zero_volume_rows = df[df['volume'] <= 0]
            if len(zero_volume_rows) > 0:
                errors.append(f'成交量为零或负数，共 {len(zero_volume_rows)} 行')
        
        # 检查成交额是否为正数
        if 'amount' in df.columns:
            zero_amount_rows = df[df['amount'] <= 0]
            if len(zero_amount_rows) > 0:
                errors.append(f'成交额为零或负数，共 {len(zero_amount_rows)} 行')
        
        return errors
    
    def _check_time_continuity(self, df):
        """
        检查时间连续性
        
        Args:
            df: 要检查的DataFrame
            
        Returns:
            list: 时间连续性错误列表
        """
        errors = []
        
        if 'date' not in df.columns:
            return errors
        
        try:
            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            
            # 按日期排序
            df_sorted = df.sort_values('date')
            
            # 计算日期差
            date_diff = df_sorted['date'].diff()
            
            # 检查是否有连续的日期
            # 注意：股票市场周末和节假日不交易，所以不能简单检查日期差是否为1天
            # 这里只检查是否有重复日期
            duplicate_dates = df_sorted[df_sorted.duplicated('date', keep=False)]
            if len(duplicate_dates) > 0:
                errors.append(f'存在重复日期，共 {len(duplicate_dates)} 行')
        except Exception as e:
            errors.append(f'检查时间连续性时发生错误: {str(e)}')
        
        return errors
    
    def generate_validation_report(self, validation_results):
        """
        生成校验报告
        
        Args:
            validation_results: 校验结果列表
            
        Returns:
            dict: 校验报告
        """
        total_stocks = len(validation_results)
        valid_stocks = sum(1 for result in validation_results if result['is_valid'])
        invalid_stocks = total_stocks - valid_stocks
        
        # 统计错误类型
        error_types = {}
        for result in validation_results:
            for error in result['errors']:
                if error not in error_types:
                    error_types[error] = 0
                error_types[error] += 1
        
        # 生成报告
        report = {
            'report_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_stocks': total_stocks,
            'valid_stocks': valid_stocks,
            'invalid_stocks': invalid_stocks,
            'validity_rate': valid_stocks / total_stocks * 100 if total_stocks > 0 else 0,
            'error_types': error_types,
            'validation_results': validation_results
        }
        
        # 记录报告到日志
        logger.info('===== 数据校验报告 =====')
        logger.info(f'报告时间: {report["report_time"]}')
        logger.info(f'总股票数: {report["total_stocks"]}')
        logger.info(f'有效股票数: {report["valid_stocks"]}')
        logger.info(f'无效股票数: {report["invalid_stocks"]}')
        logger.info(f'有效率: {report["validity_rate"]:.2f}%')
        logger.info(f'错误类型: {report["error_types"]}')
        
        return report
    
    def clean_dataframe(self, df):
        """
        清理DataFrame数据，处理无效值和缺失值
        
        Args:
            df: 要清理的DataFrame
            
        Returns:
            pd.DataFrame: 清理后的DataFrame
        """
        if df is None or df.empty:
            return df
        
        logger.debug(f'开始清理数据，共 {len(df)} 条记录')
        
        # 1. 移除重复行
        df = df.drop_duplicates()
        
        # 2. 移除缺失值过多的行
        df = df.dropna(thresh=len(self.required_fields) * 0.8)  # 至少80%的必填字段有值
        
        # 3. 填充缺失值
        for field in self.numeric_fields:
            if field in df.columns:
                # 用前一天的值填充
                df[field] = df[field].fillna(method='ffill')
                df[field] = df[field].fillna(method='bfill')
        
        # 4. 转换数据类型
        for field in self.numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        for field in self.date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')
        
        logger.debug(f'数据清理完成，剩余 {len(df)} 条记录')
        
        return df


# 测试代码
if __name__ == '__main__':
    # 创建测试数据
    test_data = {
        'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'code': ['sh.600000', 'sh.600000', 'sh.600000'],
        'open': [10.0, 11.0, 12.0],
        'high': [10.5, 11.5, 12.5],
        'low': [9.5, 10.5, 11.5],
        'close': [10.2, 11.2, 12.2],
        'preclose': [9.8, 10.2, 11.2],
        'volume': [1000000, 2000000, 3000000],
        'amount': [10200000, 22400000, 36600000],
        'adjustflag': ['3', '3', '3'],
        'turn': [0.5, 1.0, 1.5],
        'tradestatus': ['1', '1', '1'],
        'pctChg': [4.08, 9.80, 8.93],
        'peTTM': [5.0, 5.2, 5.4],
        'pbMRQ': [1.0, 1.1, 1.2],
        'psTTM': [2.0, 2.1, 2.2],
        'pcfNcfTTM': [3.0, 3.1, 3.2],
        'isST': ['0', '0', '0']
    }
    
    test_df = pd.DataFrame(test_data)
    
    # 创建校验器
    validator = DataValidator()
    
    # 验证数据
    result = validator.validate_dataframe(test_df, 'sh.600000')
    print(f'校验结果: {result}')
    
    # 生成报告
    report = validator.generate_validation_report([result])
    print(f'校验报告: {report}')
