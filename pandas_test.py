#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试pandas数据写入数据库功能的简化脚本
"""

import pandas as pd
from sqlite_db_manager import SQLiteDBManager


def test_pandas_db():
    """测试pandas数据写入和读取数据库"""
    # 创建数据库管理器实例
    db_manager = SQLiteDBManager('pandas_test.db')
    
    try:
        # 创建连接
        db_manager.connect()
        
        print("=== 测试1：创建DataFrame并写入数据库 ===")
        # 创建测试数据
        stock_data = {
            'code': ['000001', '000002', '000003', '000004', '000005'],
            'name': ['平安银行', '万科A', '国农科技', '国农科技', '世纪星源'],
            'date': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01'],
            'open': [12.34, 15.67, 23.45, 34.56, 5.67],
            'close': [12.56, 16.78, 22.34, 33.45, 5.89],
            'high': [12.78, 17.89, 24.56, 35.67, 6.01],
            'low': [12.23, 15.43, 21.23, 32.34, 5.56],
            'volume': [123456789, 987654321, 111111111, 222222222, 333333333]
        }
        
        # 创建DataFrame
        df = pd.DataFrame(stock_data)
        print("创建的股票数据DataFrame:")
        print(df)
        
        # 将DataFrame写入数据库，创建新表
        db_manager.write_dataframe(df, 'stock_data', if_exists='replace')
        print("\n✓ 成功将DataFrame写入数据库表 stock_data")
        
        print("\n=== 测试2：从数据库读取数据到DataFrame ===")
        # 从数据库读取数据到DataFrame
        df_from_db = db_manager.read_dataframe('stock_data')
        print("从数据库读取的DataFrame:")
        print(df_from_db)
        print("\n✓ 成功从数据库读取数据到DataFrame")
        
        print("\n=== 测试3：条件查询数据到DataFrame ===")
        # 条件查询：收盘价大于15的股票
        df_high_close = db_manager.read_dataframe(
            'stock_data', 
            where_clause='close > ?', 
            where_params=(15,)  # 注意参数是元组
        )
        print("收盘价大于15的股票:")
        print(df_high_close)
        print("\n✓ 成功执行条件查询并返回DataFrame")
        
        print("\n=== 测试4：追加数据到现有表 ===")
        # 创建新的测试数据
        new_stock_data = {
            'code': ['000006', '000007'],
            'name': ['深振业A', '全新好'],
            'date': ['2023-01-01', '2023-01-01'],
            'open': [6.78, 8.90],
            'close': [6.90, 9.01],
            'high': [7.01, 9.12],
            'low': [6.67, 8.78],
            'volume': [444444444, 555555555]
        }
        
        df_new = pd.DataFrame(new_stock_data)
        print("要追加的新数据:")
        print(df_new)
        
        # 追加数据到现有表
        db_manager.write_dataframe(df_new, 'stock_data', if_exists='append')
        
        # 读取追加后的数据
        df_append = db_manager.read_dataframe('stock_data')
        print("\n追加后的数据:")
        print(df_append)
        print("\n✓ 成功追加数据到现有表")
        
        print("\n=== 测试5：只读取指定列 ===")
        # 只读取code, name, close列
        df_selected = db_manager.read_dataframe(
            'stock_data', 
            columns=['code', 'name', 'close']
        )
        print("只读取指定列的数据:")
        print(df_selected)
        print("\n✓ 成功读取指定列数据")
        
        print("\n=== 所有测试完成！===")
        
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 断开连接
        db_manager.disconnect()


if __name__ == '__main__':
    test_pandas_db()
