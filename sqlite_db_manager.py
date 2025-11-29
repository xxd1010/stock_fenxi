#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQLite数据库管理类
提供数据库连接、CRUD操作、事务处理等功能的封装
"""

import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union

# 尝试导入pandas，用于DataFrame操作
try:
    import pandas as pd
except ImportError:
    pd = None

# 导入日志工具
from log_utils import get_logger


class SQLiteDBManager:
    """SQLite数据库管理类"""

    def __init__(self, db_path: str = ':memory:', log_level: Optional[int] = None):
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径，默认为内存数据库
            log_level: 日志级别，默认使用log_utils配置的级别
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
        # 获取日志记录器
        self.logger = get_logger('SQLiteDBManager')
        # 日志级别已由log_utils统一配置，无需单独设置
    
    def connect(self) -> None:
        """建立数据库连接"""
        try:
            # 创建数据库连接
            self.conn = sqlite3.connect(self.db_path)
            # 设置行返回为字典形式
            self.conn.row_factory = sqlite3.Row
            # 创建游标
            self.cursor = self.conn.cursor()
            self.logger.info(f'成功连接到数据库: {self.db_path}')
        except sqlite3.Error as e:
            self.logger.error(f'数据库连接失败: {str(e)}')
            raise
    
    def disconnect(self) -> None:
        """关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.conn:
                self.conn.close()
                self.conn = None
            self.logger.info('数据库连接已关闭')
        except sqlite3.Error as e:
            self.logger.error(f'关闭数据库连接时出错: {str(e)}')
            raise
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Any:
        """
        执行SQL查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果
        """
        try:
            if not self.conn:
                self.connect()
                
            if params:
                result = self.cursor.execute(query, params)
            else:
                result = self.cursor.execute(query)
                
            return result
        except sqlite3.Error as e:
            self.logger.error(f'执行查询失败: {str(e)}')
            self.logger.error(f'SQL: {query}')
            self.logger.error(f'参数: {params}')
            raise
    
    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """
        获取单条查询结果
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果字典，如果没有结果则返回None
        """
        result = self.execute_query(query, params)
        row = result.fetchone()
        if row:
            # 转换为字典
            return dict(row)
        return None
    
    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        获取所有查询结果
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        result = self.execute_query(query, params)
        rows = result.fetchall()
        # 转换为字典列表
        return [dict(row) for row in rows]
    
    def execute_update(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        执行更新操作（INSERT、UPDATE、DELETE）
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            影响的行数
        """
        try:
            if not self.conn:
                self.connect()
                
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
                
            # 提交事务
            self.conn.commit()
            
            # 返回影响的行数
            return self.cursor.rowcount
        except sqlite3.Error as e:
            # 发生错误时回滚事务
            if self.conn:
                self.conn.rollback()
            self.logger.error(f'执行更新操作失败: {str(e)}')
            self.logger.error(f'SQL: {query}')
            self.logger.error(f'参数: {params}')
            raise
    
    def begin_transaction(self) -> None:
        """开始事务"""
        try:
            if not self.conn:
                self.connect()
            self.execute_query('BEGIN TRANSACTION')
            self.logger.info('事务已开始')
        except sqlite3.Error as e:
            self.logger.error(f'开始事务失败: {str(e)}')
            raise
    
    def commit_transaction(self) -> None:
        """提交事务"""
        try:
            if not self.conn:
                raise sqlite3.ProgrammingError('未连接到数据库')
            self.conn.commit()
            self.logger.info('事务已提交')
        except sqlite3.Error as e:
            self.logger.error(f'提交事务失败: {str(e)}')
            raise
    
    def rollback_transaction(self) -> None:
        """回滚事务"""
        try:
            if not self.conn:
                raise sqlite3.ProgrammingError('未连接到数据库')
            self.conn.rollback()
            self.logger.info('事务已回滚')
        except sqlite3.Error as e:
            self.logger.error(f'回滚事务失败: {str(e)}')
            raise
    
    def create_table(self, table_name: str, columns: Dict[str, str]) -> None:
        """
        创建表
        
        Args:
            table_name: 表名
            columns: 列定义，格式为 {列名: 列类型}
        """
        # 构建CREATE TABLE语句
        columns_def = ', '.join([f'{col} {col_type}' for col, col_type in columns.items()])
        query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})'
        
        try:
            self.execute_update(query)
            self.logger.info(f'表 {table_name} 已创建或已存在')
        except sqlite3.Error as e:
            self.logger.error(f'创建表失败: {str(e)}')
            raise
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> int:
        """
        插入数据
        
        Args:
            table_name: 表名
            data: 要插入的数据，格式为 {列名: 值}
            
        Returns:
            插入的行数
        """
        # 构建INSERT语句
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        
        return self.execute_update(query, tuple(data.values()))
    
    def update(self, table_name: str, data: Dict[str, Any], where_clause: str = '', 
               where_params: Optional[Tuple] = None) -> int:
        """
        更新数据
        
        Args:
            table_name: 表名
            data: 要更新的数据，格式为 {列名: 值}
            where_clause: WHERE子句
            where_params: WHERE子句的参数
            
        Returns:
            更新的行数
        """
        # 构建UPDATE语句
        set_clause = ', '.join([f'{col} = ?' for col in data.keys()])
        query = f'UPDATE {table_name} SET {set_clause}'
        
        # 组合参数
        params = list(data.values())
        
        # 添加WHERE子句
        if where_clause:
            query += f' WHERE {where_clause}'
            if where_params:
                params.extend(where_params)
        
        return self.execute_update(query, tuple(params))
    
    def delete(self, table_name: str, where_clause: str = '', 
               where_params: Optional[Tuple] = None) -> int:
        """
        删除数据
        
        Args:
            table_name: 表名
            where_clause: WHERE子句
            where_params: WHERE子句的参数
            
        Returns:
            删除的行数
        """
        # 构建DELETE语句
        query = f'DELETE FROM {table_name}'
        
        # 添加WHERE子句
        if where_clause:
            query += f' WHERE {where_clause}'
        
        return self.execute_update(query, where_params)
    
    def select(self, table_name: str, columns: Union[str, List[str]] = '*', 
               where_clause: str = '', where_params: Optional[Tuple] = None,
               order_by: str = '', limit: int = 0, offset: int = 0) -> List[Dict[str, Any]]:
        """
        查询数据
        
        Args:
            table_name: 表名
            columns: 要查询的列
            where_clause: WHERE子句
            where_params: WHERE子句的参数
            order_by: ORDER BY子句
            limit: LIMIT子句
            offset: OFFSET子句
            
        Returns:
            查询结果列表
        """
        # 处理列参数
        if isinstance(columns, list):
            columns_str = ', '.join(columns)
        else:
            columns_str = columns
        
        # 构建SELECT语句
        query = f'SELECT {columns_str} FROM {table_name}'
        
        # 添加WHERE子句
        if where_clause:
            query += f' WHERE {where_clause}'
        
        # 添加ORDER BY子句
        if order_by:
            query += f' ORDER BY {order_by}'
        
        # 添加LIMIT子句
        if limit > 0:
            query += f' LIMIT {limit}'
            # 添加OFFSET子句
            if offset > 0:
                query += f' OFFSET {offset}'
        
        return self.fetch_all(query, where_params)
    
    def drop_table(self, table_name: str) -> None:
        """
        删除表
        
        Args:
            table_name: 表名
        """
        query = f'DROP TABLE IF EXISTS {table_name}'
        try:
            self.execute_update(query)
            self.logger.info(f'表 {table_name} 已删除或不存在')
        except sqlite3.Error as e:
            self.logger.error(f'删除表失败: {str(e)}')
            raise
    
    def write_dataframe(self, df: 'pd.DataFrame', table_name: str, 
                        if_exists: str = 'append', index: bool = False) -> None:
        """
        将pandas DataFrame写入数据库表
        
        Args:
            df: 要写入的pandas DataFrame
            table_name: 目标表名
            if_exists: 表已存在时的处理方式，可选值：
                      - 'fail': 如果表已存在则报错
                      - 'replace': 替换已存在的表
                      - 'append': 向已存在的表追加数据
            index: 是否将DataFrame的索引写入表中
        """
        if pd is None:
            raise ImportError('pandas未安装，请使用pip install pandas安装')
            
        if not isinstance(df, pd.DataFrame):
            raise TypeError('参数df必须是pandas DataFrame类型')
            
        try:
            if not self.conn:
                self.connect()
                
            # 使用pandas的to_sql方法写入数据
            # 只对字符串类型的列指定dtype，其他列使用默认类型
            dtype_dict = {}
            for col in df.columns:
                if df[col].dtype == 'object':
                    dtype_dict[col] = 'TEXT'
            
            df.to_sql(table_name, self.conn, if_exists=if_exists, index=index, 
                     dtype=dtype_dict)
            
            # 手动提交事务
            self.conn.commit()
            self.logger.info(f'成功将DataFrame写入表 {table_name}')
        except Exception as e:
            # 发生错误时回滚事务
            if self.conn:
                self.conn.rollback()
            self.logger.error(f'将DataFrame写入表 {table_name} 失败: {str(e)}')
            raise
    
    def read_dataframe(self, table_name: str = None, columns: Optional[List[str]] = None, 
                      where_clause: str = '', where_params: Optional[Tuple] = None, 
                      query: str = None, params: Optional[Tuple] = None) -> 'pd.DataFrame':
        """
        从数据库表读取数据到pandas DataFrame
        
        Args:
            table_name: 表名（如果提供query参数，则忽略此参数）
            columns: 要读取的列列表，默认为所有列（如果提供query参数，则忽略此参数）
            where_clause: WHERE子句（如果提供query参数，则忽略此参数）
            where_params: WHERE子句的参数（如果提供params参数，则忽略此参数）
            query: 完整的SQL查询语句（可选，优先级高于table_name等参数）
            params: 查询参数（可选，优先级高于where_params）
            
        Returns:
            包含查询结果的pandas DataFrame
        """
        if pd is None:
            raise ImportError('pandas未安装，请使用pip install pandas安装')
            
        try:
            if not self.conn:
                self.connect()
                
            # 如果提供了完整的查询语句，则使用该语句
            if query:
                # 使用提供的查询语句和参数
                final_query = query
                final_params = params
            else:
                # 构建查询语句
                if columns:
                    columns_str = ', '.join(columns)
                else:
                    columns_str = '*'
                    
                final_query = f'SELECT {columns_str} FROM {table_name}'
                
                if where_clause:
                    final_query += f' WHERE {where_clause}'
                    
                final_params = where_params
            
            # 使用pandas的read_sql方法读取数据
            df = pd.read_sql(final_query, self.conn, params=final_params)
            self.logger.info(f'成功执行查询并读取数据到DataFrame')
            return df
        except Exception as e:
            self.logger.error(f'从数据库读取数据失败: {str(e)}')
            raise


# 使用示例
if __name__ == '__main__':
    # 创建数据库管理器实例
    db_manager = SQLiteDBManager('example.db')
    
    try:
        # 创建连接
        db_manager.connect()
        
        # 创建表
        db_manager.create_table(
            'users',
            {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'name': 'TEXT NOT NULL',
                'age': 'INTEGER',
                'email': 'TEXT'
            }
        )
        
        # 插入数据
        user1 = {'name': '张三', 'age': 30, 'email': 'zhangsan@example.com'}
        user2 = {'name': '李四', 'age': 25, 'email': 'lisi@example.com'}
        
        db_manager.insert('users', user1)
        db_manager.insert('users', user2)
        
        # 查询数据
        print('所有用户:')
        users = db_manager.select('users')
        for user in users:
            print(user)
        
        # 条件查询
        print('\n25岁以上的用户:')
        old_users = db_manager.select('users', where_clause='age > ?', where_params=(25,))
        for user in old_users:
            print(user)
        
        # 更新数据
        db_manager.update('users', {'age': 31}, where_clause='name = ?', where_params=('张三',))
        
        # 查询更新后的数据
        updated_user = db_manager.fetch_one('SELECT * FROM users WHERE name = ?', ('张三',))
        print(f'\n更新后的用户: {updated_user}')
        
        # 删除数据
        db_manager.delete('users', where_clause='name = ?', where_params=('李四',))
        
        # 查询删除后的数据
        print('\n删除后的所有用户:')
        users = db_manager.select('users')
        for user in users:
            print(user)
        
        # 测试pandas数据写入（如果pandas已安装）
        if pd is not None:
            print('\n--- 测试pandas数据写入 ---')
            
            # 创建测试DataFrame
            data = {
                'name': ['王五', '赵六', '孙七'],
                'age': [28, 32, 26],
                'email': ['wangwu@example.com', 'zhaoliu@example.com', 'sunqi@example.com']
            }
            df = pd.DataFrame(data)
            print('创建的DataFrame:')
            print(df)
            
            # 将DataFrame写入数据库
            db_manager.write_dataframe(df, 'users', if_exists='append')
            print('\n成功将DataFrame写入数据库')
            
            # 从数据库读取数据到DataFrame
            df_from_db = db_manager.read_dataframe('users')
            print('\n从数据库读取的DataFrame:')
            print(df_from_db)
            
            # 条件查询到DataFrame
            df_conditional = db_manager.read_dataframe('users', where_clause='age > ?', where_params=(27,))
            print('\n条件查询(age > 27)到DataFrame:')
            print(df_conditional)
            
            # 创建新表并替换现有表
            print('\n--- 测试创建新表并替换 ---')
            new_data = {
                'product_id': [1, 2, 3],
                'name': ['产品A', '产品B', '产品C'],
                'price': [100, 200, 300]
            }
            df_products = pd.DataFrame(new_data)
            db_manager.write_dataframe(df_products, 'products', if_exists='replace')
            
            # 读取新表数据
            df_products_db = db_manager.read_dataframe('products')
            print('\n产品表数据:')
            print(df_products_db)
        else:
            print('\npandas未安装，跳过pandas测试')
            
    except Exception as e:
        print(f'发生错误: {e}')
    finally:
        # 断开连接
        db_manager.disconnect()
