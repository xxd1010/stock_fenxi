import pandas as pd
import numpy as np
from sqlite_db_manager import SQLiteDBManager
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/strategy_evaluator.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('strategy_evaluator')


class StrategyEvaluator:
    """
    策略评估模块类
    用于对比不同分析策略的效果
    """
    
    def __init__(self, db_path: str = 'stock_data.db'):
        """
        初始化策略评估模块
        
        Args:
            db_path: 数据库文件路径，默认'stock_data.db'
        """
        self.db_path = db_path
        self.db_manager = SQLiteDBManager(db_path)
        self.logger = logger
        self.logger.info(f"策略评估模块已初始化，数据库路径: {db_path}")
    
    def calculate_performance(self, strategy_name: str, start_date: str, end_date: str, stock_codes: list = None) -> dict:
        """
        计算单个策略的绩效指标
        
        Args:
            strategy_name: 策略名称
            start_date: 开始日期
            end_date: 结束日期
            stock_codes: 股票代码列表（可选）
            
        Returns:
            策略绩效指标字典
        """
        try:
            self.db_manager.connect()
            
            # 构建查询条件
            where_clause = [
                "strategy = ?",
                "analysis_date >= ?",
                "analysis_date <= ?"
            ]
            params = [strategy_name, start_date, end_date]
            
            if stock_codes:
                where_clause.append("stock_code IN ({})".format(",".join(["?"] * len(stock_codes))))
                params.extend(stock_codes)
            
            # 查询分析结果
            query = f"SELECT * FROM analysis_results WHERE {' AND '.join(where_clause)} ORDER BY analysis_date, stock_code"
            df = self.db_manager.read_dataframe(query, params=tuple(params))
            
            if df.empty:
                self.logger.warning(f"未找到策略 {strategy_name} 在 {start_date} 至 {end_date} 的分析结果")
                return {}
            
            # 计算绩效指标
            performance = {
                'strategy_name': strategy_name,
                'start_date': start_date,
                'end_date': end_date,
                'total_return': self._calculate_total_return(df),
                'annual_return': self._calculate_annual_return(df, start_date, end_date),
                'max_drawdown': self._calculate_max_drawdown(df),
                'sharpe_ratio': self._calculate_sharpe_ratio(df),
                'win_rate': self._calculate_win_rate(df),
                'profit_loss_ratio': self._calculate_profit_loss_ratio(df),
                'trades_count': len(df)
            }
            
            self.logger.info(f"成功计算策略 {strategy_name} 的绩效指标")
            return performance
        except Exception as e:
            self.logger.error(f"计算策略 {strategy_name} 的绩效指标失败: {str(e)}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def _calculate_total_return(self, df: pd.DataFrame) -> float:
        """
        计算总收益率
        
        Args:
            df: 分析结果DataFrame
            
        Returns:
            总收益率
        """
        try:
            # 对于每个股票，计算其在分析期间的收益率
            total_returns = []
            
            # 获取所有唯一的股票代码
            stock_codes = df['stock_code'].unique()
            
            for stock_code in stock_codes:
                # 获取该股票的分析结果
                stock_df = df[df['stock_code'] == stock_code].sort_values('analysis_date')
                
                if len(stock_df) < 2:
                    continue
                
                # 获取该股票的价格数据
                price_df = self._get_stock_prices(stock_code, stock_df['analysis_date'].min(), stock_df['analysis_date'].max())
                
                if price_df.empty:
                    continue
                
                # 合并分析结果和价格数据
                merged_df = pd.merge(stock_df, price_df, left_on=['analysis_date', 'stock_code'], right_on=['date', 'code'], how='inner')
                
                if len(merged_df) < 2:
                    continue
                
                # 计算该股票的总收益率
                start_price = merged_df.iloc[0]['close']
                end_price = merged_df.iloc[-1]['close']
                stock_return = (end_price - start_price) / start_price
                total_returns.append(stock_return)
            
            if not total_returns:
                return 0.0
            
            # 返回平均收益率
            return np.mean(total_returns)
        except Exception as e:
            self.logger.error(f"计算总收益率失败: {str(e)}")
            return 0.0
    
    def _calculate_annual_return(self, df: pd.DataFrame, start_date: str, end_date: str) -> float:
        """
        计算年化收益率
        
        Args:
            df: 分析结果DataFrame
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            年化收益率
        """
        try:
            # 计算持有天数
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            days = (end - start).days
            
            if days == 0:
                return 0.0
            
            # 计算总收益率
            total_return = self._calculate_total_return(df)
            
            # 计算年化收益率
            annual_return = (1 + total_return) ** (365 / days) - 1
            
            return annual_return
        except Exception as e:
            self.logger.error(f"计算年化收益率失败: {str(e)}")
            return 0.0
    
    def _calculate_max_drawdown(self, df: pd.DataFrame) -> float:
        """
        计算最大回撤
        
        Args:
            df: 分析结果DataFrame
            
        Returns:
            最大回撤
        """
        try:
            max_drawdowns = []
            
            # 获取所有唯一的股票代码
            stock_codes = df['stock_code'].unique()
            
            for stock_code in stock_codes:
                # 获取该股票的分析结果
                stock_df = df[df['stock_code'] == stock_code].sort_values('analysis_date')
                
                if len(stock_df) < 2:
                    continue
                
                # 获取该股票的价格数据
                price_df = self._get_stock_prices(stock_code, stock_df['analysis_date'].min(), stock_df['analysis_date'].max())
                
                if price_df.empty:
                    continue
                
                # 计算该股票的最大回撤
                price_df = price_df.sort_values('date')
                price_df['cumulative_return'] = (price_df['close'] / price_df['close'].iloc[0]) - 1
                price_df['running_max'] = price_df['cumulative_return'].cummax()
                price_df['drawdown'] = (price_df['running_max'] - price_df['cumulative_return']) / (price_df['running_max'] + 1)
                
                stock_max_drawdown = price_df['drawdown'].max()
                max_drawdowns.append(stock_max_drawdown)
            
            if not max_drawdowns:
                return 0.0
            
            # 返回平均最大回撤
            return np.mean(max_drawdowns)
        except Exception as e:
            self.logger.error(f"计算最大回撤失败: {str(e)}")
            return 0.0
    
    def _calculate_sharpe_ratio(self, df: pd.DataFrame, risk_free_rate: float = 0.03) -> float:
        """
        计算夏普比率
        
        Args:
            df: 分析结果DataFrame
            risk_free_rate: 无风险利率，默认0.03
            
        Returns:
            夏普比率
        """
        try:
            all_daily_returns = []
            
            # 获取所有唯一的股票代码
            stock_codes = df['stock_code'].unique()
            
            for stock_code in stock_codes:
                # 获取该股票的分析结果
                stock_df = df[df['stock_code'] == stock_code].sort_values('analysis_date')
                
                if len(stock_df) < 2:
                    continue
                
                # 获取该股票的价格数据
                price_df = self._get_stock_prices(stock_code, stock_df['analysis_date'].min(), stock_df['analysis_date'].max())
                
                if price_df.empty:
                    continue
                
                # 计算日收益率
                price_df = price_df.sort_values('date')
                price_df['daily_return'] = price_df['close'].pct_change()
                
                # 去除NaN值
                daily_returns = price_df['daily_return'].dropna().tolist()
                all_daily_returns.extend(daily_returns)
            
            if len(all_daily_returns) < 2:
                return 0.0
            
            # 计算平均日收益率
            avg_daily_return = np.mean(all_daily_returns)
            
            # 计算日收益率标准差
            std_daily_return = np.std(all_daily_returns)
            
            if std_daily_return == 0:
                return 0.0
            
            # 计算年化夏普比率
            sharpe_ratio = (avg_daily_return - risk_free_rate / 252) / std_daily_return * np.sqrt(252)
            
            return sharpe_ratio
        except Exception as e:
            self.logger.error(f"计算夏普比率失败: {str(e)}")
            return 0.0
    
    def _calculate_win_rate(self, df: pd.DataFrame) -> float:
        """
        计算胜率
        
        Args:
            df: 分析结果DataFrame
            
        Returns:
            胜率
        """
        try:
            total_trades = 0
            winning_trades = 0
            
            # 获取所有唯一的股票代码
            stock_codes = df['stock_code'].unique()
            
            for stock_code in stock_codes:
                # 获取该股票的分析结果
                stock_df = df[df['stock_code'] == stock_code].sort_values('analysis_date')
                
                if len(stock_df) < 2:
                    continue
                
                # 获取该股票的价格数据
                price_df = self._get_stock_prices(stock_code, stock_df['analysis_date'].min(), stock_df['analysis_date'].max())
                
                if price_df.empty:
                    continue
                
                # 合并分析结果和价格数据
                merged_df = pd.merge(stock_df, price_df, left_on=['analysis_date', 'stock_code'], right_on=['date', 'code'], how='inner')
                
                if len(merged_df) < 2:
                    continue
                
                # 遍历分析结果，计算交易次数和盈利次数
                for i in range(len(merged_df) - 1):
                    current_row = merged_df.iloc[i]
                    next_row = merged_df.iloc[i + 1]
                    
                    # 只考虑买入信号
                    if current_row['rating'] == 'buy':
                        total_trades += 1
                        # 计算持有期收益率
                        holding_return = (next_row['close'] - current_row['close']) / current_row['close']
                        if holding_return > 0:
                            winning_trades += 1
            
            if total_trades == 0:
                return 0.0
            
            # 计算胜率
            win_rate = winning_trades / total_trades
            return win_rate
        except Exception as e:
            self.logger.error(f"计算胜率失败: {str(e)}")
            return 0.0
    
    def _calculate_profit_loss_ratio(self, df: pd.DataFrame) -> float:
        """
        计算盈亏比
        
        Args:
            df: 分析结果DataFrame
            
        Returns:
            盈亏比
        """
        try:
            total_profit = 0.0
            total_loss = 0.0
            winning_trades = 0
            losing_trades = 0
            
            # 获取所有唯一的股票代码
            stock_codes = df['stock_code'].unique()
            
            for stock_code in stock_codes:
                # 获取该股票的分析结果
                stock_df = df[df['stock_code'] == stock_code].sort_values('analysis_date')
                
                if len(stock_df) < 2:
                    continue
                
                # 获取该股票的价格数据
                price_df = self._get_stock_prices(stock_code, stock_df['analysis_date'].min(), stock_df['analysis_date'].max())
                
                if price_df.empty:
                    continue
                
                # 合并分析结果和价格数据
                merged_df = pd.merge(stock_df, price_df, left_on=['analysis_date', 'stock_code'], right_on=['date', 'code'], how='inner')
                
                if len(merged_df) < 2:
                    continue
                
                # 遍历分析结果，计算盈利和亏损
                for i in range(len(merged_df) - 1):
                    current_row = merged_df.iloc[i]
                    next_row = merged_df.iloc[i + 1]
                    
                    # 只考虑买入信号
                    if current_row['rating'] == 'buy':
                        # 计算持有期收益率
                        holding_return = (next_row['close'] - current_row['close']) / current_row['close']
                        
                        if holding_return > 0:
                            total_profit += holding_return
                            winning_trades += 1
                        elif holding_return < 0:
                            total_loss += abs(holding_return)
                            losing_trades += 1
            
            if winning_trades == 0 or losing_trades == 0:
                return 0.0
            
            # 计算平均盈利和平均亏损
            avg_profit = total_profit / winning_trades
            avg_loss = total_loss / losing_trades
            
            if avg_loss == 0:
                return 0.0
            
            # 计算盈亏比
            profit_loss_ratio = avg_profit / avg_loss
            return profit_loss_ratio
        except Exception as e:
            self.logger.error(f"计算盈亏比失败: {str(e)}")
            return 0.0
    
    def _get_stock_prices(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票价格数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            股票价格DataFrame
        """
        try:
            self.db_manager.connect()
            
            # 查询股票价格数据
            query = """
            SELECT date, code, open, close, high, low, volume, amount 
            FROM history_k_data 
            WHERE code = ? AND date >= ? AND date <= ? 
            ORDER BY date
            """
            
            df = self.db_manager.read_dataframe(query, params=(stock_code, start_date, end_date))
            
            if not df.empty:
                df['stock_code'] = df['code']  # 保持与分析结果一致的字段名
            
            return df
        except Exception as e:
            self.logger.error(f"获取股票 {stock_code} 的价格数据失败: {str(e)}")
            return pd.DataFrame()
        finally:
            self.db_manager.disconnect()
    
    def compare_strategies(self, strategy_names: list, start_date: str, end_date: str, stock_codes: list = None) -> pd.DataFrame:
        """
        对比多个策略的表现
        
        Args:
            strategy_names: 策略名称列表
            start_date: 开始日期
            end_date: 结束日期
            stock_codes: 股票代码列表（可选）
            
        Returns:
            策略对比DataFrame
        """
        performances = []
        
        for strategy_name in strategy_names:
            try:
                performance = self.calculate_performance(strategy_name, start_date, end_date, stock_codes)
                if performance:
                    performances.append(performance)
            except Exception as e:
                self.logger.error(f"对比策略 {strategy_name} 失败: {str(e)}")
                continue
        
        if not performances:
            self.logger.warning("未获取到任何策略的绩效数据")
            return pd.DataFrame()
        
        # 转换为DataFrame
        df = pd.DataFrame(performances)
        
        # 按年化收益率排序
        df = df.sort_values('annual_return', ascending=False)
        
        self.logger.info(f"成功对比 {len(performances)} 个策略")
        return df
    
    def generate_comparison_report(self, strategy_names: list, start_date: str, end_date: str, stock_codes: list = None) -> str:
        """
        生成策略对比报告
        
        Args:
            strategy_names: 策略名称列表
            start_date: 开始日期
            end_date: 结束日期
            stock_codes: 股票代码列表（可选）
            
        Returns:
            Markdown格式的对比报告
        """
        try:
            # 获取对比数据
            comparison_df = self.compare_strategies(strategy_names, start_date, end_date, stock_codes)
            
            if comparison_df.empty:
                return "# 策略对比报告\n\n未获取到任何策略的绩效数据"
            
            # 构建报告模板
            report_template = """# 策略对比报告

## 基本信息
- 对比日期范围：{start_date} 至 {end_date}
- 对比策略数量：{strategy_count}
- 涉及股票数量：{stock_count}

## 策略绩效对比

| 策略名称 | 总收益率 | 年化收益率 | 最大回撤 | 夏普比率 | 胜率 | 盈亏比 | 交易次数 |
|---------|---------|-----------|---------|---------|------|-------|---------|
{performance_table}

## 最佳策略
- 年化收益率最高：{best_annual_return_strategy} ({best_annual_return:.2%})
- 夏普比率最高：{best_sharpe_strategy} ({best_sharpe_ratio:.2f})
- 最大回撤最小：{best_drawdown_strategy} ({best_drawdown:.2%})
- 胜率最高：{best_win_rate_strategy} ({best_win_rate:.2%})

## 策略分析
{strategy_analysis}

---

*本报告由股票分析系统自动生成，仅供参考。*
"""
            
            # 格式化绩效表格
            performance_table = ""
            for _, row in comparison_df.iterrows():
                performance_table += f"| {row['strategy_name']} | {row['total_return']:.2%} | {row['annual_return']:.2%} | {row['max_drawdown']:.2%} | {row['sharpe_ratio']:.2f} | {row['win_rate']:.2%} | {row['profit_loss_ratio']:.2f} | {row['trades_count']} |\n"
            
            # 最佳策略
            best_annual_return = comparison_df['annual_return'].max()
            best_annual_return_strategy = comparison_df[comparison_df['annual_return'] == best_annual_return]['strategy_name'].iloc[0]
            
            best_sharpe_ratio = comparison_df['sharpe_ratio'].max()
            best_sharpe_strategy = comparison_df[comparison_df['sharpe_ratio'] == best_sharpe_ratio]['strategy_name'].iloc[0]
            
            best_drawdown = comparison_df['max_drawdown'].min()
            best_drawdown_strategy = comparison_df[comparison_df['max_drawdown'] == best_drawdown]['strategy_name'].iloc[0]
            
            best_win_rate = comparison_df['win_rate'].max()
            best_win_rate_strategy = comparison_df[comparison_df['win_rate'] == best_win_rate]['strategy_name'].iloc[0]
            
            # 策略分析
            strategy_analysis = "\n".join([
                f"- **{row['strategy_name']}**：年化收益率 {row['annual_return']:.2%}，最大回撤 {row['max_drawdown']:.2%}，夏普比率 {row['sharpe_ratio']:.2f}"
                for _, row in comparison_df.iterrows()
            ])
            
            # 填充报告模板
            report = report_template.format(
                start_date=start_date,
                end_date=end_date,
                strategy_count=len(strategy_names),
                stock_count=len(stock_codes) if stock_codes else "全部",
                performance_table=performance_table,
                best_annual_return_strategy=best_annual_return_strategy,
                best_annual_return=best_annual_return,
                best_sharpe_strategy=best_sharpe_strategy,
                best_sharpe_ratio=best_sharpe_ratio,
                best_drawdown_strategy=best_drawdown_strategy,
                best_drawdown=best_drawdown,
                best_win_rate_strategy=best_win_rate_strategy,
                best_win_rate=best_win_rate,
                strategy_analysis=strategy_analysis
            )
            
            self.logger.info(f"成功生成策略对比报告")
            return report
        except Exception as e:
            self.logger.error(f"生成策略对比报告失败: {str(e)}")
            raise
    
    def save_performance(self, performance: dict):
        """
        保存绩效数据到数据库
        
        Args:
            performance: 绩效指标字典
        """
        try:
            self.db_manager.connect()
            self.db_manager.insert('strategy_performance', performance)
            self.logger.info(f"成功保存策略 {performance['strategy_name']} 的绩效数据")
        except Exception as e:
            self.logger.error(f"保存策略 {performance.get('strategy_name', '未知')} 的绩效数据失败: {str(e)}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def batch_save_performance(self, performances: list):
        """
        批量保存绩效数据
        
        Args:
            performances: 绩效指标字典列表
        """
        try:
            self.db_manager.connect()
            self.db_manager.begin_transaction()
            
            count = 0
            for performance in performances:
                try:
                    self.db_manager.insert('strategy_performance', performance)
                    count += 1
                except Exception as e:
                    self.logger.error(f"保存策略 {performance.get('strategy_name', '未知')} 的绩效数据失败: {str(e)}")
                    continue
            
            self.db_manager.commit_transaction()
            self.logger.info(f"批量保存完成，成功保存 {count} 条绩效数据")
        except Exception as e:
            self.db_manager.rollback_transaction()
            self.logger.error(f"批量保存绩效数据失败: {str(e)}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def get_strategy_performance(self, strategy_name: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取策略绩效数据
        
        Args:
            strategy_name: 策略名称（可选）
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            
        Returns:
            策略绩效DataFrame
        """
        try:
            self.db_manager.connect()
            
            # 构建查询条件
            where_clause = []
            params = []
            
            if strategy_name:
                where_clause.append("strategy_name = ?")
                params.append(strategy_name)
            
            if start_date:
                where_clause.append("start_date >= ?")
                params.append(start_date)
            
            if end_date:
                where_clause.append("end_date <= ?")
                params.append(end_date)
            
            # 查询绩效数据
            query = "SELECT * FROM strategy_performance"
            if where_clause:
                query += f" WHERE {' AND '.join(where_clause)}"
            
            query += " ORDER BY start_date DESC, strategy_name"
            
            df = self.db_manager.read_dataframe(query, params=tuple(params))
            self.logger.info(f"成功获取 {len(df)} 条策略绩效数据")
            return df
        except Exception as e:
            self.logger.error(f"获取策略绩效数据失败: {str(e)}")
            raise
        finally:
            self.db_manager.disconnect()


# 使用示例
if __name__ == '__main__':
    # 创建策略评估实例
    evaluator = StrategyEvaluator()
    
    # 示例：计算单个策略的绩效
    try:
        performance = evaluator.calculate_performance(
            strategy_name='traditional_technical_analysis',
            start_date='2023-01-01',
            end_date='2023-12-31'
        )
        print("策略绩效:")
        for key, value in performance.items():
            if isinstance(value, float):
                print(f"{key}: {value:.4f}")
            else:
                print(f"{key}: {value}")
    except Exception as e:
        print(f"计算策略绩效失败: {str(e)}")
    
    # 示例：对比多个策略
    try:
        strategies = ['traditional_technical_analysis', 'llm_analysis']
        comparison_df = evaluator.compare_strategies(
            strategy_names=strategies,
            start_date='2023-01-01',
            end_date='2023-12-31'
        )
        print("\n策略对比:")
        print(comparison_df)
    except Exception as e:
        print(f"对比策略失败: {str(e)}")
    
    # 示例：生成对比报告
    try:
        report = evaluator.generate_comparison_report(
            strategy_names=strategies,
            start_date='2023-01-01',
            end_date='2023-12-31'
        )
        print("\n策略对比报告:")
        print(report)
    except Exception as e:
        print(f"生成对比报告失败: {str(e)}")