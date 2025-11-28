import os
import pandas as pd
from log_utils import setup_logger, get_logger
from result_storage import ResultStorage

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/report_generator.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('report_generator')


class ReportGenerator:
    """
    报告生成模块类
    生成Markdown格式的分析报告
    """
    
    def __init__(self, report_dir: str = 'reports'):
        """
        初始化报告生成模块
        
        Args:
            report_dir: 报告保存目录，默认'reports'
        """
        self.report_dir = report_dir
        self.result_storage = ResultStorage()
        self.logger = logger
        
        # 确保报告目录存在
        if not os.path.exists(self.report_dir):
            os.makedirs(self.report_dir)
            self.logger.info(f"创建报告目录: {self.report_dir}")
        
        self.logger.info(f"报告生成模块已初始化，报告保存目录: {self.report_dir}")
    
    def generate_stock_report(self, stock_code: str, analysis_result: dict, stock_data: pd.DataFrame = None) -> str:
        """
        生成单只股票的分析报告
        
        Args:
            stock_code: 股票代码
            analysis_result: 分析结果字典
            stock_data: 股票数据DataFrame（可选）
            
        Returns:
            Markdown格式的报告内容
        """
        try:
            # 构建报告模板
            report_template = """# 股票分析报告

## 基本信息
- 股票代码：{stock_code}
- 分析日期：{analysis_date}
- 分析策略：{strategy}
- 大模型：{llm_info}

## 技术指标分析

### MACD指标
- DIF：{macd_dif}
- DEA：{macd_dea}
- 柱状图：{macd_hist}
- 信号：{macd_signal}

### RSI指标
- RSI6：{rsi6}
- RSI12：{rsi12}
- RSI24：{rsi24}
- 信号：{rsi_signal}

### KDJ指标
- K值：{kdj_k}
- D值：{kdj_d}
- J值：{kdj_j}
- 信号：{kdj_signal}

### 布林带指标
- 上轨：{boll_upper}
- 中轨：{boll_middle}
- 下轨：{boll_lower}
- 信号：{boll_signal}

### 均线指标
- MA5：{ma5}
- MA10：{ma10}
- MA20：{ma20}
- MA60：{ma60}
- 信号：{ma_signal}

## 投资建议
- 评级：{rating}
- 评分：{score}/100
- 风险等级：{risk_level}
- 预期收益率：{expected_return}%

## 决策依据
{decision_basis}

## 大模型分析摘要
{llm_analysis}

## 风险提示
{risk_tips}

---

*本报告由股票分析系统自动生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。*
"""
            
            # 提取信号
            signals = analysis_result.get('signals', {})
            
            # 获取最新的技术指标数据
            if stock_data is not None and not stock_data.empty:
                latest_data = stock_data.iloc[-1]
                macd_dif = latest_data.get('MACD_DIF', 'N/A')
                macd_dea = latest_data.get('MACD_DEA', 'N/A')
                macd_hist = latest_data.get('MACD_HIST', 'N/A')
                rsi6 = latest_data.get('RSI6', 'N/A')
                rsi12 = latest_data.get('RSI12', 'N/A')
                rsi24 = latest_data.get('RSI24', 'N/A')
                kdj_k = latest_data.get('KDJ_K', 'N/A')
                kdj_d = latest_data.get('KDJ_D', 'N/A')
                kdj_j = latest_data.get('KDJ_J', 'N/A')
                boll_upper = latest_data.get('BOLL_UPPER', 'N/A')
                boll_middle = latest_data.get('BOLL_MIDDLE', 'N/A')
                boll_lower = latest_data.get('BOLL_LOWER', 'N/A')
                ma5 = latest_data.get('MA5', 'N/A')
                ma10 = latest_data.get('MA10', 'N/A')
                ma20 = latest_data.get('MA20', 'N/A')
                ma60 = latest_data.get('MA60', 'N/A')
            else:
                # 默认值
                macd_dif = macd_dea = macd_hist = 'N/A'
                rsi6 = rsi12 = rsi24 = 'N/A'
                kdj_k = kdj_d = kdj_j = 'N/A'
                boll_upper = boll_middle = boll_lower = 'N/A'
                ma5 = ma10 = ma20 = ma60 = 'N/A'
            
            # 大模型信息
            llm_provider = analysis_result.get('llm_provider', '')
            llm_model = analysis_result.get('llm_model', '')
            llm_info = f"{llm_provider} {llm_model}" if llm_provider and llm_model else "未使用"
            
            # 决策依据
            decision_basis = self._generate_decision_basis(signals)
            
            # 风险提示
            risk_tips = self._generate_risk_tips(analysis_result.get('risk_level', 'medium'))
            
            # 大模型分析
            llm_analysis = analysis_result.get('llm_analysis', '无大模型分析结果')
            
            # 填充报告模板
            report_content = report_template.format(
                stock_code=stock_code,
                analysis_date=analysis_result.get('analysis_date', 'N/A'),
                strategy=analysis_result.get('strategy', 'N/A'),
                llm_info=llm_info,
                macd_dif=round(macd_dif, 4) if isinstance(macd_dif, (int, float)) else macd_dif,
                macd_dea=round(macd_dea, 4) if isinstance(macd_dea, (int, float)) else macd_dea,
                macd_hist=round(macd_hist, 4) if isinstance(macd_hist, (int, float)) else macd_hist,
                macd_signal=signals.get('macd', 'N/A'),
                rsi6=round(rsi6, 2) if isinstance(rsi6, (int, float)) else rsi6,
                rsi12=round(rsi12, 2) if isinstance(rsi12, (int, float)) else rsi12,
                rsi24=round(rsi24, 2) if isinstance(rsi24, (int, float)) else rsi24,
                rsi_signal=signals.get('rsi', 'N/A'),
                kdj_k=round(kdj_k, 2) if isinstance(kdj_k, (int, float)) else kdj_k,
                kdj_d=round(kdj_d, 2) if isinstance(kdj_d, (int, float)) else kdj_d,
                kdj_j=round(kdj_j, 2) if isinstance(kdj_j, (int, float)) else kdj_j,
                kdj_signal=signals.get('kdj', 'N/A'),
                boll_upper=round(boll_upper, 2) if isinstance(boll_upper, (int, float)) else boll_upper,
                boll_middle=round(boll_middle, 2) if isinstance(boll_middle, (int, float)) else boll_middle,
                boll_lower=round(boll_lower, 2) if isinstance(boll_lower, (int, float)) else boll_lower,
                boll_signal=signals.get('bollinger', 'N/A'),
                ma5=round(ma5, 2) if isinstance(ma5, (int, float)) else ma5,
                ma10=round(ma10, 2) if isinstance(ma10, (int, float)) else ma10,
                ma20=round(ma20, 2) if isinstance(ma20, (int, float)) else ma20,
                ma60=round(ma60, 2) if isinstance(ma60, (int, float)) else ma60,
                ma_signal=signals.get('ma', 'N/A'),
                rating=analysis_result.get('rating', 'N/A'),
                score=analysis_result.get('score', 'N/A'),
                risk_level=analysis_result.get('risk_level', 'N/A'),
                expected_return=round(analysis_result.get('expected_return', 0) * 100, 2),
                decision_basis=decision_basis,
                llm_analysis=llm_analysis,
                risk_tips=risk_tips
            )
            
            self.logger.info(f"成功生成股票 {stock_code} 的分析报告")
            return report_content
        except Exception as e:
            self.logger.error(f"生成股票 {stock_code} 的分析报告失败: {str(e)}")
            raise
    
    def _generate_decision_basis(self, signals: dict) -> str:
        """
        生成决策依据
        
        Args:
            signals: 包含各指标信号的字典
            
        Returns:
            决策依据文本
        """
        basis = []
        
        for indicator, signal in signals.items():
            if signal == 'buy':
                basis.append(f"- {self._get_indicator_name(indicator)}指标发出买入信号")
            elif signal == 'sell':
                basis.append(f"- {self._get_indicator_name(indicator)}指标发出卖出信号")
            else:
                basis.append(f"- {self._get_indicator_name(indicator)}指标发出持有信号")
        
        return '\n'.join(basis) if basis else "无明确决策依据"
    
    def _generate_risk_tips(self, risk_level: str) -> str:
        """
        生成风险提示
        
        Args:
            risk_level: 风险等级
            
        Returns:
            风险提示文本
        """
        if risk_level == 'low':
            return "- 该股票风险较低，适合稳健型投资者\n- 建议长期持有，定期关注基本面变化"
        elif risk_level == 'medium':
            return "- 该股票风险中等，适合平衡型投资者\n- 建议设置止损点，定期跟踪技术指标变化\n- 关注宏观经济和行业政策变化"
        elif risk_level == 'high':
            return "- 该股票风险较高，适合激进型投资者\n- 建议严格控制仓位，设置止损点\n- 密切关注市场情绪和资金流向\n- 注意短期波动风险"
        else:
            return "- 风险等级未知，请谨慎投资\n- 建议充分了解股票基本面和技术面\n- 控制仓位，分散投资"
    
    def _get_indicator_name(self, indicator: str) -> str:
        """
        获取指标名称
        
        Args:
            indicator: 指标英文名称
            
        Returns:
            指标中文名称
        """
        indicator_names = {
            'macd': 'MACD',
            'rsi': 'RSI',
            'kdj': 'KDJ',
            'bollinger': '布林带',
            'ma': '均线'
        }
        return indicator_names.get(indicator, indicator)
    
    def save_report_to_file(self, stock_code: str, report_content: str, analysis_date: str, format: str = 'markdown') -> str:
        """
        将报告保存到文件
        
        Args:
            stock_code: 股票代码
            report_content: 报告内容
            analysis_date: 分析日期
            format: 报告格式，默认'markdown'
            
        Returns:
            报告文件路径
        """
        try:
            # 构建文件名
            filename = f"{stock_code}_{analysis_date}.md"
            file_path = os.path.join(self.report_dir, filename)
            
            # 保存报告
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"成功将报告保存到文件: {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"保存报告到文件失败: {str(e)}")
            raise
    
    def save_report_to_db(self, report_content: str, stock_code: str, analysis_date: str, report_type: str = 'full'):
        """
        将报告保存到数据库
        
        Args:
            report_content: 报告内容
            stock_code: 股票代码
            analysis_date: 分析日期
            report_type: 报告类型，默认'full'（完整报告）
        """
        try:
            self.result_storage.save_analysis_report(stock_code, analysis_date, report_type, report_content)
            self.logger.info(f"成功将报告保存到数据库")
        except Exception as e:
            self.logger.error(f"保存报告到数据库失败: {str(e)}")
            raise
    
    def generate_and_save_report(self, stock_code: str, analysis_result: dict, stock_data: pd.DataFrame = None, 
                               save_to_file: bool = True, save_to_db: bool = False) -> str:
        """
        生成并保存报告
        
        Args:
            stock_code: 股票代码
            analysis_result: 分析结果字典
            stock_data: 股票数据DataFrame（可选）
            save_to_file: 是否保存到文件，默认True
            save_to_db: 是否保存到数据库，默认False
            
        Returns:
            报告内容
        """
        try:
            # 生成报告
            report_content = self.generate_stock_report(stock_code, analysis_result, stock_data)
            
            # 保存报告
            if save_to_file:
                self.save_report_to_file(report_content, stock_code, analysis_result.get('analysis_date', ''))
            
            if save_to_db:
                self.save_report_to_db(report_content, stock_code, analysis_result.get('analysis_date', ''))
            
            return report_content
        except Exception as e:
            self.logger.error(f"生成并保存报告失败: {str(e)}")
            raise
    
    def batch_generate_reports(self, analysis_results: dict, stock_data_dict: dict = None, 
                             save_to_file: bool = True, save_to_db: bool = False) -> dict:
        """
        批量生成报告
        
        Args:
            analysis_results: 以股票代码为键，分析结果为值的字典
            stock_data_dict: 以股票代码为键，DataFrame为值的字典（可选）
            save_to_file: 是否保存到文件，默认True
            save_to_db: 是否保存到数据库，默认False
            
        Returns:
            以股票代码为键，报告内容为值的字典
        """
        reports = {}
        
        for stock_code, result in analysis_results.items():
            try:
                stock_data = stock_data_dict.get(stock_code) if stock_data_dict else None
                report_content = self.generate_and_save_report(stock_code, result, stock_data, save_to_file, save_to_db)
                reports[stock_code] = report_content
            except Exception as e:
                self.logger.error(f"生成股票 {stock_code} 的报告失败，跳过该股票: {str(e)}")
                continue
        
        self.logger.info(f"批量生成报告完成，成功生成 {len(reports)} 份报告")
        return reports


# 使用示例
if __name__ == '__main__':
    # 创建报告生成器实例
    report_generator = ReportGenerator()
    
    # 示例分析结果
    sample_result = {
        'stock_code': 'sh.600000',
        'analysis_date': '2023-10-01',
        'strategy': 'traditional_technical_analysis',
        'rating': 'buy',
        'score': 85,
        'signals': {
            'macd': 'buy',
            'rsi': 'hold',
            'kdj': 'buy',
            'bollinger': 'hold',
            'ma': 'buy'
        },
        'risk_level': 'medium',
        'expected_return': 0.15
    }
    
    try:
        # 生成报告
        report_content = report_generator.generate_stock_report('sh.600000', sample_result)
        
        # 保存报告到文件
        file_path = report_generator.save_report_to_file(report_content, 'sh.600000', '2023-10-01')
        print(f"报告已生成并保存到: {file_path}")
        
        # 打印报告前1000个字符
        print("\n报告内容预览:")
        print(report_content[:1000] + "...")
    except Exception as e:
        print(f"生成报告失败: {str(e)}")