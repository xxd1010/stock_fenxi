# 股票分析系统

## 项目概述

股票分析系统是一个基于历史和实时股票数据的分析平台，能够生成明确的投资建议和详细的分析报告。系统支持传统技术分析和大模型分析两种方式，并提供策略评估功能，帮助投资者做出更明智的投资决策。

## 系统架构

系统采用模块化设计，各模块之间职责清晰，便于维护和扩展。主要模块包括：

```
┌─────────────────────────────────────────────────────────────────┐
│                       股票分析系统                               │
├───────────┬─────────────┬─────────────┬─────────────┬───────────┤
│ 数据读取  │ 指标计算    │ 分析引擎    │ 结果处理    │ 策略评估  │
│ 模块      │ 模块        │ 模块        │ 模块        │ 模块      │
├───────────┼─────────────┼─────────────┼─────────────┼───────────┤
│ stock_    │ technical_  │ traditional_│ result_     │ strategy_ │
│ data_     │ indicator_  │ analysis_   │ storage.py  │ evaluator_│
│ reader.py │ calculator.py│ engine.py   │             │ py        │
│           │             ├─────────────┤ report_     │           │
│           │             │ llm_analysis│ generator.py│           │
│           │             │ _engine.py  │             │           │
│           │             ├─────────────┤             │           │
│           │             │ sampling_   │             │           │
│           │             │ module.py   │             │           │
└───────────┴─────────────┴─────────────┴─────────────┴───────────┘
```

## 核心功能

### 1. 技术指标计算
- 移动平均线（MA）：5日、10日、20日、60日、120日、250日
- MACD指标：DIF、DEA、柱状图
- RSI指标：6日、12日、24日
- KDJ指标：K值、D值、J值
- 布林带指标：上轨、中轨、下轨
- 成交量均线：5日、10日、20日

### 2. 传统技术分析
基于技术指标信号生成投资建议，包括：
- MACD金叉/死叉信号
- RSI超买/超卖信号
- KDJ金叉/死叉信号
- 布林带突破信号
- 均线金叉/死叉信号

### 3. 大模型分析
支持国内外主要大模型，包括：
- 国内大模型：百度、阿里巴巴、腾讯、字节跳动、华为、智谱、商汤
- 国外大模型：OpenAI、Anthropic

### 4. 抽样策略
- 随机抽样：从股票池中随机选择股票
- 分层抽样：根据市值、行业等特征分层抽样

### 5. 结果存储
将分析结果存储到SQLite数据库，包括：
- 技术指标数据
- 分析结果
- 分析报告
- 策略绩效数据

### 6. 报告生成
生成Markdown格式的分析报告，包括：
- 基本信息
- 技术指标分析
- 投资建议
- 决策依据
- 风险提示

### 7. 策略评估
- 计算策略绩效指标：总收益率、年化收益率、最大回撤、夏普比率、胜率、盈亏比
- 对比不同分析策略的效果
- 生成策略对比报告

## 模块说明

### 1. 股票数据读取模块（stock_data_reader.py）

封装了SQLite数据库操作，用于读取股票数据。

**主要功能**：
- 获取股票代码列表
- 获取单只股票数据
- 获取多只股票数据

### 2. 技术指标计算模块（technical_indicator_calculator.py）

使用pandas计算各种技术指标。

**主要功能**：
- 计算移动平均线
- 计算MACD指标
- 计算RSI指标
- 计算KDJ指标
- 计算布林带指标
- 计算成交量均线

### 3. 传统分析引擎（traditional_analysis_engine.py）

基于技术指标信号生成投资建议。

**主要功能**：
- 分析MACD指标，生成信号
- 分析RSI指标，生成信号
- 分析KDJ指标，生成信号
- 分析布林带指标，生成信号
- 分析均线指标，生成信号
- 综合分析所有指标，生成投资建议

### 4. 大模型分析引擎（llm_analysis_engine.py）

使用大模型进行股票分析。

**主要功能**：
- 支持多种大模型提供商
- 构建分析提示词
- 调用大模型API
- 解析大模型响应

### 5. 抽样模块（sampling_module.py）

实现抽样策略，减少测试数据量。

**主要功能**：
- 随机抽样
- 分层抽样

### 6. 结果存储模块（result_storage.py）

将分析结果存储到数据库。

**主要功能**：
- 保存技术指标数据
- 保存分析结果
- 批量保存分析结果

### 7. 报告生成模块（report_generator.py）

生成Markdown格式的分析报告。

**主要功能**：
- 生成股票分析报告
- 保存报告到文件
- 保存报告到数据库

### 8. 策略评估模块（strategy_evaluator.py）

评估策略绩效，对比不同策略。

**主要功能**：
- 计算策略绩效指标
- 对比多个策略
- 生成策略对比报告
- 保存绩效数据

### 9. 主系统模块（stock_analysis_system.py）

整合所有模块，提供完整的股票分析功能。

**主要功能**：
- 分析单只股票
- 批量分析股票
- 生成策略对比报告
- 回测策略

## 安装和配置

### 安装依赖

```bash
pip install -r requirements.txt
```

### 配置文件

系统使用`config.json`文件进行配置，主要配置项包括：

- 数据源配置
- 技术指标参数
- 分析引擎配置
- 大模型配置
- 抽样配置
- 报告配置

示例配置：

```json
{
  "data_source": {
    "akshare": {
      "source_name": "stock_zh_a_hist",
      "frequency": "daily",
      "adjust": "qfq",
      "rate_limit": 5,
      "timeout": 30,
      "cache_enabled": true,
      "cache_expire_hours": 24
    }
  },
  "analysis": {
    "technical_indicators": {
      "ma_periods": [5, 10, 20, 60, 120, 250],
      "macd": {
        "fast": 12,
        "slow": 26,
        "signal": 9
      },
      "rsi_periods": [6, 12, 24],
      "kdj": {
        "length": 9,
        "signal": 3
      },
      "bollinger": {
        "length": 20,
        "std": 2
      },
      "volume_ma_periods": [5, 10, 20]
    },
    "llm": {
      "provider": "openai",
      "api_key": "your_api_key",
      "model": "gpt-4",
      "temperature": 0.7
    },
    "sampling": {
      "enabled": false,
      "mode": "random",
      "sample_size": 100,
      "stratified_columns": ["industry", "market_cap"]
    },
    "report": {
      "save_path": "reports",
      "save_to_db": true
    }
  },
  "output": {
    "database_path": "stock_data.db"
  },
  "test_mode": {
    "enabled": false,
    "max_stocks": 10
  }
}
```

## 使用方法

### 1. 分析单只股票

```python
from stock_analysis_system import StockAnalysisSystem

# 创建系统实例
system = StockAnalysisSystem()

# 分析单只股票，不使用大模型，保存报告
result = system.analyze_stock('sh.600000', use_llm=False, save_report=True)

# 分析单只股票，使用大模型，保存报告
result = system.analyze_stock('sh.600000', use_llm=True, save_report=True)
```

### 2. 批量分析股票

```python
from stock_analysis_system import StockAnalysisSystem

# 创建系统实例
system = StockAnalysisSystem()

# 获取所有股票代码
stock_codes = system.data_reader.get_stock_codes()

# 批量分析前5只股票，不使用大模型，保存报告
results = system.batch_analyze_stocks(stock_codes[:5], use_llm=False, save_reports=True)
```

### 3. 生成策略对比报告

```python
from stock_analysis_system import StockAnalysisSystem

# 创建系统实例
system = StockAnalysisSystem()

# 生成策略对比报告
report = system.generate_strategy_comparison(
    strategy_names=['traditional_technical_analysis', 'llm_analysis'],
    start_date='2023-01-01',
    end_date='2023-12-31'
)
```

### 4. 回测策略

```python
from stock_analysis_system import StockAnalysisSystem

# 创建系统实例
system = StockAnalysisSystem()

# 回测单个策略
performance = system.run_backtest(
    strategy_name='traditional_technical_analysis',
    start_date='2023-01-01',
    end_date='2023-12-31'
)

# 批量回测多个策略
performances = system.batch_run_backtest(
    strategy_names=['traditional_technical_analysis', 'llm_analysis'],
    start_date='2023-01-01',
    end_date='2023-12-31'
)
```

## 示例

### 分析报告示例

```markdown
# 股票分析报告

## 基本信息
- 股票代码：sh.600000
- 分析日期：2023-12-31
- 分析策略：traditional_technical_analysis
- 大模型：未使用

## 技术指标分析

### MACD指标
- DIF：0.56
- DEA：0.45
- 柱状图：0.22
- 信号：buy

### RSI指标
- RSI6：65.23
- RSI12：58.76
- RSI24：52.34
- 信号：hold

### KDJ指标
- K值：72.15
- D值：68.32
- J值：79.81
- 信号：buy

### 布林带指标
- 上轨：12.56
- 中轨：11.23
- 下轨：9.90
- 信号：hold

### 均线指标
- MA5：11.56
- MA10：11.34
- MA20：11.05
- MA60：10.56
- 信号：buy

## 投资建议
- 评级：buy
- 评分：85/100
- 风险等级：medium
- 预期收益率：15.0%

## 决策依据
- MACD指标出现金叉信号
- KDJ指标处于超买区域，显示强势
- 均线呈现多头排列

## 风险提示
- 该股票风险中等，适合平衡型投资者
- 建议设置止损点，定期跟踪技术指标变化
- 关注宏观经济和行业政策变化

---

*本报告由股票分析系统自动生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。*
```

## 开发和测试

### 运行测试

```bash
python -m pytest test_stock_analysis_system.py -v
```

### 日志配置

系统使用logging模块进行日志记录，日志配置位于各模块的顶部，主要配置项包括：

- log_level：日志级别（DEBUG、INFO、WARNING、ERROR、CRITICAL）
- log_file：日志文件路径
- max_bytes：日志文件最大大小
- backup_count：日志文件备份数量

## 后续改进

1. **增加更多技术指标**：如OBV、WR、CCI等
2. **优化大模型提示词**：提高大模型分析的准确性
3. **增加机器学习模型**：支持基于机器学习的股票预测
4. **可视化功能**：增加图表可视化，直观展示分析结果
5. **实时数据支持**：支持实时股票数据的分析
6. **多因子分析**：支持基于多因子的选股策略
7. **回测优化**：优化回测算法，提高回测效率
8. **Web界面**：开发Web界面，方便用户使用

## 许可证

本项目采用MIT许可证，详见LICENSE文件。

## 贡献

欢迎提交Issue和Pull Request，共同改进这个股票分析系统。

## 联系方式

如有问题或建议，请联系：

- 项目地址：[GitHub Repository]
- 邮箱：[your_email@example.com]
