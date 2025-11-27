# AkShare 集成文档

## 1. 概述

本文档介绍了如何在股票数据获取系统中集成 AkShare 数据源。AkShare 是一个开源的金融数据接口库，提供了丰富的金融数据获取功能。通过本集成，系统可以从 AkShare 获取股票、指数、基金等金融数据，并将其转换为标准格式后接入现有数据处理流程。

## 2. 功能特性

- 支持从 AkShare 获取 A 股历史行情数据
- 支持获取指数和基金历史数据
- 支持多种数据频率（日线、周线、月线）
- 支持不同复权类型（前复权、后复权、不复权）
- 实现了速率限制，避免请求过于频繁
- 实现了数据缓存，提高数据获取效率
- 实现了数据格式转换，将 AkShare 数据转换为系统标准格式
- 支持多线程并行处理

## 3. 配置说明

### 3.1 数据源配置

在 `config.json` 文件中，可以配置 AkShare 数据源的相关参数：

```json
"data_source": {
    "// 数据源类型": "支持baostock和akshare",
    "type": "akshare",
    "// akshare配置": "",
    "akshare": {
        "// 数据源名称": "akshare数据源名称，如stock_zh_a_hist",
        "source_name": "stock_zh_a_hist",
        "// 数据频率": "日k线: daily, 周k线: weekly, 月k线: monthly",
        "frequency": "daily",
        "// 复权类型": "不复权: qfq, 前复权: hfq, 后复权: qfq",
        "adjust": "qfq",
        "// 速率限制": "每秒最大请求数",
        "rate_limit": 5,
        "// 超时时间": "请求超时时间（秒）",
        "timeout": 30,
        "// 缓存启用": "是否启用数据缓存",
        "cache_enabled": true,
        "// 缓存过期时间": "缓存过期时间（小时）",
        "cache_expire_hours": 24
    }
}
```

### 3.2 配置参数说明

| 参数名 | 描述 | 默认值 | 可选值 |
|--------|------|--------|--------|
| `type` | 数据源类型 | `baostock` | `baostock`, `akshare` |
| `source_name` | AkShare 数据源名称 | `stock_zh_a_hist` | 详见 AkShare 文档 |
| `frequency` | 数据频率 | `daily` | `daily`, `weekly`, `monthly` |
| `adjust` | 复权类型 | `qfq` | `qfq`（前复权）, `hfq`（后复权）, `''`（不复权） |
| `rate_limit` | 每秒最大请求数 | 5 | 正整数 |
| `timeout` | 请求超时时间（秒） | 30 | 正整数 |
| `cache_enabled` | 是否启用数据缓存 | `true` | `true`, `false` |
| `cache_expire_hours` | 缓存过期时间（小时） | 24 | 正整数 |

## 4. 代码结构

### 4.1 核心模块

- `akshare_data_fetcher.py`: AkShare 数据获取器，负责与 AkShare API 交互，获取数据并转换为标准格式
- `stock_data_fetcher_v1.py`: 主程序，集成了 Baostock 和 AkShare 数据源

### 4.2 类和方法

#### 4.2.1 `AkShareDataFetcher` 类

**初始化方法**：
```python
def __init__(self, config=None):
    """
    初始化 AkShare 数据获取器
    
    Args:
        config: 配置字典
    """
```

**主要方法**：

- `fetch_stock_data(code, start_date, end_date)`: 获取股票历史K线数据
- `fetch_index_data(code, start_date, end_date)`: 获取指数历史数据
- `fetch_fund_data(code, start_date, end_date)`: 获取基金历史数据
- `_convert_akshare_to_standard(df, code)`: 将 AkShare 数据转换为系统标准格式
- `_check_rate_limit()`: 检查速率限制
- `_load_from_cache(code, start_date, end_date)`: 从缓存加载数据
- `_save_to_cache(code, start_date, end_date, df)`: 将数据保存到缓存

## 5. 使用方法

### 5.1 切换数据源

要使用 AkShare 数据源，只需将 `config.json` 中的 `data_source.type` 设置为 `akshare`：

```json
"data_source": {
    "type": "akshare",
    // 其他配置...
}
```

### 5.2 运行程序

```bash
python stock_data_fetcher_v1.py
```

程序将从 AkShare 获取数据，并按照配置进行处理和保存。

## 6. 数据流程

1. 程序启动，加载配置文件
2. 根据配置选择数据源（AkShare）
3. 获取所有 A 股股票代码
4. 使用多线程并行处理每个股票：
   - 检查缓存，如果缓存有效则直接使用缓存数据
   - 检查速率限制，确保不超过每秒最大请求数
   - 调用 AkShare API 获取数据
   - 将获取的数据转换为系统标准格式
   - 保存数据到缓存
   - 进行数据校验和清理
   - 保存数据到 CSV 文件
   - 将数据添加到批量写入列表
5. 当批量数据达到指定大小时，写入数据库
6. 输出统计信息

## 7. 性能优化

### 7.1 速率限制

为了避免请求过于频繁导致 AkShare API 限制，系统实现了速率限制机制。根据配置的 `rate_limit` 参数，系统会控制每秒发送的请求数量。

### 7.2 数据缓存

系统实现了数据缓存机制，将获取的数据保存到本地文件中。当再次请求相同的数据时，如果缓存未过期，则直接使用缓存数据，避免重复请求。

### 7.3 多线程并行处理

系统使用多线程并行处理股票数据，充分利用多核 CPU 资源，提高数据处理效率。

## 8. 错误处理

系统实现了完善的错误处理机制：

- 当请求失败时，会自动重试，最多重试 `retry_count` 次
- 每次重试之间会等待 `retry_interval` 秒
- 详细记录错误日志，便于调试和分析
- 当遇到网络错误或超时错误时，会进行适当的处理

## 9. 注意事项

1. AkShare 依赖于网络连接，确保网络连接正常
2. 不同的 AkShare 数据源可能有不同的参数要求，请根据实际情况调整配置
3. 速率限制参数 `rate_limit` 不宜设置过大，避免触发 AkShare API 的限制
4. 缓存过期时间 `cache_expire_hours` 可以根据数据更新频率进行调整
5. 多线程数量 `thread_count` 不宜设置过大，避免系统资源占用过高

## 10. 单元测试

系统提供了 AkShare 集成的单元测试，用于验证 AkShare 数据获取功能的正确性和可靠性。测试文件为 `test_akshare_integration.py`。

### 10.1 运行测试

```bash
python -m unittest test_akshare_integration.py
```

### 10.2 测试内容

- 数据获取器初始化测试
- 股票数据获取测试
- 指数数据获取测试
- 基金数据获取测试
- 数据格式转换测试
- 速率限制测试
- 缓存功能测试
- 无效股票代码测试
- 日期范围验证测试
- 不同频率数据获取测试

## 11. 故障排除

### 11.1 网络连接问题

如果遇到网络连接问题，可能是由于：

- 网络连接不稳定
- 代理设置问题
- AkShare API 服务不可用

**解决方案**：
- 检查网络连接
- 检查代理设置
- 稍后重试

### 11.2 请求频率过高

如果遇到请求频率过高的问题，可能是由于：

- `rate_limit` 参数设置过大
- 线程数量过多

**解决方案**：
- 减小 `rate_limit` 参数
- 减小 `thread_count` 参数

### 11.3 数据格式错误

如果遇到数据格式错误，可能是由于：

- AkShare API 返回的数据格式发生变化
- 数据转换逻辑存在问题

**解决方案**：
- 检查 AkShare 文档，了解数据格式变化
- 检查数据转换逻辑

## 12. 更新日志

### 版本 1.0.0

- 初始版本
- 支持从 AkShare 获取 A 股历史行情数据
- 支持获取指数和基金历史数据
- 支持多种数据频率和复权类型
- 实现了速率限制和数据缓存
- 实现了数据格式转换
- 支持多线程并行处理

## 13. 参考资料

- [AkShare 官方文档](https://akshare.akfamily.xyz/)
- [AkShare GitHub 仓库](https://github.com/akfamily/akshare)
- [pandas 文档](https://pandas.pydata.org/docs/)

## 14. 联系信息

如有任何问题或建议，请联系系统管理员。

---

**文档版本**：1.0.0
**最后更新**：2025-11-26
**作者**：Stock Data Fetcher Team
