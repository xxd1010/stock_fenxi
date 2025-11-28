import os
import json
from log_utils import setup_logger, get_logger

# 配置日志
log_config = {
    "log_level": "INFO",
    "log_file": "logs/llm_analysis_engine.log",
    "max_bytes": 5 * 1024 * 1024,  # 5MB
    "backup_count": 3
}

# 初始化日志
setup_logger(log_config)
logger = get_logger('llm_analysis_engine')


class LLMAnalysisEngine:
    """
    大模型分析引擎类
    支持国内外主要大模型
    """
    
    def __init__(self, config: dict = None):
        """
        初始化大模型分析引擎
        
        Args:
            config: 大模型配置字典
        """
        # 默认配置
        self.default_config = {
            "provider": "openai",
            "model_name": "gpt-4",
            "api_keys": {
                "baidu": {
                    "api_key": "",
                    "secret_key": ""
                },
                "alibaba": {
                    "api_key": ""
                },
                "tencent": {
                    "secret_id": "",
                    "secret_key": ""
                },
                "bytedance": {
                    "api_key": ""
                },
                "huawei": {
                    "api_key": "",
                    "endpoint": ""
                },
                "zhipu": {
                    "api_key": ""
                },
                "sensecore": {
                    "api_key": ""
                },
                "openai": {
                    "api_key": ""
                },
                "anthropic": {
                    "api_key": ""
                }
            },
            "temperature": 0.1,
            "max_tokens": 1000
        }
        
        # 合并配置
        if config:
            self.config = {
                **self.default_config,
                **config
            }
        else:
            self.config = self.default_config
        
        self.logger = logger
        self.logger.info(f"大模型分析引擎已初始化，当前提供商: {self.config['provider']}")
    
    def analyze_stock(self, stock_code: str, stock_data: dict, technical_indicators: dict = None) -> dict:
        """
        使用大模型分析股票
        
        Args:
            stock_code: 股票代码
            stock_data: 股票数据字典
            technical_indicators: 技术指标字典（可选）
            
        Returns:
            大模型分析结果
        """
        try:
            # 构建分析prompt
            prompt = self._build_analysis_prompt(stock_code, stock_data, technical_indicators)
            
            # 根据提供商选择不同的大模型
            provider = self.config['provider']
            
            if provider == 'baidu':
                result = self._call_baidu_ernie(prompt)
            elif provider == 'alibaba':
                result = self._call_alibaba_qwen(prompt)
            elif provider == 'tencent':
                result = self._call_tencent_hunyuan(prompt)
            elif provider == 'bytedance':
                result = self._call_bytedance_doubao(prompt)
            elif provider == 'huawei':
                result = self._call_huawei_pangu(prompt)
            elif provider == 'zhipu':
                result = self._call_zhipu_glm(prompt)
            elif provider == 'sensecore':
                result = self._call_sensecore_risen(prompt)
            elif provider == 'openai':
                result = self._call_openai_gpt(prompt)
            elif provider == 'anthropic':
                result = self._call_anthropic_claude(prompt)
            else:
                raise ValueError(f"不支持的大模型提供商: {provider}")
            
            # 解析大模型响应
            analysis_result = self._parse_llm_response(result)
            
            # 添加模型信息
            analysis_result['llm_provider'] = provider
            analysis_result['llm_model'] = self.config['model_name']
            
            self.logger.info(f"成功使用 {provider} {self.config['model_name']} 分析股票 {stock_code}")
            return analysis_result
        except Exception as e:
            self.logger.error(f"使用大模型分析股票 {stock_code} 失败: {str(e)}")
            raise
    
    def _build_analysis_prompt(self, stock_code: str, stock_data: dict, technical_indicators: dict = None) -> str:
        """
        构建分析prompt
        
        Args:
            stock_code: 股票代码
            stock_data: 股票数据字典
            technical_indicators: 技术指标字典
            
        Returns:
            构建好的prompt
        """
        # 基础prompt模板
        prompt_template = """你是一名专业的股票分析师，请基于以下股票数据和技术指标，对股票 {stock_code} 进行全面分析，并给出明确的投资建议。

## 股票基本数据
{stock_data_str}

## 技术指标
{technical_indicators_str}

## 分析要求
1. 请从技术面、基本面（如果有数据）等多个维度进行分析
2. 分析要深入、全面，有数据支撑
3. 给出明确的投资建议，包括买入、持有或卖出
4. 给出风险等级评估（低、中、高）
5. 给出预期收益率（年化）
6. 分析过程要清晰，逻辑要严谨
7. 回答要简洁明了，避免冗长

## 输出格式
请按照以下格式输出：

### 分析结果
[详细的分析内容]

### 投资建议
- 评级：[买入/持有/卖出]
- 风险等级：[低/中/高]
- 预期收益率：[年化百分比，如15%]

### 风险提示
[主要风险因素]
"""
        
        # 格式化股票数据
        stock_data_str = "\n".join([f"- {key}: {value}" for key, value in stock_data.items()])
        
        # 格式化技术指标
        if technical_indicators:
            technical_indicators_str = "\n".join([f"- {key}: {value}" for key, value in technical_indicators.items()])
        else:
            technical_indicators_str = "无"
        
        # 填充模板
        prompt = prompt_template.format(
            stock_code=stock_code,
            stock_data_str=stock_data_str,
            technical_indicators_str=technical_indicators_str
        )
        
        return prompt
    
    def _parse_llm_response(self, response: str) -> dict:
        """
        解析大模型响应
        
        Args:
            response: 大模型原始响应
            
        Returns:
            解析后的分析结果
        """
        # 这里需要根据不同大模型的响应格式进行解析
        # 示例实现，实际需要根据各平台API响应格式调整
        result = {
            "llm_analysis": response,
            "rating": "hold",
            "risk_level": "medium",
            "expected_return": 0.1
        }
        
        # 简单解析示例
        if "买入" in response:
            result["rating"] = "buy"
        elif "卖出" in response:
            result["rating"] = "sell"
        
        if "风险低" in response or "低风险" in response:
            result["risk_level"] = "low"
        elif "风险高" in response or "高风险" in response:
            result["risk_level"] = "high"
        
        return result
    
    def _call_baidu_ernie(self, prompt: str) -> str:
        """
        调用百度文心一言API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据百度文心一言的实际API进行实现
            # 示例代码，实际需要替换为真实API调用
            self.logger.info("调用百度文心一言API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['baidu']['api_key']
            secret_key = self.config['api_keys']['baidu']['secret_key']
            
            if not api_key or not secret_key:
                raise ValueError("百度文心一言API密钥未配置")
            
            # 示例响应
            return "### 分析结果\n该股票近期技术面表现良好，MACD金叉，RSI处于合理区间，均线多头排列。基本面稳健，盈利能力较强。\n\n### 投资建议\n- 评级：买入\n- 风险等级：中\n- 预期收益率：15%\n\n### 风险提示\n- 市场波动风险\n- 行业政策变化风险"
        except Exception as e:
            self.logger.error(f"调用百度文心一言API失败: {str(e)}")
            raise
    
    def _call_alibaba_qwen(self, prompt: str) -> str:
        """
        调用阿里巴巴通义千问API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据阿里巴巴通义千问的实际API进行实现
            self.logger.info("调用阿里巴巴通义千问API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['alibaba']['api_key']
            
            if not api_key:
                raise ValueError("阿里巴巴通义千问API密钥未配置")
            
            # 示例响应
            return "### 分析结果\n该股票近期走势强劲，各项技术指标向好，成交量放大，资金流入明显。\n\n### 投资建议\n- 评级：买入\n- 风险等级：中\n- 预期收益率：18%\n\n### 风险提示\n- 短期回调风险\n- 宏观经济变化风险"
        except Exception as e:
            self.logger.error(f"调用阿里巴巴通义千问API失败: {str(e)}")
            raise
    
    def _call_tencent_hunyuan(self, prompt: str) -> str:
        """
        调用腾讯混元API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据腾讯混元的实际API进行实现
            self.logger.info("调用腾讯混元API")
            
            # 检查API密钥
            secret_id = self.config['api_keys']['tencent']['secret_id']
            secret_key = self.config['api_keys']['tencent']['secret_key']
            
            if not secret_id or not secret_key:
                raise ValueError("腾讯混元API密钥未配置")
            
            # 示例响应
            return "### 分析结果\n该股票基本面良好，估值合理，技术面呈现上升趋势。\n\n### 投资建议\n- 评级：持有\n- 风险等级：低\n- 预期收益率：10%\n\n### 风险提示\n- 行业竞争加剧风险\n- 汇率波动风险"
        except Exception as e:
            self.logger.error(f"调用腾讯混元API失败: {str(e)}")
            raise
    
    def _call_bytedance_doubao(self, prompt: str) -> str:
        """
        调用字节跳动豆包API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据字节跳动豆包的实际API进行实现
            self.logger.info("调用字节跳动豆包API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['bytedance']['api_key']
            
            if not api_key:
                raise ValueError("字节跳动豆包API密钥未配置")
            
            # 示例响应
            return "### 分析结果\n该股票近期表现平平，技术指标中性，成交量萎缩。\n\n### 投资建议\n- 评级：持有\n- 风险等级：中\n- 预期收益率：5%\n\n### 风险提示\n- 业绩不及预期风险\n- 市场情绪变化风险"
        except Exception as e:
            self.logger.error(f"调用字节跳动豆包API失败: {str(e)}")
            raise
    
    def _call_huawei_pangu(self, prompt: str) -> str:
        """
        调用华为盘古API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据华为盘古的实际API进行实现
            self.logger.info("调用华为盘古API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['huawei']['api_key']
            endpoint = self.config['api_keys']['huawei']['endpoint']
            
            if not api_key or not endpoint:
                raise ValueError("华为盘古API密钥或端点未配置")
            
            # 示例响应
            return "### 分析结果\n该股票技术面出现背离，MACD死叉，RSI超买，存在回调风险。\n\n### 投资建议\n- 评级：卖出\n- 风险等级：高\n- 预期收益率：-8%\n\n### 风险提示\n- 技术面走弱风险\n- 资金流出风险"
        except Exception as e:
            self.logger.error(f"调用华为盘古API失败: {str(e)}")
            raise
    
    def _call_zhipu_glm(self, prompt: str) -> str:
        """
        调用智谱清言GLM API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据智谱清言GLM的实际API进行实现
            self.logger.info("调用智谱清言GLM API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['zhipu']['api_key']
            
            if not api_key:
                raise ValueError("智谱清言GLM API密钥未配置")
            
            # 示例响应
            return "### 分析结果\n该股票处于底部区域，估值较低，技术面有企稳迹象。\n\n### 投资建议\n- 评级：买入\n- 风险等级：中\n- 预期收益率：20%\n\n### 风险提示\n- 底部震荡风险\n- 基本面改善不及预期风险"
        except Exception as e:
            self.logger.error(f"调用智谱清言GLM API失败: {str(e)}")
            raise
    
    def _call_sensecore_risen(self, prompt: str) -> str:
        """
        调用商汤日日新API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据商汤日日新的实际API进行实现
            self.logger.info("调用商汤日日新API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['sensecore']['api_key']
            
            if not api_key:
                raise ValueError("商汤日日新API密钥未配置")
            
            # 示例响应
            return "### 分析结果\n该股票近期上涨过快，估值偏高，技术面有回调压力。\n\n### 投资建议\n- 评级：卖出\n- 风险等级：高\n- 预期收益率：-10%\n\n### 风险提示\n- 估值回归风险\n- 获利了结压力"
        except Exception as e:
            self.logger.error(f"调用商汤日日新API失败: {str(e)}")
            raise
    
    def _call_openai_gpt(self, prompt: str) -> str:
        """
        调用OpenAI GPT API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据OpenAI GPT的实际API进行实现
            self.logger.info("调用OpenAI GPT API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['openai']['api_key']
            
            if not api_key:
                raise ValueError("OpenAI GPT API密钥未配置")
            
            # 示例响应
            return "### 分析结果\nThe stock shows strong technical indicators with a bullish trend. MACD is positive, RSI is in a healthy range, and moving averages are aligned in an upward pattern. The company has solid fundamentals with consistent earnings growth.\n\n### 投资建议\n- 评级：buy\n- 风险等级：medium\n- 预期收益率：15%\n\n### 风险提示\n- Market volatility risk\n- Regulatory changes risk"
        except Exception as e:
            self.logger.error(f"调用OpenAI GPT API失败: {str(e)}")
            raise
    
    def _call_anthropic_claude(self, prompt: str) -> str:
        """
        调用Anthropic Claude API
        
        Args:
            prompt: 分析prompt
            
        Returns:
            大模型响应
        """
        try:
            # 这里需要根据Anthropic Claude的实际API进行实现
            self.logger.info("调用Anthropic Claude API")
            
            # 检查API密钥
            api_key = self.config['api_keys']['anthropic']['api_key']
            
            if not api_key:
                raise ValueError("Anthropic Claude API密钥未配置")
            
            # 示例响应
            return "### 分析结果\nThe stock is currently in a consolidation phase after a recent rally. Technical indicators are mixed, with MACD flattening and RSI neutral. Fundamental analysis shows the company has strong cash flow but faces increasing competition.\n\n### 投资建议\n- 评级：hold\n- 风险等级：medium\n- 预期收益率：7%\n\n### 风险提示\n- Competitive pressures\n- Economic slowdown risk"
        except Exception as e:
            self.logger.error(f"调用Anthropic Claude API失败: {str(e)}")
            raise
    
    def set_provider(self, provider: str):
        """
        设置大模型提供商
        
        Args:
            provider: 大模型提供商名称
        """
        supported_providers = ['baidu', 'alibaba', 'tencent', 'bytedance', 'huawei', 'zhipu', 'sensecore', 'openai', 'anthropic']
        
        if provider not in supported_providers:
            raise ValueError(f"不支持的大模型提供商: {provider}")
        
        self.config['provider'] = provider
        self.logger.info(f"大模型提供商已切换为: {provider}")
    
    def set_model(self, model_name: str):
        """
        设置大模型名称
        
        Args:
            model_name: 大模型名称
        """
        self.config['model_name'] = model_name
        self.logger.info(f"大模型名称已设置为: {model_name}")
    
    def update_api_key(self, provider: str, api_key: str, secret_key: str = None):
        """
        更新API密钥
        
        Args:
            provider: 大模型提供商名称
            api_key: API密钥
            secret_key: 密钥（如果需要）
        """
        if provider in self.config['api_keys']:
            self.config['api_keys'][provider]['api_key'] = api_key
            if secret_key:
                self.config['api_keys'][provider]['secret_key'] = secret_key
            self.logger.info(f"已更新 {provider} 的API密钥")
        else:
            raise ValueError(f"不支持的大模型提供商: {provider}")


# 使用示例
if __name__ == '__main__':
    # 创建大模型分析引擎实例
    config = {
        "provider": "openai",
        "model_name": "gpt-4"
    }
    llm_engine = LLMAnalysisEngine(config)
    
    # 示例股票数据
    stock_data = {
        "股票代码": "sh.600000",
        "股票名称": "浦发银行",
        "最新价格": 10.5,
        "涨跌幅": 0.02,
        "成交量": 10000000,
        "市盈率": 5.2,
        "市净率": 0.6
    }
    
    # 示例技术指标
    technical_indicators = {
        "MACD": "金叉",
        "RSI": 55,
        "KDJ": "金叉",
        "布林带": "中轨上方",
        "均线": "多头排列"
    }
    
    try:
        # 使用大模型分析股票
        result = llm_engine.analyze_stock("sh.600000", stock_data, technical_indicators)
        print("大模型分析结果:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"分析失败: {str(e)}")