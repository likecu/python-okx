import os
from datetime import datetime
import json
import random
from typing import Dict, Any
import requests


class GeminiModelManager:
    def __init__(self, db_manager=None):
        """
        初始化 Gemini 模型管理器
        :param db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.models = []
        self.api_keys = []
        self.current_key_index = 0
        self.api_key = None
        
        # 加载API密钥（从数据库或默认）
        self.load_api_keys()
        
        if db_manager:
            self.load_models_from_db()
        else:
            self.load_default_models()
    
    def _get_default_api_keys(self):
        """
        从环境变量获取默认API密钥
        :return: API密钥列表
        """
        default_keys = []
        gemini_key_1 = os.environ.get('GEMINI_API_KEY_1', '').strip()
        gemini_key_2 = os.environ.get('GEMINI_API_KEY_2', '').strip()
        
        if gemini_key_1:
            default_keys.append(gemini_key_1)
        if gemini_key_2:
            default_keys.append(gemini_key_2)
            
        return default_keys if default_keys else None
    
    def load_api_keys(self):
        """
        从数据库加载API密钥
        如果数据库连接失败，则从环境变量获取默认密钥
        """
        if not self.db_manager or not self.db_manager.connect():
            print("无法连接数据库，尝试从环境变量加载API密钥")
            default_keys = self._get_default_api_keys()
            if default_keys:
                self.api_keys = default_keys
                print(f"从环境变量加载了 {len(self.api_keys)} 个Gemini API密钥")
            else:
                print("警告：无法加载任何API密钥，请检查环境变量配置")
                self.api_keys = []
        else:
            try:
                with self.db_manager.connection.cursor() as cursor:
                    query = """
                    SELECT key_value FROM api_keys 
                    WHERE key_type = 'gemini' AND is_enabled = 1 
                    ORDER BY priority ASC
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    if results:
                        self.api_keys = [row['key_value'] for row in results]
                        print(f"从数据库加载了 {len(self.api_keys)} 个Gemini API密钥")
                    else:
                        print("数据库中没有找到可用的Gemini API密钥，尝试从环境变量获取")
                        default_keys = self._get_default_api_keys()
                        if default_keys:
                            self.api_keys = default_keys
                        else:
                            print("警告：无法加载任何API密钥")
                            self.api_keys = []
            except Exception as e:
                print(f"从数据库加载API密钥失败: {e}，尝试从环境变量获取")
                default_keys = self._get_default_api_keys()
                if default_keys:
                    self.api_keys = default_keys
                else:
                    print("警告：无法加载任何API密钥")
                    self.api_keys = []
            finally:
                self.db_manager.disconnect()
        
        # 初始化当前密钥
        if self.api_keys:
            self.current_key_index = random.randint(0, len(self.api_keys) - 1)
            self.api_key = self.api_keys[self.current_key_index]
    
    def load_models_from_db(self):
        """
        从数据库加载模型配置
        """
        if not self.db_manager or not self.db_manager.connect():
            print("无法连接数据库，使用默认模型配置")
            self.load_default_models()
            return
        
        try:
            with self.db_manager.connection.cursor() as cursor:
                query = '''
                SELECT model_name, model_type, priority, is_enabled, 
                       rpm_limit, tpm_limit, rpd_limit, description
                FROM gemini_models
                WHERE is_enabled = 1
                ORDER BY priority ASC
                '''
                cursor.execute(query)
                results = cursor.fetchall()
                
                self.models = []
                for row in results:
                    self.models.append({
                        'model_name': row['model_name'],
                        'model_type': row['model_type'],
                        'priority': row['priority'],
                        'rpm_limit': row['rpm_limit'],
                        'tpm_limit': row['tpm_limit'],
                        'rpd_limit': row['rpd_limit'],
                        'description': row['description']
                    })
                
                print(f"从数据库加载了 {len(self.models)} 个模型配置")
        except Exception as e:
            print(f"从数据库加载模型配置失败: {e}")
            self.load_default_models()
        finally:
            self.db_manager.disconnect()
    
    def load_default_models(self):
        """
        加载默认模型配置
        """
        self.models = [
            {'model_name': 'gemini-2.5-flash', 'model_type': 'text_only', 'priority': 1},
            {'model_name': 'gemini-2.5-flash-lite', 'model_type': 'text_only', 'priority': 2},
            {'model_name': 'gemini-1.5-flash', 'model_type': 'image_supported', 'priority': 3},
            {'model_name': 'gemini-1.5-pro', 'model_type': 'image_supported', 'priority': 4},
            {'model_name': 'gemini-2.5-pro', 'model_type': 'text_only', 'priority': 5},
            {'model_name': 'gemma-3-27b-it', 'model_type': 'text_only', 'priority': 6},
            {'model_name': 'gemma-3-12b-it', 'model_type': 'text_only', 'priority': 7},
            {'model_name': 'gemma-3-2b-it', 'model_type': 'text_only', 'priority': 8},
            {'model_name': 'gemma-3-9b-it', 'model_type': 'text_only', 'priority': 9},
            {'model_name': 'gemini-2-27b-it', 'model_type': 'text_only', 'priority': 10},
            {'model_name': 'gemini-2-9b-it', 'model_type': 'text_only', 'priority': 11},
            {'model_name': 'gemini-1.1-7b-it', 'model_type': 'text_only', 'priority': 12},
            {'model_name': 'gemini-1-7b-it', 'model_type': 'text_only', 'priority': 13},
            {'model_name': 'gemini-2-2b-it', 'model_type': 'text_only', 'priority': 14},
            {'model_name': 'gemini-1.1-2b-it', 'model_type': 'text_only', 'priority': 15},
            {'model_name': 'gemini-1-2b-it', 'model_type': 'text_only', 'priority': 16},
            {'model_name': 'gemini-nano', 'model_type': 'text_only', 'priority': 17},
            {'model_name': 'gemini-ultra', 'model_type': 'image_supported', 'priority': 18},
            {'model_name': 'gemini-experimental', 'model_type': 'text_only', 'priority': 19}
        ]
        print(f"使用默认模型配置，共 {len(self.models)} 个模型")
    
    def select_best_model(self, task_type='text_only'):
        """
        根据优先级选择最优模型
        :param task_type: 任务类型，可选值：text_only, image_supported, document_supported
        :return: 最优模型名称
        """
        for model in self.models:
            if task_type == 'text_only':
                return model['model_name']
            elif task_type == 'image_supported' and model['model_type'] in ['image_supported', 'document_supported']:
                return model['model_name']
            elif task_type == 'document_supported' and model['model_type'] == 'document_supported':
                return model['model_name']
        
        return self.models[0]['model_name'] if self.models else 'gemini-2.5-flash'
    
    def get_available_models(self):
        """
        获取所有可用模型列表
        :return: 模型列表
        """
        return self.models
    
    def switch_api_key(self):
        """
        切换到下一个 API 密钥
        """
        if len(self.api_keys) > 1:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            self.api_key = self.api_keys[self.current_key_index]
            print(f"已切换到新 API 密钥: {self.api_key[:10]}...")
            return True
        return False


class GeminiAIAnalyzer:
    def __init__(self, db_manager=None):
        """
        初始化 Gemini AI 分析器
        :param db_manager: 数据库管理器实例，用于加载模型配置
        """
        self.model_manager = GeminiModelManager(db_manager)
        self.api_key = self.model_manager.api_key
        self.model_name = self.model_manager.select_best_model(task_type='text_only')
        print(f"使用模型: {self.model_name}")
    
    def ask_question(self, question):
        """
        直接向 Gemini 提问
        :param question: 提问内容
        :return: 提问结果对象
        """
        print(f"开始提问: {question}")
        print(f"当前使用模型: {self.model_name}")
        
        max_attempts = 3
        current_attempt = 0
        
        while current_attempt < max_attempts:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent?key={self.api_key}"
                
                headers = {
                    "Content-Type": "application/json"
                }
                
                data = {
                    "contents": [
                        {
                            "parts": [
                                {"text": question}
                            ]
                        }
                    ],
                    "generationConfig": {
                        "temperature": 0.7,
                        "topK": 40,
                        "topP": 0.95,
                        "maxOutputTokens": 8192
                    }
                }
                
                response = requests.post(url, headers=headers, json=data, timeout=60)
                
                if response.status_code == 200:
                    result = response.json()
                    if "candidates" in result and len(result["candidates"]) > 0:
                        text = result["candidates"][0]["content"]["parts"][0]["text"]
                        return {
                            "success": True,
                            "response": text,
                            "model": self.model_name
                        }
                    else:
                        print(f"API 返回格式异常: {result}")
                        return None
                else:
                    error_msg = response.text
                    print(f"API 请求失败: {response.status_code}, {error_msg}")
                    
                    if "quota exceeded" in error_msg.lower() or "429" in error_msg or response.status_code == 429:
                        raise Exception("quota exceeded")
                    else:
                        return None
                
            except Exception as e:
                error_msg = str(e)
                print(f"调用 Gemini API 时发生错误: {error_msg}")
                
                if "quota exceeded" in error_msg.lower() or "429" in error_msg:
                    print(f"模型 {self.model_name} 配额已用完，尝试切换模型或 API 密钥...")
                    current_attempt += 1
                    
                    if self.model_manager.switch_api_key():
                        self.api_key = self.model_manager.api_key
                    
                    available_models = [m['model_name'] for m in self.model_manager.get_available_models()]
                    if self.model_name in available_models:
                        available_models.remove(self.model_name)
                    
                    new_model = self.model_manager.select_best_model(task_type='text_only')
                    if new_model != self.model_name:
                        self.model_name = new_model
                        print(f"已切换到新模型: {self.model_name}")
                    else:
                        print("没有可用的替代模型")
                        break
                else:
                    return None
        
        print(f"尝试了 {current_attempt} 次后仍无法完成提问")
        return None
    
    def generate_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成 AI 分析结果
        :param data: 包含价格数据、技术指标等的数据字典
        :return: 分析结果字典
        """
        system_prompt = """
        你是一位精通加密货币市场的量化分析师。请基于以下数据，对 BTC 未来 7 天的价格走势进行全面分析：

        1. 技术面分析：MA5/MA20/MA50/MA200 趋势、RSI 指标、MACD 指标
        2. 链上数据：交易所余额变化、巨鲸活动、活跃地址数
        3. 市场情绪：恐惧与贪婪指数
        4. 宏观经济：美元指数、美联储政策预期

        请提供结构化分析，包括：
        - 未来 7 天价格区间预测（精确到 $100）
        - 关键支撑位和阻力位
        - 上涨/下跌概率（百分比）
        - 主要驱动因素（至少 3 条）
        - 重大风险提示（至少 3 条）
        - 明确的交易建议

        请以 JSON 格式返回结果，包含以下字段：
        "price_range", "support_level", "resistance_level", 
        "bullish_probability", "bearish_probability", 
        "driving_factors", "risks", "trading_advice", "analysis_date"
        """
        
        user_prompt = f"""
        当前 BTC 数据：

        1. 价格数据：
           - 当前价格：${data['price_data']['price']:.2f}
           - 24h 变化：{data['price_data']['change_24h']:.2f}%
           - 市值：${data['price_data']['market_cap']:.0f}
           - 24h 交易量：${data['price_data']['volume_24h']:.0f}

        2. 技术指标：
           - MA5：${data['technical_indicators']['MA5']:.2f}
           - MA20：${data['technical_indicators']['MA20']:.2f}
           - MA50：${data['technical_indicators']['MA50']:.2f}
           - MA200：${data['technical_indicators']['MA200']:.2f}
           - RSI：{data['technical_indicators']['RSI']:.2f}
           - MACD：{data['technical_indicators']['MACD']:.2f}

        请基于以上数据，按照指定格式进行分析。
        """
        
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        result = self.ask_question(full_prompt)
        
        if result and result.get("success"):
            try:
                message_content = result["response"].replace("```json", "").replace("```", "")
                analysis_data = json.loads(message_content)
                analysis_data["analysis_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return analysis_data
            except json.JSONDecodeError as e:
                print("JSON 解析错误:", e)
                return self._get_error_response(str(e))
        else:
            return self._get_error_response("API 调用失败")
    
    def _get_error_response(self, error_msg: str) -> Dict[str, Any]:
        """
        返回错误响应
        :param error_msg: 错误消息
        :return: 错误响应字典
        """
        return {
            "error": error_msg,
            "price_range": "无法生成预测",
            "support_level": "无法生成预测",
            "resistance_level": "无法生成预测",
            "bullish_probability": "0%",
            "bearish_probability": "0%",
            "driving_factors": ["AI分析失败", "请检查API密钥", "请检查网络连接"],
            "risks": ["AI分析失败风险", "数据获取失败风险", "模型响应异常风险"],
            "trading_advice": "谨慎操作，等待系统恢复正常",
            "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


AIAnalyzer = GeminiAIAnalyzer
