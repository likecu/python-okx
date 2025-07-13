import datetime
import random
import uuid


class DcaExeStrategy:
    def __init__(self, price_drop_threshold=0.02, max_time_since_last_trade=7,
                 min_time_since_last_trade=3, take_profit_threshold=0.01,
                 initial_capital=100000, initial_investment_ratio=0.5, initial_dca_value=0.1,
                 buy_fee_rate=0.001, sell_fee_rate=0.001, database_manager=None, strategy_name=None,
                 currency=None):
        """
        初始化DCA策略参数

        参数:
        price_drop_threshold: 价格下跌触发DCA的阈值(百分比)
        max_time_since_last_trade: 最长无交易时间触发DCA(小时)
        min_time_since_last_trade: 最短无交易时间触发DCA(小时)
        take_profit_threshold: 止盈阈值(百分比)
        initial_capital: 初始资金
        initial_investment_ratio: 初始投资使用的资金比例
        buy_fee_rate: 买入交易费用比例
        sell_fee_rate: 卖出交易费用比例
        database_manager: 数据库管理器实例
        strategy_name: 策略名称，默认为随机生成的UUID
        currency: 交易对货币对
        """
        self.price_drop_threshold = price_drop_threshold
        self.max_time_since_last_trade = max_time_since_last_trade
        self.min_time_since_last_trade = min_time_since_last_trade
        self.take_profit_threshold = take_profit_threshold
        self.initial_capital = initial_capital
        self.initial_investment_ratio = initial_investment_ratio
        self.initial_dca_value = initial_dca_value
        self.buy_fee_rate = buy_fee_rate
        self.sell_fee_rate = sell_fee_rate
        self.currency = currency

        # 策略状态
        self.positions = []  # 持仓记录
        self.trades = []  # 交易记录
        self.portfolio = {
            'cash': initial_capital,
            'position': 0,
            'avg_price': 0,
            'last_trade_time': None,
            'last_trade_price': None,
            'peak_value': initial_capital
        }
        self.initial_dca_amount = None  # 记录首次DCA金额

        # 数据库相关
        self.database_manager = database_manager
        self.strategy_name = strategy_name or str(uuid.uuid4())  # 默认使用UUID作为策略名称
        self.strategy_id = None

    def execute_logic(self, current_time, current_price, inst_id=None):
        """执行交易逻辑并返回交易决策"""
        # 如果没有持仓，创建初始仓位
        if self.portfolio['position'] == 0:
            trade_info = self._create_initial_position(current_time, current_price, inst_id)
            if trade_info and self.database_manager:
                self._save_state_and_trade(trade_info, inst_id)
            return trade_info

        # 检查是否满足止盈条件
        if self._should_take_profit(current_price):
            trade_info = self._create_take_profit_order(current_time, current_price, inst_id)
            if trade_info and self.database_manager:
                self._save_state_and_trade(trade_info, inst_id)
            return trade_info

        # 检查是否满足DCA条件
        if self._should_dca(current_time, current_price):
            trade_info = self._create_dca_order(current_time, current_price, inst_id)
            if trade_info and self.database_manager:
                self._save_state_and_trade(trade_info, inst_id)
            return trade_info

        return None

    def _save_state_and_trade(self, trade_info, inst_id=None):
        """保存策略状态和交易记录到数据库"""
        if not self.database_manager:
            return

        # 如果还没有strategy_id，先保存状态获取id
        if not self.strategy_id:
            self.strategy_id = self.database_manager.save_strategy_state(
                self.strategy_name,
                self._get_strategy_params(),
                self.portfolio,
                self.initial_dca_amount
            )

        # 更新策略状态并保存交易记录
        if self.strategy_id:
            # 更新当前策略状态
            self.database_manager.save_strategy_state(
                self.strategy_name,
                self._get_strategy_params(),
                self.portfolio,
                self.initial_dca_amount
            )
            # 添加inst_id到交易信息中
            trade_info['inst_id'] = inst_id
            trade_info['strategy_id'] = self.strategy_id
            self.database_manager.save_trade_record(self.strategy_id, trade_info)

    def _get_strategy_params(self):
        """获取策略参数的字典形式"""
        return {
            'price_drop_threshold': self.price_drop_threshold,
            'max_time_since_last_trade': self.max_time_since_last_trade,
            'min_time_since_last_trade': self.min_time_since_last_trade,
            'take_profit_threshold': self.take_profit_threshold,
            'initial_capital': self.initial_capital,
            'initial_investment_ratio': self.initial_investment_ratio,
            'initial_dca_value': self.initial_dca_value,
            'buy_fee_rate': self.buy_fee_rate,
            'sell_fee_rate': self.sell_fee_rate
        }

    def load_state(self):
        """从数据库加载策略状态"""
        if not self.database_manager:
            print("未提供数据库管理器，无法加载状态")
            return False

        # 修复：正确调用load_strategy_state方法，只传递strategy_name参数
        state_data = self.database_manager.load_strategy_state(self.strategy_name)
        if not state_data:
            print(f"未找到策略 '{self.strategy_name}' 的状态记录，将使用默认参数")
            return False

        # 加载策略参数
        params = state_data['strategy_params']
        self.price_drop_threshold = params['price_drop_threshold']
        self.max_time_since_last_trade = params['max_time_since_last_trade']
        self.min_time_since_last_trade = params['min_time_since_last_trade']
        self.take_profit_threshold = params['take_profit_threshold']
        self.initial_capital = params['initial_capital']
        self.initial_investment_ratio = params['initial_investment_ratio']
        self.initial_dca_value = params['initial_dca_value']
        self.buy_fee_rate = params['buy_fee_rate']
        self.sell_fee_rate = params['sell_fee_rate']

        # 加载投资组合状态
        portfolio = state_data['portfolio']
        self.portfolio['cash'] = portfolio['cash']
        self.portfolio['position'] = portfolio['position']
        self.portfolio['avg_price'] = portfolio['avg_price']

        # 处理日期时间
        if portfolio['last_trade_time']:
            self.portfolio['last_trade_time'] = datetime.datetime.fromisoformat(portfolio['last_trade_time'])
        else:
            self.portfolio['last_trade_time'] = None

        self.portfolio['last_trade_price'] = portfolio['last_trade_price']
        self.portfolio['peak_value'] = portfolio['peak_value']

        # 加载初始DCA金额
        self.initial_dca_amount = state_data['initial_dca_amount']

        # 加载交易记录
        self.trades = list(state_data['trades'])

        # 保存strategy_id
        self.strategy_id = state_data['strategy_id']

        print(f"成功从数据库加载策略 '{self.strategy_name}' 的状态")
        return True

    def _create_initial_position(self, current_time, current_price, inst_id=None):
        """创建初始仓位"""
        # 使用设定比例的资金建立初始仓位
        amount_to_invest = self.portfolio['cash'] * self.initial_investment_ratio

        # 计算包含交易费用的总金额
        total_amount = amount_to_invest / (1 - self.buy_fee_rate)

        # 计算实际支付的交易费用
        fee = total_amount - amount_to_invest

        # 计算可购买的份额
        shares_to_buy = amount_to_invest / current_price

        # 记录交易信息
        trade_info = {
            'time': current_time,
            'type': 'INITIAL_BUY',
            'price': current_price,
            'position': shares_to_buy,
            'cash': self.portfolio['cash'] - total_amount,
            'portfolio_value': self.portfolio['cash'] + self.portfolio['position'] * current_price,
            'fee': fee,
            'amount': total_amount,
            'side': 'buy',
            'inst_id': inst_id  # 添加交易对信息
        }

        # 更新投资组合
        self.portfolio['cash'] -= total_amount
        self.portfolio['position'] = shares_to_buy
        self.portfolio['avg_price'] = current_price
        self.portfolio['last_trade_time'] = current_time
        self.portfolio['last_trade_price'] = current_price

        # 记录首次DCA金额(初始买入后第一次DCA的金额)
        self.initial_dca_amount = None

        self.trades.append(trade_info)
        return trade_info

    def _should_take_profit(self, current_price):
        """判断是否应该止盈"""
        # 计算当前持仓的收益率
        if self.portfolio['avg_price'] == 0:
            return False

        current_return = (current_price / self.portfolio['avg_price']) - 1

        # 如果收益率达到或超过止盈阈值，则止盈
        return current_return >= self.take_profit_threshold

    def _should_dca(self, current_time, current_price):
        """判断是否应该执行DCA"""
        if self.portfolio['last_trade_price'] is None:
            return False

        # 计算价格下跌幅度
        price_drop = (self.portfolio['last_trade_price'] / current_price) - 1

        # 计算自上次交易以来的时间(小时)
        if self.portfolio['last_trade_time']:
            time_since_last_trade = (current_time - self.portfolio['last_trade_time']).total_seconds() / 3600
        else:
            time_since_last_trade = float('inf')

        # 使用上次交易时间作为种子生成固定随机阈值，确保间隔均匀分布
        if self.portfolio['last_trade_time']:
            # 将时间戳转换为整数作为随机种子
            seed = int(self.portfolio['last_trade_time'].timestamp())
            random.seed(seed)
            random_time_threshold = random.uniform(self.min_time_since_last_trade, self.max_time_since_last_trade)
            print("当前选择的时间阈值", random_time_threshold, "小时，已过去", time_since_last_trade, "小时")
        else:
            random_time_threshold = 0

        # 如果价格下跌超过阈值或者无交易时间超过随机时间阈值，则执行DCA
        return (price_drop >= self.price_drop_threshold) or (time_since_last_trade >= random_time_threshold)

    def _create_dca_order(self, current_time, current_price, inst_id=None):
        """创建DCA订单"""
        # 首次DCA时记录金额
        if self.initial_dca_amount is None:
            # 使用剩余资金的一定比例作为首次DCA金额
            self.initial_dca_amount = self.portfolio['cash'] * self.initial_dca_value

        # 确保有足够的资金进行DCA
        if self.portfolio['cash'] < self.initial_dca_amount:
            # 如果资金不足，使用所有剩余资金
            amount_to_invest = self.portfolio['cash']
            if amount_to_invest <= 0:
                return None
        else:
            amount_to_invest = self.initial_dca_amount

        # 计算包含交易费用的总金额
        total_amount = amount_to_invest / (1 - self.buy_fee_rate)

        # 计算实际支付的交易费用
        fee = total_amount - amount_to_invest

        # 计算可购买的份额
        shares_to_buy = amount_to_invest / current_price

        # 更新平均价格
        total_value = (self.portfolio['position'] * self.portfolio['avg_price']) + total_amount
        total_shares = self.portfolio['position'] + shares_to_buy
        new_avg_price = total_value / total_shares

        # 记录交易信息
        trade_info = {
            'time': current_time,
            'type': 'DCA',
            'price': current_price,
            'position': total_shares,
            'cash': self.portfolio['cash'] - total_amount,
            'avg_price': new_avg_price,
            'portfolio_value': self.portfolio['cash'] + total_shares * current_price,
            'dca_amount': amount_to_invest,
            'fee': fee,
            'amount': total_amount,
            'side': 'buy',
            'inst_id': inst_id  # 添加交易对信息
        }

        # 更新投资组合
        self.portfolio['cash'] -= total_amount
        self.portfolio['position'] = total_shares
        self.portfolio['avg_price'] = new_avg_price
        self.portfolio['last_trade_time'] = current_time
        self.portfolio['last_trade_price'] = current_price

        self.trades.append(trade_info)
        return trade_info

    def _create_take_profit_order(self, current_time, current_price, inst_id=None):
        """创建止盈订单"""
        # 计算持仓价值
        position_value = self.portfolio['position'] * current_price

        # 计算交易费用
        fee = position_value * self.sell_fee_rate

        # 计算扣除交易费用后的实际收入
        actual_income = position_value - fee

        # 计算利润
        profit = actual_income - (self.portfolio['position'] * self.portfolio['avg_price'])

        # 计算卖出数量
        sell_size = self.portfolio['position']  # 全部卖出
        # 记录交易信息
        trade_info = {
            'time': current_time,
            'type': 'TAKE_PROFIT',
            'price': current_price,
            'position': 0,
            'cash': self.portfolio['cash'] + actual_income,
            'sz': sell_size,
            'profit': profit,
            'portfolio_value': self.portfolio['cash'] + actual_income,
            'fee': fee,
            'amount': position_value,
            'side': 'sell',
            'inst_id': inst_id  # 添加交易对信息
        }

        # 更新投资组合
        self.portfolio['cash'] += actual_income
        self.portfolio['position'] = 0
        self.portfolio['avg_price'] = 0
        self.portfolio['last_trade_time'] = current_time
        self.portfolio['last_trade_price'] = current_price

        self.trades.append(trade_info)
        return trade_info
