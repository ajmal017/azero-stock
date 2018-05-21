import abc


class BaseStockApi(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_trading_days(self, market, start_date=None, end_date=None):
        """get the trading days"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_stock_basicinfo(self, market, stock_type=None):
        """get the basic information of stock"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_history_kline(self, code, start=None, end=None, ktype='K_DAY', autype='qfq'):
        """
        得到本地历史k线，需先参照帮助文档下载k线
        :param code: 股票code
        :param start: 开始时间 '%Y-%m-%d'
        :param end:  结束时间 '%Y-%m-%d'
        :param ktype: k线类型， 参见 KTYPE_MAP 定义 'K_1M' 'K_DAY'...
        :param autype: 复权类型, 参见 AUTYPE_MAP 定义 'None', 'qfq', 'hfq'
        :param fields: 需返回的字段列表，参见 KL_FIELD 定义 KL_FIELD.ALL  KL_FIELD.OPEN ....
        :return: (ret, data) ret == 0 返回pd dataframe数据，表头包括'code', 'time_key', 'open', 'close', 'high', 'low',
                                        'volume', 'turnover', 'pe_ratio', 'turnover_rate' 'change_rate'
                             ret != 0 返回错误字符串
        """
        """get the historic Kline data"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_autype_list(self, code_list):
        """get the autype list"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_market_snapshot(self, code_list):
        """get teh market snapshot"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_plate_list(self, market, plate_class):
        """get stock list of the given plate"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_plate_stock(self, plate_code):
        """get the stock of the given plate"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_stock_quote(self, code_list):
        """
        :param code_list:
        :return: DataFrame of quote data

        Usage:

        After subcribe "QUOTE" type for given stock codes, invoke

        get_stock_quote to obtain the data

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_cur_kline(self, code, num, ktype='K_DAY', autype='qfq'):
        """
        get current kline
        :param code: stock code
        :param num:
        :param ktype: the type of kline
        :param autype:
        :return:
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_rt_ticker(self, code, num=500):
        """
        get transaction information
        :param code: stock code
        :param num: the default is 500
        :return: (ret_ok, ticker_frame_table)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_order_book(self, code):
        """get the order book data"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_rt_data(self, code):
        """get real-time data"""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_broker_queue(self, code):
        """get teh queue of the broker"""
        raise NotImplementedError()
