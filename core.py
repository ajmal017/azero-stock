import abc


class BaseStockApi(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_trading_days(self, market, start_date=None, end_date=None):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_stock_basicinfo(self, market, stock_type=None):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_history_kline(self, code, start=None, end=None, ktype='K_DAY', autype='qfq'):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_autype_list(self, code_list):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_market_snapshot(self, code_list):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_plate_list(self, market, plate_class):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_plate_stock(self, market, stock_code):
        raise NotImplementedError()
