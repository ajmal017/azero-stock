from core import *
import futuquant as ft


class FutuStockApi(BaseStockApi):

    def __init__(self, host='127.0.0.1'):
        self.quote_ctx = ft.OpenQuoteContext(host=host, port=11111)

    def get_trading_days(self, market, start_date=None, end_date=None):
        return self.quote_ctx.get_trading_days(market, start_date, end_date)

    def get_stock_basicinfo(self, market, stock_type=None):
        return self.quote_ctx.get_stock_basicinfo(market, stock_type)

    def get_history_kline(self, code, start=None, end=None, ktype='K_DAY', autype='qfq'):
        return self.quote_ctx.get_history_kline(code, start, end, ktype, autype)

    def get_autype_list(self, code_list):
        return self.quote_ctx.get_autype_list(code_list)

    def get_market_snapshot(self, code_list):
        return self.quote_ctx.get_market_snapshot(code_list)

    def get_plate_list(self, market, plate_class):
        return self.quote_ctx.get_plate_list(market, plate_class)

    def get_plate_stock(self, market, stock_code):
        return self.get_plate_stock(market, stock_code)
