import futuquant as ft
from utils import *
from futuquant.open_context import *


def init_quote_ctx():
    ft_config = get_config('futu')
    return ft.OpenQuoteContext(host=ft_config['ServerHost'], port=int(ft_config['ServerPort']))


quote_ctx = init_quote_ctx()


def get_trading_days(market, start_date=None, end_date=None):
    return quote_ctx.get_trading_days(market, start_date, end_date)


def get_stock_basicinfo(market, stock_type=None):
    return quote_ctx.get_stock_basicinfo(market, stock_type)


def get_history_kline(code, start=None, end=None, ktype='K_DAY', autype='qfq'):
    return quote_ctx.get_history_kline(code, start, end, ktype, autype)


def get_autype_list(code_list):
    return quote_ctx.get_autype_list(code_list)


def get_market_snapshot(code_list):
    return quote_ctx.get_market_snapshot(code_list)


def get_plate_list(market, plate_class):
    return quote_ctx.get_plate_list(market, plate_class)


def get_plate_stock(plate_code):
    return quote_ctx.get_plate_stock(plate_code)


def subscribe(stock_code, data_type, push=False):
    return quote_ctx.subscribe(stock_code, data_type, push)


def unsubscribe(stock_code, data_type, unpush=True):
    return quote_ctx.unsubscribe(stock_code, data_type, unpush)


def start():
    quote_ctx.start()


def stop():
    quote_ctx.stop()


def query_subscription(query=0):
    return quote_ctx.query_subscription(query)


def get_stock_quote(code_list, async_handler=None):
    class StockQuoteHandler(StockQuoteHandlerBase):
        def on_recv_rsp(self, rsp_str):
            ret_code, content = super(StockQuoteHandler, self).on_recv_rsp(
                rsp_str)  # 基类的on_recv_rsp方法解包返回了报价信息，格式与get_stock_quote一样
            if ret_code != RET_OK:
                print("StockQuoteTest: error, msg: %s" % content)
                return RET_ERROR, content
            async_handler(content)
            return RET_OK, content

    if async_handler:
        return quote_ctx.set_handler(StockQuoteHandler())

    return quote_ctx.get_stock_quote(code_list)


def get_cur_kline(code, num, ktype='K_DAY', autype='qfq', async_handler=None):
    class CurKlineHandler(CurKlineHandlerBase):
        def on_recv_rsp(self, rsp_str):
            ret_code, content = super(CurKlineHandler, self).on_recv_rsp(
                rsp_str)  # 基类的on_recv_rsp方法解包返回了实时K线信息，格式除了与get_cur_kline所有字段外，还包含K线类型k_type
            if ret_code != RET_OK:
                print("CurKlineTest: error, msg: %s" % content)
                return RET_ERROR, content
            async_handler(content)
            return RET_OK, content

    if async_handler:
        return quote_ctx.set_handler(CurKlineHandler())

    return quote_ctx.get_cur_kline(code, num, ktype, autype)


def get_rt_ticker(code, num=500, async_handler=None):
    class RTDataHandler(RTDataHandlerBase):
        def on_recv_rsp(self, rsp_str):
            ret_code, content = super(RTDataHandler, self).on_recv_rsp(
                rsp_str)  # 基类的on_recv_rsp方法解包返回分时数据，格式与get_rt_data一样
            if ret_code != RET_OK:
                print("RTDataTest: error, msg: %s" % content)
                return RET_ERROR, content
            async_handler(content)
            return RET_OK, content

    if async_handler:
        return quote_ctx.set_handler(RTDataHandler())

    return quote_ctx.get_rt_ticker(code, num)


def get_order_book(code, async_handler=None):
    class OrderBookHandler(OrderBookHandlerBase):
        def on_recv_rsp(self, rsp_str):
            ret_code, content = super(OrderBookHandler, self).on_recv_rsp(
                rsp_str)  # 基类的on_recv_rsp方法解包返回摆盘信息，格式与get_order_book一样
            if ret_code != RET_OK:
                print("OrderBookTest: error, msg: %s" % content)
                return RET_ERROR, content
            async_handler(content)
            return RET_OK, content

    if async_handler:
        return quote_ctx.set_handler(OrderBookHandler())

    return quote_ctx.get_order_book(code)


def get_rt_data(code, async_handler=None):
    class RTDataHandler(RTDataHandlerBase):
        def on_recv_rsp(self, rsp_str):
            ret_code, content = super(RTDataHandler, self).on_recv_rsp(
                rsp_str)  # 基类的on_recv_rsp方法解包返回分时数据，格式与get_rt_data一样
            if ret_code != RET_OK:
                print("RTDataTest: error, msg: %s" % content)
                return RET_ERROR, content
            async_handler(content)
            return RET_OK, content

    if async_handler:
        return quote_ctx.set_handler(RTDataHandler())

    return quote_ctx.get_rt_data(code)


def get_broker_queue(code, async_handler=None):
    class BrokerHandler(BrokerHandlerBase):
        def on_recv_rsp(self, rsp_str):
            ret_code, ask_content, bid_content = super(BrokerHandler, self).on_recv_rsp(
                rsp_str)  # 基类的on_recv_rsp方法解包返回经纪队列，格式与get_broker_queue一样
            if ret_code != RET_OK:
                print("BrokerTest: error, msg: %s %s " % (ask_content, bid_content))
                return RET_ERROR, ask_content, bid_content
            async_handler([ask_content, bid_content])
            return RET_OK, ask_content, bid_content

    if async_handler:
        return quote_ctx.set_handler(BrokerHandler())

    return quote_ctx.get_broker_queue(code)
