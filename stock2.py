import futuquant as ft
from futuquant.open_context import *
import numpy as np
import matplotlib.pyplot as plt


class StockQuoteTest(StockQuoteHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, content = super(StockQuoteTest, self).on_recv_rsp(
            rsp_str)  # 基类的on_recv_rsp方法解包返回了报价信息，格式与get_stock_quote一样
        if ret_code != RET_OK:
            print("StockQuoteTest: error, msg: %s" % content)
            return RET_ERROR, content
        print("StockQuoteTest ", content)  # StockQuoteTest自己的处理逻辑
        return RET_OK, content


class OrderBookTest(OrderBookHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, content = super(OrderBookTest, self).on_recv_rsp(
            rsp_str)  # 基类的on_recv_rsp方法解包返回摆盘信息，格式与get_order_book一样
        if ret_code != RET_OK:
            print("OrderBookTest: error, msg: %s" % content)
            return RET_ERROR, content
        print("OrderBookTest", content)  # OrderBookTest自己的处理逻辑
        return RET_OK, content


class CurKlineTest(CurKlineHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, content = super(CurKlineTest, self).on_recv_rsp(
            rsp_str)  # 基类的on_recv_rsp方法解包返回了实时K线信息，格式除了与get_cur_kline所有字段外，还包含K线类型k_type
        if ret_code != RET_OK:
            print("CurKlineTest: error, msg: %s" % content)
            return RET_ERROR, content
        print("CurKlineTest", content)  # CurKlineTest自己的处理逻辑
        return RET_OK, content


class RTDataTest(RTDataHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, content = super(RTDataTest, self).on_recv_rsp(rsp_str)  # 基类的on_recv_rsp方法解包返回分时数据，格式与get_rt_data一样
        if ret_code != RET_OK:
            print("RTDataTest: error, msg: %s" % content)
            return RET_ERROR, content
        print("RTDataTest", content)
        return RET_OK, content


class TickerTest(TickerHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, content = super(TickerTest, self).on_recv_rsp(rsp_str)  # 基类的on_recv_rsp方法解包返回了逐笔信息，格式与get_rt_ticker一样
        if ret_code != RET_OK:
            print("TickerTest: error, msg: %s" % content)
            return RET_ERROR, content
        print("TickerTest", content)  # StockQuoteTest自己的处理逻辑
        return RET_OK, content


class BrokerTest(BrokerHandlerBase):
    def on_recv_rsp(self, rsp_str):
        ret_code, ask_content, bid_content = super(BrokerTest, self).on_recv_rsp(
            rsp_str)  # 基类的on_recv_rsp方法解包返回经纪队列，格式与get_broker_queue一样
        if ret_code != RET_OK:
            print("BrokerTest: error, msg: %s %s " % ask_content % bid_content)
            return RET_ERROR, ask_content, bid_content
        print("BrokerTest", ask_content, bid_content)
        return RET_OK, ask_content, bid_content


def print_df(data_frame):
    if not data_frame or len(data_frame) != 2:
        print('data frame is none or not right format!')
        return
    data_frame = data_frame[1]
    keys = data_frame.keys().values
    vals = data_frame.values
    print('\t'.join(keys))
    for row in vals:
        for e in row:
            print(e, end='\t')
        print()


def main():
    quote_ctx = ft.OpenQuoteContext(host="10.140.0.5", port=11111)
    print(quote_ctx.subscribe('US.HUYA', 'QUOTE'))
    print(quote_ctx.subscribe('US.HUYA', 'TICKER'))
    print(quote_ctx.subscribe('US.HUYA', 'K_1M'))
    print(quote_ctx.subscribe('US.HUYA', 'ORDER_BOOK'))
    print(quote_ctx.subscribe('US.HUYA', 'RT_DATA'))
    print(quote_ctx.subscribe('US.HUYA', 'BROKER'))
    print(quote_ctx.query_subscription())
    print(quote_ctx.get_stock_quote('US.HUYA'))
    print_df(quote_ctx.get_cur_kline('US.HUYA', 100, ktype='K_1M', autype='qfq'))
    print_df(quote_ctx.get_rt_ticker('US.HUYA', num=500))
    print(quote_ctx.get_order_book('US.HUYA'))
    print_df(quote_ctx.get_rt_data('US.HUYA'))
    print_df(quote_ctx.get_broker_queue('US.HUYA'))
    quote_ctx.start()

    # stock_type = 'K_15M'
    # num = 500
    # print(quote_ctx.subscribe('US.HUYA', stock_type))
    # print(quote_ctx.subscribe('US.BILI', stock_type))
    # print(quote_ctx.subscribe('US.IQ', stock_type))
    # print(quote_ctx.subscribe('US.GOOG', stock_type))
    # print(quote_ctx.subscribe('US.MSFT', stock_type))
    # #kline_res = quote_ctx.get_cur_kline('US.HUYA', num, ktype=stock_type, autype='qfq')
    # kline_res2 = quote_ctx.get_cur_kline('US.BILI', num, ktype=stock_type, autype='qfq')
    # kline_res3 = quote_ctx.get_cur_kline('US.IQ', num, ktype=stock_type, autype='qfq')
    # kline_res4 = quote_ctx.get_cur_kline('US.GOOG', num, ktype=stock_type, autype='qfq')
    # kline_res5 = quote_ctx.get_cur_kline('US.MSFT', num, ktype=stock_type, autype='qfq')
    # #kline_pd = kline_res[1]
    # kline_pd2 = kline_res2[1]
    # kline_pd3 = kline_res3[1]
    # kline_pd4 = kline_res4[1]
    # kline_pd5 = kline_res5[1]
    # keys = kline_pd2.keys().values
    # #kline = kline_pd.values
    # kline2 = kline_pd2.values
    # kline3 = kline_pd3.values
    # kline4 = kline_pd4.values
    # kline5 = kline_pd5.values
    # print('\t'.join(keys))
    # for row in kline2:
    #     for e in row:
    #         print(e, end='\t')
    #     print()
    #
    # def map_str(s):
    #     s1, s2 = s.split()
    #     return int('%s%s%s' % (s1.split('-')[-1], s2.split(':')[0], s2.split(':')[1]))
    #
    # x = range(len(list(kline2[:, 1])))
    # #y1 = kline[:, 2]
    # y2 = kline2[:, 2]
    # y3 = kline3[:, 2]
    # y4 = kline4[:, 2]
    # y5 = kline5[:, 2]
    # plt.figure(dpi=300)
    # # plt.plot(x, (y1 - y1[0]) / y1, linewidth=1.0)
    # plt.plot(x, (y2 - y2[0]) / y2, 'r', linewidth=1.0)
    # # plt.plot(x, (y3 - y3[0]) / y3, 'g', linewidth=1.0)
    # plt.plot(x, (y4 - y4[0]) / y4, 'b', linewidth=1.0)
    # plt.plot(x, (y5 - y5[0]) / y5, 'g', linewidth=1.0)
    # plt.show()
    # n, m = kline.shape
    #
    # for i in range(n):
    #     for j in range(m):
    #         print(kline[i][j])


if __name__ == '__main__':
    main()
