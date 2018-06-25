import numpy as np
import pandas as pd
import futu_api as api
from futuquant import *

class Mine(object):
    BUY = [(0, 0, 0)]
    SELL = [(0, 0, 0)]

def azero():
    # quote_ctx = OpenQuoteContext(host='10.140.0.5', port=11111)
    # quote_ctx.subscribe('US.IQ', 'K_1M')
    # quote_ctx.subscribe('US.IQ', 'TICKER')
    # print(quote_ctx.get('US.IQ', 500))
    def _handler(param):
        param = param.values[0]
        if 'TT_BUY' == param[5]:
            Mine.BUY += [(param[2], int(param[3]), param[4])]
        elif 'TT_SELL' == param[5]:
            Mine.SELL += [(param[2], int(param[3]), param[4])]
        buy_total = sum(map(lambda x: x[2], Mine.BUY))
        sell_total = sum(map(lambda x: x[2], Mine.SELL))
        print((('<*>' if buy_total > sell_total else '') + 'BUY %f %d total: (%d)-----SELL %f %d total: (%d)' +
               ('<*>' if sell_total >= buy_total else '')) %
              (Mine.BUY[-1][0], Mine.BUY[-1][1], buy_total,
               Mine.SELL[-1][0], Mine.SELL[-1][1], sell_total))

    print(api.subscribe('US.IQ', 'K_1M', True))
    # print(api.subscribe('US.IQ', 'ORDER_BOOK', True))
    # print(api.subscribe('US.IQ', 'BROKER', True))
    print(api.subscribe('US.IQ', 'TICKER', True))
    # print(api.subscribe('US.IQ', 'QUOTE', True))
    # print(api.get_cur_kline('US.IQ', 1000, ktype='K_1M'))
    # print(api.get_broker_queue('US.IQ', async_handler=_handler))
    # print(api.get_order_book('US.IQ', async_handler=_handler))
    print(api.get_rt_ticker('US.IQ', num=1000, async_handler=_handler))
    # print(api.get_stock_quote(['US.IQ'], async_handler=_handler))
    api.start()


if __name__ == '__main__':
    azero()
