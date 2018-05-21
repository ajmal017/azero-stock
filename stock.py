from futu import *


def main():
    stock_api = FutuStockApi(host='10.140.0.5')
    # print(stock_api.get_trading_days('US'))
    # print(stock_api.get_history_kline('US.HUYA', start='2018-04-21', end='2018-05-20'))
    print(stock_api.subscribe('US.HUYA', 'K_1M', True))
    print(stock_api.subscribe('US.HUYA', 'RT_DATA', True))
    print(stock_api.get_rt_data('US.HUYA', async_handler=lambda x: print(x)))
    # stock_api.start()


if __name__ == '__main__':
    main()
