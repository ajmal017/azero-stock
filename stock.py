from futu import *


def main():
    stock_api = FutuStockApi()
    print(stock_api.get_trading_days('US'))


if __name__ == '__main__':
    main()
