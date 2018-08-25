import glob

from dateutil import relativedelta

from ib_api import *
from utils import *
import time

logger = setup_logger('ib_sync_log', 'ib_sync.log')


def relative_day(time, delta):
    return (datetime.datetime.strptime(time, '%Y%m%d %H:%M:%S') + delta).strftime("%Y%m%d %H:%M:%S")


def make_contract(symbol, exchange):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.primaryExchange = "ISLAND"
    return contract


def read_stock_contracts():
    with open('stock_sync_codes.txt') as f:
        symbols = list(map(lambda x: x.strip().replace('US.', ''), f.readlines()))

    codes = [make_contract(stock_code, 'SMART')
             for i, stock_code in enumerate(symbols)]
    return codes


def earliest_dt_for_symbol(symbol):
    earliest_file = sorted(glob.glob('%s/ib_data/%s*_1S.log' % (STORE_PATH, symbol)))
    if not earliest_file:
        return None
    earliest_file = list(map(lambda x: x.split('/')[-1], earliest_file))
    return relative_day('%s 23:59:59' % earliest_file[0].split('_')[1], -relativedelta.relativedelta(days=1))


def sync_stock(app, contract):
    client_id = int(app.clientId)
    symbol = contract.symbol
    dt = relative_day(datetime.datetime.today().strftime("%Y%m%d 23:59:59"), -relativedelta.relativedelta(days=1))
    latest_dt = earliest_dt_for_symbol(symbol)
    bars = []
    cnt = 0
    if latest_dt:
        dt = latest_dt

    while True:
        if cnt == 60:
            logger.info('%s waiting...' % symbol)
            time.sleep(600)
            cnt = 0
        if dt <= '20180601 00:00:00':
            return

        print(client_id + cnt)
        hist_data = app.req_historical_data(client_id + cnt, contract, dt,
                                            "1800 S", "1 secs")
        cnt += 1
        if not hist_data:
            if bars:
                bar = bars[0]
                sd = bar.date.split()[0]
                lines = ''.join(list(map(lambda x: '%s~%s~%s~%s~%s~%s\n' % (
                    x.date, x.open, x.high, x.low, x.close, x.volume
                ), bars)))
                with open('%s/ib_data/%s_%s_1S.log' % (STORE_PATH, symbol, sd), 'w') as f:
                    f.writelines(lines)
                logger.info('%s syncing %s, %s-%s done' % (datetime.datetime.today().strftime("%Y%m%d %H:%M:%S"),
                                                           symbol, bars[0].date, bars[-1].date))
                bars.clear()
                dt = relative_day('%s 23:59:59' % sd.split()[0], -relativedelta.relativedelta(days=1))
                continue
            else:
                time.sleep(600)
                return

        logger.info('%s start syncing %s, %s-%s...' % (datetime.datetime.today().strftime("%Y%m%d %H:%M:%S"),
                                                       symbol, hist_data[0][2].date, hist_data[-2][2].date))
        temp = []
        for data in hist_data:
            if data[1] == 'historical_data':
                bar = data[2]
                temp.append(bar)
            elif data[1] == 'historical_data_end':
                if hist_data[0][2].date.split()[0] != dt.split()[0] and bars:
                    bar = bars[0]
                    sd = bar.date.split()[0]
                    lines = ''.join(list(map(lambda x: '%s~%s~%s~%s~%s~%s\n' % (
                        x.date, x.open, x.high, x.low, x.close, x.volume
                    ), bars)))
                    with open('%s/ib_data/%s_%s_1S.log' % (STORE_PATH, symbol, sd), 'w') as f:
                        f.writelines(lines)
                    logger.info('%s syncing %s, %s-%s done' % (datetime.datetime.today().strftime("%Y%m%d %H:%M:%S"),
                                                               symbol, bars[0].date, bars[-1].date))
                    bars.clear()

                dt = '%s %s' % (hist_data[0][2].date.split()[0], hist_data[0][2].date.split()[1])
                bars = temp + bars



def main():
    contracts = read_stock_contracts()
    app = IBApp("localhost", 4001, 20)
    logger.info('start syncing....')

    for i, contract in enumerate(contracts):
        sync_stock(app, make_contract(contract.symbol, 'SMART'))


if __name__ == '__main__':
    main()
