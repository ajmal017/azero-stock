import glob

import pandas as pd
from dateutil import relativedelta

from ib_api import *
from utils import *

logger = setup_logger('ib_sync_log', 'ib_sync.log')

def get_earliest_time(app, contract):
    earliest_time = '%s 00:00:00' % app.req_head_time_stamp(500, contract)[0][1].split()[0]
    earliest_time = (datetime.datetime.strptime(earliest_time, '%Y%m%d %H:%M:%S') + relativedelta
                     .relativedelta(months=2)).strftime("%Y%m%d 00:00:00")
    return max('20120201 00:00:00', earliest_time)


def next_time(time):
    return (datetime.datetime.strptime(time, '%Y%m%d %H:%M:%S') + relativedelta
            .relativedelta(months=2)).strftime("%Y%m%d 00:00:00")


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
    earliest_file = sorted(glob.glob('%s/ib_data/%s*_1M.log' % (STORE_PATH, symbol)))
    if not earliest_file:
        return None
    earliest_file = list(map(lambda x: x.split('/')[-1], earliest_file))
    return '%s 00:00:00' % earliest_file[0].split('_')[1]


def sync_stock(app, contract):
    client_id = int(app.clientId)
    symbol = contract.symbol
    dt = datetime.datetime.today().strftime("%Y%m%d 00:00:00")
    early_dt = earliest_dt_for_symbol(symbol)
    if early_dt:
        dt = early_dt

    while True:
        hist_data = app.req_historical_data(client_id, contract, dt,
                                            "5 D", "30 secs")
        if not hist_data:
            return

        logger.info('%s start syncing %s, %s-%s...' % (datetime.datetime.today().strftime("%Y%m%d %H:%M:%S"),
                                                       symbol, hist_data[0][2].date, hist_data[-2][2].date))
        f_map = {}
        for data in hist_data:
            if data[1] == 'historical_data':
                bar = data[2]
                sd = bar.date.split()[0]
                if '%s_%s' % (symbol, sd) not in f_map:
                    f_map['%s_%s' % (symbol, sd)] = open('%s/ib_data/%s_%s_1M.log' % (STORE_PATH, symbol, sd), 'w')
                f = f_map['%s_%s' % (symbol, sd)]
                f.writelines('%s~%s~%s~%s~%s~%s\n' % (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume))
            elif data[1] == 'historical_data_end':
                dt = '%s %s' % (hist_data[0][2].date.split()[0], hist_data[0][2].date.split()[1])
        for f in f_map.values():
            f.close()
        logger.info('%s syncing %s, %s-%s done' % (datetime.datetime.today().strftime("%Y%m%d %H:%M:%S"),
                                                   symbol, hist_data[0][2].date, hist_data[-2][2].date))

        client_id += 1


def main():
    contracts = read_stock_contracts()
    app = IBApp("localhost", 4001, 10)
    logger.info('start syncing....')

    for i, contract in enumerate(contracts):
        sync_stock(app, make_contract(contract.symbol, 'SMART'))


if __name__ == '__main__':
    main()
