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
    return contract


def read_stock_contracts():
    stock_codes = pd.read_csv('stock_code.csv')
    codes = [make_contract(''.join(stock_code[1][3:]), 'SMART')
             for i, stock_code in enumerate(stock_codes.values)]
    return codes


def earliest_dt_for_symbol(symbol):
    earliest_file = sorted(glob.glob('ib_data/%s*.log' % symbol))
    if not earliest_file:
        return None
    return '%s 00:00:00' % earliest_file[0].split('_')[2]


def sync_stock(app, contract):
    client_id = int(app.clientId)
    symbol = contract.symbol
    dt = datetime.datetime.today().strftime("%Y%m%d 00:00:00")
    early_dt = earliest_dt_for_symbol(symbol)
    if early_dt:
        dt = early_dt

    while True:
        hist_data = app.req_historical_data(client_id, contract, dt,
                                            "2 M", "1 min")
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
                    f_map['%s_%s' % (symbol, sd)] = open('ib_data/%s_%s_1M.log' % (symbol, sd), 'w')
                f = f_map['%s_%s' % (symbol, sd)]
                f.writelines('%s~%s~%s~%s~%s~%s\n' % (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume))
            elif data[1] == 'historical_data_end':
                dt = data[2]
        for f in f_map.values():
            f.close()
            logger.info('%s syncing %s, %s-%s done' % (datetime.datetime.today().strftime("%Y%m%d %H:%M:%S"),
                                                       symbol, hist_data[0][2].date, hist_data[-2][2].date))

        client_id += 1


def main():
    contracts = read_stock_contracts()
    app = IBApp("localhost", 4001, 20)
    logger.info('start syncing....')

    for i, contract in enumerate(contracts[200:]):
        if i == 2:
            break
        sync_stock(app, make_contract(contract.symbol, 'SMART'))


if __name__ == '__main__':
    main()
