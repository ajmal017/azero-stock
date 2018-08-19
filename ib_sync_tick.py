import glob
import pandas as pd

from pytz import timezone
from ib_api import *
from utils import *
from collections import defaultdict
from dateutil import relativedelta

logger = setup_logger('ib_sync_log', 'ib_sync.log')


def convert_time_int_to_dt(time_int):
    return datetime.datetime.utcfromtimestamp(time_int).astimezone(timezone('America/New_York')) \
        .strftime('%Y%m%d %H:%M:%S')


def earlier_day(dt, days=1, hms='23:59:59'):
    return (datetime.datetime.strptime(dt, '%Y%m%d %H:%M:%S') - relativedelta
            .relativedelta(days=days)).strftime("%Y%m%d " + hms)


def make_contract(symbol, exchange):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.primaryExchange = "ISLAND"
    return contract


def _map_line(line):
    line = str(line)
    return '%s~%s~%s\n' % (convert_time_int_to_dt(int(line.split(',')[0])), '~'.join(line.split(',')[1:-1]),
                           line.split(',')[-1].strip())


def read_stock_contracts():
    with open('stock_sync_codes.txt') as f:
        symbols = list(map(lambda x: x.strip().replace('US.', ''), f.readlines()))

    codes = [make_contract(stock_code, 'SMART')
             for i, stock_code in enumerate(symbols)]
    return codes


def earliest_dt_for_symbol(symbol):
    earliest_file = sorted(glob.glob('ib_tick/%s*_tick.log' % symbol))
    if not earliest_file:
        return None
    return '%s 00:00:00' % earliest_file[0].split('_')[2]


def main():
    contracts = read_stock_contracts()
    app = IBApp("localhost", 4001, 30)
    logger.info('start syncing....')

    dt_map = {}
    contract_map = defaultdict(list)
    req_id_to_contract = {}

    for i, contract in enumerate(contracts):
        dt_default = earliest_dt_for_symbol(contract.symbol)
        if dt_default is None:
            dt_default = datetime.datetime.today().strftime("%Y%m%d 00:00:00")
            earliest_date = earlier_day(datetime.datetime.today().strftime("%Y%m%d 00:00:00"), days=5)
        else:
            earliest_date = earlier_day(datetime.datetime.today().strftime("%Y%m%d 00:00:00"), days=4, hms='00:00:00')

        last_dt = None
        while True:
            contract = make_contract(contract.symbol, 'SMART')
            client_id = int(app.clientId)
            req_id_to_contract[client_id + i] = contract.symbol

            dt = dt_map.get(contract.symbol, dt_default)
            if dt.split()[0] <= earliest_date:
                if contract_map[contract.symbol]:
                    with open('ib_tick/%s_%s_tick.log' % (contract.symbol, last_dt.split()[0]), 'w') as f:
                        contract_data = list(map(_map_line, contract_map[contract.symbol]))
                        f.writelines(contract_data)
                    print('syncing %s' % contract.symbol)
                contract_map[contract.symbol].clear()
                break

            hist_tick = app.req_historical_ticks(client_id + i, contract, '', dt, 1000)

            hist_tick_data = hist_tick.get()
            hist_tick_req_id = hist_tick_data[0]
            hist_tick_symbol = req_id_to_contract[hist_tick_req_id]
            if hist_tick_data[2]:
                hist_tick_date = convert_time_int_to_dt(hist_tick_data[2][0].time).split()[0]
                if last_dt and hist_tick_date != last_dt:
                    with open('ib_tick/%s_%s_tick.log' % (contract.symbol, last_dt.split()[0]), 'w') as f:
                        contract_data = list(map(_map_line, contract_map[contract.symbol]))
                        f.writelines(contract_data)
                    print('syncing %s' % contract.symbol)
                    contract_map[contract.symbol].clear()

                dt_map[hist_tick_symbol] = convert_time_int_to_dt(hist_tick_data[2][0].time)
                contract_map[hist_tick_symbol] = hist_tick_data[2] + contract_map[hist_tick_symbol]
                last_dt = hist_tick_date
            else:
                dt_map[hist_tick_symbol] = earlier_day(dt_map[hist_tick_symbol])

            currentDT = datetime.datetime.now(timezone('America/New_York'))
            time = currentDT.strftime("%Y-%m-%d %H:%M:%S")
            print('synced %s~%s~%s' % (hist_tick_symbol, time, dt_map[hist_tick_symbol]))


if __name__ == '__main__':
    main()
