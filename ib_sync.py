from ib_api import *
from utils import *
from dateutil import relativedelta


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


def sync_stock(app, contract):
    client_id = app.clientId
    symbol = contract.symbol
    logger_map = {}
    dt = datetime.datetime.today().strftime("%Y%m%d 00:00:00")
    while True:
        hist_data = app.req_historical_data(client_id, contract, dt,
                                            "2 M", "1 min")
        for data in hist_data:
            if data[1] == 'historical_data':
                bar = data[2]
                sd = bar.date.split()[0]
                if '%s_%s' % (symbol, sd) not in logger_map:
                    logger_map['%s_%s' % (symbol, sd)] = setup_logger('%s_%s_1M' % (symbol, sd), 'ib_data/%s_%s_1M.log'
                                                                      % (symbol, sd))
                ib_logger = logger_map['%s_%s' % (symbol, sd)]
                ib_logger.info('%s~%s~%s~%s~%s~%s' % (
                    bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume))
            elif data[1] == 'historical_data_end':
                dt = data[2]

        client_id += 1
        if not hist_data:
            app.disconnect()
            break


def main():
    app = IBApp("localhost", 4001, 2)
    sync_stock(app, make_contract('MSFT', 'ISLAND'))


if __name__ == '__main__':
    main()
