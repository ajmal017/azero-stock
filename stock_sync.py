from td_api import *
import pandas as pd
import json
import asyncio
from queue import Queue
import os
import sys
import datetime
import logging
import pytz
formatter = logging.Formatter('%(message)s')


def read_stock_codes():
    return pd.read_csv('stock_code.csv')


async def async_symbol_data(quote_api, symbol_queue, parallel_cnt=25):
    loop = asyncio.get_event_loop()
    start_date = '1990-01-01'

    def _post(symbol, frequency, times):
        return symbol, frequency, quote_api.get_history_quotes(symbol, start_date=start_date, period=1,
                                                               period_type='day',
                                                               frequency_type='minute', frequency=frequency,
                                                               need_extended_hours_data=True), times + 1

    symbols = []
    for _ in range(parallel_cnt):
        if symbol_queue.empty():
            break
        symbols.append(symbol_queue.get())

    print('syncing: ', symbols)
    futures = [
        loop.run_in_executor(
            None,
            _post,
            symbol,
            frequency,
            times
        )
        for symbol, frequency, times in symbols
    ]
    return [response for response in await asyncio.gather(*futures)]


def _filter_symbol_data(symbol_data):
    right = []
    wrong = []
    for data in symbol_data:
        if 'empty' in data[2] and data[2]['empty']:
            wrong.append(data)
        else:
            right.append(data)
    return right, wrong


def update_symbol_data(symbol, frequency, quotes):
    file_name = 'data/%s_minute_%s.json' % (symbol, frequency)
    file_data = None
    if os.path.exists(file_name):
        with open('data/%s_minute_%s.json' % (symbol, frequency)) as f:
            file_data = json.loads(''.join(f.readlines()), encoding='utf-8')
        if quotes and 'candles' in quotes:
            file_candles_datetime = set([e['datetime'] for e in file_data['candles']])
            new_candles = [e for e in quotes['candles'] if e['datetime'] not in file_candles_datetime]
            file_data['candles'] += new_candles
    with open('data/%s_minute_%s.json' % (symbol, frequency), 'w') as f:
        json.dump(file_data if file_data else quotes, f)
    print('sync: %s, freq: %s done' % (symbol, frequency))


def sync_symbol_data(quote_api, symbol_queue, parallel_cnt=25):
    total_cnt = symbol_queue.qsize()
    while not symbol_queue.empty():
        loop = asyncio.get_event_loop()
        symbol_data = async_symbol_data(quote_api, symbol_queue, parallel_cnt=parallel_cnt)
        future = loop.run_until_complete(symbol_data)
        right, wrong = _filter_symbol_data(future)

        # Put wrong symbol data to the queue
        for symbol, freq, _, times in wrong:
            if times == 3:
                continue
            symbol_queue.put((symbol, freq, times))

        for symbol, frequency, quotes, _ in right:
            update_symbol_data(symbol, frequency, quotes)

        print('Current Progress: %.2f%%' % ((1 - symbol_queue.qsize() / float(total_cnt)) * 100))

def sync_minute_data():
    api_key = 'HXSSG1124@AMER.OAUTHAP'
    quote_api = TDQuoteApi(api_key)
    stock_codes = read_stock_codes()
    q = Queue()

    start = False
    for i, stock_code in enumerate(stock_codes.values):
        code = ''.join(stock_code[1][3:])
        if not start:
            continue
        for freq in ['1', '5', '10', '15', '30']:
            q.put((code, freq, 1))

    sync_symbol_data(quote_api, q)


def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def sync_futu_premarket():
    import futu_api as api

    with open('stock_sync_codes.txt') as f:
        symbols = list(map(lambda x: x.strip(), f.readlines()))

    loggers = {
        symbol: setup_logger('%s_order_book' % symbol, 'order_book/%s_order_book.log' % symbol)
        for symbol in symbols
    }

    def _handle_order_book(param):
        currentDT = datetime.datetime.now(timezone('America/New_York'))
        time = currentDT.strftime("%Y-%m-%d %H:%M:%S")
        loggers[param['stock_code']].info('%s, %s~%s' % (str(time), param['Ask'][0], param['Bid'][0]))

    for code in symbols:
        api.subscribe(code, 'ORDER_BOOK', True)
        api.get_order_book(code, _handle_order_book)
    api.start()


def sync_rt_data():
    import futu_api as api

    with open('stock_sync_codes.txt') as f:
        symbols = list(map(lambda x: x.strip(), f.readlines()))

    loggers = {
        symbol: setup_logger('%s_rt_data' % symbol, 'rt_data/%s_rt_data.log' % symbol)
        for symbol in symbols
    }

    def _handle_rt_data(param):
        currentDT = datetime.datetime.now(timezone('America/New_York'))
        time = currentDT.strftime("%Y-%m-%d %H:%M:%S")
        symbol = param.values[0][0].replace('US.', '').strip()
        dt = param.values[0][1]
        status = param.values[0][2]
        cur_price = param.values[0][4]
        last_close = param.values[0][5]
        avg_price = param.values[0][6]
        turn_over = param.values[0][7]
        volume = param.values[0][8]
        loggers[symbol].info('%s, %s~%s~%s~%s~%s~%s~%s' % (time, dt, status, cur_price, last_close,
                                                           avg_price, turn_over, volume))

    for code in symbols:
        api.subscribe(code, 'RT_DATA', True)
        api.get_rt_data(code, _handle_rt_data)
    api.start()



def main():
    if len(sys.argv) < 2:
        raise RuntimeError('Must specify sync option')
    option = sys.argv[1]
    if option == 'sync_minute':
        sync_minute_data()
    elif option == 'sync_futu_premarket':
        sync_futu_premarket()
    elif option == 'sync_rt_data':
        sync_rt_data()
    else:
        raise RuntimeError('Option(sync_minute or sync_futu_premarket)')


if __name__ == '__main__':
    main()
