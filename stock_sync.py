from td_api import *
import pandas as pd
import json
import asyncio
from queue import Queue
import time
import os


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
            print('')
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


def main():
    api_key = 'HXSSG1124@AMER.OAUTHAP'
    quote_api = TDQuoteApi(api_key)
    stock_codes = read_stock_codes()
    q = Queue()

    start = False
    for i, stock_code in enumerate(stock_codes.values):
        code = ''.join(stock_code[1][3:])
        if code == 'SLTB':
            start = True
        if not start:
            continue
        for freq in ['1', '5', '10', '15', '30']:
            q.put((code, freq, 1))
        break
        # if i == 100:
        #     break

    sync_symbol_data(quote_api, q)
    # while start < len(stock_codes.values):
    #     print(start, 'th stock start syncing.')
    #     end = min(start + parallel_cnt, len(stock_codes.values))
    #     codes = list(map(lambda x: ''.join(x[1][3:]), stock_codes.values[start: end]))
    #     sync_symbol_data(quote_api, codes)
    #     start += parallel_cnt
    #     if start == 200:
    #         break

    # print(stock_codes)
    # api_key = 'HXSSG1124@AMER.OAUTHAP'
    # quote_api = TDQuoteApi(api_key)
    # start_date = '2017-01-01'
    # quotes = quote_api.get_history_quotes('AAPL', start_date=start_date, period=1, period_type='day',
    #                                       frequency_type='minute', frequency='1', need_extended_hours_data=True)
    # if 'error' in quotes or not quotes:
    #     print(quotes)
    # else:
    #     candles = quotes['candles']
    #     print('\n'.join(list(map(lambda x: '%s,%s,%s' % (x['datetime'], x['close'], x['volume']), candles))))


if __name__ == '__main__':
    main()
