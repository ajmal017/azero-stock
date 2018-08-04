from td_api import *
import pandas as pd
import json
import asyncio


def read_stock_codes():
    return pd.read_csv('stock_code.csv')


async def async_symbol_data(quote_api, symbols):
    loop = asyncio.get_event_loop()
    start_date = '1990-01-01'

    def _post(symbol, frequency):
        return symbol, frequency, quote_api.get_history_quotes(symbol, start_date=start_date, period=1,
                                                               period_type='day',
                                                               frequency_type='minute', frequency=frequency,
                                                               need_extended_hours_data=True)

    futures = [
        loop.run_in_executor(
            None,
            _post,
            symbol,
            frequency
        )
        for symbol in symbols for frequency in ['1', '5', '10', '15', '30']
    ]
    return [response for response in await asyncio.gather(*futures)]


def _filter_symbol_data(symbol_data):
    right = []
    wrong = []
    for data in symbol_data:
        if 'empty' in data and data['empty']:
            wrong.append(data)
        else:
            right.append(data)
    return right, wrong


def sync_symbol_data(quote_api, symbols):
    loop = asyncio.get_event_loop()
    symbol_data = async_symbol_data(quote_api, symbols)
    future = loop.run_until_complete(symbol_data)
    right, wrong = _filter_symbol_data(future)
    freq_list = ['1', '5', '10', '15', '30']
    for i, symbol in enumerate(symbols):
        for j, frequency in enumerate(freq_list):
            quotes = future[i * len(freq_list) + j]
            with open('data/%s_minute_%s.json' % (symbol, frequency), 'w') as f:
                json.dump(quotes, f)
            print('sync: %s, freq: %s done' % (symbol, frequency))


def main():
    api_key = 'HXSSG1124@AMER.OAUTHAP'
    quote_api = TDQuoteApi(api_key)
    stock_codes = read_stock_codes()
    start = 0
    parallel_cnt = 5
    while start < len(stock_codes.values):
        print(start, 'th stock start syncing.')
        end = min(start + parallel_cnt, len(stock_codes.values))
        codes = list(map(lambda x: ''.join(x[1][3:]), stock_codes.values[start: end]))
        sync_symbol_data(quote_api, codes)
        start += parallel_cnt
        if start == 200:
            break

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
