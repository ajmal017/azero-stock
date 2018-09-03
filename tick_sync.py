import os
import json
import datetime
from utils import *
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

logger = setup_logger('td_db_sync', 'td_db_sync.log')

BASE_PATH = '/mnt/disks/azero-stock/azero/stock_data/data'
TYPE_MAP = {
    '1': '11',
    '5': '12',
    '10': '13',
    '15': '14',
    '30': '15',
}


def get_td_bson_data(candles, symbol_type):
    if not candles:
        return []
    return list(map(lambda x: {
        'type': symbol_type,
        'dt': int(datetime.datetime.strptime(x['datetime'], "%Y-%m-%d %H:%M:%S").timestamp()),
        'open': x['open'],
        'close': x['close'],
        'high': x['high'],
        'low': x['low'],
        'volume': x['volume'],
        'pe': 0.0,
    }, candles))


def sync_td_data(symbol, collection, bson_data, percentage):
    result = collection.insert_many(bson_data)
    logger.info(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' +
                symbol + ' has been synced: ' + str(len(result.inserted_ids)) + (' Progress: %.2f%%' % percentage))


def main():
    logger.info('start ib tick data sync')
    files = os.listdir(BASE_PATH)
    client = MongoClient('10.140.0.2', 8081)
    db = client['azero-stock']
    total_cnt = len(files)
    for i, file in enumerate(files):
        file_path = '%s/%s' % (BASE_PATH, file)
        symbol = 'US.%s' % file.split('_')[0]
        symbol_type = TYPE_MAP[file_path.split('_')[-1].replace('.json', '')]
        with open(file_path) as f:
            try:
                file_data = json.loads(''.join(f.readlines()), encoding='utf-8')
            except Exception:
                continue
        if 'candles' not in file_data:
            continue
        bson_data = get_td_bson_data(file_data['candles'], symbol_type)
        collection = db[symbol]
        sync_td_data(symbol, collection, bson_data, (i + 1) * 100 / float(total_cnt))
    print(len(files))


if __name__ == '__main__':
    main()
