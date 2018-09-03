import datetime
import sqlite3
import os
import pandas as pd
import logging
import decimal
from collections import defaultdict
from pymongo import MongoClient, DESCENDING, ASCENDING

decimal.getcontext().prec = 3

formatter = logging.Formatter('%(message)s')
TYPE_MAP = {
    '1': 1,
    '5': 2,
    '15': 3,
    '30': 4,
    '60': 5
}
STOCKID_MAP = {

}
STOCKID_REVERSE_MAP = {

}


def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


logger = setup_logger('db_sync', 'db_sync.log')


def get_futu_mongo_1m_bson(data_row, t):
    return {
        'type': t,
        'dt': int(datetime.datetime.strptime(data_row[2], "%Y-%m-%d %H:%M:%S").timestamp()),
        'open': float('%.3f' % float(data_row[3])),
        'close': float('%.3f' % float(data_row[4])),
        'high': float('%.3f' % float(data_row[5])),
        'low': float('%.3f' % float(data_row[6])),
        'volume': float(data_row[7]),
        'pe': float(data_row[9]),
    }


def sync_1m_data(symbol, collection, data_list, percentage):
    if not data_list:
        return
    result = collection.insert_many(data_list)
    logger.info(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ':' +
                symbol + ' has been synced: ' + str(len(result.inserted_ids)) + (' Progress: %.2f%%' % percentage))


def query_1m_data(symbol, collection):
    collection.remove()
    # for data in collection.find():
    #     print(data)
    # logger.info('done')


def main():
    client = MongoClient('127.0.0.1', 8081)
    basic_info = pd.read_csv('basic_info.csv')
    for s in basic_info.values:
        STOCKID_MAP[int(s[-1])] = s[1]
        STOCKID_REVERSE_MAP[s[1]] = int(s[-1])
    db = client['azero-stock']
    dirs = list(filter(lambda x: x.startswith('us_kl_min'), os.listdir('/mnt/disks/disk-1/sd/hist_us')))
    total = len(dirs)
    start = False
    start2 = False
    for i, file in enumerate(sorted(dirs, key=lambda x: (int(x.split('_')[2].replace('min', '')),
                                                         int(x.split('_')[3].replace('.db', ''))))):
        t = TYPE_MAP[file.split('_')[2].replace('min', '')]
        if file == 'us_kl_min5_6.db':
            start = True
        if not start:
            continue
        conn = sqlite3.connect('/mnt/disks/disk-1/sd/hist_us/%s' % file)
        stock_rows = [row for row in conn.execute('SELECT * FROM KLData')]
        conn.close()
        d = defaultdict(list)
        for row in stock_rows:
            if int(row[0]) in STOCKID_MAP:
                d[STOCKID_MAP[int(row[0])]].append(row)
        for k in d:
            if k == 'US.CHAP':
                start2 = True
                continue
            if not start2:
                continue
            d[k] = list(map(lambda x: get_futu_mongo_1m_bson(x, t), d[k]))
            sync_1m_data(k, db[k], d[k], float(i) * 100 / total)
            logger.info('%s~%s~%d~%d' % (file, k, STOCKID_REVERSE_MAP[k], len(d[k])))
        # stock_rows = { for stock_id}
        # futu_bson = list(map(get_futu_mongo_1m_bson, stock_rows))

    # logger.info('start')
    # start = False
    # for i, value in enumerate(basic_info[1].values[1:]):
    #     symbol = value[0]
    #     if symbol == 'US.AGD':
    #         start = True
    #     if not start:
    #         continue
    #     logger.info('start syncing:' + symbol)
    #     data_list = get_futu_1m_data_for_symbol(symbol)
    #     logger.info(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': futu data complete')
    #     collection = db[symbol]
    #     sync_1m_data(symbol, collection, data_list, (i + 1) * 100 / float(total_cnt))
    # query_1m_data(symbol, collection)
def int_2_date(int_date):
    return datetime.datetime.fromtimestamp(int_date).strftime('%Y-%m-%d %H:%M:%S')


def main2():
    client = MongoClient('10.140.0.2', 8081)
    db = client['azero-stock']
    print(db['US.AAPL'].find({'type': 1}).limit(1).next())
    # s = set()
    # for row in db[symbol].find({'type':31}).sort('dt'):
    #     print('%s~%s' % (int_2_date(int(row['dt'])), row))
    #     s.add(int_2_date(int(row['dt'])).split()[0])
    # for e in sorted(s):
    #     print(e)


if __name__ == '__main__':
    main2()
