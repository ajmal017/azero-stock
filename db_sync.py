import futu_api as api
import datetime
from utils import *
from pymongo import MongoClient

logger = setup_logger('db_sync', 'db_sync.log')


def get_futu_mongo_1m_bson(data_row):
    return {
        'type': 1,
        'dt': int(datetime.datetime.strptime(data_row[1], "%Y-%m-%d %H:%M:%S").timestamp()),
        'open': data_row[2],
        'close': data_row[3],
        'high': data_row[4],
        'low': data_row[5],
        'volume': data_row[8],
        'pe': data_row[6],
    }


def get_futu_1m_data_for_symbol(symbol):
    now_date = datetime.datetime.today().strftime("%Y-%m-%d")
    hist_1m = api.get_history_kline(symbol, start='2000-01-01', end=now_date, ktype='K_1M')
    hist_1m_bson = list(map(get_futu_mongo_1m_bson, hist_1m[1].values))
    return hist_1m_bson


def sync_1m_data(symbol, collection, data_list, percentage):
    result = collection.insert_many(data_list)
    logger.info(symbol + ' has been synced: ' + str(len(result.inserted_ids)) + (' Progress: %.2f%%' % percentage))


def query_1m_data(symbol, collection):
    collection.remove()
    # for data in collection.find():
    #     print(data)
    # logger.info('done')


def main():
    basic_info = api.get_stock_basicinfo('US')
    client = MongoClient('10.140.0.2', 8081)
    db = client['azero-stock']
    total_cnt = len(basic_info[1].values[1:])
    logger.info('start')
    for i, value in enumerate(basic_info[1].values[1:]):
        symbol = value[0]
        logger.info('start syncing:' + symbol)
        data_list = get_futu_1m_data_for_symbol(symbol)
        collection = db[symbol]
        sync_1m_data(symbol, collection, data_list, (i + 1) * 100 / float(total_cnt))
        # query_1m_data(symbol, collection)


if __name__ == '__main__':
    main()
