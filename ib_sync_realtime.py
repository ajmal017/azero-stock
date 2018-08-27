from ib_api import *
from utils import *
from ContractSamples import ContractSamples

REQ_ID_TO_SYMBOL = {}
FILES = {}


def make_contract(symbol, exchange):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.primaryExchange = "ISLAND"
    return contract


def get_usd_contract():
    contract = Contract()
    contract.symbol = 'USD'
    contract.secType = "CASH"
    contract.currency = "CNH"
    contract.exchange = 'IDEALPRO'
    return contract


def _handler(data):
    symbol = REQ_ID_TO_SYMBOL[data[0]]
    file = FILES[symbol]
    file.info('~'.join(map(str, data[2:])))


def read_stock_contracts():
    with open('stock_sync_codes.txt') as f:
        symbols = list(map(lambda x: x.strip().replace('US.', ''), f.readlines()))

    codes = [make_contract(stock_code, 'SMART')
             for i, stock_code in enumerate(symbols)]
    codes.append(get_usd_contract())
    return codes


def sync_real_time():
    app = IBApp("localhost", 4001, 40)
    date = datetime.datetime.now().strftime('%Y%m%d')

    contracts = read_stock_contracts()
    for i, contract in enumerate(contracts):
        file_name = '%s_%s_real.log' % (contract.symbol, date)
        FILES[contract.symbol] = setup_logger(file_name, '%s/ib_realtime/%s' % (STORE_PATH, file_name))
        REQ_ID_TO_SYMBOL[1000 + i] = contract.symbol
        app.req_market_data(1000 + i, contract, _handler)


if __name__ == '__main__':
    sync_real_time()
