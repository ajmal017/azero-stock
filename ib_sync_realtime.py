from ib_api import *
from ContractSamples import ContractSamples


def make_contract(symbol, exchange):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.primaryExchange = "ISLAND"
    return contract


def _handler(data):
    print(data)


def sync_real_time():
    app = IBApp("localhost", 4001, 40)
    app.req_market_depth(1000, make_contract('MSFT', 'ISLAND'))


if __name__ == '__main__':
    sync_real_time()
