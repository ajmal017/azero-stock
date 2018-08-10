# Gist example of IB wrapper ...
#
# Download API from http://interactivebrokers.github.io/#
#
# Install python API code /IBJts/source/pythonclient $ python3 setup.py install
#
# Note: The test cases, and the documentation refer to a python package called IBApi,
#    but the actual package is called ibapi. Go figure.
#
# Get the latest version of the gateway:
# https://www.interactivebrokers.com/en/?f=%2Fen%2Fcontrol%2Fsystemstandalone-ibGateway.php%3Fos%3Dunix
#    (for unix: windows and mac users please find your own version)
#
# Run the gateway
#
# user: edemo
# pwd: demo123
#
# Now I'll try and replicate the time telling example


from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import *
from ibapi.common import *
from threading import Thread
from ibapi.ticktype import *
import datetime
from ContractSamples import ContractSamples

import queue


class TestWrapper(EWrapper):

    def __init__(self):
        """
        The wrapper deals with the action coming back from the IB gateway or TWS instance
        We override methods in EWrapper that will get called when this action happens, like currentTime
        """
        self._queue_map = {}
        self.init_queue('error')

    def init_queue(self, name):
        self._queue_map[name] = queue.Queue()
        return self._queue_map[name]

    def get_queue(self, name):
        return self._queue_map[name]

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self.get_queue('error').get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        return not self.get_queue('error').empty()

    def error(self, req_id, error_code, error_string):
        self.get_queue('error').put((req_id, error_code, error_string))

    def currentTime(self, time_from_server):
        self.get_queue('time').put(time_from_server)

    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        # print("TickSnapshotEnd:", reqId)

    def tickPrice(self, req_id: TickerId, tick_type: TickType, price: float, attrib: TickAttrib):
        super().tickPrice(req_id, tick_type, price, attrib)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt').put(('tick_price', req_id, now_date, tick_type, price, attrib))

    def tickSize(self, req_id: TickerId, tick_type: TickType, size: int):
        super().tickSize(req_id, tick_type, size)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt').put(('tick_size', req_id, now_date, tick_type, size))

    def tickString(self, req_id: TickerId, tick_type: TickType, value: str):
        super().tickString(req_id, tick_type, value)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt').put(('tick_string', req_id, now_date, tick_type, value))

    def tickGeneric(self, req_id: TickerId, tick_type: TickType, value: float):
        super().tickGeneric(req_id, tick_type, value)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt').put(('tick_generic', req_id, now_date, tick_type, value))

    def historicalData(self, reqId: int, bar: BarData):
        print("HistoricalData. ", reqId, " Date:", bar.date, "Open:", bar.open,
              "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
              "Count:", bar.barCount, "WAP:", bar.average)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd ", reqId, "from", start, "to", end)

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        print("HistoricalDataUpdate. ", reqId, " Date:", bar.date, "Open:", bar.open,
              "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
              "Count:", bar.barCount, "WAP:", bar.average)

    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTick, done: bool):
        for tick in ticks:
            print("Historical Tick. Req Id: ", reqId, ", time: ", tick.time,
                  ", price: ", tick.price, ", size: ", tick.size)

    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk,
                              done: bool):
        for tick in ticks:
            print("Historical Tick Bid/Ask. Req Id: ", reqId, ", time: ", tick.time,
                  ", bid price: ", tick.priceBid, ", ask price: ", tick.priceAsk,
                  ", bid size: ", tick.sizeBid, ", ask size: ", tick.sizeAsk)

    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        for tick in ticks:
            print("Historical Tick Last. Req Id: ", reqId, ", time: ", tick.time,
                  ", price: ", tick.price, ", size: ", tick.size, ", exchange: ", tick.exchange,
                  ", special conditions:", tick.specialConditions)

    def headTimestamp(self, reqId: int, headTimestamp: str):
        print("HeadTimestamp: ", reqId, " ", headTimestamp)


class TestClient(EClient):
    MAX_WAIT_SECONDS = 10

    def __init__(self, wrapper):
        """
        The client method
        We don't override native methods, but instead call them from our own wrappers
        """
        EClient.__init__(self, wrapper)
        self._errors = queue.Queue()

    def my_stock(self):
        # ! [stkcontract]

        contract = Contract()
        contract.symbol = "HUYA"
        contract.secType = "STK"
        contract.currency = "USD"
        #         #In the API side, NASDAQ is always defined as ISLAND in the exchange field
        contract.exchange = "ISLAND"
        # ! [stkcontract]
        return contract

    def req_cur_time(self):
        """
        Tell the Linux server time
        :return: unix time, as an int and error infos
        """

        # Make a place to store the time we're going to return
        time_storage = self.wrapper.init_queue('time')

        # This is the native method in EClient, asks the server to send us the time please
        self.reqCurrentTime()

        try:
            cur_time = time_storage.get(timeout=self.MAX_WAIT_SECONDS)
        except queue.Empty:
            print("Exceeded maximum wait for wrapper to respond")
            cur_time = None

        errors = []
        while self.wrapper.is_error():
            errors.append(self.wrapper.get_error())

        return cur_time, errors

    def req_market_data(self, contract, generic_tick_list="", snapshot=False, regular_snapshot=False, options=list()):
        print("Getting the stock data from the server... ")

        mkt_data = self.wrapper.init_queue('mkt')

        def _worker():
            while True:
                data = mkt_data.get()
                print(data)

        self.reqMktData(1000, contract, generic_tick_list, snapshot, regular_snapshot, options)

        t = Thread(target=_worker)
        t.daemon = True
        t.start()

    def get_hist(self):
        print('getting history data')

        queryTime = (datetime.datetime.today() -
                     datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")

        # self.reqHistoricalData(4101, ContractSamples.USStockAtSmart(), queryTime,
        #                          "1 M", "1 min", "Trades", 0, 1, False, [])

        self.reqHeadTimeStamp(4103, ContractSamples.USStockAtSmart(), "TRADES", 0, 1)

        # for i in range(10):
        #   self.reqHistoricalTicks(i + 100, ContractSamples.USStockAtSmart(),
        #                              "20180805 08:00:00", "", 1000, "TRADES", 1, True, [])

        print('request done')

        while self.wrapper.is_error():
            print(self.wrapper.get_error())


class IBApp(TestWrapper, TestClient):
    def __init__(self, host, port, client_id=2):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        print('connecting....')
        self.connect(host, port, client_id)
        print('connected')

        thread = Thread(target=self.run)
        thread.start()
        self._thread = thread


if __name__ == '__main__':
    app = IBApp("127.0.0.1", 4001)
    app.req_market_data(ContractSamples.USStockAtSmart(), generic_tick_list='233')
    # app.disconnect()
