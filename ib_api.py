from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import *
from ibapi.common import *
from threading import Thread
from ibapi.ticktype import *
from ib_utils import *
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
        if name not in self._queue_map:
            return None
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
        print(req_id, error_code, error_string)
        hist_q = self.get_queue('hist_%d' % req_id)
        if hist_q is not None:
            hist_q.put((req_id, 'error', error_code, error_string))

    def currentTime(self, time_from_server):
        self.get_queue('time').put(time_from_server)

    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        # print("TickSnapshotEnd:", reqId)

    def tickPrice(self, req_id: TickerId, tick_type: TickType, price: float, attrib: TickAttrib):
        super().tickPrice(req_id, tick_type, price, attrib)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt_%d' % req_id).put((req_id, 'tick_price', now_date, tick_type, price, attrib))

    def tickSize(self, req_id: TickerId, tick_type: TickType, size: int):
        super().tickSize(req_id, tick_type, size)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt_%d' % req_id).put((req_id, 'tick_size', now_date, tick_type, size))

    def tickString(self, req_id: TickerId, tick_type: TickType, value: str):
        super().tickString(req_id, tick_type, value)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt_%d' % req_id).put((req_id, 'tick_string', now_date, tick_type, value))

    def tickGeneric(self, req_id: TickerId, tick_type: TickType, value: float):
        super().tickGeneric(req_id, tick_type, value)
        now_date = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.get_queue('mkt_%d' % req_id).put((req_id, 'tick_generic', now_date, tick_type, value))

    def historicalData(self, reqId: int, bar: BarData):
        self.get_queue('hist_%d' % reqId).put((reqId, 'historical_data', bar))

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        self.get_queue('hist_%d' % reqId).put((reqId, 'historical_data_end', start, end))

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        self.get_queue('hist_%d' % reqId).put((reqId, 'historical_data_update', bar))

    def historicalTicks(self, req_id: int, ticks: ListOfHistoricalTick, done: bool):
        self.get_queue('hist_ticks_%d' % req_id).put((req_id, 'historical_ticks', ticks, done))

    def historicalTicksBidAsk(self, req_id: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        self.get_queue('hist_ticks_%d' % req_id).put((req_id, 'historical_ticks_bid_ask', ticks, done))

    def historicalTicksLast(self, req_id: int, ticks: ListOfHistoricalTickLast, done: bool):
        self.get_queue('hist_ticks_%d' % req_id).put((req_id, 'historical_ticks_last', ticks, done))

    def headTimestamp(self, reqId: int, headTimestamp: str):
        self.get_queue('head_time').put((reqId, headTimestamp))


class TestClient(EClient):
    MAX_WAIT_SECONDS = 30

    def __init__(self, wrapper):
        """
        The client method
        We don't override native methods, but instead call them from our own wrappers
        """
        EClient.__init__(self, wrapper)
        self._errors = queue.Queue()

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

    def req_market_data(self, req_id, contract, handler, generic_tick_list="", snapshot=False, regular_snapshot=False,
                        options=list()):
        print("Getting the stock data from the server... ")

        mkt_data = self.wrapper.init_queue('mkt_%d' % req_id)

        self.reqMktData(req_id, contract, generic_tick_list, snapshot, regular_snapshot, options)

        worker_thread = Thread(target=queue_consumer, args=(mkt_data, handler))
        worker_thread.daemon = True
        worker_thread.start()

    def req_historical_data(self, req_id, contract, query_time,
                            duration, bar_size_setting, what_to_know='TRADES',
                            use_rth=0, format_date=1, keep_up_to_date=False, chart_options=list()):
        print('getting history data')
        res = []

        hist_data = self.wrapper.init_queue('hist_%d' % req_id)

        self.reqHistoricalData(req_id, contract, query_time, duration, bar_size_setting, what_to_know,
                               use_rth, format_date, keep_up_to_date, chart_options)

        while True:
            data = hist_data.get()
            if data is None:
                continue
            if data[1] == 'historical_data':
                res.append(data)

            if data[1] == 'historical_data_end':
                res.append(data)
                break

            if data[1] == 'error' and data[2] != 2106:
                print(data)
                break
        return res

    def req_historical_ticks(self, req_id, contract, handler, start_date_time, end_date_time, number_of_ticks=1000,
                             what_to_know='TRADES', use_rth=0, ignore_size=True, misc_options=list()):

        print('getting historical ticks')

        hist_ticks = self.wrapper.init_queue('hist_ticks_%d' % req_id)

        self.reqHistoricalTicks(req_id, contract, start_date_time, end_date_time, number_of_ticks, what_to_know,
                                use_rth, ignore_size, misc_options)

        worker_thread = Thread(target=queue_consumer, args=(hist_ticks, handler))
        worker_thread.daemon = True
        worker_thread.start()

    def req_head_time_stamp(self, req_id, contract, what_to_know='TRADES', use_rth=0, format_date=1):

        head_time = self.wrapper.init_queue('head_time')

        self.reqHeadTimeStamp(req_id, contract, what_to_know, use_rth, format_date)

        try:
            head_time = head_time.get(timeout=self.MAX_WAIT_SECONDS)
        except queue.Empty:
            print("Exceeded maximum wait for wrapper to respond")
            head_time = None

        errors = []
        while self.wrapper.is_error():
            errors.append(self.wrapper.get_error())

        return head_time, errors


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
