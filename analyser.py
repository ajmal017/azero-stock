from pytz import timezone
from matplotlib import pyplot as plt
from matplotlib.dates import WeekdayLocator, MONDAY, DayLocator, DateFormatter, date2num
from mpl_finance import candlestick_ohlc
from dateutil import parser

import time
import datetime
import futu_api as api
import pandas as pd
import numpy as np
import argparse


class StockAnalyser(object):
    TURING_POINT_PRICE_PERCENT_THRESHOLD = 0.99

    def __init__(self) -> None:
        self.rt_data = []
        self.low = 9999999.0
        self.high = 0
        self.open = -1
        self.close = -1
        self.yesterday_close = -1

    def _get_stock_kline(self, stock, start, end, ktype='K_DAY', autype='qfq'):
        res = api.get_history_kline(stock, start, end, ktype, autype)
        if not res or res[0] != 0:
            raise RuntimeError('Get stock kline error: %s' % res[1])
        return res[1].set_index(pd.DatetimeIndex(res[1].values[:, 1]))

    def _get_stocks_kline(self, args):
        result = map(lambda e: self._get_stock_kline(e, args.start, args.end, args.ktype, args.autype),
                     args.stocks.split(','))
        return list(filter(lambda e: len(e.values) > 0, result))

    def pandas_candlestick_ohlc(self, dat, stick="day", otherseries=None):
        """
        :param dat: pandas DataFrame object with datetime64 index, and float columns "Open", "High", "Low", and "Close",
                    likely created via DataReader from "yahoo"
        :param stick: A string or number indicating the period of time covered by a single candlestick. Valid string
                    inputs include "day", "week", "month", and "year", ("day" default), and any numeric input indicates
                    the number of trading days included in a period
        :param otherseries: An iterable that will be coerced into a list, containing the columns of dat that hold other
                    series to be plotted as lines

        This will show a Japanese candlestick plot for stock data stored in dat, also plotting other series if passed.
        """
        mondays = WeekdayLocator(MONDAY)  # major ticks on the mondays
        alldays = DayLocator()  # minor ticks on the days
        dayFormatter = DateFormatter('%d')  # e.g., 12

        # Create a new DataFrame which includes OHLC data for each period specified by stick input
        transdat = dat.loc[:, ["open", "high", "low", "close"]]
        plotdat = None
        if type(stick) == str:
            if stick == "day":
                plotdat = transdat
                stick = 1  # Used for plotting
            elif stick in ["week", "month", "year"]:
                if stick == "week":
                    transdat["week"] = pd.to_datetime(transdat.index).map(
                        lambda x: x.isocalendar()[1])  # Identify weeks
                elif stick == "month":
                    transdat["month"] = pd.to_datetime(transdat.index).map(lambda x: x.month)  # Identify months
                transdat["year"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[0])  # Identify years
                grouped = transdat.groupby(list({"year", stick}))  # Group by year and other appropriate variable
                plotdat = pd.DataFrame({"open": [], "high": [], "low": [],
                                        "close": []})  # Create empty data frame containing what will be plotted
                for name, group in grouped:
                    plotdat = plotdat.append(pd.DataFrame({"open": group.iloc[0, 0],
                                                           "high": max(group.High),
                                                           "low": min(group.Low),
                                                           "close": group.iloc[-1, 3]},
                                                          index=[group.index[0]]))
                if stick == "week":
                    stick = 5
                elif stick == "month":
                    stick = 30
                elif stick == "year":
                    stick = 365

        elif type(stick) == int and stick >= 1:
            transdat["stick"] = [np.floor(i / stick) for i in range(len(transdat.index))]
            grouped = transdat.groupby("stick")
            plotdat = pd.DataFrame({"open": [], "high": [], "low": [],
                                    "close": []})  # Create empty data frame containing what will be plotted
            for name, group in grouped:
                plotdat = plotdat.append(pd.DataFrame({"open": group.iloc[0, 0],
                                                       "high": max(group.High),
                                                       "low": min(group.Low),
                                                       "close": group.iloc[-1, 3]},
                                                      index=[group.index[0]]))

        else:
            raise ValueError(
                'Valid inputs to argument "stick" include the strings "day", "week", "month", "year", or a '
                'positive integer')

        # Set plot parameters, including the axis object ax used for plotting
        fig, ax = plt.subplots()
        fig.subplots_adjust(bottom=0.2)
        # fig.set_size_inches(38.4, 21.6)
        if plotdat.index[-1] - plotdat.index[0] < pd.Timedelta('730 days'):
            weekFormatter = DateFormatter('%b %d')  # e.g., Jan 12
            ax.xaxis.set_major_locator(mondays)
            ax.xaxis.set_minor_locator(alldays)
        else:
            weekFormatter = DateFormatter('%b %d, %Y')
        ax.xaxis.set_major_formatter(weekFormatter)

        ax.grid(True)

        # Create the candelstick chart
        candlestick_ohlc(ax, list(
            zip(list(date2num(plotdat.index.tolist())), plotdat["open"].tolist(), plotdat["high"].tolist(),
                plotdat["low"].tolist(), plotdat["close"].tolist())),
                         colorup="red", colordown="green", width=stick * .4)

        # Plot other series (such as moving averages) as lines
        if otherseries is not None:
            if type(otherseries) != list:
                otherseries = [otherseries]
            dat.loc[:, otherseries].plot(ax=ax, lw=1.3, grid=True)

        ax.xaxis_date()
        plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
        plt.savefig('b.png', dpi=200)

    def _get_turning_points(self, prices, level=8):
        if len(prices) == 0:
            return []

        # stock_graph = prices.plot(y='open', title='level:%d stock price' % level,
        #                           grid=True)
        # fig = stock_graph.get_figure()
        # fig.savefig('%d.png' % level, dpi=200)

        # First find local min or max price points
        local_opt_points = [(i, prices[i] < prices[i + 1] and prices[i] < prices[i - 1])
                            for i in range(1, len(prices) - 1)
                            if (prices[i] < prices[i + 1] and prices[i] < prices[i - 1])
                            or (prices[i] > prices[i + 1] and prices[i] > prices[i - 1])]

        # Second, obtaining the higher level TPs
        def _is_p1_and_p2_less_important(p0, p1, p2, p3, is_p0_min, is_p1_min, is_p2_min, is_p3_min):
            return (not is_p0_min and is_p1_min and not is_p2_min and is_p3_min and
                    (p0 > p2 and p1 > p3 and abs(p2 - p1) < (abs(p2 - p0) + abs(p1 - p3))) or
                    (is_p0_min and not is_p1_min and is_p2_min and not is_p3_min and
                     (p0 < p2 and p1 < p3 and abs(p2 - p1) < (abs(p2 - p0) + abs(p1 - p3)))) or
                    (is_p0_min and not is_p1_min and is_p2_min and not is_p3_min and
                     (min(p1, p3) / max(p1, p3) > self.TURING_POINT_PRICE_PERCENT_THRESHOLD) and
                     (min(p0, p2) / max(p0, p2) > self.TURING_POINT_PRICE_PERCENT_THRESHOLD)) or
                    (not is_p0_min and is_p1_min and not is_p2_min and is_p3_min and
                     min(p0, p2) / max(p0, p2) > self.TURING_POINT_PRICE_PERCENT_THRESHOLD) and
                    (min(p1, p3) / max(p1, p3) > self.TURING_POINT_PRICE_PERCENT_THRESHOLD))

        res = []
        i = 0
        res.clear()
        removed_points = []
        while i < len(local_opt_points) - 3:
            if _is_p1_and_p2_less_important(prices[local_opt_points[i][0]], prices[local_opt_points[i + 1][0]],
                                            prices[local_opt_points[i + 2][0]], prices[local_opt_points[i + 3][0]],
                                            local_opt_points[i][1], local_opt_points[i + 1][1],
                                            local_opt_points[i + 2][1], local_opt_points[i + 3][1]):
                res.append(local_opt_points[i])
                res.append(local_opt_points[i + 3])
                removed_points.append(local_opt_points[i + 1][0])
                removed_points.append(local_opt_points[i + 2][0])
                i += 3
            else:
                res.append(local_opt_points[i])
                i += 1
        local_opt_points = list(res)
        print(local_opt_points)
        res = prices[list(map(lambda x: x[0], res))]
        return res if level == 0 else self._get_turning_points(prices.drop(prices[removed_points].index), level - 1)

    def _draw_stock(self, stock):
        if len(stock.values) == 0:
            return

        start = stock['time_key'][0]
        end = stock['time_key'][-1]
        code = stock['code'][0]
        open_price = stock['open']
        turning_open_price = self._get_turning_points(open_price)
        turning_open_price_index_set = set(turning_open_price.index.values)
        turning_point_index = [i for i, e in enumerate(open_price.index.values) if e in turning_open_price_index_set]
        print(turning_open_price)
        stock_graph = stock.plot(y='open', title='%s~%s~%s stock price' % (code, str(start), str(end)),
                                 grid=True, marker='o', markerfacecolor='red',
                                 markevery=turning_point_index)
        fig = stock_graph.get_figure()
        fig.savefig('%s~%s~%s.png' % (code, str(start), str(end)), dpi=200)

    def draw_stocks(self, args):
        start = parser.parse('2018-04-11')
        while start != parser.parse('2018-06-02'):
            end = start + datetime.timedelta(days=1)
            stock = self._get_stock_kline('US.MOMO', start.strftime('%Y-%m-%d'), start.strftime('%Y-%m-%d'), 'K_1M')
            self._draw_stock(stock)
            start = end
        # stocks = self._get_stocks_kline(args)
        # self.pandas_candlestick_ohlc(stocks[0])

    def get_realtime_stock_data(self, args):
        def _data_notify(resp):
            # close = resp['close'][0]
            # volume = float(resp['volume'][0])
            pass

        def _handle_K_1M_data(resp):
            if self.low == 9999999 or self.high == 0:
                return

            close = resp['close'][0]
            volume = float(resp['volume'][0])
            tz = timezone('EST')
            cur_datetime = datetime.datetime.now(tz)
            self.rt_data.append((time.time(), float('%.3f' % close), float('%.3f' % volume)))
            rt_data_p = list(map(lambda x: x[1], self.rt_data))
            rt_data_v = list(map(lambda x: x[2], self.rt_data))
            prev_percent = (close / rt_data_p[-2] - 1) if len(self.rt_data) > 1 else 0
            cur_time = cur_datetime.strftime('%H:%M:%S')
            move_avg = sum(rt_data_p[-50:]) / min(50, len(rt_data_p)) if rt_data_p else 0
            move_avg_volume = sum(rt_data_v[-50:]) / min(50, len(rt_data_v)) if rt_data_v else 0
            print('\n%s min:%.3f(+%.4f%%), max:%f(-%.4f%%), open:%.3f(%s%.4f%%), '
                  'today:(%s%.4f%%), cur:%.3f\nmove_avg:%.3f(%s%.4f%%), prev:%s%.4f, volume:%.3f(%s%.4f%%)' % (
                      cur_time,
                      self.low, (close / self.low - 1) * 100,
                      self.high, (self.high / close - 1) * 100,
                      self.open,
                      '+' if close > self.open != -1 else '',
                      (close / self.open - 1) * 100 if self.open != -1 else 0,
                      '+' if close > self.yesterday_close != -1 else '',
                      (close / self.yesterday_close - 1) * 100 if self.yesterday_close != -1 else 0,
                      move_avg,
                      close, '+' if close > move_avg > 0 else '',
                      (close / move_avg - 1) * 100 if move_avg > 0 else 0,
                      '+' if prev_percent > 0 else '', prev_percent,
                      volume, '+' if volume > move_avg_volume > 0 else '',
                      (volume / move_avg_volume - 1) * 100
                  ))
            _data_notify(resp)

        def _handle_K_DAY_data(resp):
            self.high = max(self.high, resp['high'][0])
            self.low = min(self.low, resp['low'][0])
            self.open = resp['open'][0]
            self.close = resp['close'][0]

        def _handle_data(resp):
            k_type = resp['k_type'][0]
            if k_type == 'K_1M':
                _handle_K_1M_data(resp)
            elif k_type == 'K_DAY':
                _handle_K_DAY_data(resp)

        api.subscribe(args.stocks, args.ktype, True)
        api.subscribe(args.stocks, 'K_DAY', True)
        cur_data = api.get_cur_kline(args.stocks, 2)
        self.yesterday_close = cur_data[1]['close'][0]
        api.get_cur_kline(args.stocks, 1, ktype=args.ktype, async_handler=_handle_data)
        api.get_cur_kline(args.stocks, 1, ktype='K_DAY', async_handler=_handle_data)
        api.start()

    def dispatch(self):
        parser = argparse.ArgumentParser(description='Stock analyser argument parser.')
        parser.add_argument("-command", "--command", type=str, required=True)
        parser.add_argument("-stocks", "--stocks", type=str, required=True)
        parser.add_argument("-start", "--start", type=str, required=False)
        parser.add_argument("-end", "--end", type=str, required=False)
        parser.add_argument("-ktype", "--ktype", type=str, required=False)
        parser.add_argument("-autype", "--autype", type=str, required=False)
        args = parser.parse_args()
        getattr(self, args.command)(args)
        print('%s done' % args.command)


if __name__ == '__main__':
    StockAnalyser().dispatch()
