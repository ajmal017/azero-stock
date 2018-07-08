from flask import Blueprint

import futu_api as api
from utils import *
from queue import Queue

market_app = Blueprint('market_app', __name__)


@market_app.route("/stock/trading_days")
@gzipped
def get_trading_days():
    market = request.args.get('market').upper()
    status, content = api.get_trading_days(market)
    return parse_resp(status, content)


@market_app.route("/stock/stock_basicinfo")
@gzipped
def get_stock_basicinfo():
    market = request.args.get('market').upper()
    stock_type = request.args.get('stock_type')
    status, content = api.get_stock_basicinfo(market, stock_type=stock_type)
    return parse_resp(status, content)


@market_app.route('/stock/history_kline')
@gzipped
def get_history_kline():
    code = request.args.get('code')
    start = request.args.get('start')
    end = request.args.get('end')
    ktype = request.args.get('ktype')
    autype = request.args.get('autype')
    status, content = api.get_history_kline(code, start, end, ktype, autype)
    return parse_resp(status, content)


queue = Queue()


def get_cur_kline_handler(data):
    queue.put(data.to_json(orient='records'))


def get_cur_kline_stream():
    while True:
        message = queue.get(True)
        print("Sending {}".format(message))
        yield "data: {}\n\n".format(message)


@market_app.route('/stock/cur_kline_stream')
def stream():
    code = request.args.get('code')
    ktype = request.args.get('ktype')
    api.subscribe(code, ktype, True)
    api.get_cur_kline(code, 1000, ktype=ktype, async_handler=get_cur_kline_handler)
    api.start()
    return Response(get_cur_kline_stream(), mimetype="text/event-stream")
