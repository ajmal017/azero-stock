from flask import Blueprint

import futu_api as api
from utils import *

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


@market_app.route('/stream')
def stream():
    return Response('asd', mimetype="text/event-stream")
