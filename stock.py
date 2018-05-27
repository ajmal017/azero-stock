from flask import Flask, request

import futu_api as api
from utils import *

app = Flask(__name__)


@app.route("/stock/trading_days")
def get_trading_days():
    market = request.args.get('market').upper()
    status, content = api.get_trading_days(market)
    return parse_resp(status, content)


@app.route("/stock/stock_basicinfo")
def get_stock_basicinfo():
    market = request.args.get('market').upper()
    stock_type = request.args.get('stock_type')
    status, content = api.get_stock_basicinfo(market, stock_type=stock_type)
    return parse_resp(status, content)


@app.route('/stock/history_kline')
def get_history_kline():
    code = request.args.get('code')
    start = request.args.get('start')
    end = request.args.get('end')
    ktype = request.args.get('ktype')
    autype = request.args.get('autype')
    status, content = api.get_history_kline(code, start, end, ktype, autype)
    return parse_resp(status, content)


if __name__ == "__main__":
    # kline_res = api.get_history_kline('US.JD', ktype='K_15M', start='2018-02-01', end='2018-05-25')
    # # print(kline_res)
    # kline_pd = kline_res[1]
    # keys = kline_pd.keys().values
    # kline = kline_pd.values
    #
    #
    # # kline = np.array(sorted(kline, key=lambda ele: ele[-1], reverse=True))
    # # print('\t'.join(keys))
    # # for row in kline:
    # #     for e in row:
    # #         print(e, end='\t')
    # #     print()
    #
    # def map_str(s):
    #     s1, s2 = s.split()
    #     return int('%s%s%s' % (s1.split('-')[-1], s2.split(':')[0], s2.split(':')[1]))
    #
    #
    # n = len(kline)
    # x = np.array(list(range(n)))
    # y1 = np.array(kline[:, -1], dtype=np.float32)
    # max_price = np.max(kline[:, 2])
    # min_price = np.min(kline[:, 2])
    # avg_price = float((max_price + min_price) / 2)
    # print(max_price)
    # print(min_price)
    # print(avg_price)
    #
    #
    # def _get_index(e1):
    #     res = list(filter(lambda e2: (e2[1] - e1[1]) / e1[1] >= 0.3,
    #                       [(i, e0) for i, e0 in enumerate(list(kline[:, 2]))][e1[0] + 1:]))
    #     return res[0][0] if res else -1
    #
    #
    # days = list(map(lambda e1: (e1[1], _get_index(e1), e1[2]),
    #                 [(i, e0, kline[i, 1]) for i, e0 in enumerate(list(kline[:, 2]))]))
    # sort_days = sorted(days, key=lambda e1: e1[1])
    # for eday in days:
    #     print(eday)
    # print('positive %d' % len(list(filter(lambda e1: e1[1] > 0, days))))
    # print('negative %d' % len(list(filter(lambda e1: e1[1] < 0, days))))
    # print('postive > avg price %f' % ((len(list(filter(lambda e1: e1[0] < avg_price and e1[1] > 0, days)))) /
    #                                   (len(list(filter(lambda e1: e1[0] < avg_price, days))))))
    # print('-------------------------')
    # for eday in list(filter(lambda e1: e1[0] < avg_price and e1[1] < 0, days)):
    #     print(eday)
    # x = list(map(lambda e1: e1[1], sort_days))
    # y = list(map(lambda e1: (e1[2], e1[0]), sort_days))
    # plt.figure(dpi=100)
    # plt.grid()
    # plt.plot(x, y, linewidth=1.0)
    # plt.show()

    # plt.figure(dpi=100)
    # plt.grid()
    # # fig, (ax1) = plt.subplots(1, 1, sharex=True)
    # plt.plot(x, y1, linewidth=1.0)
    # plt.fill_between(x, 0, y1)
    # # plt.savefig('test.png', dpi=300)
    # plt.show()
    # print(sum(y1) / (sum(list(map(abs, y1)))))
    cfg = get_config('server')
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(debug=True, host=cfg['ServerHost'], port=int(cfg['ServerPort']))
