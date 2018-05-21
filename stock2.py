import futuquant as ft
import numpy as np
import matplotlib.pyplot as plt


def main():
    quote_ctx = ft.OpenQuoteContext(host="localhost", port=11111)
    stock_type = 'K_15M'
    num = 500
    print(quote_ctx.subscribe('US.HUYA', stock_type))
    print(quote_ctx.subscribe('US.BILI', stock_type))
    print(quote_ctx.subscribe('US.IQ', stock_type))
    print(quote_ctx.subscribe('US.GOOG', stock_type))
    print(quote_ctx.subscribe('US.MSFT', stock_type))
    #kline_res = quote_ctx.get_cur_kline('US.HUYA', num, ktype=stock_type, autype='qfq')
    kline_res2 = quote_ctx.get_cur_kline('US.BILI', num, ktype=stock_type, autype='qfq')
    kline_res3 = quote_ctx.get_cur_kline('US.IQ', num, ktype=stock_type, autype='qfq')
    kline_res4 = quote_ctx.get_cur_kline('US.GOOG', num, ktype=stock_type, autype='qfq')
    kline_res5 = quote_ctx.get_cur_kline('US.MSFT', num, ktype=stock_type, autype='qfq')
    #kline_pd = kline_res[1]
    kline_pd2 = kline_res2[1]
    kline_pd3 = kline_res3[1]
    kline_pd4 = kline_res4[1]
    kline_pd5 = kline_res5[1]
    keys = kline_pd2.keys().values
    #kline = kline_pd.values
    kline2 = kline_pd2.values
    kline3 = kline_pd3.values
    kline4 = kline_pd4.values
    kline5 = kline_pd5.values
    print('\t'.join(keys))
    for row in kline2:
        for e in row:
            print(e, end='\t')
        print()

    def map_str(s):
        s1, s2 = s.split()
        return int('%s%s%s' % (s1.split('-')[-1], s2.split(':')[0], s2.split(':')[1]))

    x = range(len(list(kline2[:, 1])))
    #y1 = kline[:, 2]
    y2 = kline2[:, 2]
    y3 = kline3[:, 2]
    y4 = kline4[:, 2]
    y5 = kline5[:, 2]
    # plt.plot(x, (y1 - y1[0]) / y1, linewidth=1.0)
    plt.plot(x, (y2 - y2[0]) / y2, 'r', linewidth=1.0)
    # plt.plot(x, (y3 - y3[0]) / y3, 'g', linewidth=1.0)
    plt.plot(x, (y4 - y4[0]) / y4, 'b', linewidth=1.0)
    plt.plot(x, (y5 - y5[0]) / y5, 'g', linewidth=1.0)
    plt.savefig('huya.png', dpi=300)
    # n, m = kline.shape
    #
    # for i in range(n):
    #     for j in range(m):
    #         print(kline[i][j])


if __name__ == '__main__':
    main()
