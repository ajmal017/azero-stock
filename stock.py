import futuquant as ft
import numpy as np
import matplotlib.pyplot as plt


def main():
    quote_ctx = ft.OpenQuoteContext(host="35.234.49.155", port=11111)
    print(quote_ctx.subscribe('US.HUYA', 'K_1M'))
    kline_res = quote_ctx.get_cur_kline('US.HUYA', 1000, ktype='K_1M', autype='qfq')
    kline_pd = kline_res[1]
    keys = kline_pd.keys().values
    kline = kline_pd.values
    print('\t'.join(keys))
    for row in kline:
        for e in row:
            print(e, end='\t')
        print()

    def map_str(s):
        s1, s2 = s.split()
        return int('%s%s%s' % (s1.split('-')[-1], s2.split(':')[0], s2.split(':')[1]))

    x = list(map(map_str, list(kline[:, 1])))
    y1 = kline[:, 2]
    plt.plot(x, y1, linewidth=1.0)
    plt.savefig('filename.png', dpi=300)
    # n, m = kline.shape
    #
    # for i in range(n):
    #     for j in range(m):
    #         print(kline[i][j])


if __name__ == '__main__':
    main()
