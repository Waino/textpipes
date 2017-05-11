#!/usr/bin/env python3

import argparse
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

def quantize(field, df, bins=25):
    bins = np.linspace(df[field].min(), df[field].max(), 10)
    df['group_idx'] = np.digitize(df[field], bins)
    return df

def melt(x, y, df):
    y1, y2 = ['{}_{}'.format(sysid, y) for sysid in ('bl', 'sys')]
    sub = df[[x, y1, y2]]
    melted = sub.melt(id_vars=[x], var_name='sysid', value_name=y)
    print('melted')
    print(melted)
    return melted

def avglineplot(x, y, binned):
    y1, y2 = ['{}_{}'.format(sysid, y) for sysid in ('bl', 'sys')]
    #plt.figure()
    grouped = binned.groupby('group_idx')[x, y1, y2]
    print('grouped')
    print(grouped)
    means = grouped.mean()
    print('means')
    print(means)
    #means.plot()

def violinplot(x, y, binned):
    melted = melt('group_idx', y, binned)
    print('violinplot')
    print(melted)
    #plt.figure()
    #sns.violinplot(x=x, y=y, hue='sysid', data=binned, split=True);

def main(args):
    df = pd.read_csv(args.stats, sep='\t', header=0)
    # FIXME: bleu not included (add to extra_columns)
    # deltas FIXME: will move to extra_columns
    #df['delta_bleu'] = df['sys_bleu'] - df['bl_bleu']
    df['delta_chrF1'] = df['sys_chrF1'] - df['bl_chrF1']
    df['delta_chrF2'] = df['sys_chrF2'] - df['bl_chrF2']

    #binned = quantize('src_len_words', df)
    #melted = melt('src_len_words', 'sys_bleu', 'bl_bleu', 'BLEU')
    #avglineplot('src_len_words', 'BLEU', melted)
    #violinplot('src_len_words', 'BLEU', melted)
    binned = quantize('src_len_words', df)
    avglineplot('src_len_words', 'chrF1',  binned)
    violinplot('src_len_words', 'chrF1', binned)
    #plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stats', type=str,
                        help='tab-separated-values file with stats')
    parser.add_argument('--avg-line', default=False, action='store_true',
                        help='use line plot of average instead of violin plot')
    args = parser.parse_args()
    main(args)
