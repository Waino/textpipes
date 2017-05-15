#!/usr/bin/env python3

import argparse
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

def quantize(field, df, n_bins=15):
    bins = np.linspace(df[field].min(), df[field].max() + 0.1, n_bins)
    df['group_idx'] = np.digitize(df[field], bins)
    return df, bins

def melt(x, y, df):
    y1, y2 = ['{}_{}'.format(sysid, y) for sysid in ('bl', 'sys')]
    sub = df[[x, y1, y2]]
    melted = sub.melt(id_vars=[x], var_name='sysid', value_name=y)
    return melted

def binlabels(field, bins, offset=-.5):
    plt.xticks([offset + x for x in range(len(bins))],
               ['{:.1f}'.format(x) for x in (bins)])
    plt.xlabel('{} (binned)'.format(field))

def avglineplot(x, y, df, binned=True):
    y1, y2 = ['{}_{}'.format(sysid, y) for sysid in ('bl', 'sys')]
    group_field = 'group_idx' if binned else x
    grouped = df.groupby(group_field)[y1, y2]
    means = grouped.mean()
    means.plot()

def violinplot(x, y, df, binned=True):
    group_field = 'group_idx' if binned else x
    melted = melt(group_field, y, df)
    plt.figure()
    sns.violinplot(x=group_field, y=y, hue='sysid', data=melted, split=True);

def main(args):
    df = pd.read_csv(args.stats, sep='\t', header=0)

    measures = args.measures.split(',')
    for field in ('src_len_chars', 'src_len_words', 'ref_len_chars', 'ref_len_words'):
        # fields needing to be quantized
        binned, bins = quantize(field, df)
        for measure in measures:
            avglineplot(field, measure,  binned, binned=True)
            binlabels(field, bins, offset=.5)
            violinplot(field, measure, binned, binned=True)
            binlabels(field, bins, offset=-.5)
    for field in ('lnames',):
        # no need to quantize
        for measure in measures:
            avglineplot(field, measure,  df, binned=False)
            violinplot(field, measure, df, binned=False)
    for method in ('anywhere', 'conseq'):
        field = 'ref_reps_{}'.format(method)
        measure = 'reps_{}'.format(method)
        avglineplot(field, measure,  df, binned=False)
        violinplot(field, measure, df, binned=False)

    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stats', type=str,
                        help='tab-separated-values file with stats')
    parser.add_argument('--avg-line', default=False, action='store_true',
                        help='use line plot of average instead of violin plot')
    parser.add_argument('--measures', default='chrF1,chrF2,bleu',
                        help='comma separated list of measures to show')
    args = parser.parse_args()
    main(args)
