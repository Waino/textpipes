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

def sortedplot(df, field):
    plt.figure()
    plt.plot(range(len(df)), df[field].sort_values())
    plt.plot([len(df) / 2] * 2, [df[field].min(), df[field].max()], ':')
    plt.xlabel('sorted'); plt.ylabel(field)

def measurescatterplot(df, x, y):
    g = sns.jointplot(x=x, y=y, data=df, kind="kde", color="m", n_levels=20)
    g.plot_joint(plt.scatter, c="k", s=30, linewidth=1, marker="+")
    g.ax_joint.collections[0].set_alpha(0)
    g.set_axis_labels(x, y);

def main(args):
    df = pd.read_csv(args.stats, sep='\t', header=0)

    measures = args.measures.split(',')
    if args.avg_line:
        plot_func = avglineplot
        bin_offset = .5
    else:
        plot_func = violinplot
        bin_offset = -.5

    for field in ('src_len_chars', 'src_len_words',
                  'ref_len_chars', 'ref_len_words',):
        # fields needing to be quantized
        binned, bins = quantize(field, df)
        for measure in measures:
            plot_func(field, measure,  binned, binned=True)
            binlabels(field, bins, offset=bin_offset)
    for field in ('lnames',
                  'sys_reps_anywhere', 'sys_reps_conseq',):
        # no need to quantize
        for measure in measures:
            plot_func(field, measure,  df, binned=False)
    for method in ('anywhere', 'conseq'):
        field = 'ref_reps_{}'.format(method)
        measure = 'reps_{}'.format(method)
        plot_func(field, measure, df, binned=False)
    for measure in measures:
        sortedplot(df, 'delta_{}'.format(measure))
    if 'bleu' in measures and 'chrF1' in measures:
        measurescatterplot(df, 'delta_bleu', 'delta_chrF1')

    ## does ensemble only increase repetitions for already bad sentences?
    #df['delta_reps_conseq'] = df['sys_reps_conseq'] - df['bl_reps_conseq']
    #df['delta_reps_anywhere'] = df['sys_reps_anywhere'] - df['bl_reps_anywhere']
    #binned, bins = quantize('bl_chrF1', df, n_bins=20)
    #means = binned.groupby('group_idx')['delta_reps_conseq', 'delta_reps_anywhere'].mean()
    #means.plot()
    #binlabels('bl_chrF1', bins, offset=bin_offset)
    #measurescatterplot(df, 'delta_reps_conseq', 'delta_chrF1')
    #measurescatterplot(df, 'delta_reps_conseq', 'delta_chrF2')
    #measurescatterplot(df, 'delta_reps_anywhere', 'delta_chrF1')
    #measurescatterplot(df, 'delta_reps_anywhere', 'delta_chrF2')
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
