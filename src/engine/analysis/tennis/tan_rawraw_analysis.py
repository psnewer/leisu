#!/bin/bash
# -*- coding: utf-8 -*-
import numpy as np
from conf import *
from abstract_analysis import ABSTRACT_ANALYSIS

class TAN_RAWRAW_ANALYSIS(object):
    def __init__(self):
        self.name = 'TAN_RAWRAW'
        self.params = {}
        
    def process(self,seasons,ext_dir):
        analysis_dir = ext_dir.replace('analysis','test')
        all_data = []
        for season in seasons:
            data_file = analysis_dir + '/' + season + '.xlsx'
            df = pd.read_excel(data_file)
            df = self.R2T(df)
            grouped = df.groupby('team').apply(
                lambda x: pd.Series({
                            'team': x.name,
                            'neg': (x['profit'] < 0).sum(),  # 统计 -1 的个数
                            'pos': (x['profit'] > 0).sum(),  # 统计 1 的个数
                            'lapse': ((x['profit'] == 0) & (x['score'] > 0)).sum(),  # 统计 1 的个数
                            'profit': x['profit'].sum(),  # 计算 profit
                            'dash': max_consecutive_minus_ones(x['profit'])  # 计算连续 -1 的最大次数
                        })
                ).reset_index(drop=True)
            # df['fruit'] = df['fruit'].apply(lambda x: json.loads(x))
            # df['odds'] = df['odds'].apply(lambda x: json.loads(x))
            # df['posi'] = df['fruit'].apply(lambda x: x.count(1))
            # df['neg'] = df['fruit'].apply(lambda x: x.count(-1))
            # df['dash'] = df['fruit'].apply(self.max_consecutive_minus_ones)
            grouped['season'] = season
            # if not df.empty:
            #     df['profit'] = df.apply(lambda row: self.calculate_profit(row['fruit'], row['odds']), axis=1)
            # else:
            #     df['profit'] = []
            all_data.append(grouped)
        df = pd.concat(all_data, ignore_index=True)[['team', 'season', 'pos', 'neg', 'lapse', 'dash', 'profit']]
        df = df.sort_values(by=['team', 'season'])
        ext_file = ext_dir + '/' + self.name + '.xlsx'
        self.pack(df,ext_file)

    def R2T(self, df):
        ten_rawtaw_mask = (df['filter'] == 'TEN_RAWTAW') & (df['score'] > 0)
        ten_tawraw_mask = (df['filter'] == 'TEN_TAWRAW') & (df['score'] > 0)

        # 使用 np.select 进行多条件赋值
        conditions = [
            ten_rawtaw_mask & (df['profit'] == 0),
            ten_rawtaw_mask & (df['profit'] > 0),
            ten_tawraw_mask & (df['profit'] == 0),
            ten_tawraw_mask & (df['profit'] > 0)
        ]

        choices = [
            df['score'],  # 条件1：profit = score
            0,            # 条件2：profit = 0
            df['score'],  # 条件3：profit = score
            0             # 条件4：profit = 0
        ]

        # 默认保留原值
        df['profit'] = np.select(conditions, choices, default=df['profit'])
        return df
        
    def pack(self,df,ext_file):
        df.to_excel(ext_file, index=False)

def max_consecutive_minus_ones(arr):
    max_count = 0  # 记录最大的连续'-1'次数
    current_count = 0  # 当前的连续计数
    for elem in arr:
        if elem < 0:
            current_count += 1
        elif elem == 0:
            continue
        else:
            max_count = max(max_count, current_count)
            current_count = 0  # 重置计数器
    max_count = max(max_count, current_count)
    return max_count

def calculate_profit(row):
    if row['fruit'] == -1:
        return -1  # 亏损为 -1
    elif row['fruit'] == 1:
        odds = json.loads(row['odds'])
        if row['filter'] == 'draw':
            return (odds[0] - odds[2])
        else:
            scores = row['score'].strip("'").split('-')
            home_score = int(scores[0])
            away_score = int(scores[1])
            if home_score == away_score:
                return (odds[0] - 1) * 1.5
            else : 
                return (odds[0] - 1) * 0.5 - 1
    else:
        return 0  # 其他情况返回 0
