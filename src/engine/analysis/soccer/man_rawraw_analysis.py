#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_analysis import ABSTRACT_ANALYSIS

class MAN_RAWRAW_ANALYSIS(ABSTRACT_ANALYSIS):
    def __init__(self):
        self.name = 'MAN_RAWRAW'
        self.params = {}
        
    def process(self,cond_str,seasons,ext_dir):
        analysis_dir = ext_dir.replace('analysis','test')
        if self.params['density']:
            analysis_dir += '_DENSITY'
            ext_dir += '_DENSITY'
            mkdir(ext_dir)
        all_data = []
        for season in seasons:
            data_file = analysis_dir + '/' + season + '.xlsx'
            df = pd.read_excel(data_file)
            grouped = df.groupby('team').apply(
                lambda x: pd.Series({
                            'team': x.name,
                            'neg': (x['fruit'] == -1).sum(),  # 统计 -1 的个数
                            'pos': (x['fruit'] == 1).sum(),  # 统计 1 的个数
                            'profit': x.apply(lambda row: calculate_profit(row), axis=1).sum(),  # 计算 profit
                            'dash': max_consecutive_minus_ones(x['fruit'])  # 计算连续 -1 的最大次数
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
        df = pd.concat(all_data, ignore_index=True)[['team', 'season', 'pos', 'neg', 'dash', 'profit']]
        df = df.sort_values(by=['team', 'season'])
        ext_file = ext_dir + '/' + self.name + '.xlsx'
        self.pack(df,ext_file)
        
    def pack(self,df,ext_file):
        df.to_excel(ext_file, index=False)

def max_consecutive_minus_ones(arr):
    max_count = 0  # 记录最大的连续'-1'次数
    current_count = 0  # 当前的连续计数
    for elem in arr:
        if elem == -1:
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
