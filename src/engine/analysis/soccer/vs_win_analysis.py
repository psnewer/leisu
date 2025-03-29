#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_analysis import ABSTRACT_ANALYSIS

class VS_WIN_ANALYSIS(ABSTRACT_ANALYSIS):
    def __init__(self):
        self.name = 'VS_WIN'
        self.params = {}
        
    def process(self,cond_str,seasons,ext_dir):
        analysis_dir = ext_dir.replace('analysis','test')
        all_data = []
        for season in seasons:
            data_file = analysis_dir + '/' + season + '.xlsx'
            df = pd.read_excel(data_file)
            df['season'] = season
            df['fruit'] = df['fruit'].apply(lambda x: json.loads(x))
            df['odds'] = df['odds'].apply(lambda x: json.loads(x))
            df['posi'] = df['fruit'].apply(lambda x: x.count(1))
            df['neg'] = df['fruit'].apply(lambda x: x.count(-1))
            df['dash'] = df['fruit'].apply(self.max_consecutive_minus_ones)
            if not df.empty:
                df['profit'] = df.apply(lambda row: self.calculate_profit(row['fruit'], row['odds']), axis=1)
            else:
                df['profit'] = []
            all_data.append(df)
        df = pd.concat(all_data, ignore_index=True)
        df = df.sort_values(by=['team', 'thresh'])
        ext_file = ext_dir + '/' + self.name + '.xlsx'
        self.pack(df,ext_file)

    def max_consecutive_minus_ones(self, arr):
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

    def calculate_profit(self, fruit, odds):
        profit = 0
        for f, o in zip(fruit, odds):
            if (f == -1 and o[1] == 0.0):
                profit += f
            elif (f == -1 and o[1] > 0.0):
                profit += 1.0/(o[1] - 1.0) * (o[0] - 1.0) - 1.0
            elif (f == 0):
                profit += 0
            elif (f == 1):
                profit += o[0] - 1.0
        return profit
        
    def pack(self,df,ext_file):
        df.to_excel(ext_file, index=False)
