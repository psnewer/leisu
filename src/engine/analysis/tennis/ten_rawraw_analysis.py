#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *

class TEN_RAWRAW_ANALYSIS(object):
    def __init__(self):
        self.name = 'TEN_RAWRAW'
        self.params = {}
        self.params['density'] = False
        
    def process(self,seasons,ext_dir):
        analysis_dir = ext_dir.replace('analysis','test')
        if self.params['density']:
            analysis_dir += '_DENSITY'
            ext_dir += '_DENSITY'
            mkdir(ext_dir)
        all_data = []
        for season in seasons:
            data_file = analysis_dir + '/' + season + '.xlsx'
            df = pd.read_excel(data_file)
            df['season'] = season
            df['fruit'] = df['fruit'].apply(lambda x: json.loads(x))
            df['odds'] = df['odds'].apply(lambda x: json.loads(x))
            df['seal'] = df['seal'].apply(lambda x: json.loads(x))
            # df['posi'] = df['fruit'].apply(lambda x: sum(i > 0 for i in x))
            # df['neg'] = df['fruit'].apply(lambda x: sum(i < 0 for i in x))
            df['posi'] = df['odds'].apply(lambda x: sum(i > 0 for i in x))
            df['neg'] = df['odds'].apply(lambda x: sum(i < 0 for i in x))
            df['dash'] = df['fruit'].apply(self.max_consecutive_minus_ones)
            if not df.empty:
                df['profit'] = df['odds'].apply(lambda x: sum(x))
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
            # elif elem == 0:
            #     continue
            else:
                max_count = max(max_count, current_count)
                current_count = 0  # 重置计数器
        max_count = max(max_count, current_count)
        return max_count
        
    def pack(self,df,ext_file):
        df.to_excel(ext_file, index=False)
