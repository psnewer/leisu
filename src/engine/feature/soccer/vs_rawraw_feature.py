#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_feature import ABSTRACT_FEATURE

class VS_RAWRAW_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'VS_RAWRAW'
		self.params = {}

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			extract_dir = ext_dir.replace('feature','extract')
			data_file = extract_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df = self.analyze_raw(df)
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def analyze_raw(self,season_data):
		final_df = pd.DataFrame(columns = list(season_data.columns) + ['hard','soft'])
		all_teams = season_data['team'].unique()
		for team in all_teams:
			team_data = season_data[(season_data['team'] == team)]
			team_data = team_data.sort_values(by='date')
			team_data = self.compute_soft_column(team_data)
			# team_data['hard'] = team_data['raw'].cumsum().shift(fill_value=0)
			# team_data['soft'] = team_data['taw'].cumsum().shift(fill_value=0)
			final_df = pd.concat([final_df, team_data])
		return final_df
	
	def compute_soft_column(self,team_data):
		soft_values = []
		
		for i in range(len(team_data)):
			j = i - 1  # 从当前行的上一行开始向上查找
			soft_sum = 0
			while j >= 0 and team_data.iloc[j, team_data.columns.get_loc('raw')] == -1:
				soft_sum += -1
				j -= 1  # 继续向上查找
			
			soft_values.append(soft_sum)

		
		team_data['soft'] = soft_values
		return team_data

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)

		