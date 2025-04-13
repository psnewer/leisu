#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_feature import ABSTRACT_FEATURE

class TEN_TAWTAW_FEATURE(object):
	def __init__(self):
		self.name = 'TEN_TAWTAW'
		self.params = {}

	def process(self,seasons,ext_dir):
		for season in seasons:
			extract_dir = ext_dir.replace('feature','extract')
			data_file = extract_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			all_teams = df['team'].unique()
			final_df = pd.DataFrame(columns = list(df.columns) + ['soft'])
			for team in all_teams:
				team_data = df[(df['team'] == team)]
				team_data = team_data.sort_values(by='date')
				team_data = self.compute_soft_column(team_data)
				final_df = pd.concat([final_df, team_data])
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(final_df,ext_file)

	def compute_soft_column(self,team_data):
		soft_values = []
		
		for i in range(len(team_data)):
			j = i - 1  # 从当前行的上一行开始向上查找
			soft_sum = 0
			# while j >= 0 and team_data.iloc[j, team_data.columns.get_loc('profit')] == -1:
			while j >= 0 and team_data.iloc[j, team_data.columns.get_loc('seal')] == 0 and team_data.iloc[j, team_data.columns.get_loc('profit')] < 0:
				soft_sum += -1
				j -= 1  # 继续向上查找
			
			soft_values.append(soft_sum)

		
		team_data['soft'] = soft_values
		return team_data

	def pack(self,df,ext_file):
		df = df.sort_values(by=['team','date'])
		df.to_excel(ext_file, index=False)

		