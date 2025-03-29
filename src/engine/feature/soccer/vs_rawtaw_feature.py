#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_feature import ABSTRACT_FEATURE

class VS_RAWTAW_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'VS_RAWTAW'
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
			team_data['hard'] = team_data['raw'].cumsum().shift(fill_value=0)
			team_data['soft'] = team_data['taw'].cumsum().shift(fill_value=0)
			final_df = pd.concat([final_df, team_data])
		return final_df

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)

		