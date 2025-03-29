#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_filter import ABSTRACT_FILTER

class VS_RAWDRAW_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'VS_RAWDRAW'
		self.params = {}
		self.params['thresh_min'] = -8
		self.params['thresh_max'] = 4

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			feature_dir = ext_dir.replace('filter','feature')
			data_file = feature_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df = self.analyze_hard(df)
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def analyze_hard(self,df):
		teams = df['team'].unique()
		results = []
		for team in teams:
			team_matches = df[df['team'] == team]
			for thresh in range(self.params['thresh_min'], self.params['thresh_max'] + 1):
				selected_matches = team_matches[team_matches['hard'] < thresh]
				selected = selected_matches[['date', 'home_team', 'away_team', 'raw', 'raw_win', 'raw_draw']].to_dict('records')
				results.append({
					'team': team,
					'thresh': thresh,
					'selected': json.dumps(selected)
				})
		df_results = pd.DataFrame(results,columns=['team','thresh','selected'])
		return df_results

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
