#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_filter import ABSTRACT_FILTER

class NEG_RAWRAW_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'NEG_RAWRAW'
		self.params = {}
		self.params['thresh_min'] = 0
		self.params['thresh_max'] = 0

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			feature_dir = ext_dir.replace('filter','feature')
			feature_dir = feature_dir.replace('NEG','VS')
			data_file = feature_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df = df[df['soft'] < 0]
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	# def analyze_soft(self,df):
	# 	teams = df['team'].unique()
	# 	results = []
	# 	for team in teams:
	# 		team_matches = df[df['team'] == team]
	# 		for thresh in range(self.params['thresh_min'], self.params['thresh_max'] + 1):
	# 			selected_matches = team_matches[team_matches['soft'] < thresh]
	# 			selected = selected_matches[['date', 'home_team', 'away_team', 'rawraw', 'raw_win', 'raw_draw']].to_dict('records')
	# 			results.append({
	# 				'team': team,
	# 				'thresh': thresh,
	# 				'selected': json.dumps(selected)
	# 			})
	# 	df_results = pd.DataFrame(results,columns=['team','thresh','selected'])
	# 	return df_results

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
