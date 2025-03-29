#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *

class TEN_RAWRAW_FILTER(object):
	def __init__(self):
		self.name = 'TEN_RAWRAW'
		self.params = {}
		self.params['thresh_min'] = -2
		self.params['thresh_max'] = 1

	def process(self,seasons,ext_dir):
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
				selected_matches = team_matches[team_matches['soft'] < thresh].copy()
				for index, row in selected_matches.iterrows():
					home_team = row['home_team']
					away_team = row['away_team']
					date = row['date']
					match = df.query("team != @team and home_team == @home_team and away_team == @away_team and date == @date")
					if not match.empty:
						soft_value = match.iloc[0]['soft']
						if soft_value > -2:
							selected_matches.drop(index, inplace=True) 
				selected = selected_matches[['date', 'home_team', 'away_team', 'score', 'profit', 'seal']].to_dict('records')
				results.append({
					'team': team,
					'thresh': thresh,
					'selected': json.dumps(selected)
				})
		df_results = pd.DataFrame(results,columns=['team','thresh','selected'])
		return df_results

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
