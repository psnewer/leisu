#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_extractor import ABSTRACT_EXTRACTOR

class VS_DENSITY_EXTRACTOR(ABSTRACT_EXTRACTOR):
	def __init__(self):
		self.name = 'VS_DENSITY'
		self.params = {}

	def process(self,cond_str,seasons,ext_dir):
		league = cond_str.split('and')[0].split('=')[1].strip().strip("'").strip('"')
		for season in seasons:
			season_str = "season='%s'"%season
			sql_str = "select league,season,serryid,serryname,stage,date,home_team,away_team,score,procedure from matches where %s"%(season_str)
			df_season = pd.read_sql_query(sql_str,conn)
			df_home = df_season[['home_team','league','date']].rename(columns={'home_team': 'team'})
			df_away = df_season[['away_team','league','date']].rename(columns={'away_team': 'team'})
			df_all_matches = pd.concat([df_home, df_away], ignore_index=True)
			teams = df_all_matches[df_all_matches['league'] == league]['team'].unique()
			df_all_matches = df_all_matches[df_all_matches['team'].isin(teams)]
			df_sorted_matches = df_all_matches.sort_values(by='date').groupby('team')
			df = pd.concat([group for _, group in df_sorted_matches])
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)

		