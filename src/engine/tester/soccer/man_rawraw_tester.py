#!/bin/bash
# -*- coding: utf-8 -*-
import numpy as np
from conf import *
from abstract_tester import ABSTRACT_TESTER

class MAN_RAWRAW_TESTER(ABSTRACT_TESTER):
	def __init__(self):
		self.name = 'MAN_RAWRAW'
		self.params = {}

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			filter_dir = ext_dir.replace('test','filter')
			data_file = filter_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			if (self.params['density']):
				ext_dir = ext_dir + '_DENSITY'
				mkdir(ext_dir)
				df_density = pd.read_excel(data_file.replace(self.name,'VS_DENSITY'))
				if not df.empty:
					df['selected'] = df.apply(lambda row: self.filter_selected(row, df_density), axis=1)
				else:
					df['selected'] = []
			df['fruit'] = df['man'].apply(self.extract_raw_values)
			df['odds'] = df.apply(lambda row: self.extract_odds_values(row), axis=1)
			df = df[df['date'] <= 202510080000]
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def extract_raw_values(self,man):
		return man

	def extract_odds_values(self,row):
		return np.nan_to_num([float(row['draw_0']),float(row['home']),float(row['draw']),float(row['away'])], nan=0).tolist()
	
	def filter_selected(self, row, df_density):
		filtered_selected = []
		for match in json.loads(row['selected']):
			other_team = match['home_team'] if match['home_team'] != row['team'] else match['away_team']
			match_date = match['date']
			if not ((df_density['team'] == other_team) & (df_density['date'] == match_date)).any():
				filtered_selected.append(match)
		return json.dumps(filtered_selected)

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
