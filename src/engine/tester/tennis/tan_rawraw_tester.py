#!/bin/bash
# -*- coding: utf-8 -*-
import numpy as np
from conf import *
from abstract_tester import ABSTRACT_TESTER

class TAN_RAWRAW_TESTER(object):
	def __init__(self):
		self.name = 'TAN_RAWRAW'
		self.params = {}

	def process(self,seasons,ext_dir):
		for season in seasons:
			filter_dir = ext_dir.replace('test','filter')
			data_file = filter_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)
	
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
