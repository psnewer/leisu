#!/bin/bash
# -*- coding: utf-8 -*-
import numpy as np
from conf import *
from abstract_tester import ABSTRACT_TESTER

class VS_WIN_TESTER(ABSTRACT_TESTER):
	def __init__(self):
		self.name = 'VS_WIN'
		self.params = {}

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			filter_dir = ext_dir.replace('test','filter')
			data_file = filter_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df['fruit'] = df['selected'].apply(self.extract_raw_values)
			df['odds'] = df['selected'].apply(self.extract_odds_values)
			df = df.drop(columns=['selected'])
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def extract_raw_values(self,selected_list):
		selected_list = json.loads(selected_list)
		return [item['taw'] for item in selected_list]

	def extract_odds_values(self,selected_list):
		selected_list = json.loads(selected_list)
		return [np.nan_to_num([item['taw_win'],item['taw_draw']], nan=0).tolist() for item in selected_list]

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
