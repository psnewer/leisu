#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_filter import ABSTRACT_FILTER

class VS_DENSITY_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'VS_DENSITY'
		self.params = {}
		self.params['thresh'] = 4

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			feature_dir = ext_dir.replace('filter','feature')
			data_file = feature_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df_filtered = df[(df['density'] <= self.params['thresh']) & (df['density'] != 0)]
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df_filtered,ext_file)

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
