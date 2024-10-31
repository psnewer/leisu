#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_feature import ABSTRACT_FEATURE

class VS_DENSITY_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'VS_DENSITY'
		self.params = {}

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			extract_dir = ext_dir.replace('feature','extract')
			data_file = extract_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df['date_dt'] = pd.to_datetime(df['date'].astype(str).str[:8], format='%Y%m%d')
			df = df.sort_values(['team', 'date_dt']).reset_index(drop=True)
			df['density'] = df.groupby('team')['date_dt'].diff().dt.days
			df['density'] = df['density'].fillna(0)
			df = df.drop(columns=['date_dt'])
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)

		