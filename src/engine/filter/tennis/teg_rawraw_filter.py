#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *

class TEG_RAWRAW_FILTER(object):
	def __init__(self):
		self.name = 'TEG_RAWRAW'
		self.params = {}
		self.params['thresh'] = -2
		with open('../db/tennis_cand.json', 'r', encoding='utf-8') as file:
			self.cand = json.load(file)
		with open('../db/top_200.json', 'r', encoding='utf-8') as file:
			self.top_200 = json.load(file)

	def process(self,seasons,ext_dir):
		for season in seasons:
			feature_dir = ext_dir.replace('filter','feature')
			feature_dir = feature_dir.replace('TEG','TEN')
			data_file = feature_dir + '/' + season + '.xlsx'
			df = pd.read_excel(data_file)
			df = df[df['soft'] < self.params['thresh']]
			df['filter'] = 'TEN_RAWRAW'
			df = self.analyze_soft(df)
			df = df[(df['team'].isin(self.top_200))]
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def analyze_soft(self,df):
		teams = df['team'].unique()
		for team in teams:
			team_cand = next(filter(lambda item: item["team"] == team, self.cand), None)
			if team_cand:
				if team_cand.get('filter'):
					df.loc[df['team'] == team, 'filter'] = team_cand.get('filter')
		return df

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
