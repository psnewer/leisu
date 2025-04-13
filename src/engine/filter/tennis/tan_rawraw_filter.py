#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_filter import ABSTRACT_FILTER

class TAN_RAWRAW_FILTER(object):
	def __init__(self):
		self.name = 'TAN_RAWRAW'
		with open('../db/tennis_cand.json', 'r', encoding='utf-8') as file:
			self.cand = json.load(file)
		with open('../db/top_200.json', 'r', encoding='utf-8') as file:
			self.top_200 = json.load(file)

	def process(self,seasons,ext_dir):
		for season in seasons:
			feature_dir = ext_dir.replace('filter','feature')
			feature_rawraw = feature_dir.replace('TAN','TEN')
			feature_tawtaw = feature_rawraw.replace('RAWRAW','TAWTAW')
			feature_rawraw = feature_rawraw + '/' + season + '.xlsx'
			feature_tawtaw = feature_tawtaw + '/' + season + '.xlsx'
			filter_rawraw = ext_dir.replace('TAN','TEG') + '/' + season + '.xlsx'
			feature_rawraw = pd.read_excel(feature_rawraw)
			feature_tawtaw = pd.read_excel(feature_tawtaw)
			filter_rawraw = pd.read_excel(filter_rawraw)
			df = self.analyze_soft(feature_rawraw, feature_tawtaw, filter_rawraw)
			df = df[(df['team'].isin(self.top_200))]
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)

	def analyze_soft(self,feature_rawraw, feature_tawtaw, filter_rawraw):
		teams = feature_rawraw['team'].unique()
		rawraw_feature = pd.DataFrame()
		tawtaw_feature = pd.DataFrame()
		filter_rawraw['filter'] = 'TEN_RAWRAW'
		kings = []
		fishes = []
		cand_teams = []
		ignore = []
		for team in teams:
			team_cand = next(filter(lambda item: item["team"] == team, self.cand), None)
			if team_cand:
				if team_cand.get('filter'):
					if team_cand['filter'] == 'TEN_RAWRAW' or team_cand['filter'] == 'TEN_RAWTAW':
						team_feature = feature_rawraw[feature_rawraw['team'] == team].copy(deep=True)
						team_feature['filter'] = team_cand['filter']
						rawraw_feature = pd.concat([rawraw_feature,team_feature], ignore_index=True)
					elif team_cand['filter'] == 'TEN_TAWTAW' or team_cand['filter'] == 'TEN_TAWRAW':
						team_feature = feature_tawtaw[feature_tawtaw['team'] == team].copy(deep=True)
						team_feature['filter'] = team_cand['filter']
						tawtaw_feature = pd.concat([tawtaw_feature,team_feature], ignore_index=True)
					cand_teams.append(team)
				if team_cand.get('fish') and team_cand['fish']:
					fishes.append(team)
				if team_cand.get('king') and team_cand['king']:
					kings.append(team)
				if team_cand.get('ignore') and team_cand['ignore']:
					ignore.append(team)
		filtered_rawraw = filter_rawraw.copy(deep=True)
		if cand_teams:
			if not filter_rawraw.empty:
				filtered_rawraw = filter_rawraw[(~filter_rawraw['home_team'].isin(fishes) & ~filter_rawraw['away_team'].isin(fishes)) & (~filter_rawraw['home_team'].isin(kings) & ~filter_rawraw['away_team'].isin(kings))]
		if fishes:
			if not filter_rawraw.empty:
				filtered_rawraw = pd.concat([filtered_rawraw,filter_rawraw[filter_rawraw['home_team'].isin(fishes) & filter_rawraw['away_team'].isin(fishes)]], ignore_index=True)
			# if not tawtaw_feature.empty:
			# 	tawtaw_feature = tawtaw_feature[~(~tawtaw_feature['team'].isin(fishes) & (tawtaw_feature['home_team'].isin(fishes) | tawtaw_feature['away_team'].isin(fishes)))]
			if not tawtaw_feature.empty:
				tawtaw_feature = tawtaw_feature[tawtaw_feature['team'].isin(fishes)]
				tawtaw_feature = tawtaw_feature[~(tawtaw_feature['home_team'].isin(fishes) & tawtaw_feature['away_team'].isin(fishes))]
		if kings:
			if not filter_rawraw.empty:
				filtered_rawraw = pd.concat([filtered_rawraw,filter_rawraw[filter_rawraw['home_team'].isin(kings) & filter_rawraw['away_team'].isin(kings)]], ignore_index=True)
			if not rawraw_feature.empty:
				rawraw_feature = rawraw_feature[rawraw_feature['team'].isin(kings)]
				rawraw_feature = rawraw_feature[~(rawraw_feature['home_team'].isin(kings) & rawraw_feature['away_team'].isin(kings))]
		if ignore:
			if not filtered_rawraw.empty:
				filtered_rawraw = filtered_rawraw[~(filtered_rawraw['home_team'].isin(ignore) | filtered_rawraw['away_team'].isin(ignore))]
				# filtered_rawraw = pd.concat([filtered_rawraw,filter_rawraw[filter_rawraw['team'].isin(fishes) & (~filter_rawraw['home_team'].isin(fishes) | ~filter_rawraw['away_team'].isin(fishes))]], ignore_index=True)
			if not rawraw_feature.empty:
				rawraw_feature = rawraw_feature[~(rawraw_feature['home_team'].isin(ignore) | rawraw_feature['away_team'].isin(ignore))]
			if not tawtaw_feature.empty:
				tawtaw_feature = tawtaw_feature[~(tawtaw_feature['home_team'].isin(ignore) | tawtaw_feature['away_team'].isin(ignore))]
		merged_df = pd.concat([filtered_rawraw, rawraw_feature, tawtaw_feature], ignore_index=True)
		if (not gflags.FLAGS.predict):
			merged_df = merged_df.groupby(['home_team', 'away_team', 'date']).apply(self.get_first_non_zero)
		merged_df = merged_df.dropna(how='all').reset_index(drop=True).sort_values(['team','date'])
		return merged_df
	
	def get_first_non_zero(self, group):
		non_zero = group[group['score'] != 0]  # 筛选 man 不等于 0 的行
		if not non_zero.empty:  # 如果有 man 不等于 0 的行
			return non_zero.iloc[0]  # 返回第一个
		return group.iloc[0]  # 如果没有，返回 None

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
