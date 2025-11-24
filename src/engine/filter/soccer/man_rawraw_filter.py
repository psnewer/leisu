#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
from abstract_filter import ABSTRACT_FILTER

class MAN_RAWRAW_FILTER(ABSTRACT_FILTER):
	def __init__(self):
		self.name = 'MAN_RAWRAW'
		with open(gflags.FLAGS.predict_cand, 'r', encoding='utf-8') as file:
			self.cand = json.load(file)

	def process(self,cond_str,seasons,ext_dir):
		for season in seasons:
			feature_dir = ext_dir.replace('filter','feature')
			feature_rawraw = feature_dir.replace('MAN','VS')
			feature_tawtaw = feature_rawraw.replace('RAWRAW','TAWTAW')
			feature_rawraw = feature_rawraw + '/' + season + '.xlsx'
			feature_tawtaw = feature_tawtaw + '/' + season + '.xlsx'
			filter_rawraw = ext_dir.replace('MAN','NEG') + '/' + season + '.xlsx'
			feature_rawraw = pd.read_excel(feature_rawraw)
			feature_tawtaw = pd.read_excel(feature_tawtaw)
			filter_rawraw = pd.read_excel(filter_rawraw)
			league = ext_dir.split('/')[-2]
			df, diff = self.analyze_soft(feature_rawraw, feature_tawtaw, filter_rawraw, league)
			ext_file = ext_dir + '/' + season + '.xlsx'
			self.pack(df,ext_file)
			ext_file = ext_file.replace('MAN_RAWRAW','DIFF_RAWRAW')
			self.pack(diff,ext_file)

	def analyze_soft(self,feature_rawraw, feature_tawtaw, filter_rawraw, league):
		teams = feature_rawraw['team'].unique()
		rawraw_feature = pd.DataFrame()
		tawtaw_feature = pd.DataFrame()
		fishes = []
		cand_teams = []
		ignore = []
		laizy = []
		for team in teams:
			team_cand = next(filter(lambda item: item["team"] == team, self.cand[league]), None)
			if team_cand:
				if team_cand.get('filter'):
					if team_cand['filter'] == 'VS_RAWRAW':
						rawraw_feature = pd.concat([rawraw_feature,feature_rawraw[feature_rawraw['team'] == team]], ignore_index=True)
					elif team_cand['filter'] == 'VS_TAWTAW':
						tawtaw_feature = pd.concat([tawtaw_feature,feature_tawtaw[feature_tawtaw['team'] == team]], ignore_index=True)
					elif team_cand['filter'] == 'VS_EITHER':
						rawraw_feature = pd.concat([rawraw_feature,feature_rawraw[feature_rawraw['team'] == team]], ignore_index=True)
						tawtaw_feature = pd.concat([tawtaw_feature,feature_tawtaw[feature_tawtaw['team'] == team]], ignore_index=True)
					cand_teams.append(team)
				if team_cand.get('fish') and team_cand['fish']:
					fishes.append(team)
				if team_cand.get('ignore'):
					ignore.append(team)
				if team_cand.get('laizy'):
					laizy.append(team)
		filtered_rawraw = filter_rawraw.copy(deep=True)
		if cand_teams:
			if not filter_rawraw.empty:
				filtered_rawraw = filter_rawraw[~filter_rawraw['home_team'].isin(cand_teams) & ~filter_rawraw['away_team'].isin(cand_teams)]
		if fishes:
			if not filter_rawraw.empty:
				filtered_rawraw = pd.concat([filtered_rawraw,filter_rawraw[filter_rawraw['home_team'].isin(fishes) & filter_rawraw['away_team'].isin(fishes)]], ignore_index=True)
				filtered_rawraw = pd.concat([filtered_rawraw,filter_rawraw[~filter_rawraw['team'].isin(fishes) & (filter_rawraw['home_team'].isin(fishes) | filter_rawraw['away_team'].isin(fishes))]], ignore_index=True)
			# if not tawtaw_feature.empty:
			# 	tawtaw_feature = tawtaw_feature[~(~tawtaw_feature['team'].isin(fishes) & (tawtaw_feature['home_team'].isin(fishes) | tawtaw_feature['away_team'].isin(fishes)))]
			if not tawtaw_feature.empty:
				tawtaw_feature = tawtaw_feature[~tawtaw_feature['home_team'].isin(fishes) & ~tawtaw_feature['away_team'].isin(fishes)]
		if ignore:
			if not filtered_rawraw.empty:
				filtered_rawraw = filtered_rawraw[~(filtered_rawraw['home_team'].isin(ignore) | filtered_rawraw['away_team'].isin(ignore))]
				# filtered_rawraw = pd.concat([filtered_rawraw,filter_rawraw[filter_rawraw['team'].isin(fishes) & (~filter_rawraw['home_team'].isin(fishes) | ~filter_rawraw['away_team'].isin(fishes))]], ignore_index=True)
			if not rawraw_feature.empty:
				rawraw_feature = rawraw_feature[~(rawraw_feature['home_team'].isin(ignore) | rawraw_feature['away_team'].isin(ignore))]
			if not tawtaw_feature.empty:
				tawtaw_feature = tawtaw_feature[~(tawtaw_feature['home_team'].isin(ignore) | tawtaw_feature['away_team'].isin(ignore))]
		if laizy:
			if not filtered_rawraw.empty:
				filtered_rawraw = filtered_rawraw[~filtered_rawraw['team'].isin(laizy) ]
			if not rawraw_feature.empty:
				rawraw_feature = rawraw_feature[~rawraw_feature['team'].isin(laizy)]
			if not tawtaw_feature.empty:
				tawtaw_feature = tawtaw_feature[~((~tawtaw_feature['team'].isin(laizy) & (tawtaw_feature['home_team'].isin(laizy) | tawtaw_feature['away_team'].isin(laizy))) | (tawtaw_feature['home_team'].isin(laizy) & tawtaw_feature['away_team'].isin(laizy)))]
		merged_df = pd.concat([filtered_rawraw, rawraw_feature, tawtaw_feature], ignore_index=True)
		if (not gflags.FLAGS.predict):
			merged_df = merged_df.groupby(['home_team', 'away_team', 'date']).apply(self.get_first_non_zero)
		merged_df = merged_df.dropna(how='all').reset_index(drop=True).sort_values(['team','date'])
		diff = feature_rawraw.merge(merged_df, on=['home_team','away_team'], how='left', indicator=True)
		diff = diff[['_merge'] == 'left_only'].drop('_merge', axis=1).drop_duplicates(subset=['home_team', 'away_team'], keep='first')
		diff = diff[~diff['home_team'].isin(ignore) & ~diff['away_team'].isin(ignore)]
		return merged_df,diff
	
	def get_first_non_zero(self, group):
		non_zero = group[group['man'] != 0]  # 筛选 man 不等于 0 的行
		if not non_zero.empty:  # 如果有 man 不等于 0 的行
			return non_zero.iloc[0]  # 返回第一个
		return group.iloc[0]  # 如果没有，返回 None

	def pack(self,df,ext_file):
		df.to_excel(ext_file, index=False)
