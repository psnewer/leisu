#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd

class ABSTRACT_TESTER():
	params = {}

	def __init__(self):
		self.name = 'ABSTRACT_TESTER'

	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

	def analysis(self,condition,df_team):
		res_list = []
		feature_list = []
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		dates = df['date'].unique()
		f_res = open(gflags.FLAGS.test_final,'a+')
		for date in dates:
			df_date = df.query("date=='%s'"%date)
			date_dic = {}
			date_dic['date'] = date
			date_dic['posi_home_team'] = []
			date_dic['posi_away_team'] = []
			date_dic['neg_home_team'] = []
			date_dic['neg_away_team'] = []
			date_dic['teams'] = []
			for index,row in df_date.iterrows():
				res = self.get_team_tar(row)
				if (res == 1):
					date_dic['posi_home_team'].append(row['home_team_id'])
					date_dic['posi_away_team'].append(row['away_team_id'])
				elif (res==2):
					date_dic['neg_home_team'].append(row['home_team_id'])
					date_dic['neg_away_team'].append(row['away_team_id'])
				date_dic['teams'].append(row['home_team_id'])
				date_dic['teams'].append(row['away_team_id'])
			res_list.append(date_dic)
		df_date = pd.DataFrame(res_list)
		df_team = conciseDate(df_team)
		df_team_posi = df_team[['date','team_id']]
		df_team_posi = df_team_posi.groupby("date",as_index=False).agg({'team_id': lambda x: list(x)})
		anadf = pd.merge(df_date,df_team_posi,how='inner',on='date')
		succ = 0
		fail = 0
		for index,row in anadf.iterrows():
			if (index == 0):
				continue
			date = row['date']
			posi_home_team = row['posi_home_team']
			posi_away_team = row['posi_away_team']
			neg_home_team = row['neg_home_team']
			neg_away_team = row['neg_away_team']
			team_id = row['team_id']
			if (self.params['lateral'] == 1):
				for idx,home_team in enumerate(posi_home_team):
					away_team = posi_away_team[idx]
#					if (home_team not in team_feature or away_team not in team_feature):
#						f_res.write('date: ' + date + ' ' + str(home_team) + ' or ' + str(away_team) + ' not in team_feature' + '\n')
#						continue
					if home_team in team_id:
						succ = succ + 1
						f_res.write("success match: " + "date: " + date + " home_team: " + str(home_team) + '\n')
					elif away_team in team_id:
						succ = succ + 1
						f_res.write("success match: " + "date: " + date + " away_team: " + str(away_team) + '\n')
				for idx,home_team in enumerate(neg_home_team):
					away_team = neg_away_team[idx]
#					if (home_team not in team_feature or away_team not in team_feature):
#						f_res.write('date: ' + date + ' ' + str(home_team) + ' or ' + str(away_team) + ' not in team_feature' + '\n')
#						continue
					if home_team in team_id:
						fail = fail + 1
						f_res.write("failure match: " + "date: " + date + " home_team: " + str(home_team) + '\n')
					elif away_team in team_id:
						fail = fail + 1
						f_res.write("failure match: " + "date: " + date + " away_team: " + str(home_team) + '\n')
			else:
				for idx,home_team in enumerate(posi_home_team):
					away_team = posi_away_team[idx]
#					if (home_team not in team_feature or away_team not in team_feature):
#						f_res.write('date: ' + date + ' ' + str(home_team) + ' or ' + str(away_team) + ' not in team_feature' + '\n')
#						continue
					if home_team in team_id and away_team in team_id:
						succ = succ + 1
						f_res.write("success match: " + "date: " + date + " team: " + str(home_team) + "," + str(away_team) + '\n')
				for idx,home_team in enumerate(neg_home_team):
					away_team = neg_away_team[idx]
#					if (home_team not in team_feature or away_team not in team_feature):
#						f_res.write('date: ' + date + ' ' +  str(home_team) + ' or ' + str(away_team) + ' not in team_feature' + '\n')
#						continue
					if home_team in team_id and away_team in team_id:
						fail = fail + 1
						f_res.write("failure match: " + "date: " + date + " team: " + str(home_team) + "," + str(away_team) + '\n')
		dic_ = {}
		dic_['success'] = succ
		dic_['failure'] = fail
		dic_['last_date'] = df.iloc[-1]['date']
		dic_['league_id'] = df.iloc[-1]['league_id']
		dic_['experiment_id'] = GlobalVar.get_experimentId()
		dic_str = json.dumps(dic_)
		f_res.write(dic_str+'\n')
		f_res.close()
	
#	def get_team_feature(self, index, df):
#		current_row = df.iloc[index]
#		current_teams = current_row['teams']
#		team_feature = {}
#		for i in range(index - 1, -1, -1):
#			if (len(team_feature ) == len(current_teams)):
#				break
#			current_pre = df.iloc[i]
#			current_pre_team = current_pre['teams']
#			current_team_id = current_pre['team_id']
#			for team in current_pre_team:
#				if team in team_feature or team not in current_teams:
#					continue
#				if not isinstance(current_team_id,float) and team in current_team_id:
#					team_feature[team] = True
#				else:
#					team_feature[team] = False
#		return team_feature 
			
	def get_team_tar(self,row):
		return False
