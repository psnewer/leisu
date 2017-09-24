#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class PRE_RANK_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'PRE_RANK_FEATURE'
		self.params['to_last'] = 1
	
	def execute(self,condition,action):
		if (action == 'predict'):
			return self.execute_predict(condition)
		elif (action == 'test'):
			return self.execute_test(condition)
			
	def execute_predict(self,condition):
		cond_str = ' and '.join(condition)
		sql_str = "select * from TMatch where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		df = df.sort_values(by='date')
		serries = df['serry'].unique()
		dates = df['date'].unique()
		f_res = open(gflags.FLAGS.res_path,'a+')
		for date in dates:
			df_date = df.query("date=='%s'"%date)
			res = self.process(df_date,df,serries)
			for _res in res:
				res_str = json.dumps(_res)
				res_test.write(_res+'\n')
		f_res.close()

	def execute_test(self,condition):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		df = df.sort_values(by='date')
		serries = df['serry'].unique()
		dates = df['date'].unique()
		team_res = []
		res_test = open(gflags.FLAGS.res_test,'a+')
		for date in dates:
			df_date = df.query("date=='%s'"%date)
			res = self.process(df_date,df,serries)
			for _res in res:
				res_str = json.dumps(_res)
				res_test.write(res_str+'\n')
				team_res.append(_res)
		res_test.close()
		return team_res

	def process(self,df_date,df_league,serries):
		team_res = []
		for index,row in df_date.iterrows():
			home_team = row['home_team_id']
			away_team = row['away_team_id']	
			date = row['date']
			min_length = min(self.params['to_last'],len(serries))
			for i in range(0,min_length):
				serry = serries[len(serries)-i-1]
				df_serry = df_league.query("serry=='%s'"%serry)
				df_home_team = df_serry.query("home_team_id==%d | away_team_id == %d"%(home_team,home_team))
				df_away_team = df_serry.query("home_team_id==%d | away_team_id == %d"%(away_team,away_team))	
				res_home = self.get_numandscore(df_home_team,home_team)
				res_away = self.get_numandscore(df_away_team,away_team)
				res_h = {}
				res_h['pre'] = i
				res_h['toteam'] = away_team
				res_h['weight_score'] = res_home['score'] - res_away['score'] 
				res_h['weight_num'] = res_home['number'] - res_away['number']
				res_dic_home = {}
				res_dic_home['team_id'] = home_team
				res_dic_home['date']= date
				res_dic_home[self.name] = res_h
				team_res.append(res_dic_home)
				res_a = {}
				res_a['pre'] = i
				res_a['toteam'] = home_team
				res_a['weight_score'] = res_away['score'] - res_home['score']
				res_a['weight_num'] = res_away['number'] - res_home['number']
				res_dic_away = {}
				res_dic_away['team_id'] = away_team
				res_dic_away['date'] = date
				res_dic_away[self.name] = res_a
				team_res.append(res_dic_away)
		return team_res

	def get_numandscore(self,df_team,team):
		res = {}
		res['score'] = 0
		res['number'] = len(df_team)
		for index,row in df_team.iterrows():
			if (row['home_team_id'] == team):
				if (row['home_goal'] > row['away_goal']):
					res['score'] += 3
				elif(row['home_goal'] < row['away_goal']):
					res['score'] += 0
				else:
					res['score'] += 1
			else:
				if (row['home_goal'] > row['away_goal']):
					res['score'] += 3
				elif(row['home_goal'] < row['away_goal']):
					res['score'] += 0
				else:
					res['score'] += 1
		return res
