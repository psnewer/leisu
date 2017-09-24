#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class LAST_WIN_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'LAST_WIN_FEATURE'
		self.params['period']	= 5

	def execute(self,condition,action):
		if (action == 'predict'):
			return self.execute_predict(condition)
		elif (action == 'test'):
			return self.execute_test(condition)
	
	def execute_predict(self,conditon):
		f_res = open(gflags.FLAGS.res_path,'a')
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		teams = df['home_team_id'].unique()
		for team in teams:
			res_dic = {}
			res_dic['team_id'] = team
			df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
			df_team = df_team.sort_values(by='date')
			res = self.process(df_team,team)
			res_dic[self.name] = res
			res_str = json.dumps(res_dic)
			f_res.write(res_str+'\n')
		f_res.close()

	def execute_test(self,condition):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		teams = df['home_team_id'].unique()
		team_res = []
		res_test = open(gflags.FLAGS.res_test,'a+')
		for team in teams:
			df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
			df_team = df_team.sort_values(by='date')
			length = len(df_team)
			for i in range(1,length+1):
				res_dic = {}
				res_dic['team_id'] = team
				df_stage = df_team[0:i]
				res_dic['date'] = df_stage.iloc[-1]['date']
				res = self.process(df_stage,team)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic)
				res_test.write(res_str+'\n')
				team_res.append(res_dic)
		res_test.close()
		return team_res

	def process(self,df,team):
		row = df.iloc[-1]
		res = {}
		if (row['home_team_id'] == team):
			res['area'] = 1
			if (row['home_goal'] > row['away_goal']):
				res['win'] = 3
			elif (row['home_goal'] < row['away_goal']):
				res['win'] = 0
			else:
				res['win'] = 1
		elif (row['away_team_id'] == team):
			res['area'] = 2
			if (row['away_goal'] > row['away_goal']):
				res['win'] = 3
			elif (row['away_goal'] < row['away_goal']):
				res['win'] = 0
			else:
				res['win'] = 1
		return res
