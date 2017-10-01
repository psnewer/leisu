#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class CURRENT_MATCH_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'CURRENT_MATCH_FEATURE'
		self.params = {}
		self.params['period']	= 5

	def execute(self,condition,action):
		if (action == 'predict'):
			return self.execute_predict(condition)
		elif (action == 'test'):
			return self.execute_test(condition)
	
	def execute_predict(self,condition):
		f_res = open(gflags.FLAGS.res_path,'a')
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		home_teams = df['home_team_id']
		away_teams = df['away_team_id']
		teams = home_teams.append(away_teams).unique()
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
		if len(df) < 1:
			return []
		df = conciseDate(df)
		home_teams = df['home_team_id']
		away_teams = df['away_team_id']
		teams = home_teams.append(away_teams).unique()
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
#				res_str = json.dumps(res_dic)
#				res_test.write(res_str+'\n')
				team_res.append(res_dic)
		res_test.close()
		return team_res

	def process(self,df,team):
		length = len(df)
		last = max(-1,length - 2 - self.params['period'])
		res = {}
		for idx in range(length-2,last,-1):
			row = df.iloc[idx]
			pre = length -1 - idx
			res[pre] = {}
			if (row['home_team_id'] == team):
				res[pre]['area'] = 1
				res[pre]['toteam'] = row['away_team_id']
				res[pre]['home_goal'] = row['home_goal']
				res[pre]['away_goal'] = row['away_goal']
			elif (row['away_team_id'] == team):
				res[pre]['area'] = 2
				res[pre]['toteam'] = row['home_team_id']
				res[pre]['home_goal'] = row['home_goal']
				res[pre]['away_goal'] = row['away_goal']
		return res
