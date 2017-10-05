#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class ONE_MAYOR_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'ONE_MAYOR_FEATURE'
	
	def execute(self,condition,action,feature_log):
		if (action == 'predict'):
			return self.execute_predict(condition)
		elif (action == 'test'):
			return self.execute_test(condition,feature_log)
			
	def execute_predict(self,condition):
		f_res = open(gflags.FLAGS.res_path,'a')
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		home_teams = df['home_team_id']
		away_teams = df['away_team_id']
		teams = home_teams.append(away_teams).unique()
		for team in teams:
			res_dic = {}
			res_dic['team_id'] = team
			df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
			df_team = df_team.sort_values(by='date')
			res = self.process(df_team)
			res_dic[self.name] = res
			res_str = json.dumps(res_dic)
			f_res.write(res_str+'\n')
		f_res.close()

	def execute_test(self,condition,feature_log):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		home_teams = df['home_team_id']
		away_teams = df['away_team_id']
		teams = home_teams.append(away_teams).unique()
		team_res = []
		for team in teams:
			df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
			df_team = df_team.sort_values(by='date')
			length = len(df_team)
			for i in range(1,length+1):
				res_dic = {}
				res_dic['team_id'] = team
				df_stage = df_team[0:i]
				res_dic['date'] = df_stage.iloc[-1]['date']
				res = self.process(df_stage)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic,cls=GenEncoder)
				feature_log.write(res_str+'\n')
				team_res.append(res_dic)
		return team_res
		

	def process(self,df):
		if len(df) < 2:
			return 0
		last_row = df.iloc[-2]
		if (last_row['home_goal'] + last_row['away_goal'] < 3):
			return 1
		else:
			return 2
			
