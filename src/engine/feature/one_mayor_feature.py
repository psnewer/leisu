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
	
	def execute_predict(self,league_id,serryid,df_serry,feature_log):
		sql_str = "select * from Match where league_id=%d and serryid='%s'"%(league_id,serryid)
		df = pd.read_sql_query(sql_str,conn)
		if len(df) < 1:
			return []
		df = conciseDate(df)
		team_res = []
		for idx,row in df_serry.iterrows():
			teams = []
			teams.append(row['home_team_id'])
			teams.append(row['away_team_id'])
			date = row['date']
			for team in teams:
				df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
				df_team = df_team.sort_values(by='date')
				res_dic = {}
				res_dic['team_id'] = team
				res_dic['date'] = date
				res = self.process(df_team)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic,cls=GenEncoder)
				feature_log.write(res_str+'\n')
				team_res.append(res_dic)
		return team_res

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
			for i in range(0,length):
				res_dic = {}
				res_dic['team_id'] = team
				res_dic['date'] = df_team.iloc[i]['date']
				df_stage = df_team[0:i]
				res = self.process(df_stage)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic,cls=GenEncoder)
				feature_log.write(res_str+'\n')
				team_res.append(res_dic)
		return team_res
		

	def process(self,df):
		if len(df) < 1:
			return 0
		last_row = df.iloc[-1]
		if (last_row['home_goal'] + last_row['away_goal'] < 3):
			return 1
		else:
			return 2
			
