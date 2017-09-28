#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class MINOR_PERIOD_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'MINOR_PERIOD_FEATURE'
		self.params = {}
		self.params['min_length'] = 2
		self.params['period']	= 5
		self.params['minrt'] = 0.6
		self.params['mayrt'] = 0.4

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

	def execute_test(self,condition):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
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
				res = self.process(df_stage)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic)
				res_test.write(res_str+'\n')
				team_res.append(res_dic)
		res_test.close()
		return team_res

	def process(self,df):
		length = len(df) - 1 
		num_minor = 0
		_length = 0
		if length < self.params['min_length']:
			return 0
		if(length < self.params['period']):
			num_minor = self.analysis(df[0:length])
			_length = length
		else:
			num_minor = self.analysis(df[length-self.params['period']:length])
			_length = self.params['period']
		res = 3
		if (num_minor >= self.params['minrt']*_length):
			res = 1
		elif (num_minor <= self.params['mayrt']*_length):
			res = 2
		return res

	def analysis(self,df):
		minor_num = 0
		mayor_num = 0
		for index, row in df.iterrows():
			if(int(row['home_goal']) + int(row['away_goal']) < 3):
				minor_num = minor_num + 1
			else:
				mayor_num = mayor_num + 1
		return minor_num
