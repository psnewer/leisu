#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class PRE_VS_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'PRE_VS_FEATURE'
		self.params['to_last'] = 2
	
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
				df_vs = df_serry.query("((home_team_id==%d & away_team_id == %d) | (away_team_id==%d & home_team_id == %d)) & date < '%s'"%(home_team,away_team,home_team,away_team,date))
				res_vs = self.get_vs(df_vs,home_team,away_team,i)
				res_home = {}
				res_home['date'] = date
				res_home['team_id'] = home_team
				res_home[self.name] = res_vs[0]
				team_res.append(res_home)
				res_away = {}
				res_away['date'] = date
				res_away['team_id'] = away_team
				res_away[self.name] = res_vs[1]
				team_res.append(res_away)
		return team_res

	def get_vs(self,df_vs,home_team,away_team,pre):
		team_res = []
		_res_home = []
		_res_away = []
		for index,row in df_vs.iterrows():
			row_home_team_id = row['home_team_id']
			row_away_team_id = row['away_team_id']
			res_home = {}
			res_away = {}
			if (row_home_team_id==home_team):
				res_home['area'] = 1
				res_home['pre'] = pre
				res_home['date'] = row['date']
				res_home['goal'] = row['home_goal']
				res_home['to_team'] = row['away_team_id']
				res_away['area'] = 2
				res_away['pre'] = pre
				res_away['date'] = row['date']
				res_away['goal'] = row['away_goal']
				res_away['to_team'] = row['home_team_id']
				_res_home.append(res_home)
				_res_away.append(res_away)		
			else:
				res_home['area'] = 2
				res_home['pre'] = pre
				res_home['date'] = row['date']
				res_home['goal'] = row['away_goal']
				res_home['to_team'] = row['home_team_id']
				res_away['area'] = 1
				res_away['pre'] = pre
				res_away['date'] = row['date']
				res_away['goal'] = row['home_goal']
				res_away['to_team'] = row['away_team_id']
				_res_home.append(res_home)
				_res_away.append(res_away)
		team_res.append(_res_home)
		team_res.append(_res_away)
		return team_res
