#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
from abstract_feature import *

class RANK_STATUS_FEATURE(ABSTRACT_FEATURE):
	def __init__(self):
		self.name = 'RANK_STATUS_FEATURE'
	
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
		dates = df['date'].unique()
		f_res = open(gflags.FLAGS.res_path,'a+')
		for date in dates:
			res_date = self.season_rank(condition,date)
			df_date = df.query("date=='%s'"%date)
			res = self.process(df_date,res_date)
			for _res in res:
				res_str = json.dumps(_res)
				res_test.write(_res+'\n')
		f_res.close()

	def execute_test(self,condition):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df = conciseDate(df)
		dates = df['date'].unique()
		team_res = []
		res_test = open(gflags.FLAGS.res_test,'a+')
		for date in dates:
			res_date = self.season_rank(condition,date)
			df_date = df.query("date=='%s'"%date)
			res = self.process(df_date,res_date)
			for _res in res:
				res_str = json.dumps(_res)
				res_test.write(res_str+'\n')
				team_res.append(_res)
		res_test.close()
		return team_res

	def process(self,df_date,res_date):
		team_res = []
		for index,row in df_date.iterrows():
			home_team = row['home_team_id']
			away_team = row['away_team_id']	
			date = row['date']
			home_team_score = 0
			away_team_score = 0
			home_team_number = 0
			away_team_number = 0
			if home_team in res_date:
				home_team_score = res_date[home_team]['score']
				home_team_number = res_date[home_team]['number']
			if away_team in res_date:
				away_team_score = res_date[away_team]['score']
				away_team_number = res_date[away_team]['number']
			res_home = {}
			res_h = {}
			res_h['toteam'] = away_team
			res_h['weight_score'] = home_team_score - away_team_score 
			res_h['weight_num'] = home_team_number - away_team_number
			res_home['team_id'] = home_team
			res_home['date'] = date
			res_home[self.name] = res_h
			team_res.append(res_home)
			res_away = {}
			res_a = {}
			res_a['toteam'] = home_team
			res_a['weight_score'] = away_team_score - home_team_score
			res_a['weight_num'] = away_team_number - home_team_number
			res_away['team_id'] = away_team
			res_away['date'] = date
			res_away[self.name] = res_a
			team_res.append(res_away)
		return team_res

	def season_rank(self,condition,date):
		cond_str = ' and '.join(condition)
		sql_str = "select * from Match where %s"%(cond_str)
		df = pd.read_sql_query(sql_str,conn)
		df_dates = df.query("date<'%s'"%date)
		res_dic = {}
		for index,row in df_dates.iterrows():
			home_team = row['home_team_id']
			away_team = row['away_team_id']
			if home_team not in res_dic:
				res_dic[home_team] = {}
				res_dic[home_team]['score'] = 0
				res_dic[home_team]['number'] = 0
			if away_team not in res_dic:
				res_dic[away_team] = {}
				res_dic[away_team]['score'] = 0
				res_dic[away_team]['number'] = 0
			res_dic[home_team]['number'] += 1
			res_dic[away_team]['number'] += 1
			if row['home_goal'] > row['away_goal']:
				res_dic[home_team]['score'] += 3
				res_dic[away_team]['score'] += 0
			elif row['home_goal'] < row['away_goal']:
				res_dic[home_team]['score'] += 0
				res_dic[away_team]['score'] += 3
			else:
				res_dic[home_team]['score'] += 1
				res_dic[away_team]['score'] += 1
		return res_dic
