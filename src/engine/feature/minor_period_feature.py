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
				if team == row['home_team_id']:
					res_dic['area'] = 1
					res_dic['toteam'] = row['away_team_id']
				else:
					res_dic['area'] = 2
					res_dic['toteam'] = row['home_team_id']
				res = self.process(df_team)
				res_dic[self.name] = res
				res_str = json.dumps(res_dic,cls=GenEncoder)
				feature_log.write(res_str+'\n')
				team_res.append(res_dic)
		return team_res

	def execute_test(self,condition,feature_log):
		cond = list(condition)
		cond_str = ' and '.join(cond)
		sql_str = "select distinct serryid from Match where %s order by date desc"%(cond_str)
		df_serry = pd.read_sql_query(sql_str,conn)
		if gflag.FLAGS.update:
			df_serry = df_serry.head(1)
		team_res = []
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			cond[1] = "serryid='%s'"%serryid
			cond_str = ' and '.join(cond)
			sql_str = "select * from Match where %s"%(cond_str)
			df = pd.read_sql_query(sql_str,conn)
			df = conciseDate(df)
			home_teams = df['home_team_id']
			away_teams = df['away_team_id']
			teams = home_teams.append(away_teams).unique()
			for team in teams:
				df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team))
				df_team = df_team.sort_values(by='date')
				length = len(df_team)
				for i in range(0,length):
					row = df_team.iloc[i]
					res_dic = {}
					res_dic['team_id'] = team
					res_dic['date'] = row['date']
					if team == row['home_team_id']:
						res_dic['area'] = 1
						res_dic['toteam'] = row['away_team_id']
					else:
						res_dic['area'] = 2
						res_dic['toteam'] = row['home_team_id']
					df_stage = df_team[0:i]
					res = self.process(df_stage)
					res_dic[self.name] = res
					res_str = json.dumps(res_dic,cls=GenEncoder)
					feature_log.write(res_str+'\n')
					team_res.append(res_dic)
		return team_res

	def process(self,df):
		length = len(df) 
		num_minor = 0
		_length = 0
		if length < self.params['min_length']:
			return 0
		if(length < self.params['period']):
			num_minor = self.analysis(df)
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
