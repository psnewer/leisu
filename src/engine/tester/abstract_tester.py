#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import codecs
import pandas as pd
import numpy as np

class ABSTRACT_TESTER():
	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_TESTER'

	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

	def analysis(self,condition,df_team):
		team_res = []
		cond = list(condition)
		cond_str = ' and '.join(cond)
		sql_str = "select distinct serryid from Match where %s order by date desc"%(cond_str)
		df_serry = pd.read_sql_query(sql_str,conn)
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			cond[1] = "serryid='%s'"%serryid
			res_list = []
			cond_str = ' and '.join(cond)
			sql_str = "select * from Match where %s"%(cond_str)
			df = pd.read_sql_query(sql_str,conn)
			df = conciseDate(df)
			dates = df['date'].unique()
			for date in dates:
				df_date = df.query("date=='%s'"%date)
				date_dic = {}
				date_dic['date'] = date
				date_dic['posi_home_team'] = []
				date_dic['posi_away_team'] = []
				date_dic['neg_home_team'] = []
				date_dic['neg_away_team'] = []
				date_dic['neutral_home_team'] = []
				date_dic['neutral_away_team'] = []
				date_dic['teams'] = []
				for index,row in df_date.iterrows():
					res = self.get_team_tar(row)
					if (res == 1):
						date_dic['posi_home_team'].append(row['home_team_id'])
						date_dic['posi_away_team'].append(row['away_team_id'])
					elif (res==2):
						date_dic['neg_home_team'].append(row['home_team_id'])
						date_dic['neg_away_team'].append(row['away_team_id'])
					elif (res==0):
						date_dic['neutral_home_team'].append(row['home_team_id'])
						date_dic['neutral_away_team'].append(row['away_team_id'])
					date_dic['teams'].append(row['home_team_id'])
					date_dic['teams'].append(row['away_team_id'])
				res_list.append(date_dic)
			df_date = pd.DataFrame(res_list)
			df_team_posi = df_team[['date','team_id','area']]
			df_team_posi = df_team_posi.groupby(['date','area']).apply(lambda x: list(x['team_id'])).unstack('area').reset_index(0)
			anadf = pd.merge(df_date,df_team_posi,how='inner',on='date')
			succ = 0
			fail = 0
			neu = 0
			for index,row in anadf.iterrows():
				date = row['date']
				posi_home_team = row['posi_home_team']
				posi_away_team = row['posi_away_team']
				neg_home_team = row['neg_home_team']
				neg_away_team = row['neg_away_team']
				neutral_home_team = row['neutral_home_team']
				neutral_away_team = row['neutral_away_team']
				home_teams = None
				away_teams = None
				if 1 in row:
					home_teams = row[1]
				if 2 in row:
					away_teams = row[2]
				if (self.params['lateral'] == 1):
					for idx,home_team in enumerate(posi_home_team):
						away_team = posi_away_team[idx]
						_res = {}
						_res['name'] = self.name
						_res['serryid'] = serryid
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						if home_teams is not None and home_team in home_teams:
							succ = succ + 1
							_res['res'] = 1
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
						elif away_teams is not None and away_team in away_teams:
							succ = succ + 1
							_res['res'] = 1
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
					for idx,home_team in enumerate(neg_home_team):
						away_team = neg_away_team[idx]
						_res = {}
						_res['name'] = self.name
						_res['serryid'] = serryid
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						if home_teams is not None and home_team in home_teams:
							fail = fail + 1
							_res['res'] = 2
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
						elif away_teams is not None and away_team in away_teams:
							fail = fail + 1
							_res['res'] = 2
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
					for idx,home_team in enumerate(neutral_home_team):
						away_team = neutral_away_team[idx]
						_res = {}
						_res['name'] = self.name
						_res['serryid'] = serryid
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						if (home_teams is not None and home_team in home_teams) or (away_teams is not None and away_team in away_teams):
							neu = neu + 1
							_res['res'] = 0
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
				else:
					for idx,home_team in enumerate(posi_home_team):
						away_team = posi_away_team[idx]
						_res = {}
						_res['name'] = self.name
						_res['serryid'] = serryid
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						if home_teams is not None and away_teams is not None and home_team in home_teams and away_team in away_teams:
							succ = succ + 1
							_res['res'] = 1
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
					for idx,home_team in enumerate(neg_home_team):
						away_team = neg_away_team[idx]
						_res = {}
						_res['name'] = self.name
						_res['serryid'] = serryid
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						if home_teams is not None and away_teams is not None and home_team in home_teams and away_team in away_teams:
							fail = fail + 1
							_res['res'] = 2
							team_res.append(_res)
#							_res_str = json.dumps(_res)
#							tester_log.write(_res_str + '\n')
					for idx,home_team in enumerate(neutral_home_team):
						away_team = neutral_away_team[idx]
						_res = {}
						_res['name'] = self.name
						_res['serryid'] = serryid
						_res['date'] = date
						_res['home_team'] = home_team
						_res['away_team'] = away_team
						if home_teams is not None and away_teams is not None and home_team in home_teams and away_team in away_teams:
							neu = neu + 1
							_res['res'] = 0
							team_res.append(_res)
		return team_res
	
	def get_team_tar(self,row):
		return False
