#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import pandas as pd
import gflags
import json
import copy
import numpy as np
import csv
import h5py
from abstract_extractor import *

class GOAL_RAWALL_EXTRACTOR(ABSTRACT_EXTRACTOR):
	def __init__(self):
		self.name = 'GOAL_RAWALL_EXTRACTOR'
		self.params = {}
		self.params['length'] = 4
		self.params['levels'] = 3
		self.params['testrt'] = 0.1

	def process(self,cond,train_txt,test_txt,ext_train,ext_test):
		league_cond = cond[0]
		cond_str = ' and '.join(cond)
		sql_str = "select distinct serryid from Match where %s order by date desc"%(cond_str)
		df_serry = pd.read_sql_query(sql_str,conn)
		serry_res = self.get_serryDic(league_cond,df_serry)
		serry_feature = self.get_serryFeature(league_cond,df_serry,serry_res)
		current_serryfeature = self.get_currentSerryFeature(league_cond,df_serry,serry_res)
		match_feature = self.get_currentMatchFeature(league_cond,df_serry,serry_res)
		self.pack(league_cond,df_serry,serry_feature,current_serryfeature,match_feature,train_txt,test_txt,ext_train,ext_test)

	def get_serryFeature(self,league_cond,df_serry,serry_res):
		res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = league_cond + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			serry_dic = serry_res[serryid]
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				res[serryid][team] = {}
				team_dic = serry_res[serryid][team]
				level_res = {}
				level_res['self'] = {}
				level_res['to'] = {}
				for i in range(0,self.params['levels']):
					level_res['self'][i] = {}
					level_res['self'][i]['num'] = 0
					level_res['self'][i]['score'] = 0
					level_res['to'][i] = {}
					level_res['to'][i]['num'] = 0
					level_res['to'][i]['score'] = 0
				home_num = 0
				home_score = 0
				away_num = 0
				away_score = 0
				for date in team_dic:
					date_dic = team_dic[date]
					if date_dic['area'] == 1:
						home_num += 1
						home_score += date_dic['goal']
					elif date_dic['area'] == 2:
						away_num += 1
						away_score += date_dic['goal']
					level_dic = date_dic['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['self'][level]['num'] += 1
						level_res['self'][level]['score'] += date_dic['goal']
					to_team = date_dic['toteam']
					level_dic = serry_res[serryid][to_team][date]['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['to'][level]['num'] += 1
						level_res['to'][level]['score'] += date_dic['goal']
				num = home_num + away_num
				num_score = home_score + away_score
				if num > 0:
					mean_score = float(num_score)/num
					if home_num > 0:
						home_mean = float(home_score)/home_num
					else:
						home_mean = mean_score
					if away_num > 0:
						away_mean = float(away_score)/away_num
					else:
						away_mean = mean_score
#					for level in range(0,self.params['levels']):
#						if level_res['self'][level]['num'] > 0:
#							level_res['self'][level]['score'] = float(level_res['self'][level]['score'])/level_res['self'][level]['num']
#						else:
#							level_res['self'][level]['score'] = mean_score
#						if level_res['to'][level]['num'] > 0:
#							level_res['to'][level]['score'] = float(level_res['to'][level]['score'])/level_res['to'][level]['num']
#						else:
#							level_res['to'][level]['score'] = mean_score
					res[serryid][team]['level'] = level_res
					res[serryid][team]['home_score'] = home_score
					res[serryid][team]['home_num'] = home_num
					res[serryid][team]['away_score'] = away_score
					res[serryid][team]['away_num'] = away_num
		return res

	def get_currentSerryFeature(self,league_cond,df_serry,serry_res):
		res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = league_cond + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			serry_dic = serry_res[serryid]
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				res[serryid][team] = {}
				team_dic = serry_res[serryid][team]
				home_num = 0
				home_score = 0
				away_num = 0
				away_score = 0
				home_mean = 0.0
				away_mean = 0.0
				level_res = {}
				level_res['self'] = {}
				level_res['to'] = {}
				for i in range(0,self.params['levels']):
					level_res['self'][i] = {}
					level_res['to'][i] = {}
					level_res['self'][i]['num'] = 0
					level_res['to'][i]['num'] = 0
					level_res['self'][i]['score'] = 0
					level_res['to'][i]['score'] = 0
				_level_res = copy.deepcopy(level_res)
				dates = sorted(team_dic.keys())
				for date in dates:
					res[serryid][team][date] = {}
					res[serryid][team][date]['level'] = copy.deepcopy(_level_res)
					res[serryid][team][date]['home_score'] = home_score
					res[serryid][team][date]['home_num'] = home_num
					res[serryid][team][date]['away_score'] = away_score
					res[serryid][team][date]['away_num'] = away_num
					date_dic = team_dic[date]
					if date_dic['area'] == 1:
						home_num += 1
						home_score += date_dic['goal']
					elif date_dic['area'] == 2:
						away_num += 1
						away_score += date_dic['goal']
					level_dic = date_dic['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['self'][level]['num'] += 1
						level_res['self'][level]['score'] += date_dic['goal']
					to_team = date_dic['toteam']
					level_dic = serry_res[serryid][to_team][date]['to_feature']
					if -1 in level_dic:
						level = level_dic[-1]['mom']
						level_res['to'][level]['num'] += 1
						level_res['to'][level]['score'] += date_dic['goal']
					_level_res = copy.deepcopy(level_res)
					num = home_num + away_num
					num_score = home_score + away_score
					if num > 0:
						mean_score = float(num_score)/num
						if home_num > 0:
							home_mean = float(home_score)/home_num
						else:
							home_mean = mean_score
						if away_num > 0:
							away_mean = float(away_score)/away_num
						else:
							away_mean = mean_score
#						for level in range(0,self.params['levels']):
#							if level_res['self'][level]['num'] > 0:
#								_level_res['self'][level]['score'] = float(level_res['self'][level]['score'])/level_res['self'][level]['num']
#							else:
#								_level_res['self'][level]['score'] = mean_score
#							if level_res['to'][level]['num'] > 0:
#								_level_res['to'][level]['score'] = float(level_res['to'][level]['score'])/level_res['to'][level]['num']
#							else:
#								_level_res['to'][level]['score'] = mean_score
		return res

	def get_currentMatchFeature(self,league_cond,df_serry,serry_res):
		res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = league_cond + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			serry_dic = serry_res[serryid]
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				res[serryid][team] = {}
				df_team = df.query("home_team_id==%d | away_team_id==%d"%(team,team)).sort_values(by='date').reset_index()
				for idx,row in df_team.iterrows():
					if idx < self.params['length']:
						continue
					date = row['date']
					res[serryid][team][date] = {}
					level_res = {}
					for i in range(0,self.params['levels']):
						level_res[i] = {}
						level_res[i]['num'] = 0
						level_res[i]['score'] = 0
					home_num = 0
					home_score = 0
					away_num = 0
					away_score = 0
					for i in range(0,self.params['length']):
						torow = df_team.iloc[idx - i -1]
						todate = torow['date']
						toscore = serry_dic[team][todate]['goal']
						if torow['home_team_id'] == team:
							toteam = torow['away_team_id']
							home_num += 1
							home_score += toscore
						else:
							toteam = torow['home_team_id']
							away_num += 1
							away_score += toscore
						if i not in serry_dic[toteam][todate]['to_feature'] or serry_dic[toteam][todate]['to_feature'][i]['todate'] >= date:
							continue
						tomom = serry_dic[toteam][todate]['to_feature'][i]['mom']
						level_res[tomom]['num'] += 1
						level_res[tomom]['score'] += toscore
					num = home_num + away_num
					num_score = home_score + away_score
					if num > 0:
						mean_score = float(num_score)/num
						if home_num > 0:
							home_mean = float(home_score)/home_num
						else:
							home_mean = mean_score
						if away_num > 0:
							away_mean = float(away_score)/away_num
						else:
							away_mean = mean_score
#						for level in level_res:
#							if level_res[level]['num'] > 0:
#								level_res[level]['score'] = float(level_res[level]['score'])/level_res[level]['num']
#							else:
#								level_res[level]['score'] = mean_score
						res[serryid][team][date]['level'] = level_res
						res[serryid][team][date]['home_score'] = home_score
						res[serryid][team][date]['home_num'] = home_num
						res[serryid][team][date]['away_score'] = away_score
						res[serryid][team][date]['away_num'] = away_num
		return res		
	
	def get_serryDic(self,league_cond,df_serry):
		serry_res = {}
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			serry_res[serryid] = {}
			serry_cond = "serryid='%s'"%serryid
			cond = league_cond + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn)
			home_teams = df['home_team_id'].tolist()
			away_teams = df['away_team_id'].tolist()
			teams = set(home_teams + away_teams)
			for team in teams:
				serry_res[serryid][team] = self.get_teamInfo(df,team)
		return serry_res

	def get_teamInfo(self,df,team_id):
		df_team = df.query("home_team_id==%d | away_team_id==%d"%(team_id,team_id)).reset_index()
		df_team = df_team.sort_values(by='date')
		f1 = lambda x: 1 if x['home_team_id']==team_id else 2
		f2 = lambda x: 3 if (x['area']==1 and x['home_goal']>x['away_goal']) or (x['area']==2 and x['away_goal']>x['home_goal']) else 1 if x['home_goal']==x['away_goal'] else 0
		f3 = lambda x: 0 if x['home_goal'] + x['away_goal'] < 2.5 else 1
		df_team['area'] = df_team.apply(f1, axis=1)
		df_team['score'] = df_team.apply(f2, axis=1)
		df_team['goal'] = df_team.apply(f3, axis=1)
		res = {}
		for idx,row in df_team.iterrows():
			date = row['date']
			res[date] = {}		
			res[date]['area'] = row['area']
			res[date]['score'] = row['score']
			res[date]['goal'] = row['goal']
			if row['area'] == 1:
				res[date]['toteam'] = row['away_team_id']
			else:
				res[date]['toteam'] = row['home_team_id']
			res[date]['to_feature'] = {}
			for i in range(-1,self.params['length']):
				first = idx - self.params['length'] + 1 + i
				if first < 0:
					continue
				end = idx + i + 1
				if end > len(df_team):
					continue
				mom_score = 0
				todate = df_team.iloc[-1]['date']
				for j in range(first,end):
					torow = df_team.iloc[j]
					mom_score += torow['score']
				for level in range(0,self.params['levels']):
					if mom_score <= float(3*self.params['length'])/self.params['levels']*(level+1):
						res[date]['to_feature'][i] = {}
						res[date]['to_feature'][i]['mom'] = level
						res[date]['to_feature'][i]['todate'] = todate
						break
		return res

	def pack(self,league_cond,df_serry,serry_feature,current_serryfeature,match_feature,train_txt,test_txt,ext_train,ext_test):
		home_res = []
		away_res = []
		label_res = []
		for idx,serry in df_serry.iterrows():
			serryid = serry['serryid']
			pre_idx = idx + 1
			if pre_idx >= len(df_serry):
				continue
			pre_serryid = df_serry.iloc[pre_idx]['serryid']
			if not self.is_adjacent(serryid,pre_serryid,league_cond):
				continue
			serry_cond = "serryid='%s'"%serryid
			cond = league_cond + ' and ' + serry_cond
			sql_str = "select * from Match where %s"%cond
			df = pd.read_sql_query(sql_str,conn).sort_values(by='date')
			df['res'] = df.apply(lambda x: [0] if x['home_goal'] + x['away_goal'] < 2.5 else [1], axis=1)
			serry_match_feature = match_feature[serryid]
			serry_serry_feature = serry_feature[serryid]
			serry_current_serryfeature = current_serryfeature[serryid]
			pre_serry_feature = serry_feature[pre_serryid]
			for ind,row in df.iterrows():
				home_team_id = row['home_team_id']
				away_team_id = row['away_team_id']
				date = row['date']
				if home_team_id not in serry_match_feature or away_team_id not in serry_match_feature:
					continue
				if date not in serry_match_feature[home_team_id] or date not in serry_match_feature[away_team_id]:
					continue
				if 'level' not in serry_match_feature[home_team_id][date] or 'level' not in serry_match_feature[away_team_id][date]:
					continue
				if home_team_id not in pre_serry_feature or away_team_id not in pre_serry_feature:
					continue
				home_match_score = []
				home_match_num = []
				for i in range(0,self.params['levels']):
					home_match_score.append(serry_match_feature[home_team_id][date]['level'][i]['score'])
					home_match_num.append(serry_match_feature[home_team_id][date]['level'][i]['num'])
				home_match_score.append(serry_match_feature[home_team_id][date]['home_score'])
				home_match_num.append(serry_match_feature[home_team_id][date]['home_num'])
				home_match_score.append(serry_match_feature[home_team_id][date]['away_score'])
				home_match_num.append(serry_match_feature[home_team_id][date]['away_num'])
				away_match_score = []
				away_match_num = []
				for i in range(0,self.params['levels']):
					away_match_score.append(serry_match_feature[away_team_id][date]['level'][i]['score'])
					away_match_num.append(serry_match_feature[away_team_id][date]['level'][i]['num'])
				away_match_score.append(serry_match_feature[away_team_id][date]['home_score'])
				away_match_num.append(serry_match_feature[away_team_id][date]['home_num'])
				away_match_score.append(serry_match_feature[away_team_id][date]['away_score'])
				away_match_num.append(serry_match_feature[away_team_id][date]['away_num'])
				home_current_self_serryscore = []
				home_current_self_serrynum = []
				home_current_to_serryscore = []
				home_current_to_serrynum = []
				for i in range(0,self.params['levels']):
					home_current_self_serryscore.append(serry_current_serryfeature[home_team_id][date]['level']['self'][i]['score'])
					home_current_self_serrynum.append(serry_current_serryfeature[home_team_id][date]['level']['self'][i]['num'])
					home_current_to_serryscore.append(serry_current_serryfeature[home_team_id][date]['level']['to'][i]['score'])
					home_current_to_serrynum.append(serry_current_serryfeature[home_team_id][date]['level']['to'][i]['num'])
				home_current_self_serryscore.append(serry_current_serryfeature[home_team_id][date]['home_score'])
				home_current_self_serrynum.append(serry_current_serryfeature[home_team_id][date]['home_num'])
				home_current_to_serryscore.append(serry_current_serryfeature[home_team_id][date]['home_score'])
				home_current_to_serrynum.append(serry_current_serryfeature[home_team_id][date]['home_num'])
				home_current_self_serryscore.append(serry_current_serryfeature[home_team_id][date]['away_score'])
				home_current_self_serrynum.append(serry_current_serryfeature[home_team_id][date]['away_num'])
				home_current_to_serryscore.append(serry_current_serryfeature[home_team_id][date]['away_score'])
				home_current_to_serrynum.append(serry_current_serryfeature[home_team_id][date]['away_num'])
				away_current_self_serryscore = []
				away_current_self_serrynum = []
				away_current_to_serryscore = []
				away_current_to_serrynum = []
				for i in range(0,self.params['levels']):
					away_current_self_serryscore.append(serry_current_serryfeature[away_team_id][date]['level']['self'][i]['score'])
					away_current_self_serrynum.append(serry_current_serryfeature[away_team_id][date]['level']['self'][i]['num'])
					away_current_to_serryscore.append(serry_current_serryfeature[away_team_id][date]['level']['to'][i]['score'])
					away_current_to_serrynum.append(serry_current_serryfeature[away_team_id][date]['level']['to'][i]['num'])
				away_current_self_serryscore.append(serry_current_serryfeature[away_team_id][date]['home_score'])
				away_current_self_serrynum.append(serry_current_serryfeature[away_team_id][date]['home_num'])
				away_current_to_serryscore.append(serry_current_serryfeature[away_team_id][date]['home_score'])
				away_current_to_serrynum.append(serry_current_serryfeature[away_team_id][date]['home_num'])
				away_current_self_serryscore.append(serry_current_serryfeature[away_team_id][date]['away_score'])
				away_current_self_serrynum.append(serry_current_serryfeature[away_team_id][date]['away_num'])
				away_current_to_serryscore.append(serry_current_serryfeature[away_team_id][date]['away_score'])
				away_current_to_serrynum.append(serry_current_serryfeature[away_team_id][date]['away_num'])
				home_pre_self_score = []
				home_pre_self_num = []
				home_pre_to_score = []
				home_pre_to_num = []
				for i in range(0,self.params['levels']):
					home_pre_self_score.append(pre_serry_feature[home_team_id]['level']['self'][i]['score'])
					home_pre_self_num.append(pre_serry_feature[home_team_id]['level']['self'][i]['num'])
					home_pre_to_score.append(pre_serry_feature[home_team_id]['level']['to'][i]['score'])
					home_pre_to_num.append(pre_serry_feature[home_team_id]['level']['to'][i]['num'])
				home_pre_self_score.append(pre_serry_feature[home_team_id]['home_score'])
				home_pre_self_num.append(pre_serry_feature[home_team_id]['home_num'])
				home_pre_to_score.append(pre_serry_feature[home_team_id]['home_score'])
				home_pre_to_num.append(pre_serry_feature[home_team_id]['home_num'])
				home_pre_self_score.append(pre_serry_feature[home_team_id]['away_score'])
				home_pre_self_num.append(pre_serry_feature[home_team_id]['away_num'])
				home_pre_to_score.append(pre_serry_feature[home_team_id]['away_score'])
				home_pre_to_num.append(pre_serry_feature[home_team_id]['away_num'])
				away_pre_self_score = []
				away_pre_self_num = []
				away_pre_to_score = []
				away_pre_to_num = []
				for i in range(0,self.params['levels']):
					away_pre_self_score.append(pre_serry_feature[away_team_id]['level']['self'][i]['score'])
					away_pre_self_num.append(pre_serry_feature[away_team_id]['level']['self'][i]['num'])
					away_pre_to_score.append(pre_serry_feature[away_team_id]['level']['to'][i]['score'])
					away_pre_to_num.append(pre_serry_feature[away_team_id]['level']['to'][i]['num'])
				away_pre_self_score.append(pre_serry_feature[away_team_id]['home_score'])
				away_pre_self_num.append(pre_serry_feature[away_team_id]['home_num'])
				away_pre_to_score.append(pre_serry_feature[away_team_id]['home_score'])
				away_pre_to_num.append(pre_serry_feature[away_team_id]['home_num'])
				away_pre_self_score.append(pre_serry_feature[away_team_id]['away_score'])
				away_pre_self_num.append(pre_serry_feature[away_team_id]['away_num'])
				away_pre_to_score.append(pre_serry_feature[away_team_id]['away_score'])
				away_pre_to_num.append(pre_serry_feature[away_team_id]['away_num'])
				home_res.append([[home_match_score,home_current_self_serryscore,home_current_to_serryscore,home_pre_self_score,home_pre_to_score],[home_match_num,home_current_self_serrynum,home_current_to_serrynum,home_pre_self_num,home_pre_to_num]])
				away_res.append([[away_match_score,away_current_self_serryscore,away_current_to_serryscore,away_pre_self_score,away_pre_to_score],[away_match_num,away_current_self_serrynum,away_current_to_serrynum,away_pre_self_num,away_pre_to_num]])
				label_res.append(row['res'])
		home_res = np.array(home_res)
		away_res = np.array(away_res)
		label_res = np.array(label_res)
		rng_state = np.random.get_state()
		np.random.shuffle(home_res)
		np.random.set_state(rng_state)
		np.random.shuffle(away_res)
		np.random.set_state(rng_state)
		np.random.shuffle(label_res)
		length = len(label_res)
		test_length = int(length * self.params['testrt'])
		train_length = length - test_length
		if test_length == 0:
			return
		with h5py.File(ext_train, 'w-') as T_train:
			T_train.create_dataset("home_res", data=home_res[0:train_length])
			T_train.create_dataset("away_res", data=away_res[0:train_length])
			T_train.create_dataset("labels", data=label_res[0:train_length])	
		with h5py.File(ext_test, 'w-') as T_test:
			T_test.create_dataset("home_res", data=home_res[train_length:length])
			T_test.create_dataset("away_res", data=away_res[train_length:length])
			T_test.create_dataset("labels", data=label_res[train_length:length])
		trainTxtFile = open(train_txt, 'w+')
		testTxtFile = open(test_txt, 'w+')
		trainTxtFile.write(ext_train+'\n')
		testTxtFile.write(ext_test+'\n')
		trainTxtFile.close()
		testTxtFile.close()

	def is_adjacent(self,serryid,pre_serryid,league_cond):
		serry_cond = "serryid='%s'"%serryid
		cond = league_cond + ' and ' + serry_cond
		sql_str = "select * from Match where %s"%cond
		date = pd.read_sql_query(sql_str,conn).sort_values(by='date').iloc[-1]['date'][0:4]
		serry_cond = "serryid='%s'"%pre_serryid
		cond = league_cond + ' and ' + serry_cond
		sql_str = "select * from Match where %s"%cond
		pre_date = pd.read_sql_query(sql_str,conn).sort_values(by='date').iloc[-1]['date'][0:4]
		delta = int(date) - int(pre_date)
		if delta >= 2:
			return False
		else:
			return True
