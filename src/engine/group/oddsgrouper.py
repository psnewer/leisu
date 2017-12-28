# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import gflags
import codecs
import threading
from trend_creator import *
from trenf_creator import *
from trent_creator import *
from conf import *

class OddsGrouper():
	def __init__(self,tester_creator,experiments,condition):
		self.tester_creator = tester_creator
		self.experiments = experiments
		self.trend_experiments = {}
		f_exp = codecs.open(gflags.FLAGS.trend_path, 'r', encoding='utf-8')
		data_algs = json.load(f_exp)
		features = data_algs['features']
		filters = data_algs['filters']
		self.trend_creator = Trend_Creator(features)
		self.trenf_creator = Trenf_Creator(filters)
		self.trent_creator = Trent_Creator(self.trend_creator,self.trenf_creator)
		if gflags.FLAGS.test:
			trend_experiments = data_algs['experiments']
			for exp in trend_experiments:
				experiment_id = exp['experiment_id']
				algs = exp['algs']
				filters = exp['filters']
				self.trend_experiments[experiment_id] = {}
				self.trend_experiments[experiment_id]['feature'] = []
				self.trend_experiments[experiment_id]['filter'] = []
				for alg in algs:
					self.trend_experiments[experiment_id]['feature'].append(alg)
				for filter in filters:
					self.trend_experiments[experiment_id]['filter'].append(filter)
		f_exp.close()
		condition = json.loads(condition)
		league_str = str(condition['league_id'])
		self.conditions = []
		league_cond = 'league_id=' + league_str
		for serryname in condition['serryname']:
			serry_cond = "serryname='%s'"%serryname
			self.conditions.append([league_cond,serry_cond])
		self.group_directory = gflags.FLAGS.group_path + league_str
		os.system(r'mkdir %s'%self.group_directory)

	def group(self):
		self.process()
#		self.analysis()

	def process(self):
		for cond in self.conditions:
			league_id = int(cond[0].split('=')[1])
			serryname = cond[1].split('=')[1].strip('\'')
			group_directory = self.group_directory + '/' + serryname
			os.system(r'mkdir %s'%group_directory)
			feature_res = group_directory + '/feature_res.txt'
			test_res = group_directory + '/test_res.txt'
			feature_res = codecs.open(feature_res,'w+',encoding='utf-8')
			test_res = codecs.open(test_res,'w+',encoding='utf-8')
			self.tester_creator.group(cond,feature_log=feature_res)
			df = pd.DataFrame([])
			for experiment_id in self.experiments:
				exp = self.experiments[experiment_id]
				features = exp['feature']
				filters = exp['filter']
				testers = exp['tester']
				team_res = self.tester_creator.test(filters,testers,cond)
				df_res = pd.DataFrame(team_res)
				df_res['experiment_id'] = experiment_id
				df_res['league_id'] = league_id
				df_res['serryname'] = serryname
				df = df.append(df_res)
			df.to_csv(test_res)
			feature_res.close()
			test_res.close()

	def analysis(self):
		for cond in self.conditions:
			league_id = int(cond[0].split('=')[1])
			serryname = cond[1].split('=')[1].strip('\'')
			group_directory = self.group_directory + '/' + serryname
			test_res_file = group_directory + '/test_res.txt'
			trend_res_file = group_directory + '/trend_res.txt'
			test_final_file = group_directory + '/test_final.txt'
			if os.path.exists(test_res_file):
				df_test = pd.DataFrame.from_csv(test_res_file).astype({'serryid': str})
				dic_res = {}
				serryids = df_test['serryid'].unique()
				sql_str = "select distinct serryid from Match where league_id=%d and serryname='%s' order by date desc"%(league_id,serryname)
				_serryids = pd.read_sql_query(sql_str,conn)
				started = True
				first_serryid = _serryids.iloc[0][0]
				if not gflags.FLAGS.test_all:
					sql_str = "select count(*) from TMatch where league_id=%d and serryid='%s'"%(league_id,first_serryid)
					count = pd.read_sql_query(sql_str,conn).iloc[0][0]
					if count == 0:
						started = False	
				experiment_ids = df_test['experiment_id'].unique()
				for experiment_id in experiment_ids:
					df_experiment = df_test[df_test['experiment_id']==experiment_id]
					dic_res[experiment_id] = {}
					for idx,row in _serryids.iterrows():
						_serryid = row['serryid']
						pre = idx
						if not started:
							pre = pre + 1
						if _serryid in serryids:
							pre = idx
							if not started:
								pre = pre + 1
							dic_res[experiment_id][pre] = df_experiment[df_experiment['serryid']==_serryid].to_dict('record')
						else:
							dic_res[experiment_id][pre] = []
				trend_res = codecs.open(trend_res_file,'w+',encoding='utf-8')
				test_final = codecs.open(test_final_file,'w+',encoding='utf-8')
				self.trent_creator.execute(dic_res,trend_log=trend_res)
				if gflags.FLAGS.test:
					team_res = []
					for experiment_id in self.trend_experiments:
						exp = self.trend_experiments[experiment_id]
						features = exp['feature']
						filters = exp['filter']
						team_res.extend(self.trent_creator.test(filters,dic_res,test_id=experiment_id))
					df_res = pd.DataFrame(team_res)
					df_res['serryname'] = serryname
					df_res.to_csv(test_final)
				else:
					self.trent_creator.filter_experiment()
				trend_res.close()
				test_final.close()

	def getProfit(self, res_dic):
		tester = res_dic['name']
		home_team_id = res_dic['home_team']
		away_team_id = res_dic['away_team']
		date = res_dic['date']
		str_sql = "select * from Odds where home_team_id=%d and away_team_id=%d and date='%s'"%(home_team_id,away_team_id,date)
		cur.execute(str_sql)
		row = cur.fetchone()
		if row is None:
			return 0
		elif res_dic['res'] == 2 or res_dic['res'] == 0:
			return -1
		res = 0
		if tester == 'MIN_GOALS_TESTER' :
			odds = row[14]
			dapan = row[13]
			if odds > 0:
				if dapan <= '2.5':
					res = odds
				elif dapan == '2.5/3':
					res = -0.5
				elif dapan == '2/2.5':
					res = 0.5 * odds
		elif tester == 'MAY_GOALS_TESTER':
			odds = row[12]
			dapan = row[13]
			if odds > 0:
				if dapan >= '2.5':
					res = odds
				elif dapan == '2.5/3':
					res = 0.5 * odds
				elif dapan == '2/2.5':
					res = -0.5
		elif tester == 'HOME_WIN_TESTER':
			odds = row[9]
			if odds > 0:
				res = odds - 1
		elif tester == 'AWAY_WIN_TESTER':
			odds = row[11]
			if odds > 0:
				res = odds - 1
		return res
