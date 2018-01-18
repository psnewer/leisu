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
from plain_trena import *
from conf import *

class OddsGrouper():
	def __init__(self,tester_creator,experiments,condition):
		self.tester_creator = tester_creator
		self.experiments = experiments
		self.trend_experiments = {}
		f_exp = codecs.open(gflags.FLAGS.trend_path, 'r', encoding='utf-8')
		data_algs = json.load(f_exp)
		f_exp.close()
		features = data_algs['features']
		filters = data_algs['filters']
		self.trend_creator = Trend_Creator(features)
		self.trenf_creator = Trenf_Creator(filters)
		self.trent_creator = Trent_Creator(self.trend_creator,self.trenf_creator)
		self.plain_trena = Plain_Trena()
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
#		self.process()
		self.analysis()

	def process(self):
		for cond in self.conditions:
			league_id = int(cond[0].split('=')[1])
			serryname = cond[1].split('=')[1].strip('\'').strip(' ')
			group_directory = self.group_directory + '/' + serryname
			os.system(r'mkdir %s'%group_directory)
			if gflags.FLAGS.update:
				feature_res = group_directory + '/feature_res_0.txt'
				test_res = group_directory + '/test_res_0.txt'
			else:
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
			trend_final_file = group_directory + '/trend_final.txt'
			if os.path.exists(test_res_file):
				df_test = pd.DataFrame.from_csv(test_res_file)
				if len(df_test) == 0:
					continue
				if gflags.FLAGS.update:
					test_res_file0 = group_directory + '/test_res_0.txt'
					if not os.path.exists(test_res_file0):
						continue
					df_test_0 = pd.DataFrame.from_csv(test_res_file0)
					if len(df_test_0) > 0:
						first_serryid = df_test_0.iloc[0]['serryid']
						df_test = df_test[df_test['serryid'] != first_serryid]
						df_test = df_test.append(df_test_0)
				df_test = df_test.astype({'serryid': str, "date": str})
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
				if gflags.FLAGS.test:
					trend_res = codecs.open(trend_res_file,'w+',encoding='utf-8')
					self.trent_creator.execute(dic_res,trend_log=trend_res)
					team_res = []
					kind_trend_file = codecs.open(gflags.FLAGS.kind_trend,'r',encoding='utf-8')
					kind_trend = json.load(kind_trend_file)
					df_trena = pd.DataFrame([])
					for row in kind_trend:
						trend_experiment = row['trend_experiment']
						exp = self.trend_experiments[trend_experiment]
						features = exp['feature']
						filters = exp['filter']
						trent_res = self.trent_creator.test(filters,dic_res,test_id=trend_experiment)
						df_trent = pd.DataFrame(trent_res)
						df_trena = df_trena.append(df_trent)
					df_trena.to_csv(group_directory + '/trent_res.txt')
					test_final = codecs.open(test_final_file,'w+',encoding='utf-8')
					self.plain_trena.execute(df_trena,test_final=test_final) 
					trend_res.close()
					test_final.close()
					kind_trend_file.close()
				trend_res = codecs.open(trend_res_file,'w+',encoding='utf-8')
				self.trent_creator.predict(dic_res,trend_log=trend_res)
				test_final = codecs.open(test_final_file,'r',encoding='utf-8')
				trend_final = codecs.open(trend_final_file,'w+',encoding='utf-8')
				trena_dic = json.load(test_final)
				filtered_exps = {}
				for key in trena_dic:
					te = trena_dic[key]['te']
					trena_thresh = trena_dic[key]['limi_odds']
					for test_id_str in te: 
						test_id = int(test_id_str) 
						experiment_ids = te[test_id_str]
						exp = self.trend_experiments[test_id]
						features = exp['feature']
						filters = exp['filter']
						trenf_res = self.trent_creator.get_filtered(filters)
						if trenf_res is not None and len(trenf_res) > 0:
							filtered_experiments = trenf_res['experiment_id'].unique().tolist()
							for experiment_id in experiment_ids:
								if experiment_id in filtered_experiments:
									if experiment_id not in filtered_exps:
										filtered_exps[experiment_id] = {}
										filtered_exps[experiment_id]['limi_odds'] = trena_thresh
									elif trena_thresh < filtered_exps[experiment_id]['limi_odds']:
										filtered_exps[experiment_id]['limi_odds'] = trena_thresh
				json.dump(filtered_exps,trend_final)
				self.trent_creator.filter_experiment()
				trend_res.close()
				test_final.close()
				trend_final.close()
