# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
from common_tester import *
from conf import *

class Grouper(object):
	def __init__(self,feature_creator,tester_creator,experiments):
		self.feature_creator = feature_creator
		self.tester_creator = tester_creator
		self.experiments = experiments	

	def set_tester(self,tester_list):
		self.tester_cand = []
		for dic in tester_list:
			self.testers[dic['name']].setParams(dic['params'])
			self.tester_cand.append(dic['name'])

	def execute(self):
		self.process()
		self.analysis()

	def process(self):
		f_group = open(gflags.FLAGS.group_path, 'r')
		for row in f_group:
			cond = json.loads(row)
			for experiment_id in self.experiments:
				GlobalVar.set_experimentId(experiment_id)	
				exp = self.experiments[experiment_id]
				features = exp['feature']
				testers = exp['tester']
				self.tester_creator.feature_creator.set_features(features)
				self.tester_creator.set_tester(testers)
				self.tester_creator.execute(cond)
		f_group.close()

	def analysis(self):
		f_exp = open(gflags.FLAGS.test_final,'r')
		f_group_final = open(gflags.FLAGS.group_final, 'w+')
		res_list = []
		for row in f_exp:
			if row.startswith('{'):
				res_dic = json.loads(row)
				res_list.append(res_dic)
		df =pd.DataFrame(res_list)
		leagues = df['league_id'].unique()
		for league in leagues:
			df_league = df.query("league_id==%d"%league)
			exps = df_league['experiment_id'].unique()
			min_all = 100.0
			min_current = 100.0
			dic_res = {}
			dic_res['league_id'] = league
			for exp in exps:
				df_exp = df_league.query("experiment_id==%d"%exp).sort_values(by='last_date')
				all_succ = sum(df_exp['success'])
				all_fail = sum(df_exp['failure'])
				current_succ = sum(df_exp.tail(2)['success'])
				current_fail = sum(df_exp.tail(2)['failure'])
				if (all_succ != 0 and float(all_fail)/float(all_succ) < min_all):
					dic_res['long_id'] = exp
					dic_res['long_fail'] = all_fail
					dic_res['long_succ'] = all_succ
				if (current_succ != 0 and float(current_fail)/float(current_succ) < min_current):
					dic_res['short_id'] = exp
					dic_res['short_fail'] = current_fail
					dic_res['short_succ'] = current_succ
			if ('long_id' not in dic_res):
				dic_res['long_id'] = float('nan')
				dic_res['long_fail'] = float('nan')
				dic_res['long_succ'] = 0
			if ('short_id' not in dic_res):
				dic_res['short_id'] = float('nan')
				dic_res['short_fail'] = float('nan')
				dic_res['short_fail'] = 0
			dic_res_str = json.dumps(dic_res)
			f_group_final.write(dic_res_str+'\n')
		f_exp.close()
		f_group_final.close()
