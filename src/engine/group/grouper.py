# -*- coding: utf-8 -*-

import json
import pandas as pd
import gflags
import codecs
from min_goals_tester import *
from conf import *

class Grouper(object):
	def __init__(self,feature_creator,tester_creator,experiments):
		self.feature_creator = feature_creator
		self.tester_creator = tester_creator
		self.experiments = experiments	
		group_final = open(gflags.FLAGS.group_final,'w+')
		group_final.close()
		group_detail = open(gflags.FLAGS.group_detail,'w+')
		group_detail.close()

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
				filters = exp['filter']
				testers = exp['tester']
				self.tester_creator.feature_creator.set_features(features)
				self.tester_creator.filter_creator.set_filters(filters)
				self.tester_creator.set_tester(testers)
				self.tester_creator.execute(cond)
		f_group.close()

	def analysis(self):
		f_exp = open(gflags.FLAGS.test_final,'r')
		f_group_final = codecs.open(gflags.FLAGS.group_final, 'a+', encoding='utf-8')
		res_list = []
		for row in f_exp:
			if row.startswith('{'):
				res_dic = json.loads(row)
				res_list.append(res_dic)
		df =pd.DataFrame(res_list)[['league_id','serryname','last_date','experiment_id','success','failure']]
		df.to_csv(gflags.FLAGS.group_detail,encoding='utf-8')
		leagues = df['league_id'].unique()
		for league in leagues:
			df_league = df.query("league_id==%d"%league)
			serrynames = df_league['serryname'].unique()
			for serryname in serrynames:
				df_serryname = df_league[df_league.serryname==serryname]
				exps = df_serryname['experiment_id'].unique()
				min_all = 100.0
				min_c0 = 100.0
				min_c1 = 100.0
				min_c2 = 100.0
				dic_res = {}
				dic_res['league_id'] = league
				dic_res['serryname'] = serryname
				for exp in exps:
					df_exp = df_serryname.query("experiment_id==%d"%exp).sort_values(by='last_date')
					all_succ = sum(df_exp['success'])
					all_fail = sum(df_exp['failure'])
					current_succ0 = sum(df_exp.tail(1)['success'])
					current_fail0 = sum(df_exp.tail(1)['failure'])
					current_succ1 = sum(df_exp.tail(2)['success']) - current_succ0
					current_fail1 = sum(df_exp.tail(2)['failure']) - current_fail0
					current_succ2 = sum(df_exp.tail(3)['success']) - current_succ0
					current_fail2 = sum(df_exp.tail(3)['failure']) - current_fail0
					if (all_succ != 0 and float(all_fail)/float(all_succ) < min_all):
						dic_res['all_id'] = exp
						dic_res['all_fail'] = all_fail
						dic_res['all_succ'] = all_succ
						min_all = float(all_fail)/float(all_succ)
					if (current_succ2 != 0 and float(current_fail2)/float(current_succ2) < min_c2):
						dic_res['c2_id'] = exp
						dic_res['c2_fail'] = current_fail2
						dic_res['c2_succ'] = current_succ2
						min_c2 = float(current_fail2)/float(current_succ2)
					if (current_succ1 != 0 and float(current_fail1)/float(current_succ1) < min_c1):
						dic_res['c1_id'] = exp
						dic_res['c1_fail'] = current_fail1
						dic_res['c1_succ'] = current_succ1
						min_c1 = float(current_fail1)/float(current_succ1)
					if (current_succ0 != 0 and float(current_fail0)/float(current_succ0) < min_c0):
						dic_res['c0_id'] = exp
						dic_res['c0_fail'] = current_fail0
						dic_res['c0_succ'] = current_succ0
						min_c0 = float(current_fail0)/float(current_succ0)
				if ('all_id' not in dic_res):
					dic_res['all_id'] = 0
					dic_res['all_fail'] = 0
					dic_res['all_succ'] = 0
				if ('c2_id' not in dic_res):
					dic_res['c2_id'] = 0
					dic_res['c2_succ'] = 0
					dic_res['c2_fail'] = 0
				if ('c1_id' not in dic_res):
					dic_res['c1_id'] = 0
					dic_res['c1_succ'] = 0
					dic_res['c1_fail'] = 0
				if ('c0_id' not in dic_res):
					dic_res['c0_id'] = 0
					dic_res['c0_succ'] = 0
					dic_res['c0_fail'] = 0
				dic_res_str = json.dumps(dic_res,ensure_ascii=False)
				f_group_final.write(dic_res_str+'\n')
		f_exp.close()
		f_group_final.close()
