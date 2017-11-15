# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import gflags
import codecs
import threading
from min_goals_tester import *
from conf import *

class OddsGrouper():
	def __init__(self,tester_creator,experiments,condition):
		self.tester_creator = tester_creator
		self.experiments = experiments
		condition = json.loads(condition)
		league_str = str(condition['league_id'])
		self.conditions = []
		league_cond = 'league_id=' + league_str
		for serryname in condition['serryname']:
			serry_cond = "serryname='%s'"%serryname
			self.conditions.append([league_cond,serry_cond])
		group_directory = gflags.FLAGS.group_path + league_str
		os.system(r'mkdir %s'%group_directory)
		self.group_detail = group_directory + '/group_detail.txt'
		self.group_final = group_directory + '/group_final.txt'
		self.feature_res = group_directory + '/feature_res.txt'
		self.test_res = group_directory + '/test_res.txt'

	def group(self):
		self.process()
		self.analysis()

	def process(self):
		feature_res = codecs.open(self.feature_res,'w+',encoding='utf-8')
		test_res = codecs.open(self.test_res,'w+',encoding='utf-8')
		for cond in self.conditions:
			self.tester_creator.group(cond,feature_log=feature_res)
			for experiment_id in self.experiments:
				GlobalVar.set_experimentId(experiment_id)
				exp = self.experiments[experiment_id]
				features = exp['feature']
				filters = exp['filter']
				testers = exp['tester']
				self.tester_creator.test(filters,testers,cond,tester_log=test_res)
		feature_res.close()
		test_res.close()

	def analysis(self):
		f_kind = codecs.open(gflags.FLAGS.kind_file,'r',encoding='utf-8')
		test_res = codecs.open(self.test_res,'r',encoding='utf-8')
		kinds = json.load(f_kind)
		res_list = []
		profit = 0.0
		for row in test_res:
			res_dic = json.loads(row)
			if 'experiment_id' in res_dic:
				res_dic['profit'] = profit
				profit = 0.0
				res_list.append(res_dic)
			else:
				profit += self.getProfit(res_dic)
		if len(res_list) > 0:
			group_final = codecs.open(self.group_final,'w+',encoding='utf-8')
			df = pd.DataFrame(res_list)[['league_id','serryname','last_date','experiment_id','success','failure','neutral','profit']]
			df.to_csv(self.group_detail,encoding='utf-8')
			leagues = df['league_id'].unique()
			for league in leagues:
				df_league = df.query("league_id==%d"%league)
				serrynames = df_league['serryname'].unique()
				for serryname in serrynames:
					df_serryname = df_league[df_league.serryname==serryname]
					for kind in kinds:
						kind_name = kind['name']
						exps = kind['exps']
						min_all = 100.0
						min_c0 = 100.0
						min_c1 = 100.0
						min_c2 = 100.0
						min_all_with_neu = 100.0
						min_c0_with_neu = 100.0
						min_c1_with_neu = 100.0
						min_c2_with_neu = 100.0
						max_allprofit = 0.0
						max_c0profit = 0.0
						max_c1profit = 0.0
						max_c2profit = 0.0
						dic_res = {}
						dic_res['league_id'] = league
						dic_res['serryname'] = serryname
						dic_res['kind'] = kind_name
						for exp in exps:
							df_exp = df_serryname.query("experiment_id==%d"%exp).sort_values(by='last_date')
							all_limi = 0.0
							all_limi_with_neu = 0.0
							c0_limi = 0.0
							c0_limi_with_neu = 0.0
							c1_limi = 0.0
							c1_limi_with_neu = 0.0
							c2_limi = 0.0	
							c2_limi_with_neu = 0.0
							all_succ = sum(df_exp['success'])
							all_fail = sum(df_exp['failure'])
							all_neu = sum(df_exp['neutral'])
							all_profit = sum(df_exp['profit'])
							current_succ0 = sum(df_exp.tail(1)['success'])
							current_fail0 = sum(df_exp.tail(1)['failure'])
							current_neu0 = sum(df_exp.tail(1)['neutral'])
							current_profit0 = sum(df_exp.tail(1)['profit'])
							current_succ1 = sum(df_exp.tail(2)['success']) - current_succ0
							current_fail1 = sum(df_exp.tail(2)['failure']) - current_fail0
							current_neu1 = sum(df_exp.tail(2)['neutral']) - current_neu0
							current_profit1 = sum(df_exp.tail(2)['profit']) - current_profit0
							current_succ2 = sum(df_exp.tail(3)['success']) - current_succ0 - current_succ1
							current_fail2 = sum(df_exp.tail(3)['failure']) - current_fail0 - current_fail1
							current_neu2 = sum(df_exp.tail(3)['neutral']) - current_neu0 - current_neu1
							current_profit2 = sum(df_exp.tail(3)['profit']) - current_profit0 - current_profit1
							if (all_succ != 0):
								all_limi = (float(all_succ) + float(all_fail))/float(all_succ)
								all_limi_with_neu = (float(all_succ) + float(all_fail) + float(all_neu))/float(all_succ)
							if (current_succ0 != 0):
								c0_limi = (float(current_succ0) + float(current_fail0))/float(current_succ0)
								c0_limi_with_neu = (float(current_succ0) + float(current_fail0) + float(current_neu0))/float(current_succ0)
							if (current_succ1 != 0):
								c1_limi = (float(current_succ1) + float(current_fail1))/float(current_succ1)
								c1_limi_with_neu = (float(current_succ1) + float(current_fail1) + float(current_neu1))/float(current_succ1)
							if (current_succ2 != 0):
								c2_limi = (float(current_succ2) + float(current_fail2))/float(current_succ2)
								c2_limi_with_neu = (float(current_succ2) + float(current_fail2) + float(current_neu2))/float(current_succ2)
	#						if (all_limi != 0.0 and all_limi < min_all):
	#							dic_res['all_limi_with_neu'] = all_limi_with_neu
	#							min_all = all_limi
							if (all_limi_with_neu != 0.0 and all_limi_with_neu < min_all_with_neu):
								dic_res['all_id_with_neu'] = exp
								dic_res['all_limi'] = all_limi
								dic_res['all_limi_with_neu'] = all_limi_with_neu
								min_all_with_neu = all_limi_with_neu
	#						if (c2_limi != 0 and c2_limi < min_c2):
	#							dic_res['c2_id'] = exp
	#							min_c2 = c2_limi
							if (c2_limi_with_neu != 0 and c2_limi_with_neu < min_c2_with_neu):
								dic_res['c2_id_with_neu'] = exp
								dic_res['c2_limi'] = c2_limi
								dic_res['c2_limi_with_neu'] = c2_limi_with_neu
								min_c2_with_neu = c2_limi_with_neu
	#						if (c1_limi != 0 and c1_limi < min_c1):
	#							dic_res['c1_id'] = exp
	#							min_c1 = c1_limi
							if (c1_limi_with_neu != 0 and c1_limi_with_neu < min_c1_with_neu):
								dic_res['c1_id_with_neu'] = exp
								dic_res['c1_limi'] = c1_limi
								dic_res['c1_limi_with_neu'] = c1_limi_with_neu
								min_c1_with_neu = c1_limi_with_neu
	#						if (c0_limi != 0 and c0_limi < min_c0):
	#							dic_res['c0_id'] = exp
	#							min_c0 = c0_limi
							if (c0_limi_with_neu != 0 and c0_limi_with_neu < min_c0_with_neu):
								dic_res['c0_id_with_neu'] = exp
								dic_res['c0_limi'] = c0_limi
								dic_res['c0_limi_with_neu'] = c0_limi_with_neu
								min_c0_with_neu = c0_limi_with_neu
							if (all_profit > max_allprofit):
								dic_res['all_profit'] = all_profit
								dic_res['all_p_id'] = exp
								dic_res['all_p_limi'] = all_limi
								dic_res['all_p_limi_with_neu'] = all_limi_with_neu
								dic_res['all_meanPorift'] = all_profit/(all_succ+all_fail)
								max_allprofit = all_profit
							if (current_profit0 > max_c0profit):
								dic_res['c0_profit'] = current_profit0
								dic_res['c0_p_id'] = exp
								dic_res['c0_p_limi'] = c0_limi
								dic_res['c0_p_limi_with_neu'] = c0_limi_with_neu
								dic_res['c0_meanPorift'] = current_profit0/(current_succ0+current_fail0)
								max_c0profit = current_profit0
							if (current_profit1 > max_c1profit):
								dic_res['c1_profit'] = current_profit1
								dic_res['c1_p_id'] = exp
								dic_res['c1_p_limi'] = c1_limi
								dic_res['c1_p_limi_with_neu'] = c1_limi_with_neu
								dic_res['c1_meanPorift'] = current_profit1/(current_succ1+current_fail1)
								max_c1profit = current_profit1
							if (current_profit2 > max_c2profit):
								dic_res['c2_profit'] = current_profit2
								dic_res['c2_p_id'] = exp
								dic_res['c2_p_limi'] = c2_limi
								dic_res['c2_p_limi_with_neu'] = c2_limi_with_neu
								dic_res['c2_meanPorift'] = current_profit2/(current_succ2+current_fail2)
								max_c2profit = current_profit2
						if ('all_id_with_neu' not in dic_res):
							dic_res['all_id_with_neu'] = 0
						if ('c2_id_with_neu' not in dic_res):
							dic_res['c2_id_with_neu'] = 0
						if ('c1_id_with_neu' not in dic_res):
							dic_res['c1_id_with_neu'] = 0
						if ('c0_id_with_neu' not in dic_res):
							dic_res['c0_id_with_neu'] = 0
						if ('all_p_id' not in dic_res):
							dic_res['all_p_id'] = 0
						if ('c2_p_id' not in dic_res):
							dic_res['c2_p_id'] = 0
						if ('c1_p_id' not in dic_res):
							dic_res['c1_p_id'] = 0
						if ('c0_p_id' not in dic_res):
							dic_res['c0_p_id'] = 0
						dic_res_str = json.dumps(dic_res,cls=GenEncoder,ensure_ascii=False)
						group_final.write(dic_res_str+'\n')
			group_final.close()
		f_kind.close()
		test_res.close()

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
