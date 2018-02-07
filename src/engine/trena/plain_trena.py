# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import gflags
import sqlite3
import os
import re
import copy
import collections 
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import codecs
import operator
import string
from dateutil.parser import parse
from conf import *

class Plain_Trena():
	def __init__(self):
		kind_data = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/kinds.txt','r',encoding='utf-8')
		kind_list = json.load(kind_data)
		f_exp = codecs.open(gflags.FLAGS.trena_path,'r',encoding='utf-8')
		experiments = json.load(f_exp)
		self.experiments = {}
		for exp in experiments:
			flag = exp['flag']
			params = exp['params']
			kinds = exp['kind']
			exps = []
			for kind in kinds:
				exps.extend(kind_list[kind])
			self.experiments[flag] = {}
			self.experiments[flag]['exps'] = exps
			self.experiments[flag]['params'] = params
		kind_data.close()
		f_exp.close()

	def execute(self,df_trent,test_final):
		res = {}
		if len(df_trent) > 0:
			for flag in self.experiments:
				res_ele = {}
				exps = self.experiments[flag]['exps']
				params = self.experiments[flag]['params']
				test_ids = df_trent['test_id'].unique()
				for test_id in test_ids:
					df_testid = df_trent[df_trent['test_id']==test_id]
					experiment_ids = df_testid['experiment_id'].unique()
					for experiment_id in experiment_ids:
						if experiment_id not in exps:
							continue
						posi = 0
						neg = 0
						df_experiment = df_testid[df_testid['experiment_id']==experiment_id]
						pres = df_experiment['pre'].unique()
						for pre in pres:
							row_pre = df_experiment[df_experiment['pre']==pre].iloc[0]
							if pre==0 and row_pre['limi_odds_with_neu'] > params['thresh_odds']:
								break
							df_pres = df_experiment[df_experiment['pre'] > pre].head(params['num'])
							if len(df_pres) < params['min_num'] or pre==0:
								continue
							num_neg = len(df_pres[df_pres['limi_odds_with_neu'] > params['neg_odds']])
							num_posi = len(df_pres) - num_neg
							if float(num_posi) / float(num_posi + num_neg) >= params['posi_rt']:
								if row_pre['limi_odds_with_neu'] > params['posi_odds']:
									neg += 1
								else:
									posi += 1
						if posi > params['posi_num'] and float(posi) / float(posi + neg) >= params['trena_rt']:
							if test_id in res_ele:
								res_ele[test_id].append(experiment_id)
							else:
								res_ele[test_id] = [experiment_id]
				if res_ele:
					res[flag] = {}
					res[flag]['te'] = res_ele
					res[flag]['limi_odds'] = params['thresh_odds']
		json.dump(res, test_final, ensure_ascii=False)
						
			

	
