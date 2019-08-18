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
		kind_data = codecs.open('./conf/kinds.txt','r',encoding='utf-8')
		kind_list = json.load(kind_data)
		bs_data = codecs.open('./conf/bs.txt','r',encoding='utf-8')
		bs_list = json.load(bs_data)
		self.test_ids = bs_list['buy']
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
		bs_data.close()
		f_exp.close()

	def execute(self,df_trent,test_final):
		res = {}
		if len(df_trent) > 0:
			test_ids = df_trent['test_id'].unique()
			for flag in sorted(self.experiments.keys()):
				res[flag] = {}
				exps = self.experiments[flag]['exps']
				params = self.experiments[flag]['params']
				for test_id in test_ids:
					if test_id not in self.test_ids:
						continue
					res[flag][test_id] = {}	
					df_testid = df_trent[df_trent['test_id']==test_id]
					experiment_ids = df_testid['experiment_id'].unique()
					for experiment_id in experiment_ids:
						if experiment_id not in exps:
							continue
						limi_odds_with_neu = 100.0
						df_experiment = df_testid[df_testid['experiment_id']==experiment_id]
						pres = df_experiment['pre'].unique()
						pres.sort()
						num = 0
						num_one = 0
						for pre in pres:
							row_pre = df_experiment[df_experiment['pre']==pre].iloc[0]
							if pre==0:
								continue
							odds = row_pre['limi_odds_with_neu']
							if odds < limi_odds_with_neu:
								if odds == 1.0:
									if num_one > 0:
										limi_odds_with_neu = odds
									num_one = num_one + 1
								else:
									limi_odds_with_neu = odds
							num = num + 1
							if num >= params['num']:
								break
							res[flag][test_id][experiment_id] = limi_odds_with_neu
		json.dump(res, test_final, ensure_ascii=False)
						
			

	
