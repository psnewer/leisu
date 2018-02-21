# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/Users/miller/Documents/workspace/leisu/src/engine')
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

kind_data = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/kinds.txt','r',encoding='utf-8')
kind_list = json.load(kind_data)
kind_data.close()

f_exp = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/trena.conf','r',encoding='utf-8')
experiments = json.load(f_exp)
sexperiments = {}
for exp in experiments:
	flag = exp['flag']
	params = exp['params']
	kinds = exp['kind']
	exps = []
	for kind in kinds:
		exps.extend(kind_list[kind])
	sexperiments[flag] = {}
	sexperiments[flag]['exps'] = exps
	sexperiments[flag]['params'] = params
f_exp.close()

walks = os.walk('/Users/miller/Documents/workspace/leisu/res/group',topdown=False)
for root,dirs,files in walks:
	if 'trent_res.txt' in files:
		trent_file = os.path.join(root,'trent_res.txt')
		df_trent = pd.DataFrame.from_csv(trent_file)
		if len(df_trent) == 0:
			continue
		res = {}
		for flag in sexperiments:
			res_ele = {}
			exps = sexperiments[flag]['exps']
			params = sexperiments[flag]['params']
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
		test_final = codecs.open(os.path.join(root,'test_final.txt'),'w+',encoding='utf-8')
		json.dump(res, test_final, ensure_ascii=False)
		test_final.close()
						
			

	
