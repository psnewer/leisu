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

f_exp = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/trena.conf','r',encoding='utf-8')
experiments = json.load(f_exp)
sexperiments = {}
for exp in experiments:
	flag = exp['flag']
	params = exp['params']
	sexperiments[flag] = params
f_exp.close()

posi = 0
neg = 0
walks = os.walk('/Users/miller/Documents/workspace/leisu/res/group',topdown=False)
for root,dirs,files in walks:
	if 'trent_res.txt' in files and root=='/Users/miller/Documents/workspace/leisu/res/group/521/default':
		trent_file = os.path.join(root,'trent_res.txt')
		df_trent = pd.DataFrame.from_csv(trent_file)
		if len(df_trent) == 0:
			continue
		res = {}
		for flag in sexperiments:
			res_ele = {}
			params = sexperiments[flag]
			test_ids = df_trent['test_id'].unique()
			for test_id in test_ids:
				df_testid = df_trent[df_trent['test_id']==test_id]
				experiment_ids = df_testid['experiment_id'].unique()
				for experiment_id in experiment_ids:
					df_experiment = df_testid[df_testid['experiment_id']==experiment_id]
					pres = df_experiment['pre'].unique()
					for pre in pres:
						row_pre = df_experiment[df_experiment['pre']==pre].iloc[0]
						df_pres = df_experiment[df_experiment['pre'] > pre]
						if len(df_pres) < params['num']:
							continue
						num_neg = len(df_pres[df_pres['limi_odds'] > params['neg_odds']])
						num_posi = len(df_pres) - num_neg
						if float(num_posi) / float(num_posi + num_neg) >= params['posi_rt']:
							if row_pre['limi_odds'] > params['posi_odds']:
								neg += 1
							else:
								neg += 1
								if test_id in res_ele:
									res_ele[test_id].append(experiment_id)
								else:
									res_ele[test_id] = [experiment_id]
								posi += 1
			if posi > 0 and float(posi) / float(posi + neg) < params['trena_rt']:
				res[flag] = {}
				res[flag]['te'] = res_ele
				res[flag]['limi_odds'] = params['posi_odds']
		test_final = codecs.open(os.path.join(root,'test_final.txt'),'w+',encoding='utf-8')
		json.dump(res, test_final, ensure_ascii=False)
		test_final.close()
						
			

	
