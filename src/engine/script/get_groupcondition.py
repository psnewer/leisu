# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/..'))
import gflags
import json
import pandas as pd
import sqlite3
from conf import *

conn = sqlite3.connect('/Users/miller/Desktop/soccer.db')

if __name__ == "__main__":
	sql_str = "select id from League"
	df = pd.read_sql_query(sql_str,conn)
	f_group = open('../conf/group.txt','w+')
	res_list = []
	for index,row in df.iterrows():
		cond_res = {}
		league_id = row['id']
		cond_res['league_id'] = league_id
		cond_res['serryid'] = []
		where = "league_id=%s"%league_id
		sql_str = "select distinct serryid from Match where %s"%where
		df_serry = pd.read_sql_query(sql_str,conn)
		for i,_serry in df_serry.iterrows():
			serry = _serry['serryid']
			cond_res['serryid'].append(serry)
		res_list.append(cond_res)
	json.dump(res_list,f_group,cls=GenEncoder)
	f_group.close()
	
