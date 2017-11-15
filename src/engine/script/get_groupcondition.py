# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/..'))
import gflags
import json
import pandas as pd
import sqlite3
import codecs
from conf import *

conn = sqlite3.connect('/Users/miller/Desktop/soccer.db')

if __name__ == "__main__":
	sql_str = "select * from TMatch"
	df = pd.read_sql_query(sql_str,conn)
	df = conciseDate(df)
	leagues = df['league_id'].unique()
	f_group = codecs.open('../conf/group.txt','w+',encoding='utf-8')
	res_list = []
	for league_id in leagues:
		cond_res = {}
		cond_res['league_id'] = league_id
		cond_res['serryname'] = []
		where = "league_id=%s"%league_id
		sql_str = "select distinct serryname from Match where %s"%where
		df_serry = pd.read_sql_query(sql_str,conn)
		for i,_serry in df_serry.iterrows():
			serryname = _serry['serryname']
			cond_res['serryname'].append(serryname)
		res_list.append(cond_res)
	json.dump(res_list,f_group,cls=GenEncoder,ensure_ascii=False)
	f_group.close()
