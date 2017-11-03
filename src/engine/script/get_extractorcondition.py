# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/..'))
import gflags
import codecs
import json
import pandas as pd
import sqlite3
from conf import *

conn = sqlite3.connect('/Users/miller/Desktop/soccer.db')

if __name__ == "__main__":
	sql_str = "select * from Match"
	df = pd.read_sql_query(sql_str,conn)
	df = conciseDate(df)
	leagues = df['league_id'].unique()
	f_group = codecs.open('../conf/extract.txt','w+',encoding='utf-8')
	res_list = []
	for league_id in leagues:
		cond_res = {}
		cond_res['league_id'] = league_id
		cond_res['serryname'] = []
		where = "league_id=%s"%league_id
		sql_str = "select serryid,serryname,count(*) as count from Match where %s group by serryid"%where
		df_serry = pd.read_sql_query(sql_str,conn)
		for i,_serry in df_serry.iterrows():
			serryid = _serry['serryid']
			serryname = _serry['serryname']
			count = _serry['count']
			where = "league_id=%s and serryid=%s"%(league_id,serryid)
			sql_str = "select distinct home_team_id from Match where %s"%where
			teams = pd.read_sql_query(sql_str,conn)
			team_cnt = len(teams)
			mean_match = float(count)/float(team_cnt)
			if mean_match > 4:
				cond_res['serryname'].append(serryname)
		cond_res['serryname'] = list(set(cond_res['serryname']))
		if cond_res['serryname'] != []:
			res_list.append(cond_res)
	json.dump(res_list,f_group,cls=GenEncoder,ensure_ascii=False)
	f_group.close()
