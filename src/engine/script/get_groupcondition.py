# -*- coding: utf-8 -*-

import sys
import os
import gflags
import json
import pandas as pd
import sqlite3

conn = sqlite3.connect('/Users/miller/Desktop/soccer.db')

if __name__ == "__main__":
	sql_str = "select id from League"
	df = pd.read_sql_query(sql_str,conn)
	f_group = open('../conf/group.txt','w+')
	for index,row in df.iterrows():
		league_id = row['id']
		where = "league_id=%s"%league_id
		sql_str = "select distinct serry from Match where %s"%where
		df_serry = pd.read_sql_query(sql_str,conn)
		for i,_serry in df_serry.iterrows():
			serry = _serry['serry']
			cond1 = "league_id=%s"%league_id
			cond2 = "serry=%s"%serry
			cond = [cond1, cond2]
			cond_str = json.dumps(cond)
			f_group.write(cond_str+'\n')
	f_group.close()
	
