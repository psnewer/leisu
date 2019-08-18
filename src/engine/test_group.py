# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import json
import pandas as pd
import gflags
import codecs
from conf import *

if __name__ == "__main__":
	walks = os.walk('../res/group',topdown=False)
	for root,dirs,files in walks:
		if 'group_detail.txt' in files:
			f_kind = codecs.open('./conf/kinds.txt','r',encoding='utf-8')
			kinds = json.load(f_kind)
			group_detail_file = os.path.join(root,'group_detail.txt')
			group_final_file = os.path.join(root,'group_final.txt')
			group_final = codecs.open(group_final_file,'w+',encoding='utf-8')
			df = pd.read_csv(group_detail_file)
			leagues = df['league_id'].unique()
			for league in leagues:
				df_league = df.query("league_id==%d"%league)
				serrynames = df_league['serryname'].unique()
				for serryname in serrynames:
					df_serryname = df_league[df_league.serryname==serryname]
					for kind in kinds:
						kind_name = kind['name']
						exps = kind['exps']
#						min_all = 100.0
						min_c0 = 100.0
						min_c1 = 100.0
#						min_c2 = 100.0
#						min_all_with_neu = 100.0
						min_c0_with_neu = 100.0
						min_c1_with_neu = 100.0
#						min_c2_with_neu = 100.0
#						max_allprofit = 0.0
#						max_c0profit = 0.0
#						max_c1profit = 0.0
#						max_c2profit = 0.0
						dic_res = {}
						dic_res['league_id'] = league
						dic_res['serryname'] = serryname
						dic_res['kind'] = kind_name
						dic_res['c0_profit'] = []
						dic_res['c0_p_id'] = []
						dic_res['c0_p_limi'] = []
						dic_res['c0_p_limi_with_neu'] = []
						dic_res['c0_meanProfit'] = []
						for exp in exps:
							df_exp = df_serryname.query("experiment_id==%d"%exp).sort_values(by='last_date')
#							all_limi = 0.0
#							all_limi_with_neu = 0.0
							c0_limi = 0.0
							c0_limi_with_neu = 0.0
							c1_limi = 0.0
							c1_limi_with_neu = 0.0
#							c2_limi = 0.0	
#							c2_limi_with_neu = 0.0
#							all_succ = sum(df_exp['success'])
#							all_fail = sum(df_exp['failure'])
#							all_neu = sum(df_exp['neutral'])
#							all_profit = sum(df_exp['profit'])
							current_succ0 = sum(df_exp.tail(1)['success'])
							current_fail0 = sum(df_exp.tail(1)['failure'])
							current_neu0 = sum(df_exp.tail(1)['neutral'])
							current_profit0 = sum(df_exp.tail(1)['profit'])
							c0_meanProfit = 0.0
							current_succ1 = sum(df_exp.tail(2)['success']) - current_succ0
							current_fail1 = sum(df_exp.tail(2)['failure']) - current_fail0
							current_neu1 = sum(df_exp.tail(2)['neutral']) - current_neu0
							current_profit1 = sum(df_exp.tail(2)['profit']) - current_profit0
							c1_meanProfit = 0.0
#							current_succ2 = sum(df_exp.tail(3)['success']) - current_succ0 - current_succ1
#							current_fail2 = sum(df_exp.tail(3)['failure']) - current_fail0 - current_fail1
#							current_neu2 = sum(df_exp.tail(3)['neutral']) - current_neu0 - current_neu1
#							current_profit2 = sum(df_exp.tail(3)['profit']) - current_profit0 - current_profit1
#							if (all_succ != 0):
#								all_limi = (float(all_succ) + float(all_fail))/float(all_succ)
#								all_limi_with_neu = (float(all_succ) + float(all_fail) + float(all_neu))/float(all_succ)
							if (current_succ0 != 0):
								c0_limi = (float(current_succ0) + float(current_fail0))/float(current_succ0)
								c0_limi_with_neu = (float(current_succ0) + float(current_fail0) + float(current_neu0))/float(current_succ0)
								c0_meanProfit = current_profit0/(current_succ0+current_fail0)
							if (current_succ1 != 0):
								c1_limi = (float(current_succ1) + float(current_fail1))/float(current_succ1)
								c1_limi_with_neu = (float(current_succ1) + float(current_fail1) + float(current_neu1))/float(current_succ1)
								c1_meanProfit = current_profit1/(current_succ1+current_fail1)
#							if (current_succ2 != 0):
#								c2_limi = (float(current_succ2) + float(current_fail2))/float(current_succ2)
#								c2_limi_with_neu = (float(current_succ2) + float(current_fail2) + float(current_neu2))/float(current_succ2)
#							if (all_limi != 0.0 and all_limi < min_all):
#								dic_res['all_limi_with_neu'] = all_limi_with_neu
#								min_all = all_limi
#							if (all_limi_with_neu != 0.0 and all_limi_with_neu < min_all_with_neu):
#								dic_res['all_id_with_neu'] = exp
#								dic_res['all_limi'] = all_limi
#								dic_res['all_limi_with_neu'] = all_limi_with_neu
#								min_all_with_neu = all_limi_with_neu
#							if (c2_limi != 0 and c2_limi < min_c2):
#								dic_res['c2_id'] = exp
#								min_c2 = c2_limi
#							if (c2_limi_with_neu != 0 and c2_limi_with_neu < min_c2_with_neu):
#								dic_res['c2_id_with_neu'] = exp
#								dic_res['c2_limi'] = c2_limi
#								dic_res['c2_limi_with_neu'] = c2_limi_with_neu
#								min_c2_with_neu = c2_limi_with_neu
#							if (c1_limi != 0 and c1_limi < min_c1):
#								dic_res['c1_id'] = exp
#								min_c1 = c1_limi
#							if (c1_limi_with_neu != 0 and c1_limi_with_neu < min_c1_with_neu):
#								dic_res['c1_id_with_neu'] = exp
#								dic_res['c1_limi'] = c1_limi
#								dic_res['c1_limi_with_neu'] = c1_limi_with_neu
#								min_c1_with_neu = c1_limi_with_neu
#							if (c0_limi != 0 and c0_limi < min_c0):
#								dic_res['c0_id'] = exp
#								min_c0 = c0_limi
							if (c0_limi_with_neu != 0 and c0_limi_with_neu < min_c0_with_neu):
								dic_res['c0_id_with_neu'] = exp
								dic_res['c0_limi'] = c0_limi
								dic_res['c0_limi_with_neu'] = c0_limi_with_neu
								dic_res['c0_limi_profit'] = current_profit0
								dic_res['c0_limi_meanProfit'] = c0_meanProfit
								min_c0_with_neu = c0_limi_with_neu
#							if (all_profit > max_allprofit):
#								dic_res['all_profit'] = all_profit
#								dic_res['all_p_id'] = exp
#								dic_res['all_p_limi'] = all_limi
#								dic_res['all_p_limi_with_neu'] = all_limi_with_neu
#								dic_res['all_meanProfit'] = all_profit/(all_succ+all_fail)
#								max_allprofit = all_profit
							if (current_profit0 > 0.0):
								dic_res['c0_profit'].append(current_profit0)
								dic_res['c0_p_id'].append(exp)
								dic_res['c0_p_limi'].append(c0_limi)
								dic_res['c0_p_limi_with_neu'].append(c0_limi_with_neu)
								dic_res['c0_meanProfit'].append(c0_meanProfit)
#								max_c0profit = current_profit0
#							if (current_profit1 > max_c1profit):
#								dic_res['c1_profit'] = current_profit1
#								dic_res['c1_p_id'] = exp
#								dic_res['c1_p_limi'] = c1_limi
#								dic_res['c1_p_limi_with_neu'] = c1_limi_with_neu
#								dic_res['c1_meanProift'] = current_profit1/(current_succ1+current_fail1)
#								max_c1profit = current_profit1
#							if (current_profit2 > max_c2profit):
#								dic_res['c2_profit'] = current_profit2
#								dic_res['c2_p_id'] = exp
#								dic_res['c2_p_limi'] = c2_limi
#								dic_res['c2_p_limi_with_neu'] = c2_limi_with_neu
#								dic_res['c2_meanProfit'] = current_profit2/(current_succ2+current_fail2)
#								max_c2profit = current_profit2
#						ef ('all_id_with_neu' not in dic_res):
#							dic_res['all_id_with_neu'] = 0
#						if ('c2_id_with_neu' not in dic_res):
#							dic_res['c2_id_with_neu'] = 0
#						if ('c1_id_with_neu' not in dic_res):
#							dic_res['c1_id_with_neu'] = 0
						if ('c0_id_with_neu' not in dic_res):
							dic_res['c0_id_with_neu'] = 0
#						if ('all_p_id' not in dic_res):
#							dic_res['all_p_id'] = 0
#						if ('c2_p_id' not in dic_res):
#							dic_res['c2_p_id'] = 0
#						if ('c1_p_id' not in dic_res):
#							dic_res['c1_p_id'] = 0
						if ('c0_p_id' not in dic_res):
							dic_res['c0_p_id'] = 0
						dic_res_str = json.dumps(dic_res,cls=GenEncoder,ensure_ascii=False)
						group_final.write(dic_res_str+'\n')
			f_kind.close()
			group_final.close()


