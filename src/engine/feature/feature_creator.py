# -*- coding: utf-8 -*-
from feature import *

class Feature_Creator(object):
	def __init__(self):
		self.featurer_cand = []
		f_exp = codecs.open(gflags.FLAGS.feature_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			featurer_ins = eval(candstr)
			featurer_ins.setParams(cand['params'])
			self.featurer_cand.append(featurer_ins)

	def execute(self,condition):
		# condition = json.loads(condition)
		league_str = condition['league']
		league_cond = "league='%s'"%league_str
		league_dir = os.path.abspath(gflags.FLAGS.feature_path + league_str)
		mkdir(league_dir)
		if 'serryname' not in condition:
			cond = [league_cond]
			cond_str = ' and '.join(cond)
			sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
			seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
			for featurer in self.featurer_cand:
				featurer_dir = league_dir + '/' + featurer.name
				mkdir(featurer_dir)
				featurer.process(cond_str,seasons,featurer_dir)
		else:
			for serryname in condition['serryname']:
				serry_dir = league_dir+'/'+serryname
				mkdir(serry_dir)
				serry_cond = "serryname='%s'"%serryname
				cond = [league_cond,serry_cond]
				cond_str = ' and '.join(cond)
				sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
				seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
				for featurer in self.featurer_cand:
					featurer_dir = serry_dir + '/' + featurer.name
					mkdir(featurer_dir)
					featurer.process(cond_str,seasons,featurer_dir)
