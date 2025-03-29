# -*- coding: utf-8 -*-
from analysis import *

class Analysis_Creator(object):
	def __init__(self):
		self.analysis_cand = []
		f_exp = codecs.open(gflags.FLAGS.analysis_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			analysis_ins = eval(candstr)
			analysis_ins.setParams(cand['params'])
			self.analysis_cand.append(analysis_ins)

	def execute(self,condition):
		# condition = json.loads(condition)
		league_str = condition['league']
		league_cond = "league='%s'"%league_str
		league_dir = os.path.abspath(gflags.FLAGS.res_path + 'soccer/analysis/' + league_str)
		mkdir(league_dir)
		if 'serryname' not in condition:
			cond = [league_cond]
			cond_str = ' and '.join(cond)
			sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
			seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
			for analysis in self.analysis_cand:
				analysis_dir = league_dir + '/' + analysis.name
				mkdir(analysis_dir)
				analysis.process(cond_str,seasons,analysis_dir)
		else:
			for serryname in condition['serryname']:
				serry_dir = league_dir+'/'+serryname
				mkdir(serry_dir)
				serry_cond = "serryname='%s'"%serryname
				cond = [league_cond,serry_cond]
				cond_str = ' and '.join(cond)
				sql_str = "select distinct season from matches where %s order by date desc"%(cond_str)
				seasons = pd.read_sql_query(sql_str,conn)['season'].to_numpy()
				for analysis in self.analysis_cand:
					analysis_dir = serry_dir + '/' + analysis.name
					mkdir(analysis_dir)
					analysis.process(cond_str,seasons,analysis_dir)
						
