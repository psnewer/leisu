# -*- coding: utf-8 -*-
from analysis import *

class Ten_Analysis_Creator(object):
	def __init__(self):
		self.analysis_cand = []
		f_exp = codecs.open(gflags.FLAGS.analysis_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			analysis_ins = eval(candstr)
			self.analysis_cand.append(analysis_ins)

	def execute(self):
		sql_str = "select distinct season from tennis"
		seasons = pd.read_sql_query(sql_str,cont)['season'].to_numpy()
		if (gflags.FLAGS.predict):
			seasons = [max(seasons, key=str)]
		for analysis in self.analysis_cand:
			analysis_dir = os.path.abspath(gflags.FLAGS.res_path + 'tennis/analysis') + '/' + analysis.name
			mkdir(analysis_dir)
			analysis.process(seasons,analysis_dir)
						
