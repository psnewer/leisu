# -*- coding: utf-8 -*-
from filter import *

class Ten_Filter_Creator(object):
	def __init__(self):
		self.filter_cand = []
		f_exp = codecs.open(gflags.FLAGS.filter_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			filter_ins = eval(candstr)
			self.filter_cand.append(filter_ins)

	def execute(self):
		sql_str = "select distinct season from tennis"
		seasons = pd.read_sql_query(sql_str,cont)['season'].to_numpy()
		if (gflags.FLAGS.predict):
			seasons = [max(seasons, key=str)]
		for filter in self.filter_cand:
			filter_dir = os.path.abspath(gflags.FLAGS.res_path + 'tennis/filter') + '/' + filter.name
			mkdir(filter_dir)
			filter.process(seasons,filter_dir)

