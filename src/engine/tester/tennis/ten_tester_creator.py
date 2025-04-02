# -*- coding: utf-8 -*-
from tester import *

class Ten_Tester_Creator(object):
	def __init__(self):
		self.tester_cand = []
		f_exp = codecs.open(gflags.FLAGS.test_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			tester_ins = eval(candstr)
			self.tester_cand.append(tester_ins)

	def execute(self):
		sql_str = "select distinct season from tennis"
		seasons = pd.read_sql_query(sql_str,cont)['season'].to_numpy()
		if (gflags.FLAGS.predict):
			seasons = [max(seasons, key=str)]
		for tester in self.tester_cand:
			tester_dir = os.path.abspath(gflags.FLAGS.res_path + 'tennis/test') + '/' + tester.name
			mkdir(tester_dir)
			tester.process(seasons,tester_dir)
						
