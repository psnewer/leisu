# -*- coding: utf-8 -*-
from feature import *

class Ten_Feature_Creator(object):
	def __init__(self):
		self.featurer_cand = []
		f_exp = codecs.open(gflags.FLAGS.feature_conf, 'r', encoding='utf-8')
		data_cands = json.load(f_exp)
		cands = data_cands['cands']
		for cand in cands:
			candstr = cand['name'] + '()'
			featurer_ins = eval(candstr)
			self.featurer_cand.append(featurer_ins)

	def execute(self):
		sql_str = "select distinct season from tennis"
		seasons = pd.read_sql_query(sql_str,cont)['season'].to_numpy()
		if (gflags.FLAGS.predict):
			seasons = [max(seasons, key=str)]
		for feature in self.featurer_cand:
			feature_dir = os.path.abspath(gflags.FLAGS.res_path + 'tennis/feature') + '/' + feature.name
			mkdir(feature_dir)
			feature.process(seasons,feature_dir)
