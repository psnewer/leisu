# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#sys.path.append('../../../../workspace/caffe/python')
#import caffe
import gflags
from conf import *
from processor import *

gflags.DEFINE_string('db_path', '', 'db path')
gflags.DEFINE_string('alg_path', '', 'server.cfg')  
gflags.DEFINE_string('experiment_path', '', 'experiment.conf')
gflags.DEFINE_string('experiment_fix', '', 'fix_experiment.conf')
gflags.DEFINE_string('trend_path', '', 'trend.conf')
gflags.DEFINE_string('trena_path', '', 'trena.conf')
gflags.DEFINE_string('strena_path', '', 'strena.conf')
gflags.DEFINE_string('trena_thresh', '', 'trena_thresh.conf')
gflags.DEFINE_string('extract_conf', '', 'extract.conf')
gflags.DEFINE_string('fix_leagues', '', 'fix leagues')
gflags.DEFINE_bool('predict', False, 'predict')
gflags.DEFINE_bool('predict_fix', False, 'predict fix')
gflags.DEFINE_bool('group', False, 'group')
gflags.DEFINE_bool('test', False, 'test')
gflags.DEFINE_bool('update', False, 'update')
gflags.DEFINE_bool('test_all', False, 'test_all')
gflags.DEFINE_bool('trend_test', False, 'trend test')
gflags.DEFINE_bool('plot', False, 'plot')
gflags.DEFINE_bool('with_neu', False, 'with neutral')
gflags.DEFINE_bool('extract', False, 'extract')
gflags.DEFINE_string('group_path', '', 'group path')
gflags.DEFINE_string('extract_path', '', 'extract path')
gflags.DEFINE_string('predict_path', '', 'predict path')
gflags.DEFINE_string('predict_summary', '', 'predict summary')
gflags.DEFINE_string('predict_s_summary', '', 'predict sell summary')
gflags.DEFINE_string('league_cond', '', 'league cond')
gflags.DEFINE_string('kind_file', '', 'kind file')
gflags.DEFINE_string('kind_trend', '', 'kind trend')
gflags.DEFINE_string('rt_path', '', 'rt file')
gflags.DEFINE_string('prototxt', '', 'caffe proto')
gflags.DEFINE_string('weights', '', 'caffe weights')
gflags.DEFINE_string('date_thresh', '', 'date thresh')
Flags = gflags.FLAGS

if __name__ == "__main__":
	Flags(sys.argv)
	pro = Processor()
	if (gflags.FLAGS.predict):
		pro.predict()
	elif (gflags.FLAGS.predict_fix):
		pro.predict_fix()
	elif (gflags.FLAGS.group):
		pro.group()
	elif (gflags.FLAGS.extract):
		pro.extract()
	elif (gflags.FLAGS.plot):
		pro.plot()
	elif (gflags.FLAGS.trend_test):
		pro.trend_test()
	else:
		pro.process()
	pro.close()
