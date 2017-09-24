# -*- coding: utf-8 -*-

import sys
import gflags
from conf import *
from processor import *

gflags.DEFINE_string('db_path', '', 'db path')
gflags.DEFINE_string('alg_path', '', 'server.cfg')  
gflags.DEFINE_string('experiment_path', '', 'experiment.conf')
gflags.DEFINE_string('buckets_path', '', 'bucket.conf')
gflags.DEFINE_string('res_path', '', 'result path')
gflags.DEFINE_string('res_test', '', 'result path')
gflags.DEFINE_string('test_final', '', 'result path')
gflags.DEFINE_bool('test', False, 'test')
gflags.DEFINE_bool('group', False, 'group')
gflags.DEFINE_string('group_path', '', 'group path')
gflags.DEFINE_string('group_final', '', 'group path')
Flags = gflags.FLAGS

if __name__ == "__main__":
	Flags(sys.argv)
	pro = Processor()
	if (gflags.FLAGS.test):
		pro.test()
	elif (gflags.FLAGS.group):
		pro.group()
	else:
		pro.process()
	pro.close()
