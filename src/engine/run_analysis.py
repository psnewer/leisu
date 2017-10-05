# -*- coding: utf-8 -*-

import sys
import gflags
from conf import *
from processor import *

gflags.DEFINE_string('db_path', '', 'db path')
gflags.DEFINE_string('alg_path', '', 'server.cfg')  
gflags.DEFINE_string('experiment_path', '', 'experiment.conf')
gflags.DEFINE_string('buckets_path', '', 'bucket.conf')
gflags.DEFINE_bool('test', False, 'test')
gflags.DEFINE_bool('group', False, 'group')
gflags.DEFINE_string('group_path', '', 'group path')
gflags.DEFINE_string('group_cond', '', 'group cond')
gflags.DEFINE_string('league_cond', '', 'league cond')
gflags.DEFINE_string('kind_file', '', 'kind file')
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
