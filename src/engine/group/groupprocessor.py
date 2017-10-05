# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import gflags
import codecs
import multiprocessing
from oddsgrouper import *
from min_goals_tester import *
from conf import *

class GroupProcessor():
	def __init__(self,feature_creator,tester_creator,experiments):
		self.feature_creator = feature_creator
		self.tester_creator = tester_creator
		self.experiments = experiments

	def run(self):
		pool=multiprocessing.Pool(processes=gflags.FLAGS.maxProcessor)
		f_group = open(gflags.FLAGS.group_cond, 'r')
		rows = json.load(f_group)
		for row in rows:
			pool.apply_async(self.group,(row,))
		pool.close()
		pool.join()
	
	def group(self,cond):
		oddsgrouper = OddsGrouper(cond,self.feature_creator,self.tester_creator,self.experiments)
		oddsgrouper.run()
