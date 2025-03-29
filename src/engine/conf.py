#!/usr/bin/python
# coding: utf-8

import sys
import os
import json
import numpy
import sqlite3
import gflags
import pandas as pd
import codecs

sys.path.append(os.path.split(os.path.realpath(__file__))[0])
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/feature/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/filter/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/tester/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/analysis/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/group/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/trend/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/trenf/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/trent/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/trena/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/plot/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/extractor/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/predictor/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/manager/')

conn = sqlite3.connect(os.path.split(os.path.realpath(__file__))[0]+'/../db/matches.db')
cont = sqlite3.connect(os.path.split(os.path.realpath(__file__))[0]+'/../db/tennis.db')
cur = conn.cursor()

conf = {
   'input': '/home/input',
   'output': '/home/output'
}

gflags.DEFINE_string('db_path', '../db/soccer.db', 'db path')
gflags.DEFINE_string('alg_path', './conf/server.cfg', 'server.cfg')  
gflags.DEFINE_string('experiment_path', './conf/experiment.conf', 'experiment.conf')
gflags.DEFINE_string('extract_conf', './conf/extract.conf', 'extract.conf')
gflags.DEFINE_string('feature_conf', './conf/feature.conf', 'feature.conf')
gflags.DEFINE_string('filter_conf', './conf/filter.conf', 'filter.conf')
gflags.DEFINE_string('test_conf', './conf/test.conf', 'test.conf')
gflags.DEFINE_string('analysis_conf', './conf/analysis.conf', 'analysis.conf')
gflags.DEFINE_bool('test', False, 'test')
gflags.DEFINE_bool('update', False, 'update')
gflags.DEFINE_bool('plot', False, 'plot')
gflags.DEFINE_bool('extract', False, 'extract')
gflags.DEFINE_bool('feature', False, 'feature')
gflags.DEFINE_bool('filter', False, 'filter')
gflags.DEFINE_bool('analysis', False, 'analysis')
gflags.DEFINE_bool('predict', False, 'predict')
gflags.DEFINE_bool('soccer', False, 'predict')
gflags.DEFINE_bool('tennis', False, 'predict')
gflags.DEFINE_string('res_path', '../../res/', 'res path')
gflags.DEFINE_string('predict_path', '../../res/predict/', 'predict path')
gflags.DEFINE_string('predict_cand', '../db/predict_cand.json', 'predict cand')

Flags = gflags.FLAGS

class GlobalVar:
	id_experiment = 0

	@staticmethod
	def set_experimentId(id):
		GlobalVar.id_experiment = id

	@staticmethod
	def get_experimentId():
		return GlobalVar.id_experiment

def conciseDate(df):
	df['date']=df['date'].apply(lambda x:x[0:8])
	return df

def mkdir(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)

class GenEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.int64):
            return int(obj)
        else:
            return super(GenEncoder, self).default(obj)
