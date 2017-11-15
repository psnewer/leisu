#!/bin/bash
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import csv
import h5py
import json
import codecs
import numpy as np

if __name__ == "__main__":
	f_exp = codecs.open('/Users/miller/Documents/workspace/leisu/src/engine/conf/extract.conf', 'r', encoding='utf-8')
	data_features = json.load(f_exp)
	features = data_features['features']
	experiments = data_features['experiments']
	total_file = '/Users/miller/Documents/workspace/leisu/res/extract/total'
	os.system(r'rm -rf %s'%total_file)
	os.system(r'mkdir %s'%total_file)
	for feature in features:
		flag = feature['flag']
		featurestr = feature['name']
		if flag in experiments:
			train_home = []
			train_away = []
			train_label = []
			test_home = []
			test_away = []
			test_label = []
			total_file_train_csv = total_file + '/' + featurestr + '_' + 'train.csv'
			total_file_test_csv = total_file + '/' + featurestr + '_' + 'test.csv'
			total_file_train_txt = total_file + '/' + featurestr + '_' + 'train.txt'
			total_file_test_txt = total_file + '/' + featurestr + '_' + 'test.txt'
			train_csv = featurestr + '_' + 'extract_train.csv'
			test_csv = featurestr + '_' + 'extract_test.csv'
			walks = os.walk('/Users/miller/Documents/workspace/leisu/res/extract',topdown=False)
			for root,dirs,files in walks:
				if test_csv in files:
					train_file = os.path.join(root,train_csv)
					test_file = os.path.join(root,test_csv)
					with h5py.File(train_file, 'r') as T:
						home_res = T['home_res'][:]
						away_res = T['away_res'][:]
						label_res = T['labels'][:]
						if len(home_res) <= 0:
							continue
						if train_home == []:
							train_home = home_res
							train_away = away_res
							train_label = label_res
						else:
							train_home = np.append(home_res,train_home,axis=0)
							train_away = np.append(away_res,train_away,axis=0)
							train_label = np.append(label_res,train_label,axis=0)
					with h5py.File(test_file, 'r') as T:
						home_res = T['home_res'][:]
						away_res = T['away_res'][:]
						label_res = T['labels'][:]
						if len(home_res) <= 0:
							continue
						if test_home == []:
							test_home = home_res
							test_away = away_res
							test_label = label_res
						else:
							test_home = np.append(home_res,test_home,axis=0)
							test_away = np.append(away_res,test_away,axis=0)
							test_label = np.append(label_res,test_label,axis=0)
			print (len(train_label))
			print (len(test_label))
			with h5py.File(total_file_train_csv, 'w-') as T_train:
				T_train.create_dataset("home_res", data=train_home)
				T_train.create_dataset("away_res", data=train_away)
				T_train.create_dataset("labels", data=train_label)
			with h5py.File(total_file_test_csv, 'w-') as T_test:
				T_test.create_dataset("home_res", data=test_home)
				T_test.create_dataset("away_res", data=test_away)
				T_test.create_dataset("labels", data=test_label)
			trainTxtFile = open(total_file_train_txt, 'w+')
			testTxtFile = open(total_file_test_txt, 'w+')
			trainTxtFile.write(total_file_train_csv+'\n')
			testTxtFile.write(total_file_test_csv+'\n')
			trainTxtFile.close()
			testTxtFile.close()
	f_exp.close()
				
