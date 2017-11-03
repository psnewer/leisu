#!/bin/bash
# -*- coding: utf-8 -*-
import os
import csv
import h5py
import numpy as np

if __name__ == "__main__":
	total_file = '/Users/miller/Documents/workspace/leisu/res/extract/total'
	os.system(r'rm -rf %s'%total_file)
	os.system(r'mkdir %s'%total_file)
	total_file_train_csv = total_file + '/' + 'train.csv'
	total_file_test_csv = total_file + '/' + 'test.csv'
	total_file_train_txt = total_file + '/' + 'train.txt'
	total_file_test_txt = total_file + '/' + 'test.txt'
	train_home = [] 
	train_away = []
	train_label = []
	test_home = []
	test_away = []
	test_label = []
	walks = os.walk('/Users/miller/Documents/workspace/leisu/res/extract',topdown=False)
	for root,dirs,files in walks:
		if 'VS_PLAIN_EXTRACTOR_extract_test.csv' in files:
			train_file = os.path.join(root,'VS_PLAIN_EXTRACTOR_extract_train.csv')
			test_file = os.path.join(root,'VS_PLAIN_EXTRACTOR_extract_test.csv')
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
				
