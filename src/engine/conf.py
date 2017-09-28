#!/usr/bin/python
# coding: utf-8

import sys
import os
import sqlite3
import pandas as pd
sys.path.append(os.path.split(os.path.realpath(__file__))[0])
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/feature/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/filter/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/tester/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/group/')
sys.path.append(os.path.split(os.path.realpath(__file__))[0] + '/manager/')

conn = sqlite3.connect('/Users/miller/Desktop/soccer.db')

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