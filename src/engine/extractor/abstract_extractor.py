#!/bin/bash
# -*- coding: utf-8 -*-
import conf
import sqlite3
import pandas as pd

class ABSTRACT_EXTRACTOR():
	
	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_EXTRACTOR'
	
	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

	def process(self,cond,ext_train_txt,ext_test_txt,ext_train,ext_test):
		pass
