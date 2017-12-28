#!/bin/bash
# -*- coding: utf-8 -*-
import conf
import sqlite3
import pandas as pd

class ABSTRACT_TREND():
	
	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_TREND'
	
	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

	def execute(self,df):
		return false	
