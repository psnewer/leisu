#!/bin/bash
# -*- coding: utf-8 -*-
import conf
import sqlite3
import pandas as pd

class ABSTRACT_FEATURE():
	params = {}

	def __init__(self):
		self.team = 'ABSTRACT_FEATURE'
	
	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

	def execute(self,df):
		return false	
