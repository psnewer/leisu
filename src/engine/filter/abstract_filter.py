#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd

class ABSTRACT_FILTER(object):

	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_FILTER'
	
	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]		

