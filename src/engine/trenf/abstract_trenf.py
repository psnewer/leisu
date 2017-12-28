#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd

class ABSTRACT_TRENF(object):

	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_TRENF'
	
	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]		

