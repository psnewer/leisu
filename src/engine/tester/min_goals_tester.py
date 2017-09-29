#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd
from abstract_tester import *

class MIN_GOALS_TESTER(ABSTRACT_TESTER):
	def __init__(self):
		self.name = 'MIN_GOALS_TESTER'
		self.params = {}
		self.params['lateral'] = 1

	def get_team_tar(self,row):
		if row['home_goal'] + row['away_goal'] < 3 :
			return 1
		else:
			return 2
