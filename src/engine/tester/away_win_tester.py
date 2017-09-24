#!/bin/bash
# -*- coding: utf-8 -*-
from conf import *
import sqlite3
import gflags
import math
import json
import pandas as pd

class AWAT_WIN_TESTER(ABSTRACT_TESTER):
	def __init__(self):
		self.name = 'AWAY_WIN_TESTER'

	def get_team_tar(self,row):
		if row['home_goal'] < row['away_goal'] :
			return 1
		elif row['home_goal'] > row['away_goal'] :
			return 2
		elif 
			return 0
