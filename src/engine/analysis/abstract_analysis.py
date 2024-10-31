#!/bin/bash
# -*- coding: utf-8 -*-
from analysis import *

class ABSTRACT_ANALYSIS():
	def __init__(self):
		self.params = {}
		self.name = 'ABSTRACT_ANALYSIS'

	def setParams(self,params):
		for key in params:
			self.params[key] = params[key]

