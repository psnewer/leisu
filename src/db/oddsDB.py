# -*- coding: utf-8 -*-
import json
import sqlite3
import os
import re
import xml.etree.ElementTree as ET
import collections 
from datetime import datetime
from datetime import timedelta
from time import strptime
import time
import codecs
import operator
import sys
import string
from dateutil.parser import parse

oddsDirectory = '/Users/miller/Documents/workspace/leisu/leisu/odds.json'
idsDirectory = '/Users/miller/Documents/workspace/leisu/src/db/ids.json'
db = '/Users/miller/Desktop/soccer.db'
errorFile = '/Users/hugomathien/Documents/workspace/footballdata/match_error.txt'
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute('''delete from TMatch''')

def saveOdds(filepath, idspath):
	data = open(filepath)
	idsdata = codecs.open(idspath,'r+',encoding='utf-8')
	ids = json.load(idsdata)
	for row in data:
		rowdata = json.loads(row)
		league = rowdata['league']
		season = rowdata['season']
		date = rowdata['date']
		home_team = rowdata['home_team']
		away_team = rowdata['away_team']
		league_id = -1
		home_team_id = -1
		away_team_id = -1
		rangzhu = -1
		rangpan = -1
		rangke = -1
		biaozhu = -1
		biaoping = -1
		biaoke = -1
		da = -1
		dapan = -1
		xiao = -1
		if league in ids:
			league_id = ids[league]
		else:
			print "league not found"
			break
		if home_team in ids:
			home_team_id = ids[home_team]
		else:
			print home_team
			print "home team not found"
			break
		if away_team in ids:
			away_team_id = ids[away_team]
		else:
			print away_team
			print "away team not found"
			break
		if 'rangzhu' in rowdata:
			rangzhu = rowdata['rangzhu']
		if 'rangpan' in rowdata:
			rangpan = rowdata['rangpan']
		if 'rangke' in rowdata:
			rangke = rowdata['rangke']
		if 'biaozhu' in rowdata:
			biaozhu = rowdata['biaozhu']
		if 'biaoping' in rowdata:
			biaoping = rowdata['biaoping']
		if 'biaoke' in rowdata:
			biaoke = rowdata['biaoke']
		if 'da' in rowdata:
			da = rowdata['da']
		if 'dapan' in rowdata:
			dapan = rowdata['dapan']
		if 'xiao' in rowdata:
			xiao = rowdata['xiao']
		cur.execute('''INSERT OR IGNORE INTO Odds (league_id, season, date, home_team_id, away_team_id, rangzhu, rangpan, rangke, biaozhu, biaoping, biaoke, da, dapan, xiao) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (league_id, season, date, home_team_id, away_team_id, rangzhu, rangpan, rangke, biaozhu, biaoping, biaoke, da, dapan, xiao,))
	idsdata.close()
	data.close()
	conn.commit()
	conn.close()

saveOdds(oddsDirectory,idsDirectory)
