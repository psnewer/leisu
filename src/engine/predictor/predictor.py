# -*- coding: utf-8 -*-
from conf import *
from datetime import datetime

class Predictor():
	def __init__(self):
		pass

	def predict(self):
		f_exp = codecs.open('../db/predict_cand.json', 'r', encoding='utf-8')
		league_cands = json.load(f_exp)
		leagues = league_cands.keys()
		combined_df = pd.DataFrame()
		today = int(datetime.today().strftime('%Y%m%d%H%M'))
		for league in leagues:
			cands = league_cands[league]
			for cand in cands:
				team = cand['team']
				f_filter = gflags.FLAGS.filter_path + league + '/' + cand['filter'] + '/' + cand['season'] + '.xlsx'
				if 'serryname' in cand:
					f_filter = gflags.FLAGS.filter_path + league + '/' + cand['serryname'] + '/' + cand['filter'] + '/' + cand['season'] + '.xlsx'
				df = pd.read_excel(f_filter)
				df_team = df[
    				(df['team'] == team) &                   
    				(df[list(cand['params'])] == pd.Series(cand['params'])).all(axis=1)  # 和params的键值对对应
					].copy()
				df_team['filtered_selected'] = df_team.apply(lambda row: self.extract_rows(row, today, league, cand['filter'], cand['params']['thresh'], team), axis=1)
				df_filtered = pd.DataFrame([item for sublist in df_team['filtered_selected'] for item in sublist])
				if cand['density']:
					f_density = gflags.FLAGS.filter_path + league + '/' + 'VS_DENSITY' + '/' + cand['season'] + '.xlsx'
					df_density = pd.read_excel(f_density)
					df_filtered = df_filtered[~df_filtered.apply(lambda row: self.should_delete_row(row, df_density), axis=1)]
				combined_df = pd.concat([combined_df, df_filtered], ignore_index=True)
		ext_file = gflags.FLAGS.predict_path + 'predict.json'
		self.pack(combined_df,ext_file)

	def extract_rows(self, row, today, league, filter, thresh, team):
		filtered_dicts = []
		for record in json.loads(row['selected']):
			if record['date'] >= today:
				filtered_dicts.append({
					'league': league,
					'date': record['date'],
					'team': team,
					'home_team': record['home_team'],
					'away_team': record['away_team'],
					'filter': filter,
					'thresh': thresh
					})
				return filtered_dicts

	def filter_selected(self, row, df_density):
		filtered_selected = []
		for match in json.loads(row['selected']):
			other_team = match['home_team'] if match['home_team'] != row['team'] else match['away_team']
			match_date = match['date']
			if not ((df_density['team'] == other_team) & (df_density['date'] == match_date)).any():
				filtered_selected.append(match)
		return json.dumps(filtered_selected)

	def should_delete_row(self, row, df_density):
		date = row['date']
		team = row['team']
		home_team = row['home_team']
		away_team = row['away_team']
		other_team = home_team if home_team != team else away_team
		match_t = df_density[(df_density['date'] == date) & (df_density['team'] == other_team)]
		match_r = df_density[(df_density['date'] == date) & (df_density['team'] == team)]
		if not match_t.empty and match_r.empty and 'TAW' in row['filter'] :
			return True
		elif match_t.empty and not match_r.empty and 'RAW' in row['filter'] :
			return True
		return False  # 不删除

	def pack(self,df,ext_file):
		records = df.to_dict(orient='records')
		with open(ext_file, 'w') as f:
			json.dump(records, f, indent=4)