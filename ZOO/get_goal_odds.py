import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import json
import re

if __name__ == "__main__":
	dic_res = {}
	walks = os.walk('/Users/miller/Documents/workspace/leisu/res/extract',topdown=False)
	for root,dirs,files in walks:
		if 'test_log.txt' in files:
			test_log_file = os.path.join(root,'test_log.txt')
			test_log = open(test_log_file,'r')
			lines = test_log.readlines()
			min_line = lines[-2].strip()
			may_line = lines[-1].strip()
			matchObj = re.match( r'.*/(.*)/(.*)$', root)
			league_id = matchObj.group(1)
			serryname = matchObj.group(2)
			if league_id=='308':
				print serryname
			matchObj = re.match( r'(.*) per_correct = (.*)', min_line)
			if matchObj:
				min_rt = float(matchObj.group(2))
			matchObj = re.match( r'(.*) per_correct = (.*)', may_line)
			if matchObj:
				may_rt = float(matchObj.group(2))
			if league_id not in dic_res:
				dic_res[league_id] = {}
			dic_res[league_id][serryname] = {}
			dic_res[league_id][serryname]['min_rt'] = min_rt
			dic_res[league_id][serryname]['may_rt'] = may_rt
			test_log.close()
	res_file = open('/Users/miller/Documents/workspace/leisu/ZOO/goal_rt.json','w+')
	json.dump(dic_res,res_file,ensure_ascii=False)
			
			
				
