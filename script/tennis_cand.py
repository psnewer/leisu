import pandas as pd
import json
import sqlite3
from datetime import datetime

def abbreviate_player_name(full_name):
    """处理网球运动员姓名简写，支持多种姓名格式"""
    parts = full_name.split()
    
    if len(parts) == 1:
        return full_name  # 单名不处理
    
    # 处理类似 "Mpetshi Perricard Giovanni" -> "Mpetshi Perricard G."
    if len(parts) > 2:
        # 保留前n-1部分作为姓氏，最后部分首字母作为名字
        surname = " ".join(parts[:-1])
        initial = parts[-1][0] + "."
        return f"{surname} {initial}"
    
    # 标准双名情况 "Sinner Jannik" -> "Sinner J."
    return f"{parts[0]} {parts[1][0]}."

def get_top_200_players():
    """获取排名前200的运动员（带智能简写）"""
    conn = sqlite3.connect('./src/db/tennis.db')
    query = "SELECT player FROM ranking ORDER BY rank ASC LIMIT 200"
    top_200 = pd.read_sql(query, conn)['player'].tolist()
    # conn.close()
    
    return [abbreviate_player_name(name) for name in top_200]

def calculate_team_strength(df, prefix):
    """计算每个团队的优势/弱势指标"""
    results = []
    
    for team, group in df.groupby('team'):
        # 计算基本统计量
        n_pos = len(group[group['pos'] >= group['neg'] * 2])
        n_neg = len(group[group['pos'] < group['neg'] * 2])
        sum_pos = group['pos'].sum()
        sum_neg = group['neg'].sum()
        sum_lapse = group['lapse'].sum()
        
        # 判断强弱
        strength = None
        if (n_pos >= n_neg) and (sum_pos >= sum_neg * 2):
            strength = True
        elif (n_pos <= n_neg) and (sum_pos < sum_neg):
            strength = False
        
        results.append({
            'team': team,
            f'{prefix}_n_pos': n_pos,
            f'{prefix}_n_neg': n_neg,
            f'{prefix}_sum_pos': sum_pos,
            f'{prefix}_sum_neg': sum_neg,
            f'{prefix}_sum_lapse': sum_lapse,
            f'{prefix}_strength': strength
        })
    
    return pd.DataFrame(results)

def process_data():
    # 获取前200名运动员
    top_200 = get_top_200_players()
    current_season = datetime.now().year
    df_rawraw = pd.read_excel('./res/tennis/analysis/TEN_RAWRAW/TEN_RAWRAW.xlsx')
    df_tawtaw = pd.read_excel('./res/tennis/analysis/TEN_TAWTAW/TEN_TAWTAW.xlsx')
    seasons = df_rawraw['season'].unique()
    df_rawraw = []
    df_tawtaw = []
    for season in seasons:
        rawraw_dir = './res/tennis/feature/TEN_RAWRAW'
        tawtaw_dir = './res/tennis/feature/TEN_TAWTAW'
        feature_rawraw = rawraw_dir + '/' + str(season) + '.xlsx'
        feature_tawtaw = tawtaw_dir + '/' + str(season) + '.xlsx'
        feature_rawraw = pd.read_excel(feature_rawraw)
        feature_tawtaw = pd.read_excel(feature_tawtaw)
        grouped = feature_rawraw.groupby('team').apply(
                lambda x: pd.Series({
                            'team': x.name,
                            'neg': ((x['profit'] < 0) & (x['score'] < 0)).sum(),  # 统计 -1 的个数
                            'pos': (x['score'] > 0).sum(),  # 统计 1 的个数
                            'sum': (x['score'] != 0).sum(),  # 计算 profit
                            'lapse': ((x['profit'] == 0) & (x['score'] > 0)).sum()
                        })
                ).reset_index(drop=True)
        grouped['season'] = season
        df_rawraw.append(grouped)
        grouped = feature_tawtaw.groupby('team').apply(
                lambda x: pd.Series({
                            'team': x.name,
                            'neg': ((x['profit'] < 0) & (x['score'] < 0)).sum(),  # 统计 -1 的个数
                            'pos': (x['score'] > 0).sum(),  # 统计 1 的个数
                            'sum': (x['score'] != 0).sum(),  # 计算 profit
                            'lapse': ((x['profit'] == 0) & (x['score'] > 0)).sum()
                        })
                ).reset_index(drop=True)
        grouped['season'] = season
        df_tawtaw.append(grouped)
    df_rawraw = pd.concat(df_rawraw, ignore_index=True)[['team', 'season', 'pos', 'neg', 'lapse', 'sum']]
    df_tawtaw = pd.concat(df_tawtaw, ignore_index=True)[['team', 'season', 'pos', 'neg', 'lapse', 'sum']]
    
    # 过滤数据
    raw_filter = (df_rawraw['team'].isin(top_200)) & (df_rawraw['season'] < current_season)
    taw_filter = (df_tawtaw['team'].isin(top_200)) & (df_tawtaw['season'] < current_season)

    df_rawraw = df_rawraw[raw_filter]
    df_tawtaw = df_tawtaw[taw_filter]

    # 计算指标
    raw_results = calculate_team_strength(df_rawraw, 'raw')
    taw_results = calculate_team_strength(df_tawtaw, 'taw')
    
    # 合并结果
    final_results = pd.merge(raw_results, taw_results, on='team', how='outer')
    
    # 保存结果
    with open('src/db/top_200.json', 'w', encoding='utf-8') as f:
        json.dump(top_200, f, ensure_ascii=False, indent=4)  # indent参数用于美化格式
    final_results.to_excel('./res/tennis/analysis/team_strength_analysis.xlsx', index=False)
    print("分析结果已保存到 team_strength_analysis.xlsx")
    
    return final_results

def generate_filter_array(final_results):
    filter_array = []
    
    for _, row in final_results.iterrows():
        team = row['team']
        entry = {"team": team}
        
        entry['filter'] = 'TEN_RAWRAW'
        # 条件1: raw_strong为true且taw_weak为true
        if row['raw_strength'] is True and row['taw_strength'] is False:
            entry["king"] = True
        if row['taw_strength'] is True and row['raw_strength'] is False:
            entry["fish"] = True
            entry['filter'] = 'TEN_TAWTAW'
        if (row['raw_strength'] is True and row['taw_strength'] is True) or (row['raw_strength'] is False and row['taw_strength'] is False):
            entry['ignore'] = True
        # if row['raw_strength'] is None and row['taw_strength'] is None:
        #     entry['ignore'] = True
        
        # 条件3: taw_strong和raw_strong都为true 或 taw_weak和raw_weak都为true
        # elif (row['taw_strong'] and row['raw_strong']) or (row['taw_weak'] and row['raw_weak']):
        #     entry["filter"] = "TEN_TAWTAW"
        #     entry["ignore"] = True

        if entry.get('filter') and entry['filter'] == 'TEN_RAWRAW':
            if row['raw_sum_pos'] - row['raw_sum_lapse'] < row['raw_sum_lapse']:
                entry['filter'] = 'TEN_RAWTAW'
        elif entry.get('filter') and entry['filter'] == 'TEN_TAWTAW':
            if row['taw_sum_pos'] - row['taw_sum_lapse'] < row['taw_sum_lapse']:
                entry['filter'] = 'TEN_TAWRAW'

        if 'filter' in entry:
            filter_array.append(entry)
    
    return filter_array

if __name__ == "__main__":
    results = process_data()
    filter_array = generate_filter_array(results)
    with open('src/db/tennis_cand.json', 'w', encoding='utf-8') as f:
        json.dump(filter_array, f, ensure_ascii=False, indent=2)
