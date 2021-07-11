from luigi import Task, run, LocalTarget

import requests
import pandas as pd
import numpy as np
import datetime as dt
import plotly.graph_objects as go
import time

from api_keys import api_key

def ms_to_date(ms):
    ans = pd.DataFrame()
    for i in range(len(ms)):
        ans=ans.append([dt.datetime(1970, 1, 1) + dt.timedelta(hours=3, milliseconds=int(ms[i]))])
    return ans

class create_pdf_with_IEM_Cologne(Task):

    fig_name = "matchs_table_"+str(dt.datetime.now().strftime("%Y_%m_%d_%H"))+".pdf"

    def create_table(self):
        url_matchs = "http://api.isportsapi.com/esport/csgo/match/recently"
        url_team = "http://api.isportsapi.com/esport/csgo/team/basic_info"
        url_player = "http://api.isportsapi.com/esport/csgo/player/stats"

        league_id = 1925

        params_matchs = {
            'api_key' : api_key,
            'offset' : '0',
            'limit' : '100'
        }

        response_matchs = requests.get(url_matchs, params=params_matchs)
        df_matchs = pd.DataFrame(response_matchs.json()['data'])
        df_time_copy = df_matchs[df_matchs['league_id']==league_id].sort_values(
            by='match_time')[:3]

        df_time_copy['match_time'] = ms_to_date(
            df_time_copy['match_time'].array)[0].array

        df_final = df_time_copy[['team_b_id', 'team_a_id', 'match_id', 'match_time', 'bo', 'stage']].reset_index().drop(['index'], axis=1)

        teams_id = df_final.team_a_id.append(df_final.team_b_id).unique()

        df_team_players_stats = pd.DataFrame()

        for j in teams_id:
            if j != 0:
                params_team_j = {
                    'api_key' : api_key,
                    'team_id' : j
                }

                response_team_j = requests.get(url_team, params=params_team_j)

                df_team_0 = pd.DataFrame(response_team_j.json()['data'])
                df_team_param = df_team_0[['world_rank', 'name', 'team_id']]
                df_players = pd.DataFrame(df_team_0.player_list.array)

                df_players_stats = pd.DataFrame()

                for i in df_players.player_id.array:
                    params_player_i = {
                        'api_key' : api_key,
                        'player_id' : i
                    }

                    response_player_i = requests.get(url_player, params=params_player_i)
                    df_players_stats = df_players_stats.append(pd.DataFrame([response_player_i.json()['data']]))

                df_top_player = pd.DataFrame([
                    df_players.merge(
                        df_players_stats[['player_id', 'damage_average', 'rating']],
                                        how='inner', on='player_id'
                        ).sort_values(
                        by='damage_average', ascending=False
                    ).iloc[0][['player_id', 'nickname', 'team_id', 'damage_average']]])
                df_team_players_stats = df_team_players_stats.append(
                    pd.DataFrame([df_team_param.merge(df_top_player, how='inner', on='team_id').iloc[0]]))


        
        qq = df_final[['match_time', 'bo','stage', 'team_b_id', 'team_a_id']].merge(
                        df_team_players_stats.add_suffix('__bbb'),
                        how='left', left_on='team_b_id', right_on='team_id__bbb')

        qq = qq.merge(df_team_players_stats.add_suffix('__aaa'),
                     how='left', left_on='team_a_id', right_on='team_id__aaa')

        qq_f = qq[['stage', 'match_time','bo', 'name__bbb', 'world_rank__bbb', 'name__aaa', 'world_rank__aaa', 'nickname__bbb',
           'damage_average__bbb','nickname__aaa', 'damage_average__aaa' ]]

        fig = go.Figure(data=[go.Table(
                columnwidth = [70,100],
                        header=dict(values=['<b>Параметры</b>','<b>Первый матч</b>','<b>Второй матч</b>','<b>Третий матч</b>'],
                                    line_color='darkslategray',
                    fill_color='royalblue',
                    align=['left','center'],
                    font=dict(color='white', size=12),
                    height=40),
                        cells=dict(values=[
                            ['<b>Стадия турнира</b>','<b>Время матча</b>','<b>Формат встречи (bo)</b>',
                            '<b>Название команды АА</b>', '<b>Место в мировом рейтинге АА</b>',
                            '<b>Название команды ВВ</b>', '<b>Место в мировом рейтинге ВВ</b>',
                            '<b>Игрок с наибольшим "damage average" АА</b>', '<b>damage average AA</b>',
                            '<b>Игрок с наибольшим "damage average" ВВ</b>', '<b>damage average ВВ</b>'],
                            
                            list(qq_f.iloc[0]),
                            list(qq_f.iloc[1]),
                            list(qq_f.iloc[2])
                        ], line_color='darkslategray',
                    fill=dict(color=['paleturquoise', 'white']),
                    align=['left', 'center'],
                    font_size=12,
                    height=30))
                    ])

        fig.write_image(self.fig_name, width=1000, height=800)
                    
    def run(self):
        self.create_table()

    def output(self):
        return LocalTarget(self.fig_name)


if __name__ == '__main__':
    run()
