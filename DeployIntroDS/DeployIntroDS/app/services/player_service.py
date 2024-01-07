import os
import re
import csv
import json
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from io import BytesIO
import base64

sns.set(style='darkgrid')
player_path = '''../static/data/player_all_season.csv'''


class PlayerService:
    def __init__(self, player_path):
        self.player_df = pd.read_csv(player_path)
        self.player_df.drop('Unnamed: 0', axis=1, inplace=True)
        self.player_df = self.player_df.replace(-1, 0)
        self.player_df = self.player_df.replace("-1", None)

    def generate_age_histogram(self):
        plt.figure(figsize=(16, 8))
        self.player_df['age'].hist(bins=25, grid=False)
        plt.xticks(np.arange(0, 50, step=5))
        plt.xlabel('Age')
        plt.ylabel('Count')
        plt.title('Players in all season\'s age histogram')

        img = BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_url = base64.b64encode(img.getvalue()).decode()
        plt.close()

        return plot_url

    def get_n_top_player_with_the_most_x(self, n, x, get_top=True, season='All'):
        # Group by 'id' and 'season' and calculate the sum of 'x' for each group
        if season == 'All':

            grouped_players = self.player_df.groupby(['playerId', 'name'])[
                x].sum().reset_index()

            top_n_players_with_the_most_x = grouped_players.sort_values(
                by=[x], ascending=False).head(n)
    #             top_n_players_with_the_most_x = player_df.sort_values(by=[x], ascending=False).head(n)
    # top_n_players_with_the_most_x.plot.bar(x='name', y=x, figsize=(n,10), color=sns.color_palette("magma"))

        else:
            grouped_players = self.player_df[self.player_df['season'] == season]

            top_n_players_with_the_most_x = grouped_players.sort_values(
                by=[x], ascending=False).head(n)

        # Convert the DataFrame to a list of dictionaries
        player_data = [{'name': player['name'], 'score': player[x], 'id': player['playerId']}
                       for _, player in top_n_players_with_the_most_x.iterrows()]

        return player_data

    def get_player_details(self, player_id):
        # Lọc dữ liệu cho một cầu thủ cụ thể dựa trên player_id
        player_data = self.player_df[self.player_df['playerId'] == player_id]

        # Nếu không tìm thấy cầu thủ, trả về None hoặc một giá trị tương tự để thể hiện rằng cầu thủ không tồn tại
        if player_data.empty:
            return None

        # Gộp các thuộc tính của cầu thủ
        aggregated_data = player_data.groupby(
            ['playerId', 'name', 'position', 'shirtNum', 'positionInfo', 'country', 'birth']).sum().reset_index().iloc[0]

        return aggregated_data.to_dict()
