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
team_path = '''../static/data/teams_all_season.csv'''


class TeamService:
    def __init__(self, team_path):
        self.teams_df = pd.read_csv(team_path)
        self.teams_df.drop(['Unnamed: 0'], axis=1, inplace=True)
        self.teams_df = self.teams_df.replace("-1", None)
        self.teams_df = self.teams_df.replace(-1, 0)
        self.teams_df = self.teams_df.replace("tie", "draw")

    def plot_combined_bar_line_chart_result(self, team_name):
        team_data = self.teams_df.loc[self.teams_df['name'] == team_name]
        # Group by season and result
        result_counts = team_data.groupby(
            ['season', 'result']).size().unstack(fill_value=0)
        # Calculate total matches per season
        result_counts['total_matches'] = result_counts.sum(axis=1)

        # Calculate percentage of wins, draws, and losses
        result_counts['win_percentage'] = result_counts['win'] / \
            result_counts['total_matches'] * 100
        result_counts['draw_percentage'] = result_counts['draw'] / \
            result_counts['total_matches'] * 100
        result_counts['lose_percentage'] = result_counts['lose'] / \
            result_counts['total_matches'] * 100

        # Plot combined bar and line chart
        fig, ax1 = plt.subplots(figsize=(10, 6))

        # Bar plot for wins, draws, and losses
        result_counts[['win', 'draw', 'lose']].plot(
            kind='bar', stacked=True, ax=ax1, color=['green', 'orange', 'red'])

        # Line plot for win percentage, draw percentage, and lose percentage
        ax2 = ax1.twinx()
        result_counts[['win_percentage', 'draw_percentage', 'lose_percentage']].plot(
            kind='line', marker='o', ax=ax2, color=['green', 'orange', 'red'])

        # Set labels and title
        ax1.set_xlabel('Season')
        ax1.set_ylabel('Number of Matches')
        ax2.set_ylabel('Percentage')
        plt.title(f'Combined Bar and Line Chart for {team_name}')

        # Show the plot
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)

        # Use tight_layout to further adjust spacing
        plt.tight_layout()

        img = BytesIO()
        plt.savefig(img, format='png', pad_inches=0)
        img.seek(0)
        plot_url = base64.b64encode(img.getvalue()).decode()

        return plot_url

    def line_chart_attibute(self, team_name, attribute=["total_red_card", "total_yel_card"]):
        team_data = self.teams_df.loc[self.teams_df['name'] == team_name]
        team_data = team_data[["season"] + attribute]
        team_data = team_data.groupby("season").sum().reset_index()
        # Group by season and result
        # Define list of x,y values
        seasons = list(team_data['season'])
        plt.figure(figsize=(30, 7))
        for att in attribute:

            red = list(team_data[att])
            # Plot data
            plt.plot(seasons, red, marker='.', label=att)
        plt.legend(attribute)
        plt.xlabel('Season')
        plt.ylabel('Nums')
        plt.title(f'{team_name} Stats From Season 92/93 to 22/23')
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)

        # Use tight_layout to further adjust spacing
        plt.tight_layout()

        img = BytesIO()
        plt.savefig(img, format='png', pad_inches=0)
        img.seek(0)
        plot_url = base64.b64encode(img.getvalue()).decode()

        return plot_url

    def compare_team_attributes(self, teamA, teamB, filter_type='Season-Season', season='2022-23', attribute='open_play_pass'):
        if filter_type == 'One Season':
            data_df = self.teams_df.loc[(self.teams_df["season"] == season) & (
                self.teams_df["name"] == teamB) | (self.teams_df["name"] == teamA)]
            data_df = data_df[["name", attribute]]
            data_df = data_df.groupby("name").sum().reset_index()
            # Set the team column as the index for better visualization
            data_df.set_index('name', inplace=True)

            # Plotting single-bar chart
            fig, ax = plt.subplots(figsize=(8, 5))
            data_df[attribute].plot(
                kind='bar', color=['skyblue', 'orange'], ax=ax)
            ax.set_title(f'Comparison of {attribute} between Teams')
            ax.set_xlabel('Teams')
            ax.set_ylabel(f'{attribute} Values')
            ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
            plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)

            # Use tight_layout to further adjust spacing
            plt.tight_layout()

            img = BytesIO()
            plt.savefig(img, format='png', pad_inches=0)
            img.seek(0)
            plot_url = base64.b64encode(img.getvalue()).decode()

            return plot_url
        elif filter_type == 'Season-Season':
            data_df = self.teams_df.loc[(self.teams_df["name"] == teamB) | (
                self.teams_df["name"] == teamA)]
            data_df = data_df[["name", attribute]]
            data_df = data_df.groupby("name").sum().reset_index()
            print(data_df)
            # Set the team column as the index for better visualization
            data_df.set_index('name', inplace=True)
            # Plotting single-bar chart
            fig, ax = plt.subplots(figsize=(8, 5))
            data_df[attribute].plot(
                kind='bar', color=['skyblue', 'orange'], ax=ax)
            ax.set_title(f'Comparison of {attribute} between Teams')
            ax.set_xlabel('Teams')
            ax.set_ylabel(f'{attribute} Values')
            ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
            plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)

            # Use tight_layout to further adjust spacing
            plt.tight_layout()

            img = BytesIO()
            plt.savefig(img, format='png', pad_inches=0)
            img.seek(0)
            plot_url = base64.b64encode(img.getvalue()).decode()

            return plot_url
        else:
            raise Exception()
