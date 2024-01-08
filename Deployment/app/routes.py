from app import app
from flask import Flask, render_template, request
import matplotlib.pyplot as plt
import numpy as np
import os
from io import BytesIO
import base64
from app.services.player_service import PlayerService
from app.services.team_service import TeamService
import json

# -*- coding: utf-8 -*-
# coding: utf8
player_path = os.path.join(app.static_folder, 'data', 'player_all_season.csv')
team_path = os.path.join(app.static_folder, 'data', 'teams_all_season.csv')
team_service = TeamService(team_path)
player_service = PlayerService(player_path)

football_teams = [
    "Arsenal", "Aston Villa", "Barnsley", "Birmingham City", "Blackburn Rovers",
    "Blackpool", "Bolton Wanderers", "Bournemouth", "Bradford City", "Brentford",
    "Brighton & Hove Albion", "Burnley", "Cardiff City", "Charlton Athletic", "Chelsea",
    "Coventry City", "Crystal Palace", "Derby County", "Everton", "Fulham",
    "Huddersfield Town", "Hull City", "Ipswich Town", "Leeds United", "Leicester City",
    "Liverpool", "Manchester City", "Manchester United", "Middlesbrough", "Newcastle United",
    "Norwich City", "Nottingham Forest", "Oldham Athletic", "Portsmouth", "Queens Park Rangers",
    "Reading", "Sheffield United", "Sheffield Wednesday", "Southampton", "Stoke City",
    "Sunderland", "Swansea City", "Swindon Town", "Tottenham Hotspur", "Watford",
    "West Bromwich Albion", "West Ham United", "Wigan Athletic", "Wimbledon", "Wolverhampton Wanderers"
]


@app.route('/')
@app.route('/home')
def index():

    return render_template('index.html', title="Home")


@app.route('/player')
# @app.route('/player?<int:player_id>')
def player():
    seasons = ['All',
               '1992-93', '1993-94', '1994-95', '1995-96', '1996-97',
               '1997-98', '1998-99', '1999-00', '2000-01', '2001-02',
               '2002-03', '2003-04', '2004-05', '2005-06', '2006-07',
               '2007-08', '2008-09', '2009-10', '2010-11', '2011-12',
               '2012-13', '2013-14', '2014-15', '2015-16', '2016-17',
               '2017-18', '2018-19', '2019-20', '2020-21', '2021-22',
               '2022-23'
               ]
    type_labels = [
        {'label': 'Wins', 'value': 'wins'},
        {'label': 'Goals', 'value': 'goals'},
        {'label': 'Losses', 'value': 'losses'},
        {'label': 'Goal assist', 'value': 'goal_assist'},
        {'label': 'Yellow card', 'value': 'yellow_card'},
        {'label': 'Red card', 'value': 'red_card'},
        {'label': 'Fouls', 'value': 'fouls'},
        {'label': 'Save', 'value': 'saves'},
        {'label': 'Appearances', 'value': 'appearances'}
    ]
    type_param = request.args.get('type', default='wins')
    quantity_param = request.args.get('quantity', default=10)
    season = request.args.get('season', default='All')

    # Convert quantity to an integer
    try:
        quantity = int(quantity_param)
    except ValueError:
        # Handle the case where the quantity parameter is not a valid integer
        quantity = 10
    player_data = player_service.get_n_top_player_with_the_most_x(
        x=type_param, n=quantity, season=season)

    return render_template('player.html', type_labels=type_labels, quantity_param=quantity_param, type_param=type_param, player_data=player_data, seasons=seasons, season=season)


@app.route('/player_detail/<int:player_id>')
def player_detail(player_id):
    # Retrieve player details using the player_id
    # You may need to modify this based on your actual implementation
    player_details = player_service.get_player_details(player_id)

    return render_template('player_detail.html', title='Player Detail', player_details=player_details)


@app.route('/team')
def team():
    options = [
        {"label": "Goals", "value": "goals"},
        {"label": "Shots on target", "value": "ontarget_scoring_att"},
        {"label": "Shots", "value": "total_scoring_att"},
        {"label": "Touches", "value": "touches"},
        {"label": "Passes", "value": "total_pass"},
        {"label": "Tackles", "value": "total_tackle"},
        {"label": "Clearances", "value": "total_clearance"},
        {"label": "Corners", "value": "corner_taken"},
        {"label": "Yellow cards", "value": "total_yel_card"},
        {"label": "Fouls conceded", "value": "fk_foul_lost"}
    ]
    team_name = request.args.get('team_name', default='Manchester United')
    type_param = json.loads(request.args.get('type', default='["goals"]'))
    plot_url = team_service.line_chart_attibute(
        team_name, attribute=type_param)

    return render_template('team.html', title='Team', plot_url=plot_url, football_teams=football_teams, team_name=team_name, options=options, type=type_param)


@app.route('/compare')
def compare():
    seasons = ['All',
               '1992-93', '1993-94', '1994-95', '1995-96', '1996-97',
               '1997-98', '1998-99', '1999-00', '2000-01', '2001-02',
               '2002-03', '2003-04', '2004-05', '2005-06', '2006-07',
               '2007-08', '2008-09', '2009-10', '2010-11', '2011-12',
               '2012-13', '2013-14', '2014-15', '2015-16', '2016-17',
               '2017-18', '2018-19', '2019-20', '2020-21', '2021-22',
               '2022-23'
               ]
    options = [
        {"label": "Goals", "value": "goals"},
        {"label": "Shots on target", "value": "ontarget_scoring_att"},
        {"label": "Shots", "value": "total_scoring_att"},
        {"label": "Touches", "value": "touches"},
        {"label": "Passes", "value": "total_pass"},
        {"label": "Tackles", "value": "total_tackle"},
        {"label": "Clearances", "value": "total_clearance"},
        {"label": "Corners", "value": "corner_taken"},
        {"label": "Yellow cards", "value": "total_yel_card"},
        {"label": "Fouls conceded", "value": "fk_foul_lost"}
    ]
    team_name1 = request.args.get('team_name1', default='Manchester United')
    team_name2 = request.args.get('team_name2', default='Manchester City')
    type_param = request.args.get('type', default='goals')
    season = request.args.get('season', default='All')
    if season == 'All':
        plot_url = team_service.compare_team_attributes(
            team_name1, team_name2, attribute=type_param)
    else:
        plot_url = team_service.compare_team_attributes(
            team_name1, team_name2, filter_type='One Season', season=season, attribute=type_param)

    return render_template('compare2team.html', seasons=seasons, season=season, title='Compare', plot_url=plot_url, football_teams=football_teams, team_name1=team_name1, team_name2=team_name2, options=options, type=type_param)
