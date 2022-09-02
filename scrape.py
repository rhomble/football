# scrapes WhoScored.com for event data
# https://github.com/Ali-Hasan-Khan/Scrape-Whoscored-Event-Data/blob/main/tutorial.ipynb

from selenium import webdriver
import json
import re
from collections import OrderedDict
import pandas as pd
import numpy as np


def getMatchData(driver, url, display=True, close_window=True):
    driver.get(url)

    # get script data from page source
    script_content = driver.find_element_by_xpath('//*[@id="layout-wrapper"]/script[1]').get_attribute('innerHTML')


    # clean script content
    script_content = re.sub(r"[\n\t]*", "", script_content)
    script_content = script_content[script_content.index("matchId"):script_content.rindex("}")]


    # this will give script content in list form 
    script_content_list = list(filter(None, script_content.strip().split(',            ')))
    metadata = script_content_list.pop(1) 


    # string format to json format
    match_data = json.loads(metadata[metadata.index('{'):])
    keys = [item[:item.index(':')].strip() for item in script_content_list]
    values = [item[item.index(':')+1:].strip() for item in script_content_list]
    for key,val in zip(keys, values):
        match_data[key] = json.loads(val)


    # get other details about the match
    region = driver.find_element_by_xpath('//*[@id="breadcrumb-nav"]/span[1]').text
    league = driver.find_element_by_xpath('//*[@id="breadcrumb-nav"]/a').text.split(' - ')[0]
    season = driver.find_element_by_xpath('//*[@id="breadcrumb-nav"]/a').text.split(' - ')[1]
    if len(driver.find_element_by_xpath('//*[@id="breadcrumb-nav"]/a').text.split(' - ')) == 2:
        competition_type = 'League'
        competition_stage = ''
    elif len(driver.find_element_by_xpath('//*[@id="breadcrumb-nav"]/a').text.split(' - ')) == 3:
        competition_type = 'Knock Out'
        competition_stage = driver.find_element_by_xpath('//*[@id="breadcrumb-nav"]/a').text.split(' - ')[-1]
    else:
        print('Getting more than 3 types of information about the competition.')

    match_data['region'] = region
    match_data['league'] = league
    match_data['season'] = season
    match_data['competitionType'] = competition_type
    match_data['competitionStage'] = competition_stage


    # sort match_data dictionary alphabetically
    match_data = OrderedDict(sorted(match_data.items()))
    match_data = dict(match_data)
    if display:
        print('Region: {}, League: {}, Season: {}, Match Id: {}'.format(region, league, season, match_data['matchId']))
    
    
    if close_window:
        driver.close()
        
    return match_data


def createMatchesDF(data):
    columns_req_ls = ['matchId', 'attendance', 'venueName', 'startTime', 'startDate',
                      'score', 'home', 'away', 'referee']
    matches_df = pd.DataFrame(columns=columns_req_ls)
    if type(data) == dict:
        matches_dict = dict([(key,val) for key,val in data.items() if key in columns_req_ls])
        matches_df = matches_df.append(matches_dict, ignore_index=True)
    else:
        for match in data:
            matches_dict = dict([(key,val) for key,val in match.items() if key in columns_req_ls])
            matches_df = matches_df.append(matches_dict, ignore_index=True)
    
    matches_df = matches_df.set_index('matchId')        
    return matches_df


def createEventsDF(data):
    
    events = data['events']
    for event in events:
        event.update({'matchId' : data['matchId'],
                     'startDate' : data['startDate'],
                     'startTime' : data['startTime'],
                     'score' : data['score'],
                     'ftScore' : data['ftScore'],
                     'htScore' : data['htScore'],
                     'etScore' : data['etScore'],
                     'venueName' : data['venueName'],
                     'maxMinute' : data['maxMinute']})
    events_df = pd.DataFrame(events)


    # clean period column
    events_df['period'] = pd.json_normalize(events_df['period'])['displayName']

    # clean type column
    events_df['type'] = pd.json_normalize(events_df['type'])['displayName']

    # clean outcomeType column
    events_df['outcomeType'] = pd.json_normalize(events_df['outcomeType'])['displayName']

    # clean outcomeType column
    try:
        x = events_df['cardType'].fillna({i: {} for i in events_df.index})
        events_df['cardType'] = pd.json_normalize(x)['displayName'].fillna(False)
    except KeyError:
        events_df['cardType'] = False

    # clean satisfiedEventTypes column
    eventTypeDict = data['matchCentreEventTypeJson']
    for i in range(len(events_df)):
        row = events_df.loc[i, 'satisfiedEventsTypes'].copy()
        events_df['satisfiedEventsTypes'].loc[i] = [list(eventTypeDict.keys())[list(eventTypeDict.values()).index(event)] for event in row]

    # clean qualifiers column
    try:
        for i in events_df.index:
            row = events_df.loc[i, 'qualifiers'].copy()
            if len(row) != 0:
                for irow in range(len(row)):
                    row[irow]['type'] = row[irow]['type']['displayName']
    except TypeError:
        pass

    # clean isShot column
    if 'isShot' in events_df.columns:
        events_df['isShot'] = events_df['isShot'].replace(np.nan, False)
    else:
        events_df['isShot'] = False

    # clean isGoal column
    if 'isGoal' in events_df.columns:
        events_df['isGoal'] = events_df['isGoal'].replace(np.nan, False)
    else:
        events_df['isGoal'] = False

    # add player name column
    events_df.loc[events_df.playerId.notna(), 'playerId'] = events_df.loc[events_df.playerId.notna(), 'playerId'].astype(int).astype(str)    
    player_name_col = events_df.loc[:, 'playerId'].map(data['playerIdNameDictionary']) 
    events_df.insert(loc=events_df.columns.get_loc("playerId")+1, column='playerName', value=player_name_col)

    # add home/away column
    h_a_col = events_df['teamId'].map({data['home']['teamId']:'h', data['away']['teamId']:'a'})
    events_df.insert(loc=events_df.columns.get_loc("teamId")+1, column='h_a', value=h_a_col)

    # adding shot body part column
    events_df['shotBodyType'] =  np.nan
    for i in events_df.loc[events_df.isShot==True].index:
        for j in events_df.loc[events_df.isShot==True].qualifiers.loc[i]:
            if j['type'] == 'RightFoot' or j['type'] == 'LeftFoot' or j['type'] == 'Head' or j['type'] == 'OtherBodyPart':
                events_df['shotBodyType'].loc[i] = j['type']

    # adding shot situation column
    events_df['situation'] =  np.nan
    for i in events_df.loc[events_df.isShot==True].index:
        for j in events_df.loc[events_df.isShot==True].qualifiers.loc[i]:
            if j['type'] == 'FromCorner' or j['type'] == 'SetPiece' or j['type'] == 'DirectFreekick':
                events_df['situation'].loc[i] = j['type']
            if j['type'] == 'RegularPlay':
                events_df['situation'].loc[i] = 'OpenPlay'   

    # adding other event types columns
    event_types = list(data['matchCentreEventTypeJson'].keys())
    for event_type in event_types:
        events_df[event_type] = pd.Series([event_type in row for row in list(events_df['satisfiedEventsTypes'])])         

    return events_df


# --------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    driver = webdriver.Chrome('chromedriver.exe')
    
# whoscored match centre url of the required match (Example: Barcelona vs Sevilla)
url = "https://www.whoscored.com/Matches/1491995/Live/Spain-LaLiga-2020-2021-Barcelona-Sevilla"
match_data = getMatchData(driver, url, close_window=True)

# Match dataframe containing info about the match
matches_df = createMatchesDF(match_data)

# Events dataframe      
events_df = createEventsDF(match_data)

# match Id
matchId = match_data['matchId']

# Information about respective teams as dictionary
home_data = matches_df['home'][matchId]
away_data = matches_df['away'][matchId]