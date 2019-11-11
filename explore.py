import twitterscraper as ts
import pyreadr
import pandas as pd
import pickle as p
from importlib import reload
from TwitterAPI import TwitterAPI
import logging
import json
import math
import numpy as np
import datetime as dt
import time
import html
import sys
import os.path

weeks = [i for i in range(19, 33)]
week_paths = {}

consumer_key = 'IBhVufG7rKxePLVvox1P3QE5a'
consumer_secret = 'xC7J0idIFjbZ9Z9Rg7P7q76eHIfPI9OECwB6CpAeFI79AJijJK'
oauth_token = '1186988901052887040-dF5De5ZF2v4ap8CQcaD7SSQYXrnoRs'
oauth_secret = 'QlISqDCoTnwLbwNZJ1e4Rm43eGZhc3olyIAEfgKvZGWHA'

api = TwitterAPI(consumer_key, consumer_secret, oauth_token, oauth_secret)

def init():
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_rows', 50)
    logging.disable(logging.INFO)

    for week in weeks:
        projects_path = 'weeks/2019_week{}/all_projects.rds'.format(week)
        info_path = 'weeks/2019_week{}/info.rds'.format(week)
        # pledge_path = 'weeks/2019_week{}/pledge.rds'.format(week)
        # posts_path = 'weeks/2019_week{}/posts.rds'.format(week)
        week_paths[week] = [projects_path, info_path]


def read_projects_data():
    result = pd.DataFrame()
    for week in weeks:
        projects_path, _ = week_paths[week]
        p_df = pyreadr.read_r(projects_path)[None]
        result = result.append(p_df[['project_slug', 'project_name', 'creator_name']])
    result.drop_duplicates(inplace=True)
    return result


def read_info_data():
    result = pd.DataFrame()
    for week in weeks:
        _, info_path = week_paths[week]
        i_df = pyreadr.read_r(info_path)[None]
        i_df['week_no'] = week
        i_df['deadline_year'] = i_df['Deadline'].apply(convert_to_year)
        i_df['deadline_week'] = i_df['Deadline'].apply(convert_to_week_number)
        result = result.append(i_df[['project_slug', 'Category', 'Goal_USD', 'Pledge_USD', 'Number_Backers',
                                     'Launched_at', 'Deadline', 'Project_description', 'week_no', 'deadline_year',
                                     'deadline_week']])

    result = result.loc[result['Category'] == 'Tabletop Games']
    result['optimal'] = np.where((result.deadline_week < result.week_no) &
                                 (result.deadline_year == 2019), True, False)
    result = result.loc[result['optimal'] == True]
    result.drop_duplicates('project_slug', inplace=True, keep='first')
    return result


def read_data():
    p_df = read_projects_data()
    i_df = read_info_data()
    df = p_df.merge(i_df, how='right')
    df.drop_duplicates(subset='project_slug', inplace=True, keep='first')
    df = df.reset_index(drop=True)
    return df


def read_p_data():
    path = 'df.p'
    if os.path.exists(path):
        with open(path, 'rb') as in_file: res = p.load(in_file)
        in_file.close()
    return res


def convert_to_year(ts):
    if math.isnan(float(ts)):
        return 0
    else:
        return dt.datetime.utcfromtimestamp(int(ts)).isocalendar()[0]


def convert_to_week_number(ts):
    if math.isnan(float(ts)):
        return 0
    else:
        return dt.datetime.utcfromtimestamp(int(ts)).isocalendar()[1]


def convert_to_date(ts):
    if math.isnan(float(ts)):
        return 0
    else:
        return dt.datetime.utcfromtimestamp(int(ts)).date()


# def find_all_projects(df):
#     rows = []
#     for index, row in df.iterrows():
#         res_dict = find_project(row['project_name'], convert_to_date(row['Launched_at']), convert_to_date(row['Deadline']))
#         rows.append(res_dict)
#         if index == 5:
#             break
#     return pd.DataFrame(rows)


def find_project(project_name, begin_dt=dt.date(2019, 3, 1), end_dt=dt.date.today(), limit=500):
    tweets = ts.query_tweets('"{}"'.format(project_name), begindate=begin_dt, enddate=end_dt, limit=limit)
    likes, retweets, replies, reach = 0, 0, 0, 0
    tweets_found = {}
    users_found = {}
    for tweet in tweets:
        if tweet.tweet_id not in tweets_found:
            likes += tweet.likes
            retweets += tweet.retweets
            replies += tweet.replies
            tweets_found[tweet.tweet_id] = tweet
            if tweet.username not in users_found:
                user = ts.query_user_info(tweet.screen_name)
                if user is not None:
                    reach += user.followers
                    users_found[tweet.username] = user
    return {'project_name': project_name, 'tweets': len(tweets_found), 'users': len(users_found),
            'likes': likes, 'retweets': retweets, 'replies': replies, 'reach': reach}, list(tweets_found.values())


def find_projects_subset(df_subset):
    dict_list = []
    for index, row in df_subset.iterrows():
        name = get_ascii(row['project_name'])
        launched = convert_to_date(row['Launched_at'])
        deadline = convert_to_date(row['Deadline'])
        res_dict, _ = find_project(name, launched, deadline)
        dict_list.append(res_dict)
        print('PROJECT DONE: {}'.format(name))
        print(dict_list)
        time.sleep(5)
    return dict_list


def find_projects(df, start=0, size=5):
    for i in range(start, df.shape[0], size):
        df_subset = df.loc[i:i+size-1]
        dict_list = find_projects_subset(df_subset)
        with open('results/tweets/subset_{}-{}.p'.format(i, i + size-1), 'wb') as out_file: p.dump(dict_list, out_file)


def load_files(type, limit, size=5):
    result = []
    for i in range(0, limit, size):
        path = 'results/{}/subset_{}-{}.p'.format(type, i, i + size-1)
        if os.path.exists(path):
            with open(path, 'rb') as in_file: res = p.load(in_file)
            in_file.close()
            result.extend(res)
    return result


def find_user(id, query):
    users = list(api.request('users/search', {'q': get_ascii(query)}))
    users.extend(list(api.request('users/search', {'q': get_ascii(query).replace(" ", "")})))
    potential_users = []
    for user in users:
        if 'kickstarter' in str(user).lower() or 'kck.st' in str(user).lower():
            name, username = user['name'], user['screen_name']
            followers, following = user['followers_count'], user['friends_count']
            tweets = user['statuses_count']
            found = {'id': id, 'name': name, 'username': username, 'followers': followers,
                     'following': following, 'tweets': tweets}
            potential_users.append(found)

    best_user = {'id': id, 'name': "", 'username': "", 'followers': 0, 'following': 0, 'tweets': 0}
    if len(potential_users) == 1:
        best_user = potential_users[0]
    elif len(potential_users) > 1:
        for user in potential_users:
            if user['followers'] >= best_user['followers']:
                best_user = user
    return best_user


def find_users(df, start=0, size=100):
    for i in range(start, df.shape[0], size):
        df_subset = df.loc[i:i+size-1]
        dict_list = []
        for _, row in df_subset.iterrows():
            id = row['project_slug']
            query = row['creator_name']
            dict_list.append(find_user(id, query))
            time.sleep(1)
        with open('results/users/subset_{}-{}.p'.format(i, i+size-1), 'wb') as out_file: p.dump(dict_list, out_file)


def get_ascii(str):
    str = str.encode('ascii', errors='ignore').decode()
    return html.unescape(str)


def main(argv):
    df = read_p_data()
    find_projects(df, int(argv), 5)


if __name__ == '__main__':
    main(sys.argv[1])

