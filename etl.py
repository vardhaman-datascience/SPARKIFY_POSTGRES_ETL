#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 14:29:52 2021

@author: ekmey
"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from fnmatch import fnmatch

dbname="sparkifydb"
host="divn-test.cuvxpqcqzvyz.ap-northeast-1.rds.amazonaws.com"
port="5432"
user="postgres"
password="qnJn%9BD6`Vt.t(N"

conn=psycopg2.connect(
    dbname=dbname,
    host=host,
    port=port,
    user=user,
    password=password)


cur=conn.cursor()
try:
    filepath1='data/song_data/'
    filepath2='data/log_data'
    
    song_file_paths=[]
    log_file_paths=[]
    
    # collecting all json file paths from song_data
    pattern = "*.json"
    root1=filepath1
    for path, subdirs, files in os.walk(root1):
        for name in files:
            if fnmatch(name, pattern):
                song_file_paths.append(os.path.join(path, name))
     #collecting all json file paths from log_data           
    pattern = "*.json"
    root2=filepath2
    for path, subdirs, files in os.walk(root2):
        for name in files:
            if fnmatch(name, pattern):
                log_file_paths.append(os.path.join(path, name))            
    
    #creating the dataframs of song_data and log_data              
    log_df=pd.DataFrame(columns=['artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
           'length', 'level', 'location', 'method', 'page', 'registration',
           'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'])
    
    song_df=pd.DataFrame(columns=['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
           'artist_location', 'artist_name', 'song_id', 'title', 'duration',
           'year'])
    
    #reading and collating all the data 
    for i in range(len(log_file_paths)):      
        df = pd.read_json(log_file_paths[i], lines=True)
        log_df=pd.concat([log_df,df])
    
    for i in range(len(song_file_paths)):
        dff = pd.read_json(song_file_paths[i], dtype={'year': int}, lines=True)
        song_df=pd.concat([song_df,dff])
        
    log_df=log_df.head(10)
    
    #SONG DATA
    #inserting data into songs table
    for i in range(len(song_df)):
        song_data = song_df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[i].tolist()
        cur.execute(song_table_insert, song_data)
        conn.commit()
        print(song_data) 
            
    #inserting data into artist data
    for i in range(len(song_df)):
        artist_data = song_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[i].tolist()
        cur.execute(artist_table_insert, artist_data)
        conn.commit()
        print(artist_data)
        
    #LOG DATA
    ldf = log_df[log_df['page'] == 'NextSong']
    t = pd.to_datetime(ldf["ts"], unit='ms')
    t=pd.DataFrame(t)
    t['hour']=t['ts'].apply(lambda x :x.hour)
    t['day']=t['ts'].apply(lambda x :x.day)
    t['week']=t['ts'].apply(lambda x :x.week)
    t['month']=t['ts'].apply(lambda x :x.month)
    t['year']=t['ts'].apply(lambda x :x.year)
    t['weekday']=t['ts'].apply(lambda x :x.weekday())
    columns= ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    t.columns=columns
    print(len(t))
    print(t.shape)
    
    
    #inserting data into time table
    k=0
    for i in range(len(t)):
        time_data = t.values[i].tolist()
        cur.execute(time_table_insert, time_data)
        conn.commit()
        k=k+1
        print("time table row",i)
        
    #inserting data into users table
    user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]]
    for i in range(len(user_df)):
        row=user_df.values[i].tolist()
        cur.execute(user_table_insert, row)
        conn.commit()
        
    log_df['ts'] = pd.to_datetime(log_df['ts'], unit='ms')
    for index, row in log_df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # insert songplay record
        songplay_data = [index+1, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()
        print('songplay',row)
    
    conn.close()
         
except Exception as e:
    print(e)
    conn.close()