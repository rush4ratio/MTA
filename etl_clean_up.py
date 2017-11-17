#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 13:53:06 2017

@author: rush
"""

import pandas as pd
import numpy as np
import logging
import os 

from tqdm import tqdm as tqdm
from sqlalchemy import create_engine
from datetime import datetime as dt

logging.basicConfig(format='%(asctime)s %(message)s', 
                    handlers=[logging.FileHandler("etl_mta.log"),
                              logging.StreamHandler()], filelevel=logging.DEBUG)

def convert_to_datetime(row):
    return pd.to_datetime(row['date']+" "+row['time'], format="%m/%d/%Y %H:%M:%S")

def return_key(x):
    return x[0] + x[1] + x[2]

def process_df(df):
    df.index = df.apply(convert_to_datetime, axis=1)
    
    df['a_key'] = df[['C_A','UNIT','SCP']].apply(return_key, axis = 1)
    
    if not os.path.isfile('MTA_q3.csv'):
        df.to_csv('MTA_q3.csv',index_label='timestamp')
    else:
        df.to_csv('MTA_q3.csv',mode = 'a', index_label='timestamp', header=None)
    

chunk_size = 20000
j = 0


quarter_3_range = [str(item.date()) for item in pd.date_range(start='07-01-2013', 
                   end='09-30-2013')]

# Takes 588 seconds: completed  2855479 (2860000) rows
start = dt.now()
for df in pd.read_csv('MTA_Pivotdown.txt', chunksize=chunk_size, iterator=True, encoding='utf-8'):
    process_df(df)

    j+=1
    print('{} seconds: completed {} rows'.format((dt.now() - start).seconds, j*chunk_size))


data = pd.read_csv('MTA_q3.csv')

data['timestamp'] = pd.to_datetime(data['timestamp'])

data.index  = data['timestamp']

data.drop('timestamp', axis =1, inplace=True)

def process_tallies(lst_days):
    for day in lst_days:
        logging.info("Processing date : {}".format(day))
        process_day(data, day)


def process_day(data, day):
    df = data[day]

    for key in tqdm(df['a_key'].unique()):
    
        observations = df[df['a_key'] == key].copy()
        
        observations['val_entries'] = observations.entries - observations.entries.shift(1)
    
        observations['val_exits'] = observations.exits - observations.exits.shift(1)
    
        mask = (observations['val_entries'] > 0) & (observations['val_exits'] > 0) & \
                (observations['val_entries'] < 4000) & (observations['val_exits'] < 4000)
        
        observations = observations[mask]
        observations.drop(['entries','exits','date','time'], inplace = True, axis = 1)
    
        if not os.path.isfile('clean_data/{}.csv'.format(day)):
            observations.to_csv('clean_data/{}.csv'.format(day),index_label='timestamp')
        else:
            observations.to_csv('clean_data/{}.csv'.format(day),mode = 'a', index_label='timestamp', header=None)


def chunks(L, n):
    """ Yield successive n-sized chunks from L.
    """
    for i in range(0, len(L), n):
        yield L[i:i+n]

generate_dates = chunks(quarter_3_range,23)

import multiprocessing as mp

processes = [mp.Process(target=process_tallies, args=(x,)) for x in generate_dates]


# Run processes
for p in processes:
    p.start()

# Exit the completed processes
for p in processes:
    p.join()


# process_tallies(['2013-09-28','2013-09-29','2013-09-30'])?









