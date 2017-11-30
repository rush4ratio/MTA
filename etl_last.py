#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 21:06:37 2017

@author: rush
"""

import pandas as pd
import numpy as np
import logging
import os 

from tqdm import tqdm as tqdm
from sqlalchemy import create_engine
from datetime import datetime as dt

data = pd.read_csv('MTA_q3.csv')
data['timestamp'] = pd.to_datetime(data['timestamp'])

data.index  = data['timestamp']


quarter_3_range = [str(item.date()) for item in pd.date_range(start='07-01-2013', 
                   end='09-30-2013')]

def process_day(data, day):
    df_now = data[day]
    df_next = data[str(pd.to_datetime(day).date() + pd.to_timedelta("1 day"))]
    
    for key in tqdm(df_now['a_key'].unique()):
    
        observations = df_now[df_now['a_key'] == key].copy()
        
        last_entry = observations[observations.timestamp == observations.timestamp.max()].entries.values[0]
        last_exit = observations[observations.timestamp == observations.timestamp.max()].exits.values[0]
        
        observations = df_next[df_next['a_key'] == key].copy()
        
        first_entry = observations[observations.timestamp == observations.timestamp.min()].entries.values[0]
        first_exit = observations[observations.timestamp == observations.timestamp.min()].exits.values[0]
        
        lst_of_records = []
        
        record = {}
        
      
        record['timestamp'] = pd.to_datetime('{} 00:00:00'.format(day))
        record['C_A'] = observations['C_A'][0]
        record['UNIT'] =  observations['UNIT'][0]
        record['SCP'] =  observations['SCP'][0]
        record['desc'] =  "custom last add"
        record['a_key'] = key
    
        record['val_entries'] = first_entry - last_entry
        record['val_exits'] = first_exit - last_exit 
        
        
        lst_of_records.append(record)
        
        
        obs = pd.DataFrame.from_records(lst_of_records)
        
        print(obs.iloc[0])

        mask = (obs['val_entries'] > 0) & (obs['val_exits'] > 0) & \
                (obs['val_entries'] < 5000) & (obs['val_exits'] < 5000)
        
        obs = obs[mask]
        
        obs = obs[['timestamp', 'C_A', 'UNIT', 'SCP', 'desc', 'a_key', 'val_entries','val_exits']]
        
        if not os.path.isfile('last_data/{}.csv'.format(day)):
            obs.to_csv('last_data/{}.csv'.format(day),index=False)
        else:
            obs.to_csv('last_data/{}.csv'.format(day),mode = 'a', index=False, header=None)
            
def process_tallies(lst_days):
    for day in lst_days:
        logging.info("Processing date : {}".format(day))
        process_day(data, day)
                              


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

                                
                                