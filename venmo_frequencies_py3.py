#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 13:56:42 2018

@author: aubreyo
"""
import pandas as pd
import numpy as np
#%% GROUP BY TIME OF DAY (MORNING, AFTERNOON, EVENING). 
# Note we will alter need to deal with time zones

#Convert unicode to three categories: morning, afternoon, evening
data2['time_stamp'] = pd.to_datetime(data2['time_stamp'],unit='s')

hours = data2.time_stamp.dt.hour.values
times = np.array(['Morning', 'Afternoon', 'Evening'])
data2=data2.assign(timeOfDay=times[np.array([12, 17]).searchsorted(hours)])

(data.loc[data[3] == "Food",:]
                    .groupby("1")["student_id"]

           .count()).to_frame()

timeOfDay = data2.groupby([ "timeOfDay"] ).count()

#%% Frequenceis of words in messages column


#%%
#FREQUENCIES OF TWITTER USERNAMES - TEST IF DUPLICATES AND REINDEX TO GET NEW NODE ID
from collections import Counter
counter = Counter(data2['message'])
counts = counter.keys()
counts = counter.values()
temp_count = pd.DataFrame.from_dict(counter, orient='index').reset_index()
temp_count = temp_count.rename(columns={'index':'message', 0:'count'})

count = temp_count

word_count['word_count'] = count['message'].apply(lambda x: Counter(x.lower().split()))

 word_count = count['message'].str.get_dummies(sep=' ').T.dot(count['count'])