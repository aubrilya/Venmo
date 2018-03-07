#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 13:36:29 2017

@author: aubreyo
Based on: https://gist.github.com/patrickvossler18/e1c99e4ff8eacceaeb72

This script is iteratively scrapes venmo data (20 transactions at t time), collects information in json format, converts 
it to a pandas dataframe for analysis, and saves to csv. 
"""

import requests
import json
from datetime import datetime
import csv
import time
import pickle as pkl
import pandas as pd
import bz2
import os

class scraping:

    
    def __init__(self):
        raw_input = time.time()
        self.last_touched = int(raw_input)
        self.output_file = csv.writer(open('venmo_output.csv','a'),delimiter='\t')

    def get_unix(self,timestamp):
        return int(datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').strftime('%s'))

    def pull_data(self):
        try:
            URL = "https://venmo.com/api/v5/public"
            return json.loads(requests.get(URL).text)
        except (requests.exceptions.ConnectionError,ValueError) as e:
            return None

    def transform_data(self,json_object):
        output_array = []
        try:
            json_array = json_object['data']
            for item in json_array:
                try:
                    output_array.append([item['story_id'],self.get_unix(item['updated_time']),item['actor']['name'],
                        item['actor']['picture'],item['actor']['id'],
                        item['transactions'][0]['target']['name'],
                        item['transactions'][0]['target']['picture'],
                        item['transactions'][0]['target']['id'],
                        item['message'],item['type']])
                except TypeError:
                    pass
        except (TypeError, KeyError) as e:
            pass
        return output_array

    def write_data(self,raw_array):
        for row in raw_array:
            if row[1] > self.last_touched:
                self.last_touched = row[1]
                self.output_file.writerow([str(s).encode("utf-8") for s in row])

if __name__ == '__main__':
    instance = scraping()
    data = pd.DataFrame()
    
    while True:
        for i in range (0,3):

            if len(data.index) <= 10000:
                
                start_time = int(time.time())
                json_object = instance.pull_data()
                raw_array = instance.transform_data(json_object)
                temp_data = pd.DataFrame(raw_array)
                temp_data.columns = ['transaction_id','time_stamp','actor_name','actor_profile','actor_id','target_name','target_profile','target_id','message','transaction_type']
                data = data.append(temp_data)
                instance.write_data(raw_array)
                end_time = int(time.time())
                if end_time - start_time > 10:
                    pass
                else:
                    time.sleep(10 - (end_time-start_time))
            
            #When dataframe is large enough, export and upload to bucket
            else:
                filename = datetime.now().strftime("%Y%m%d-%H%M%S")
                filename_csv = filename+ ".csv.bz2"
                filename_pkl = filename+ ".pkl.bz2"
                filename_bz2 = filename+ ".pkl.bz2"
                #with open(filename, 'wb') as f:
                 #   pkl.dump(data, f)
                pkl.dump( data,  bz2.open( filename_pkl,  'wb' )) 
                cmd_upload = 's3cmd put '+filename_bz2+' s3://'+'venmo-scrape'+'/'
                os.system(cmd_upload)
                data = pd.DataFrame()
                #time.sleep(10 - (end_time-start_time))
                cmd_delete = 'find . -name "*.bz2" -type f -delete'
                os.system(cmd_delete)

            
            
            

#EXPORT TO S3 Bucket s3.console.aws.amazon.com/s3/buckets/venmo-scrape/
            '''
    if len(data.index) == 200000:
        
        filename = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename_csv = filename+ ".csv.bz2"
        filename_pkl = filename+ ".pkl.bz2"
        filename_bz2 = filename+ ".pkl.bz2"
        #with open(filename, 'wb') as f:
         #   pkl.dump(data, f)
        pkl.dump( data,  bz2.open( filename_pkl,  'wb' )) 
        
        cmd = 's3cmd put '+filename_bz2+' s3://'+'venmo-scrape'+'/'

        os.system(cmd)
        
        data = []
        '''
#SAVE your work   
'''
pd.DataFrame(raw_array).to_csv("test2.csv", encoding='utf8')
with open('test.pkl', 'wb') as f:
    pkl.dump(raw_array, f)
    
#REOPEN your work
with open('20180118-1611520.pkl', 'rb') as f:
    test = pkl.load(f)
    
     temp_array = instance.transform_data(json_object)
        raw_array = raw_array.append(temp_array)
'''