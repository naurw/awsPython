#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 17:28:35 2022

@author: William
"""

import pandas as pd 
import json 
import numpy as np 
import boto3 
from boto3 import client 
from pandas_profiling import ProfileReport
import os 

s3_client = boto3.client("s3", aws_access_key_id='AWS_KEY', aws_secret_access_key='AWS_SECRET')
myBucket = 'testBin'

s3 = boto3.resource('s3')
s3bucket = s3.Bucket('testBin')
for file in s3bucket.objects.all(): 
    print(file.key)
    
def loadData(csvFile):
    responses = [] 
    response = s3_resource.get_object(Bucket=myBucket, Key= csvFile)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        temp = pd.read_csv(response.get("Body"), na_values=' ', low_memory= False, error_bad_lines= False, engine= 'python')
        temp = temp.drop(temp.columns[0], axis=1)
        print('Succesfully loaded dataframe from S3')
        responses.append(temp)
        
        profile = ProfileReport(temp, title = 'Pandas Profiling Report', minimal = True)
        jsonOutput = profile.to_json() 
        
        path = os.getcwd() 
        newpath = path + '/filename.json'
        
        with open(jsonOutput, 'w') as f: 
            f.write(jsonOutput)
            f.close()
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}") 
    return responses