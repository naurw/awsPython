#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 13 00:29:14 2022

@author: William
"""

import pandas as pd
import boto3
from boto3 import client
import os 
from pandas_profiling import ProfileReport
import csv
import json

s3_client = boto3.client("s3", aws_access_key_id='insert_here', aws_secret_access_key='insert_here')
s3_resource = boto3.resource('s3', aws_access_key_id='insert_here', aws_secret_access_key='insert_here')

for file in s3_resource.Bucket('myBucket').objects.all(): 
    tempList = [] 
    tempList = file.key
    #print(f'"{tempList}"', end= ',\n')
    print('"{}"'.format(tempList), end=',\n')
    
csvList = ['output_list']

def loadData(i): 
    csv = []
    response = s3_client.get_object(Bucket='myBucket', Key=i)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        #temp = pd.read_csv(response.get("Body"), error_bad_lines = False, engine = 'python')
        temp = pd.read_csv(response.get('Body'), low_memory=False)
        csv.append(temp)
        print(temp)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    return csv 

obj = s3_resource_put.Object('myBucket, csvList[0])
obj_data =obj.get()['Body'].read().decode('utf-8').splitlines()
lines = csv.reader(obj_data)
headers = next(lines)
df = pd.DataFrame(lines, columns=headers)
df

s3bucket = s3_resource_put.Bucket('myBucket')
listOutput = []
for file in s3bucket.objects.all(): 
    print(file.key)
    listOutput.append(file.key)
    

def remove_a_key(d, remove_key):
    if isinstance(d, dict):
        for key in list(d.keys()):
            if key == remove_key:
                del d[key]
            else:
                remove_a_key(d[key], remove_key)
    
for i in listOutput:

    fileName = i 
    fileNamenoCSV = fileName.replace(".csv","")

    obj = s3_resource_put.Object('myBucket', fileName)
    obj_data =obj.get()['Body'].read().decode('utf-8').splitlines()
    lines = csv.reader(obj_data)
    headers = next(lines)
    df = pd.DataFrame(lines, columns=headers)

    outputHtml = ProfileReport(df, title="test", minimal= True)
    outputJson = outputHtml.to_json()

    dataJson = json.loads(outputJson)
    dataJsonTest = dataJson['variables']

    remove_a_key(dataJsonTest, 'value_counts')
    remove_a_key(dataJsonTest, 'value_counts_with_nan')
    remove_a_key(dataJsonTest, 'value_counts_without_nan')

    outputHtml.to_file("/Users/William/Desktop/dump/" + fileName + ".html")
    outputHtml.to_file("/Users/William/Desktop/dump/" + fileName + ".json")

    print('Done with ' + fileName)
    
# remove dataJson, outputHtml, outputJson from memory
del dataJson
del outputHtml
del outputJson
# use gc to remove dataJson, outputHtml, outputJson from memory
gc.collect()